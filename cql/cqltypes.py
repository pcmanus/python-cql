# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from functools import partial

apache_cassandra_type_prefix = 'org.apache.cassandra.db.marshal.'

def trim_if_startswith(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s

def unix_time_from_uuid1(u):
    return (u.get_time() - 0x01B21DD213814000) / 10000000.0

_casstypes = {}
_cqltypes = {}

class register_casstype(type):
    def __new__(metacls, name, bases, dct):
        cls = type.__new__(metacls, name, bases, dct)
        if not name.startswith('_'):
            _casstypes[name] = cls
            _cqltypes[cls.typename] = cls
        return cls

def lookup_casstype(casstype):
    args = ()
    casstype = trim_if_startswith(casstype, apache_cassandra_type_prefix)
    if '(' in casstype:
        # do we need to support arbitrary nesting? if so, this is where
        # we need to tokenize and parse
        assert casstype.endswith(')'), casstype
        casstype, args = casstype[:-1].split('(', 1)
        args = [lookup_casstype(s.strip()) for s in args.split(',')]
    try:
        typeclass = _casstypes[casstype]
    except KeyError:
        typeclass = mkUnrecognizedType(casstype)
    if args:
        typeclass = partial(typeclass, *args)
    return typeclass

def lookup_cqltype(cqltype):
    args = ()
    if '<' in cqltype:
        # do we need to support arbitrary nesting? if so, this is where
        # we need to tokenize and parse
        assert cqltype.endswith('>'), cqltype
        cqltype, args = cqltype[:-1].split('<', 1)
        args = [lookup_cqltype(s.strip()) for s in args.split(',')]
    typeclass = _cqltypes[cqltype]
    if args:
        typeclass = partial(typeclass, *args)
    return typeclass

class _CassandraType(object):
    __metaclass__ = register_casstype
    subtypes = ()

    def __init__(self, val):
        self.val = val

    @classmethod
    def cql_parameterized_type(cls):
        return cls.typename

    @classmethod
    def cass_parameterized_type(cls):
        return cls.__name__

    def __str__(self):
        return '<%s( %r )>' % (self.cql_parameterized_type(), self.val)
    __repr__ = __str__

class _UnrecognizedType(_CassandraType):
    def __init__(self, *args):
        self.subtypes = args[:-1]
        _CassandraType.__init__(self, args[-1])

    @classmethod
    def cass_parameterized_type(cls):
        return cls.typename

    def __str__(self):
        if self.subtypes:
            subt = '<%s>' % ', '.join(s.__name__ for s in self.subtypes)
        else:
            subt = ''
        return '<%s%s( %r )>' % (self.__class__.__name__, subt, self.val)
    __repr__ = __str__

def mkUnrecognizedType(casstypename):
    # note that this still gets the register_casstype metaclass, so future
    # lookups of this class name should get the same constructed class
    return type(casstypename,
                (_UnrecognizedType,),
                {'typename': "'%s'" % casstypename})

class BytesType(_CassandraType):
    typename = 'blob'

class DecimalType(_CassandraType):
    typename = 'decimal'

class UUIDType(_CassandraType):
    typename = 'uuid'

class BooleanType(_CassandraType):
    typename = 'boolean'

class AsciiType(_CassandraType):
    typename = 'ascii'

class FloatType(_CassandraType):
    typename = 'float'

class DoubleType(_CassandraType):
    typename = 'double'

class LongType(_CassandraType):
    typename = 'bigint'

class Int32Type(_CassandraType):
    typename = 'int'

class IntegerType(_CassandraType):
    typename = 'varint'

class CounterColumnType(_CassandraType):
    typename = 'counter'

class DateType(_CassandraType):
    typename = 'timestamp'

    def my_timestamp(self):
        return self.val

class TimeUUIDType(DateType):
    typename = 'timeuuid'

    def my_timestamp(self):
        return unix_time_from_uuid1(self.val)

class UTF8Type(_CassandraType):
    typename = 'text'

# name alias
_cqltypes['varchar'] = _cqltypes['text']

class _ParameterizedType(_CassandraType):
    num_subtypes = 0
    subtypes = ()

    def __init__(self, val):
        raise ValueError("%s type with no parameters can't be instantiated" % (self.typename,))

    @classmethod
    def cql_parameterized_type_with(cls, subtypes):
        return '%s<%s>' % (cls.typename, ', '.join(styp.cql_parameterized_type() for styp in subtypes))

    @classmethod
    def apply_parameters(cls, *subtypes):
        if len(subtypes) != cls.num_subtypes:
            raise ValueError("%s types require %d subtypes (%d given)"
                             % (cls.typename, cls.num_subtypes, len(subtypes)))
        newname = cls.cql_parameterized_type_with(subtypes)
        return type(newname, (cls,), {'subtypes': subtypes, '__init__': cls.parameterized_init})

    @classmethod
    def cql_parameterized_type(cls):
        if not cls.subtypes:
            return cls.typename
        return cls.cql_parameterized_type_with(cls.subtypes)

    @classmethod
    def cass_parameterized_type(cls):
        if not cls.subtypes:
            return _CassandraType.cass_parameterized_type(cls)
        return '%s(%s)' % (cls.__name__, ', '.join(styp.cass_parameterized_type() for styp in subtypes))

    # staticmethod cause it will become a real method for generated subclasses
    @staticmethod
    def parameterized_init(self, val):
        _CassandraType.__init__(self, val)

class _SimpleCollectionType(_ParameterizedType):
    def parameterized_init(self, val):
        _CassandraType.__init__(self, map(self.subtypes[0], val))

class ListType(_SimpleCollectionType):
    typename = 'list'

class SetType(_SimpleCollectionType):
    typename = 'set'

class MapType(_ParameterizedType):
    typename = 'map'

    def parameterized_init(self, val):
        subkeytype, subvaltype = self.subtypes
        myval = dict([(subkeytype(k), subvaltype(v)) for (k, v) in val.items()])
        _CassandraType.__init__(self, myval)

def is_counter_type(t):
    if isinstance(t, basestring):
        t = lookup_casstype(t)
    return t.typename == 'counter'

cql_type_to_apache_class = dict([(c, t.__class__.__name__) for (c, t) in _cqltypes.items()])
apache_class_to_cql_type = dict([(n, t.typename) for (n, t) in _casstypes.items()])

cql_types = sorted(_cqltypes.keys())

def cql_typename(casstypename):
    return lookup_casstype(casstypename).cql_parameterized_type()

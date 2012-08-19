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

from cql.apivalues import Binary, UUID
from cql.marshal import (int8_pack, int8_unpack, uint16_pack, uint16_unpack,
                         int32_pack, int32_unpack, int64_pack, int64_unpack,
                         float_pack, float_unpack, double_pack, double_unpack,
                         varint_pack, varint_unpack)
from decimal import Decimal
import time
import calendar

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

apache_cassandra_type_prefix = 'org.apache.cassandra.db.marshal.'

def trim_if_startswith(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s

def unix_time_from_uuid1(u):
    return (u.get_time() - 0x01B21DD213814000) / 10000000.0

_casstypes = {}
_cqltypes = {}

class CassandraTypeType(type):
    def __new__(metacls, name, bases, dct):
        dct.setdefault('cassname', name)
        cls = type.__new__(metacls, name, bases, dct)
        if not name.startswith('_'):
            _casstypes[name] = cls
            _cqltypes[cls.typename] = cls
        return cls

def lookup_casstype(casstype):
    args = ()
    shortname = trim_if_startswith(casstype, apache_cassandra_type_prefix)
    if '(' in shortname:
        # do we need to support arbitrary nesting? if so, this is where
        # we need to tokenize and parse
        assert shortname.endswith(')'), shortname
        shortname, args = shortname[:-1].split('(', 1)
        args = [lookup_casstype(s.strip()) for s in args.split(',')]
    try:
        typeclass = _casstypes[shortname]
    except KeyError:
        typeclass = mkUnrecognizedType(casstype)
    if args:
        typeclass = typeclass.apply_parameters(*args)
    return typeclass

def lookup_cqltype(cqltype):
    args = ()
    if cqltype.startswith("'") and cqltype.endswith("'"):
        return lookup_casstype(cqltype[1:-1].replace("''", "'"))
    if '<' in cqltype:
        # do we need to support arbitrary nesting? if so, this is where
        # we need to tokenize and parse
        assert cqltype.endswith('>'), cqltype
        cqltype, args = cqltype[:-1].split('<', 1)
        args = [lookup_cqltype(s.strip()) for s in args.split(',')]
    typeclass = _cqltypes[cqltype]
    if args:
        typeclass = typeclass.apply_parameters(*args)
    return typeclass

class _CassandraType(object):
    __metaclass__ = CassandraTypeType
    subtypes = ()
    num_subtypes = 0
    empty_binary_ok = False

    def __init__(self, val):
        self.val = self.validate(val)

    def __str__(self):
        return '<%s( %r )>' % (self.cql_parameterized_type(), self.val)
    __repr__ = __str__

    @staticmethod
    def validate(val):
        return val

    @classmethod
    def from_binary(cls, byts):
        if byts is None:
            return None
        if byts == '' and not cls.empty_binary_ok:
            return None
        return cls.deserialize(byts)

    @classmethod
    def to_binary(cls, val):
        if val is None:
            return ''
        return cls.serialize(val)

    @staticmethod
    def deserialize(byts):
        return byts

    @staticmethod
    def serialize(val):
        return val

    @classmethod
    def cass_parameterized_type_with(cls, subtypes, full=False):
        cname = cls.cassname
        if full and '.' not in cname:
            cname = apache_cassandra_type_prefix + cname
        if not subtypes:
            return cname
        sublist = ', '.join(styp.cass_parameterized_type(full=full) for styp in subtypes)
        return '%s(%s)' % (cname, sublist)

    @classmethod
    def apply_parameters(cls, *subtypes):
        if cls.num_subtypes != 'UNKNOWN' and len(subtypes) != cls.num_subtypes:
            raise ValueError("%s types require %d subtypes (%d given)"
                             % (cls.typename, cls.num_subtypes, len(subtypes)))
        newname = cls.cass_parameterized_type_with(subtypes)
        return type(newname, (cls,), {'subtypes': subtypes, 'cassname': cls.cassname})

    @classmethod
    def cql_parameterized_type(cls):
        if not cls.subtypes:
            return cls.typename
        return '%s<%s>' % (cls.typename, ', '.join(styp.cql_parameterized_type() for styp in cls.subtypes))

    @classmethod
    def cass_parameterized_type(cls, full=False):
        return cls.cass_parameterized_type_with(cls.subtypes, full=full)

class _UnrecognizedType(_CassandraType):
    num_subtypes = 'UNKNOWN'

def mkUnrecognizedType(casstypename):
    return CassandraTypeType(casstypename,
                             (_UnrecognizedType,),
                             {'typename': "'%s'" % casstypename})

class BytesType(_CassandraType):
    typename = 'blob'
    empty_binary_ok = True

    @staticmethod
    def validate(val):
        return Binary(val)

    @staticmethod
    def serialize(val):
        return str(val)

class DecimalType(_CassandraType):
    typename = 'decimal'

    @staticmethod
    def validate(val):
        return Decimal(val)

    @staticmethod
    def deserialize(byts):
        scale = int32_unpack(byts[:4])
        unscaled = varint_unpack(byts[4:])
        return Decimal('%de%d' % (unscaled, -scale))

    @staticmethod
    def serialize(dec):
        sign, digits, exponent = dec.as_tuple()
        unscaled = int(''.join([str(digit) for digit in digits]))
        if sign:
            unscaled *= -1
        scale = int32_pack(-exponent)
        unscaled = varint_pack(unscaled)
        return scale + unscaled

class UUIDType(_CassandraType):
    typename = 'uuid'

    @staticmethod
    def deserialize(byts):
        return UUID(bytes=byts)

    @staticmethod
    def serialize(uuid):
        return uuid.bytes

class BooleanType(_CassandraType):
    typename = 'boolean'

    @staticmethod
    def validate(val):
        return bool(val)

    @staticmethod
    def deserialize(byts):
        return bool(int8_unpack(byts))

    @staticmethod
    def serialize(truth):
        return int8_pack(bool(truth))

class AsciiType(_CassandraType):
    typename = 'ascii'
    empty_binary_ok = True

class FloatType(_CassandraType):
    typename = 'float'

    deserialize = staticmethod(float_unpack)
    serialize = staticmethod(float_pack)

class DoubleType(_CassandraType):
    typename = 'double'

    deserialize = staticmethod(double_unpack)
    serialize = staticmethod(double_pack)

class LongType(_CassandraType):
    typename = 'bigint'

    deserialize = staticmethod(int64_unpack)
    serialize = staticmethod(int64_pack)

class Int32Type(_CassandraType):
    typename = 'int'

    deserialize = staticmethod(int32_unpack)
    serialize = staticmethod(int32_pack)

class IntegerType(_CassandraType):
    typename = 'varint'

    deserialize = staticmethod(varint_unpack)
    serialize = staticmethod(varint_pack)

class CounterColumnType(_CassandraType):
    typename = 'counter'

    deserialize = staticmethod(int64_unpack)
    serialize = staticmethod(int64_pack)

cql_time_formats = (
    '%Y-%m-%d %H:%M',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%d'
)

class DateType(_CassandraType):
    typename = 'timestamp'

    @classmethod
    def validate(cls, date):
        if isinstance(date, basestring):
            date = cls.interpret_datestring(date)
        return date

    @staticmethod
    def interpret_datestring(date):
        if date[-5] in ('+', '-'):
            offset = (int(date[-4:-2]) * 3600 + int(date[-2:]) * 60) * int(date[-5] + '1')
            date = date[:-5]
        else:
            offset = -time.timezone
        for tformat in cql_time_formats:
            try:
                tval = time.strptime(date, tformat)
            except ValueError:
                continue
            return calendar.timegm(tval) + offset
        else:
            raise ValueError("can't interpret %r as a date" % (date,))

    def my_timestamp(self):
        return self.val

    @staticmethod
    def deserialize(byts):
        return int64_unpack(byts) / 1000.0

    @staticmethod
    def serialize(timestamp):
        return int64_pack(timestamp * 1000)

class TimeUUIDType(DateType):
    typename = 'timeuuid'

    def my_timestamp(self):
        return unix_time_from_uuid1(self.val)

    @staticmethod
    def deserialize(byts):
        return UUID(bytes=byts)

    @staticmethod
    def serialize(timeuuid):
        return timeuuid.bytes

class UTF8Type(_CassandraType):
    typename = 'text'
    empty_binary_ok = True

    @staticmethod
    def deserialize(byts):
        return byts.decode('utf8')

    @staticmethod
    def serialize(ustr):
        return ustr.encode('utf8')

# name alias
_cqltypes['varchar'] = _cqltypes['text']

class _ParameterizedType(_CassandraType):
    def __init__(self, val):
        if not self.subtypes:
            raise ValueError("%s type with no parameters can't be instantiated" % (self.typename,))
        _CassandraType.__init__(self, val)

    @classmethod
    def deserialize(cls, byts):
        if not cls.subtypes:
            raise NotImplementedError("can't deserialize unparameterized %s"
                                      % self.typename)
        return cls.deserialize_safe(byts)

    @classmethod
    def serialize(cls, val):
        if not cls.subtypes:
            raise NotImplementedError("can't serialize unparameterized %s"
                                      % self.typename)
        return cls.serialize_safe(val)

class _SimpleParameterizedType(_ParameterizedType):
    @classmethod
    def validate(cls, val):
        subtype, = cls.subtypes
        return cls.adapter([subtype.validate(subval) for subval in val])

    @classmethod
    def deserialize_safe(cls, byts):
        subtype, = cls.subtypes
        numelements = uint16_unpack(byts[:2])
        p = 2
        result = []
        for n in xrange(numelements):
            itemlen = uint16_unpack(byts[p:p+2])
            p += 2
            item = byts[p:p+itemlen]
            p += itemlen
            result.append(subtype.from_binary(item))
        return cls.adapter(result)

    @classmethod
    def serialize_safe(cls, items):
        subtype, = cls.subtypes
        buf = StringIO()
        buf.write(uint16_pack(len(items)))
        for item in items:
            itembytes = subtype.to_binary(item)
            buf.write(uint16_pack(len(itembytes)))
            buf.write(itembytes)
        return buf.getvalue()

class ListType(_SimpleParameterizedType):
    typename = 'list'
    num_subtypes = 1
    adapter = tuple

class SetType(_SimpleParameterizedType):
    typename = 'set'
    num_subtypes = 1
    adapter = set

class MapType(_ParameterizedType):
    typename = 'map'
    num_subtypes = 2

    @classmethod
    def validate(cls, val):
        subkeytype, subvaltype = cls.subtypes
        return dict((subkeytype.validate(k), subvaltype.validate(v)) for (k, v) in val.iteritems())

    @classmethod
    def deserialize_safe(cls, byts):
        subkeytype, subvaltype = cls.subtypes
        numelements = uint16_unpack(byts[:2])
        p = 2
        themap = {}
        for n in xrange(numelements):
            key_len = uint16_unpack(byts[p:p+2])
            p += 2
            keybytes = byts[p:p+key_len]
            p += key_len
            val_len = uint16_unpack(byts[p:p+2])
            p += 2
            valbytes = byts[p:p+val_len]
            p += val_len
            key = subkeytype.from_binary(keybytes)
            val = subvaltype.from_binary(valbytes)
            themap[key] = val
        return themap

    @classmethod
    def serialize_safe(cls, themap):
        subkeytype, subvaltype = cls.subtypes
        buf = StringIO()
        buf.write(uint16_pack(len(themap)))
        for key, val in themap.iteritems():
            keybytes = subkeytype.to_binary(key)
            valbytes = subvaltype.to_binary(val)
            buf.write(uint16_pack(len(keybytes)))
            buf.write(keybytes)
            buf.write(uint16_pack(len(valbytes)))
            buf.write(valbytes)
        return buf.getvalue()

def is_counter_type(t):
    if isinstance(t, basestring):
        t = lookup_casstype(t)
    return t.typename == 'counter'

cql_type_to_apache_class = dict([(c, t.cassname) for (c, t) in _cqltypes.items()])
apache_class_to_cql_type = dict([(n, t.typename) for (n, t) in _casstypes.items()])

cql_types = sorted(_cqltypes.keys())

def cql_typename(casstypename):
    return lookup_casstype(casstypename).cql_parameterized_type()

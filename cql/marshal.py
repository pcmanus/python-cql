
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

import re
import struct
from decimal import Decimal

import cql

__all__ = ['prepare', 'cql_quote', 'unmarshal_noop', 'unmarshallers']

if hasattr(struct, 'Struct'): # new in Python 2.5
   _have_struct = True
   _long_packer = struct.Struct('>q')
   _int32_packer = struct.Struct('>i')
   _float_packer = struct.Struct('>f')
   _double_packer = struct.Struct('>d')
else:
    _have_struct = False

try:
    from uuid import UUID  # new in Python 2.5
except ImportError:
    class UUID:
        def __init__(self, bytes):
            self.bytes = bytes

_param_re = re.compile(r"""
    (                           # stuff that is not substitution markers
      (?:  ' [^']* '            # string literal; ignore colons in here
        |  [^']                 # eat anything else
      )*?
    )
    (?<! [a-zA-Z0-9_'] )        # no colons immediately preceded by an ident or str literal
    :
    ( [a-zA-Z_][a-zA-Z0-9_]* )  # the param name
""", re.S | re.X)

_comment_re = re.compile(r"""
       // .*? $
    |  -- .*? $
    |  /\* .*? \*/
    | ' [^']* '
""", re.S | re.M | re.X)

BYTES_TYPE = "org.apache.cassandra.db.marshal.BytesType"
ASCII_TYPE = "org.apache.cassandra.db.marshal.AsciiType"
BOOLEAN_TYPE = "org.apache.cassandra.db.marshal.BooleanType"
DATE_TYPE = "org.apache.cassandra.db.marshal.DateType"
DECIMAL_TYPE = "org.apache.cassandra.db.marshal.DecimalType"
UTF8_TYPE = "org.apache.cassandra.db.marshal.UTF8Type"
INT32_TYPE = "org.apache.cassandra.db.marshal.Int32Type"
INTEGER_TYPE = "org.apache.cassandra.db.marshal.IntegerType"
LONG_TYPE = "org.apache.cassandra.db.marshal.LongType"
FLOAT_TYPE = "org.apache.cassandra.db.marshal.FloatType"
DOUBLE_TYPE = "org.apache.cassandra.db.marshal.DoubleType"
UUID_TYPE = "org.apache.cassandra.db.marshal.UUIDType"
LEXICAL_UUID_TYPE = "org.apache.cassandra.db.marshal.LexicalType"
TIME_UUID_TYPE = "org.apache.cassandra.db.marshal.TimeUUIDType"
COUNTER_COLUMN_TYPE = "org.apache.cassandra.db.marshal.CounterColumnType"

def blank_comments(query):
    def teh_blanker(match):
        m = match.group(0)
        if m.startswith("'"):
            return m
        return ' ' * len(m)
    return _comment_re.sub(teh_blanker, query)

def prepare(query, params):
    """
    For every match of the form ":param_name", call cql_quote
    on kwargs['param_name'] and replace that section of the query
    with the result
    """

    # kill comments first, so that we don't have to try to parse around them.
    # but keep the character count the same, so that location-tagged error
    # messages still work
    query = blank_comments(query)
    return _param_re.sub(lambda m: m.group(1) + cql_quote(params[m.group(2)]), query)

def cql_quote(term):
    if isinstance(term, unicode):
        return "'%s'" % __escape_quotes(term.encode('utf8'))
    elif isinstance(term, str):
        return "'%s'" % __escape_quotes(term)
    else:
        return str(term)

def unmarshal_noop(bytestr):
    return bytestr

def unmarshal_bool(bytestr):
    if not bytestr:
        return None
    return bool(ord(bytestr[0]))

def unmarshal_utf8(bytestr):
    return bytestr.decode("utf8")

if _have_struct:
    def unmarshal_int32(bytestr):
        if not bytestr:
            return None
        return _int32_packer.unpack(bytestr)[0]
else:
    def unmarshal_int32(bytestr):
        if not bytestr:
            return None
        return struct.unpack(">i", bytestr)[0]

def unmarshal_int(bytestr):
    if not bytestr:
        return None
    return decode_bigint(bytestr)

if _have_struct:
    def unmarshal_long(bytestr):
        if not bytestr:
            return None
        return _long_packer.unpack(bytestr)[0]
else:
    def unmarshal_long(bytestr):
        if not bytestr:
            return None
        return struct.unpack(">q", bytestr)[0]

if _have_struct:
    def unmarshal_float(bytestr):
        if not bytestr:
            return None
        return _float_packer.unpack(bytestr)[0]
else:
    def unmarshal_float(bytestr):
        if not bytestr:
            return None
        return struct.unpack(">f", bytestr)[0]

if _have_struct:
    def unmarshal_double(bytestr):
        if not bytestr:
            return None
        return _double_packer.unpack(bytestr)[0]
else:
    def unmarshal_double(bytestr):
        if not bytestr:
            return None
        return struct.unpack(">d", bytestr)[0]

def unmarshal_date(bytestr):
    if not bytestr:
        return None
    return unmarshal_long(bytestr) / 1000.0

def unmarshal_decimal(bytestr):
    if not bytestr:
        return None
    scale = unmarshal_int32(bytestr[:4])
    unscaled = decode_bigint(bytestr[4:])
    return Decimal('%de%d' % (unscaled, -scale))

def unmarshal_uuid(bytestr):
    if not bytestr:
        return None
    return UUID(bytes=bytestr)

unmarshallers = {BYTES_TYPE:          unmarshal_noop,
                 ASCII_TYPE:          unmarshal_noop,
                 BOOLEAN_TYPE:        unmarshal_bool,
                 DATE_TYPE:           unmarshal_date,
                 DECIMAL_TYPE:        unmarshal_decimal,
                 UTF8_TYPE:           unmarshal_utf8,
                 INT32_TYPE:          unmarshal_int32,
                 INTEGER_TYPE:        unmarshal_int,
                 LONG_TYPE:           unmarshal_long,
                 FLOAT_TYPE:          unmarshal_float,
                 DOUBLE_TYPE:         unmarshal_double,
                 UUID_TYPE:           unmarshal_uuid,
                 LEXICAL_UUID_TYPE:   unmarshal_uuid,
                 TIME_UUID_TYPE:      unmarshal_uuid,
                 COUNTER_COLUMN_TYPE: unmarshal_long}
for name, typ in unmarshallers.items():
    short_name = name.split('.')[-1]
    unmarshallers[short_name] = typ

def decode_bigint(term):
    val = int(term.encode('hex'), 16)
    if (ord(term[0]) & 128) != 0:
        val = val - (1 << (len(term) * 8))
    return val

def __escape_quotes(term):
    assert isinstance(term, basestring)
    return term.replace("'", "''")

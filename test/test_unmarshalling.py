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

import unittest
from decimal import Decimal
from uuid import UUID
import cql
from cql.marshal import unmarshallers, unmarshal_noop

demarshal_me = (
    ('lorem ipsum dolor sit amet', 'AsciiType', 'lorem ipsum dolor sit amet'),
    ('', 'AsciiType', ''),
    ('\x01', 'BooleanType', True),
    ('\x00', 'BooleanType', False),
    ('', 'BooleanType', None),
    ('\xff\xfe\xfd\xfc\xfb', 'BytesType', '\xff\xfe\xfd\xfc\xfb'),
    ('', 'BytesType', ''),
    ('\x7f\xff\xff\xff\xff\xff\xff\xff', 'CounterColumnType',  9223372036854775807),
    ('\x80\x00\x00\x00\x00\x00\x00\x00', 'CounterColumnType', -9223372036854775808),
    ('', 'CounterColumnType', None),
    ('\x00\x00\x013\x7fb\xeey', 'DateType', 1320692149.881),
    ('', 'DateType', None),
    ('\x00\x00\x00\r\nJ\x04"^\x91\x04\x8a\xb1\x18\xfe', 'DecimalType', Decimal('1243878957943.1234124191998')),
    ('\x00\x00\x00\x06\xe5\xde]\x98Y', 'DecimalType', Decimal('-112233.441191')),
    ('\x00\x00\x00\x14\x00\xfa\xce', 'DecimalType', Decimal('0.00000000000000064206')),
    ('\x00\x00\x00\x14\xff\x052', 'DecimalType', Decimal('-0.00000000000000064206')),
    ('\xff\xff\xff\x9c\x00\xfa\xce', 'DecimalType', Decimal('64206e100')),
    ('', 'DecimalType', None),
    ('@\xd2\xfa\x08\x00\x00\x00\x00', 'DoubleType', 19432.125),
    ('\xc0\xd2\xfa\x08\x00\x00\x00\x00', 'DoubleType', -19432.125),
    ('\x7f\xef\x00\x00\x00\x00\x00\x00', 'DoubleType', 1.7415152243978685e+308),
    ('', 'DoubleType', None),
    ('F\x97\xd0@', 'FloatType', 19432.125),
    ('\xc6\x97\xd0@', 'FloatType', -19432.125),
    ('\xc6\x97\xd0@', 'FloatType', -19432.125),
    ('\x7f\x7f\x00\x00', 'FloatType', 338953138925153547590470800371487866880.0),
    ('', 'FloatType', None),
    ('\x7f\x50\x00\x00', 'Int32Type', 2135949312),
    ('\xff\xfd\xcb\x91', 'Int32Type', -144495),
    ('', 'Int32Type', None),
    ('f\x1e\xfd\xf2\xe3\xb1\x9f|\x04_\x15', 'IntegerType', 123456789123456789123456789),
    ('', 'IntegerType', None),
    ('\x7f\xff\xff\xff\xff\xff\xff\xff', 'LongType',  9223372036854775807),
    ('\x80\x00\x00\x00\x00\x00\x00\x00', 'LongType', -9223372036854775808),
    ('', 'LongType', None),
    ('\xe3\x81\xbe\xe3\x81\x97\xe3\x81\xa6', 'UTF8Type', u'\u307e\u3057\u3066'),
    ('\xe3\x81\xbe\xe3\x81\x97\xe3\x81\xa6' * 1000, 'UTF8Type', u'\u307e\u3057\u3066' * 1000),
    ('', 'UTF8Type', u''),
    ('\xff' * 16, 'UUIDType', UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')),
    ('I\x15~\xfc\xef<\x9d\xe3\x16\x98\xaf\x80\x1f\xb4\x0b*', 'UUIDType', UUID('49157efc-ef3c-9de3-1698-af801fb40b2a')),
    ('', 'UUIDType', None),
)

class TestUnmarshal(unittest.TestCase):
    def test_unmarshalling(self):
        for serializedval, valtype, marshaledval in demarshal_me:
            unmarshaller = unmarshallers.get(valtype, unmarshal_noop)
            whatwegot = unmarshaller(serializedval)
            self.assertEqual(whatwegot, marshaledval,
                             msg='Unmarshaller for %s (%s) failed: unmarshal(%r) got %r instead of %r'
                                 % (valtype, unmarshaller, serializedval, whatwegot, marshaledval))
            self.assertEqual(type(whatwegot), type(marshaledval),
                             msg='Unmarshaller for %s (%s) gave wrong type (%s instead of %s)'
                                 % (valtype, unmarshaller, type(whatwegot), type(marshaledval)))

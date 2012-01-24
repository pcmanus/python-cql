
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
import cql
from cql.marshal import prepare

# TESTS[i] ARGUMENTS[i] -> STANDARDS[i]
TESTS = (
"""
SELECT :a,:b,:c,:d FROM ColumnFamily WHERE KEY = :e AND 'col' = :f;
""",
"""
USE Keyspace;
""",
"""
SELECT :a..:b FROM ColumnFamily;
""",
"""
CREATE KEYSPACE foo WITH strategy_class='SimpleStrategy' AND strategy_options:replication_factor = :_a_;
""",
"""
CREATE COLUMNFAMILY blah WITH somearg:another=:opt AND foo='bar':baz AND option=:value:suffix;
""",
"""
SELECT :lo..:hi FROM ColumnFamily WHERE KEY=':dontsubstthis' AND col > /* ignore :this */ :colval23;
""",
"""
USE :some_ks;
""",
"""
INSERT INTO cf (key, col1, col2) VALUES ('http://one.two.three/four?five /* :name */', :name);
""",
)

ARGUMENTS = (
    {'a': 1, 'b': 3, 'c': long(1000), 'd': long(3000), 'e': "key", 'f': unicode("val")},
    {},
    {'a': "a'b", 'b': "c'd'e"},
    {'_a_': 12},
    {'opt': "abc'", 'unused': 'thatsok', 'value': '\n'},
    {'lo': ' ', 'hi': ':hi', 'colval23': 0.2},
    {'some_ks': 'abc'},
    {'name': "// a literal 'comment'"},
)

STANDARDS = (
"""
SELECT 1,3,1000,3000 FROM ColumnFamily WHERE KEY = 'key' AND 'col' = 'val';
""",
"""
USE Keyspace;
""",
"""
SELECT 'a''b'..'c''d''e' FROM ColumnFamily;
""",
"""
CREATE KEYSPACE foo WITH strategy_class='SimpleStrategy' AND strategy_options:replication_factor = 12;
""",
"""
CREATE COLUMNFAMILY blah WITH somearg:another='abc''' AND foo='bar':baz AND option='
':suffix;
""",
"""
SELECT ' '..':hi' FROM ColumnFamily WHERE KEY=':dontsubstthis' AND col >                    0.2;
""",
"""
USE 'abc';
""",
"""
INSERT INTO cf (key, col1, col2) VALUES ('http://one.two.three/four?five /* :name */', '// a literal ''comment''');
""",
)

class TestPrepare(unittest.TestCase):
    def test_prepares(self):
        "test prepared queries against known standards"
        for test, args, standard in zip(TESTS, ARGUMENTS, STANDARDS):
            prepared = prepare(test, args)
            self.assertEqual(prepared, standard)

    def test_bad(self):
        "ensure bad calls raise exceptions"
        self.assertRaises(KeyError, prepare, ":a :b", {'a': 1})

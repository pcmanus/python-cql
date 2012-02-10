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

# to configure behavior, define $CQL_TEST_HOST to the destination address
# for Thrift connections, and $CQL_TEST_PORT to the associated port.

import sys
import os
import unittest
import random
import decimal

TEST_HOST = os.environ.get('CQL_TEST_HOST', 'localhost')
TEST_PORT = int(os.environ.get('CQL_TEST_PORT', 9170))
TEST_CQL_VERSION = '3.0.0-beta1'

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import cql


class TestPreparedQueries(unittest.TestCase):
    cursor = None
    dbconn = None

    def setUp(self):
        self.dbconn = cql.connect(TEST_HOST, TEST_PORT, cql_version=TEST_CQL_VERSION)
        self.cursor = self.dbconn.cursor()
        self.keyspace = self.create_schema()

    def tearDown(self):
        try:
            self.cursor.execute("drop keyspace %s" % self.keyspace)
        except:
            pass

    def create_schema(self):
        ksname = 'CqlDriverTest_%d' % random.randrange(0x100000000)
        self.cursor.execute("""create keyspace %s
                                 with strategy_class='SimpleStrategy'
                                 and strategy_options:replication_factor=1"""
                            % ksname)
        self.cursor.execute('use %s' % ksname)
        self.cursor.execute("""create columnfamily abc (thekey timestamp primary key,
                                                        theint int,
                                                        thefloat float,
                                                        thedecimal decimal,
                                                        theblob blob)""")
        self.cursor.execute("insert into abc (thekey, thedecimal) values ('1999-12-31+0000', '-14.400')")
        self.cursor.execute("insert into abc (thekey, theblob) values ('1969-08-15+0000', '00ff8008')")
        self.cursor.execute("insert into abc (thekey, theint) values ('2012-12-21+0000', 666)")
        self.cursor.execute("insert into abc (thekey, thefloat) values ('2002-09-20+0000', 0.15)")
        self.cursor.execute("""create columnfamily counterito (id int,
                                                               name text,
                                                               feet counter,
                                                               PRIMARY KEY (id, name))
                                                   with compact storage""")
        return ksname

    def test_prepared_select(self):
        q = self.cursor.prepare_query("select thekey, thedecimal, theblob from abc where thekey=:key")

        self.cursor.execute_prepared(q, {'key': '1999-12-31+0000'})
        results = self.cursor.fetchone()
        self.assertEqual(results[1], decimal.Decimal('-14.400'))

        self.cursor.execute_prepared(q, {'key': '1969-08-15+0000'})
        results = self.cursor.fetchone()
        self.assertEqual(results[2], '\x00\xff\x80\x08')

    def test_prepared_insert(self):
        q = self.cursor.prepare_query("insert into abc (thekey, theint) values (:key, :ival)")

        self.cursor.execute_prepared(q, {'key': '1991-10-05+0000', 'ival': 2})
        self.cursor.execute("select thekey, theint from abc where thekey='1991-10-05+0000'")
        results = self.cursor.fetchone()
        self.assertEqual(results[1], 2)

        self.cursor.execute_prepared(q, {'key': '1964-06-23+0000', 'ival': -200000})
        self.cursor.execute("select thekey, theint from abc where thekey='1964-06-23+0000'")
        results = self.cursor.fetchone()
        self.assertEqual(results[1], -200000)

    def test_prepared_update(self):
        q = self.cursor.prepare_query("update abc set theblob=:myblob where thekey = :mykey")

        self.cursor.execute_prepared(q, {'mykey': '2305-07-13+0000', 'myblob': '\0foo\0'})
        self.cursor.execute("select thekey, theblob from abc where thekey='2305-07-13+0000'")
        results = self.cursor.fetchone()
        self.assertEqual(results[1], '\0foo\0')

        self.cursor.execute_prepared(q, {'mykey': '1993-08-16+0000', 'myblob': ''})
        self.cursor.execute("select thekey, theblob from abc where thekey='1993-08-16+0000'")
        results = self.cursor.fetchone()
        self.assertEqual(results[1], '')

    def test_prepared_increment(self):
        q = self.cursor.prepare_query("update counterito set feet=feet + :inc where id = :id and name = 'krang'")

        self.cursor.execute_prepared(q, {'inc': 12, 'id': 1})
        self.cursor.execute("select id, feet from counterito where id=1 and name = 'krang'")
        results = self.cursor.fetchone()
        self.assertEqual(results[1], 12)

        self.cursor.execute_prepared(q, {'inc': -4, 'id': 1})
        self.cursor.execute("select id, feet from counterito where id=1 and name = 'krang'")
        results = self.cursor.fetchone()
        self.assertEqual(results[1], 8)

    def test_prepared_decrement(self):
        q = self.cursor.prepare_query("update counterito set feet=feet - :inc where id = :id and name = 'krang'")

        self.cursor.execute_prepared(q, {'inc': -100, 'id': 2})
        self.cursor.execute("select id, feet from counterito where id=2 and name = 'krang'")
        results = self.cursor.fetchone()
        self.assertEqual(results[1], 100)

        self.cursor.execute_prepared(q, {'inc': 99, 'id': 2})
        self.cursor.execute("select id, feet from counterito where id=2 and name = 'krang'")
        results = self.cursor.fetchone()
        self.assertEqual(results[1], 1)

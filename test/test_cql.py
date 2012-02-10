# -*- coding: utf-8 -*-
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

# to run a single test, run from trunk/:
# PYTHONPATH=test nosetests --tests=test_cql:TestCql.test_column_count

# Note that some tests will be skipped if run against a cluster with
# RandomPartitioner.

# to configure behavior, define $CQL_TEST_HOST to the destination address
# for Thrift connections, and $CQL_TEST_PORT to the associated port.

from os.path import abspath, dirname, join
from random import choice
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol
import sys, os, uuid, time
import unittest

TEST_HOST = os.environ.get('CQL_TEST_HOST', 'localhost')
TEST_PORT = int(os.environ.get('CQL_TEST_PORT', 9170))

sys.path.append(join(abspath(dirname(__file__)), '..'))

import cql
from cql.cassandra import Cassandra

def get_thrift_client(host=TEST_HOST, port=TEST_PORT):
    socket = TSocket.TSocket(host, port)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Cassandra.Client(protocol)
    client.transport = transport
    client.transport.open()
    return client
thrift_client = get_thrift_client()

def uuid1bytes_to_millis(uuidbytes):
    return (uuid.UUID(bytes=uuidbytes).get_time() / 10000) - 12219292800000L

def randstring(len=8, prefix=""):
    chars = [chr(i) for i in range(97,123)]    # characters in range a-z
    randchars = "".join([choice(chars) for x in range(len)])
    return "%s%s" % (prefix, randchars)

def create_schema(cursor, randstr):
    keyspace = randstr

    # Create (and USE) new keyspace
    cursor.execute("""
        CREATE KEYSPACE :keyspace WITH strategy_class = SimpleStrategy
            AND strategy_options:replication_factor = 1
    """, dict(keyspace=keyspace))
    cursor.execute("USE " + keyspace)

    # Create column families and indexes
    cursor.execute("""
        CREATE COLUMNFAMILY StandardString1 (KEY text PRIMARY KEY)
            WITH comparator = ascii AND default_validation = ascii;
    """)
    cursor.execute("""
        CREATE TABLE StandardString2 (KEY text PRIMARY KEY)
            WITH comparator = ascii AND default_validation = ascii;
    """)
    cursor.execute("""
        CREATE COLUMNFAMILY StandardUtf82 (KEY text PRIMARY KEY)
            WITH comparator = text AND default_validation = ascii;
    """)
    cursor.execute("""
        CREATE COLUMNFAMILY StandardLongA (KEY text PRIMARY KEY)
            WITH comparator = bigint AND default_validation = ascii;
    """)
    cursor.execute("""
        CREATE TABLE StandardIntegerA (KEY text PRIMARY KEY)
            WITH comparator = varint AND default_validation = ascii;
    """)
    cursor.execute("""
        CREATE COLUMNFAMILY StandardUUID (KEY text PRIMARY KEY)
            WITH comparator = uuid AND default_validation = ascii;
    """)
    cursor.execute("""
        CREATE COLUMNFAMILY StandardTimeUUID (KEY text PRIMARY KEY)
            WITH comparator = uuid AND default_validation = ascii;
    """)
    cursor.execute("""
        CREATE COLUMNFAMILY StandardTimeUUIDValues (KEY text PRIMARY KEY)
            WITH comparator = ascii AND default_validation = uuid;
    """)
    cursor.execute("""
        CREATE COLUMNFAMILY IndexedA (KEY text PRIMARY KEY, birthdate bigint)
            WITH comparator = ascii AND default_validation = ascii;
    """)
    cursor.execute("""
        CREATE TABLE CounterCF (KEY text PRIMARY KEY, count_me counter)
            WITH comparator = ascii AND default_validation = counter;
    """)
    cursor.execute("CREATE INDEX ON IndexedA (birthdate)")

    return keyspace

def load_sample(dbconn):
    query = "UPDATE StandardString1 SET :c1 = :v1, :c2 = :v2 WHERE KEY = :key"
    dbconn.execute(query, dict(c1="ca1", v1="va1", c2="col", v2="val", key="ka"))
    dbconn.execute(query, dict(c1="cb1", v1="vb1", c2="col", v2="val", key="kb"))
    dbconn.execute(query, dict(c1="cc1", v1="vc1", c2="col", v2="val", key="kc"))
    dbconn.execute(query, dict(c1="cd1", v1="vd1", c2="col", v2="val", key="kd"))

    dbconn.execute("""
    BEGIN BATCH USING CONSISTENCY ONE
     UPDATE StandardLongA SET 1='1', 2='2', 3='3', 4='4' WHERE KEY='aa'
     UPDATE StandardLongA SET 5='5', 6='6', 7='8', 9='9' WHERE KEY='ab'
     UPDATE StandardLongA SET 9='9', 8='8', 7='7', 6='6' WHERE KEY='ac'
     UPDATE StandardLongA SET 5='5', 4='4', 3='3', 2='2' WHERE KEY='ad'
     UPDATE StandardLongA SET 1='1', 2='2', 3='3', 4='4' WHERE KEY='ae'
     UPDATE StandardLongA SET 1='1', 2='2', 3='3', 4='4' WHERE KEY='af'
     UPDATE StandardLongA SET 5='5', 6='6', 7='8', 9='9' WHERE KEY='ag'
    APPLY BATCH
    """)

    dbconn.execute("""
    BEGIN BATCH USING CONSISTENCY ONE
      UPDATE StandardIntegerA SET 10='a', 20='b', 30='c', 40='d' WHERE KEY='k1';
      UPDATE StandardIntegerA SET 10='e', 20='f', 30='g', 40='h' WHERE KEY='k2';
      UPDATE StandardIntegerA SET 10='i', 20='j', 30='k', 40='l' WHERE KEY='k3';
      UPDATE StandardIntegerA SET 10='m', 20='n', 30='o', 40='p' WHERE KEY='k4';
      UPDATE StandardIntegerA SET 10='q', 20='r', 30='s', 40='t' WHERE KEY='k5';
      UPDATE StandardIntegerA SET 10='u', 20='v', 30='w', 40='x' WHERE KEY='k6';
      UPDATE StandardIntegerA SET 10='y', 20='z', 30='A', 40='B' WHERE KEY='k7';
    APPLY BATCH
    """)

    dbconn.execute("""
    BEGIN BATCH
    UPDATE IndexedA SET 'birthdate'=100, 'unindexed'=250 WHERE KEY='asmith';
    UPDATE IndexedA SET 'birthdate'=100, 'unindexed'=200 WHERE KEY='dozer';
    UPDATE IndexedA SET 'birthdate'=175, 'unindexed'=200 WHERE KEY='morpheus';
    UPDATE IndexedA SET 'birthdate'=150, 'unindexed'=250 WHERE KEY='neo';
    UPDATE IndexedA SET 'birthdate'=125, 'unindexed'=200 WHERE KEY='trinity';
    APPLY BATCH
    """)


class TestCql(unittest.TestCase):
    cursor = None
    keyspace = None

    def setUp(self):
        dbconn = cql.connect(TEST_HOST, TEST_PORT)
        self.cursor = dbconn.cursor()
        self.randstr = randstring()
        self.keyspace = create_schema(self.cursor, self.randstr)
        self.keyspaces_to_drop = [self.keyspace]
        load_sample(self.cursor)

    def tearDown(self):
        # Cleanup keyspaces created by test-cases
        for ks in self.keyspaces_to_drop:
            try:
                self.cursor.execute("DROP KEYSPACE :ks", dict(ks=ks))
            except:
                pass

    def make_keyspace_name(self, desc):
        ksname = self.randstr + desc
        self.keyspaces_to_drop.append(ksname)
        return ksname

    def get_partitioner(self):
        return thrift_client.describe_partitioner()

        
    def test_select_simple(self):
        "single-row named column queries"
        cursor = self.cursor
        cursor.execute("SELECT KEY, ca1 FROM StandardString1 WHERE KEY='ka'")
        r = cursor.fetchone()
        d = cursor.description

        self.assertEqual(d[0][0], 'KEY')
        self.assertEqual(r[0], 'ka')

        self.assertEqual(d[1][0], 'ca1')
        self.assertEqual(r[1], 'va1')

        # retrieve multiple columns
        # (we deliberately request columns in non-comparator order)
        cursor.execute("""
            SELECT ca1, col, cd1 FROM StandardString1 WHERE KEY = 'kd'
        """)

        d = cursor.description
        self.assertEqual(['ca1', 'col', 'cd1'], [col_dscptn[0] for col_dscptn in d])
        row = cursor.fetchone()
        # check that the column that didn't exist in the row comes back as null
        self.assertEqual([None, 'val', 'vd1'], row)

    def test_select_row_range(self):
        "retrieve a range of rows with columns"

        if self.get_partitioner().split('.')[-1] == 'RandomPartitioner':
            # skipTest is >= Python 2.7
            if hasattr(self, 'skipTest'):
                self.skipTest("Key ranges don't make sense under RP")
            else: return None

        # everything
        cursor = self.cursor
        cursor.execute("SELECT * FROM StandardLongA")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['aa', 'ab', 'ac', 'ad', 'ae', 'af', 'ag'], keys)

        # [start, end], mid-row
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY >= 'ad' AND KEY <= 'ag'")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['ad', 'ae', 'af', 'ag'], keys)

        # (start, end), mid-row
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'ad' AND KEY < 'ag'")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['ae', 'af'], keys)

        # [start, end], full-row
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY >= 'aa' AND KEY <= 'ag'")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['aa', 'ab', 'ac', 'ad', 'ae', 'af', 'ag'], keys)

        # (start, end), full-row
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'a' AND KEY < 'g'")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['aa', 'ab', 'ac', 'ad', 'ae', 'af', 'ag'], keys)

        # LIMIT tests

        # no WHERE
        cursor.execute("SELECT * FROM StandardLongA LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['aa'], keys)

        # with >=, non-existing key
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY >= 'a' LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['aa'], keys)

        # with >=, existing key
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY >= 'aa' LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['aa'], keys)

        # with >, non-existing key
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'a' LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['aa'], keys)

        # with >, existing key
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'aa' LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['ab'], keys)

        # with both > and <, existing keys
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'aa' and KEY < 'ag' LIMIT 5")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['ab', 'ac', 'ad', 'ae', 'af'], keys)

        # with both > and <, non-existing keys
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'a' and KEY < 'b' LIMIT 5")
        keys = [row[0] for row in cursor.fetchall()]
        self.assertEqual(['aa', 'ab', 'ac', 'ad', 'ae'], keys)

    def test_select_columns_slice(self):
        "column slice tests"
        cursor = self.cursor

        # * includes row key, explicit slice does not
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'ka';")
        row = cursor.fetchone()
        self.assertEqual(['ka', 'va1', 'val'], row)

        cursor.execute("SELECT ''..'' FROM StandardString1 WHERE KEY = 'ka';")
        row = cursor.fetchone()
        self.assertEqual(['va1', 'val'], row)

        # column subsets
        cursor.execute("SELECT 1..3 FROM StandardLongA WHERE KEY = 'aa';")
        self.assertEqual(cursor.rowcount, 1)
        row = cursor.fetchone()
        self.assertEqual(['1', '2', '3'], row)
        
        # range of columns (slice) by row with FIRST
        cursor.execute("SELECT FIRST 1 1..3 FROM StandardLongA WHERE KEY = 'aa'")
        self.assertEqual(cursor.rowcount, 1)
        row = cursor.fetchone()
        self.assertEqual(['1'], row)

        # range of columns (slice) by row reversed
        cursor.execute("SELECT FIRST 2 REVERSED 3..1 FROM StandardLongA WHERE KEY = 'aa'")
        self.assertEqual(cursor.rowcount, 1)
        row = cursor.fetchone()
        self.assertEqual(['3', '2'], row)


    def test_select_range_with_single_column_results(self):
        "range should not fail when keys were not set"
        cursor = self.cursor
        cursor.execute("""
          BEGIN BATCH
            UPDATE StandardString2 SET name='1',password='pass1' WHERE KEY = 'user1'
            UPDATE StandardString2 SET name='2',password='pass2' WHERE KEY = 'user2'
            UPDATE StandardString2 SET password='pass3' WHERE KEY = 'user3'
          APPLY BATCH
        """)

        cursor.execute("""
          SELECT KEY, name FROM StandardString2
        """)

        self.assertEqual(cursor.rowcount, 3, msg="expected 3 results, got %d" % cursor.rowcount)

        # if using RP, these won't be sorted, so we'll sort. other tests take care of
        # checking sorted-ness under non-RP partitioners anyway.
        rows = sorted(cursor.fetchall())

        # two of three results should contain one column "name", third should be empty
        for i in range(1, 3):
            r = rows[i - 1]
            self.assertEqual(len(r), 2)
            self.assertEqual(r[0], "user%d" % i)
            self.assertEqual(r[1], "%s" % i)

        r = rows[2]
        self.assertEqual(len(r), 2, msg='r is %r, but expected length 2' % (r,))
        self.assertEqual(r[0], "user3")
        self.assertEqual(r[1], None)

    def test_index_scan_equality(self):
        "indexed scan where column equals value"
        cursor = self.cursor
        cursor.execute("""
            SELECT KEY, birthdate FROM IndexedA WHERE birthdate = 100
        """)
        self.assertEqual(cursor.rowcount, 2)

        r = cursor.fetchone()
        self.assertEqual(r[0], "asmith")
        self.assertEqual(len(r), 2, msg='r is %r, but expected length 2' % (r,))

        r = cursor.fetchone()
        self.assertEqual(r[0], "dozer")
        self.assertEqual(len(r), 2, msg='r is %r, but expected length 2' % (r,))

    def test_index_scan_greater_than(self):
        "indexed scan where a column is greater than a value"
        cursor = self.cursor
        cursor.execute("""
            SELECT KEY, 'birthdate' FROM IndexedA 
            WHERE 'birthdate' = 100 AND 'unindexed' > 200
        """)
        self.assertEqual(cursor.rowcount, 1)
        row = cursor.fetchone()
        self.assertEqual(row[0], "asmith")

    def test_index_scan_with_start_key(self):
        "indexed scan with a starting key"

        if self.get_partitioner().split('.')[-1] == 'RandomPartitioner':
            # skipTest is Python >= 2.7
            if hasattr(self, 'skipTest'):
                self.skipTest("Key ranges don't make sense under RP")
            else: return None

        cursor = self.cursor
        cursor.execute("""
            SELECT KEY, 'birthdate' FROM IndexedA 
            WHERE 'birthdate' = 100 AND KEY >= 'asmithZ'
        """)
        self.assertEqual(cursor.rowcount, 1)
        r = cursor.fetchone()
        self.assertEqual(r[0], "dozer")

    def test_no_where_clause(self):
        "empty where clause (range query w/o start key)"
        cursor = self.cursor
        cursor.execute("SELECT KEY, 'col' FROM StandardString1 LIMIT 3")
        self.assertEqual(cursor.rowcount, 3)
        rows = sorted(cursor.fetchmany(3))
        self.assertEqual(rows[0][0], "ka")
        self.assertEqual(rows[1][0], "kb")
        self.assertEqual(rows[2][0], "kc")

    def test_column_count(self):
        "getting a result count instead of results"
        cursor = self.cursor
        cursor.execute("SELECT COUNT(*) FROM StandardLongA")
        r = cursor.fetchone()
        self.assertEqual(r[0], 7)
        cursor.execute("SELECT COUNT(1) FROM StandardLongA")
        r = cursor.fetchone()
        self.assertEqual(r[0], 7)

        # count(*) and count(1) are only supported operations
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "SELECT COUNT(name) FROM StandardLongA")
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "SELECT COUNT(1..2) FROM StandardLongA")
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "SELECT COUNT(1, 2, 3) FROM StandardLongA")

    def test_truncate_columnfamily(self):
        "truncating a column family"
        cursor = self.cursor
        cursor.execute("SELECT * FROM StandardString1")
        assert cursor.rowcount > 0
        cursor.execute('TRUNCATE StandardString1;')
        cursor.execute("SELECT * FROM StandardString1")
        self.assertEqual(cursor.rowcount, 0)

        # truncate against non-existing CF
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "TRUNCATE notExistingCFAAAABB")

    def test_delete_columns(self):
        "delete columns from a row"
        cursor = self.cursor
        cursor.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        desc = [col_d[0] for col_d in cursor.description]
        self.assertEqual(['cd1', 'col'], desc)

        cursor.execute("""
            DELETE 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        cursor.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        row = cursor.fetchone()
        self.assertEqual([None, None], row)

    def test_delete_columns_multi_rows(self):
        "delete columns from multiple rows"
        cursor = self.cursor

        # verify rows exist initially
        cursor.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kc'")
        row = cursor.fetchone()
        self.assertEqual(['val'], row)
        cursor.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kd'")
        row = cursor.fetchone()
        self.assertEqual(['val'], row)

        # delete and verify data is gone
        cursor.execute("""
            DELETE 'col' FROM StandardString1 WHERE KEY IN ('kc', 'kd')
        """)
        cursor.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kc'")
        row = cursor.fetchone()
        self.assertEqual([None], row)
        cursor.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kd'")
        r = cursor.fetchone()
        self.assertEqual([None], r)

    def test_delete_rows(self):
        "delete entire rows"
        cursor = self.cursor
        cursor.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        self.assertEqual(['cd1', 'col'], [col_d[0] for col_d in cursor.description])
        cursor.execute("DELETE FROM StandardString1 WHERE KEY = 'kd'")
        cursor.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        row = cursor.fetchone()
        self.assertEqual([None, None], row)

    def test_create_keyspace(self):
        "create a new keyspace"
        cursor = self.cursor
        ksname1 = self.make_keyspace_name('TestKeyspace42')
        ksname2 = self.make_keyspace_name('TestKeyspace43')
        cursor.execute("""
        CREATE SCHEMA :ks WITH strategy_options:DC1 = '1'
            AND strategy_class = 'NetworkTopologyStrategy'
        """, {'ks': ksname1})

        cursor.execute("""
        CREATE SCHEMA :ks WITH strategy_options:1 = 2 AND strategy_options:2 = 3
            AND strategy_class = 'NetworkTopologyStrategy'
        """, {'ks': ksname2})

        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace(ksname1)

        strategy_class = "org.apache.cassandra.locator.NetworkTopologyStrategy"
        self.assertEqual(ksdef.strategy_class, strategy_class)
        self.assertEqual(ksdef.strategy_options['DC1'], "1")

        ksdef = thrift_client.describe_keyspace(ksname2)

        strategy_class = "org.apache.cassandra.locator.NetworkTopologyStrategy"
        self.assertEqual(ksdef.strategy_class, strategy_class)
        self.assertEqual(ksdef.strategy_options['1'], '2')
        self.assertEqual(ksdef.strategy_options['2'], '3')

    def test_drop_keyspace(self):
        "removing a keyspace"
        cursor = self.cursor
        ksname = self.make_keyspace_name('Keyspace4Drop')
        cursor.execute("""
               CREATE KEYSPACE :ks WITH strategy_options:replication_factor = '1'
                   AND strategy_class = 'SimpleStrategy'
        """, {'ks': ksname})

        # TODO: temporary (until this can be done with CQL).
        thrift_client.describe_keyspace(ksname)

        cursor.execute('DROP SCHEMA :ks;', {'ks': ksname})

        # Technically this should throw a ttypes.NotFound(), but this is
        # temporary and so not worth requiring it on PYTHONPATH.
        self.assertRaises(Exception,
                          thrift_client.describe_keyspace,
                          ksname)

    def test_create_column_family(self):
        "create a new column family"
        cursor = self.cursor
        ksname = self.make_keyspace_name('CreateCFKeyspace')
        cursor.execute("""
               CREATE SCHEMA :ks WITH strategy_options:replication_factor = '1'
                   AND strategy_class = 'SimpleStrategy';
        """, {'ks': ksname})
        cursor.execute("USE :ks;", {'ks': ksname})

        cursor.execute("""
            CREATE COLUMNFAMILY NewCf1 (
                KEY varint PRIMARY KEY,
                'username' text,
                'age' varint,
                'birthdate' bigint,
                'id' uuid
            ) WITH comment = 'shiny, new, cf' AND default_validation = ascii;
        """)

        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace(ksname)
        self.assertEqual(len(ksdef.cf_defs), 1)
        cfam= ksdef.cf_defs[0]
        self.assertEqual(len(cfam.column_metadata), 4)
        self.assertEqual(cfam.comment, "shiny, new, cf")
        self.assertEqual(cfam.default_validation_class, "org.apache.cassandra.db.marshal.AsciiType")
        self.assertEqual(cfam.comparator_type, "org.apache.cassandra.db.marshal.UTF8Type")
        self.assertEqual(cfam.key_validation_class, "org.apache.cassandra.db.marshal.IntegerType")

        # Missing primary key
        self.assertRaises(cql.ProgrammingError, cursor.execute, "CREATE COLUMNFAMILY NewCf2")

        # column name should not match key alias
        self.assertRaises(cql.ProgrammingError, cursor.execute, "CREATE COLUMNFAMILY NewCf2 (id 'utf8' primary key, id bigint)")

        # Too many primary keys
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          """CREATE COLUMNFAMILY NewCf2
                                 (KEY varint PRIMARY KEY, KEY text PRIMARY KEY)""")

        # No column defs
        cursor.execute("""CREATE COLUMNFAMILY NewCf3
                            (KEY varint PRIMARY KEY) WITH comparator = bigint""")
        ksdef = thrift_client.describe_keyspace(ksname)
        self.assertEqual(len(ksdef.cf_defs), 2)
        cfam = [i for i in ksdef.cf_defs if i.name == "NewCf3"][0]
        self.assertEqual(cfam.comparator_type, "org.apache.cassandra.db.marshal.LongType")

        # Column defs, defaults otherwise
        cursor.execute("""CREATE COLUMNFAMILY NewCf4
                            (KEY varint PRIMARY KEY, 'a' varint, 'b' varint)
                            WITH comparator = text;""")
        ksdef = thrift_client.describe_keyspace(ksname)
        self.assertEqual(len(ksdef.cf_defs), 3)
        cfam = [i for i in ksdef.cf_defs if i.name == "NewCf4"][0]
        self.assertEqual(len(cfam.column_metadata), 2)
        for coldef in cfam.column_metadata:
            assert coldef.name in ("a", "b"), "Unknown column name " + coldef.name
            assert coldef.validation_class.endswith("marshal.IntegerType")

    def test_drop_columnfamily(self):
        "removing a column family"
        cursor = self.cursor
        ksname = self.make_keyspace_name('Keyspace4CFDrop')
        cursor.execute("""
               CREATE KEYSPACE :ks WITH strategy_options:replication_factor = '1'
                   AND strategy_class = 'SimpleStrategy';
        """, {'ks': ksname})
        cursor.execute('USE :ks;', {'ks': ksname})
        cursor.execute('CREATE COLUMNFAMILY CF4Drop (KEY varint PRIMARY KEY);')

        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace(ksname)
        assert len(ksdef.cf_defs), "Column family not created!"

        cursor.execute('DROP COLUMNFAMILY CF4Drop;')

        ksdef = thrift_client.describe_keyspace(ksname)
        assert not len(ksdef.cf_defs), "Column family not deleted!"

    def test_create_indexs(self):
        "creating column indexes"
        cursor = self.cursor
        cursor.execute("USE " + self.keyspace)
        cursor.execute("CREATE COLUMNFAMILY CreateIndex1 (KEY text PRIMARY KEY, items text, stuff bigint)")
        cursor.execute("CREATE INDEX namedIndex ON CreateIndex1 (items)")
        cursor.execute("CREATE INDEX ON CreateIndex1 (stuff)")

        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace(self.keyspace)
        cfam = [i for i in ksdef.cf_defs if i.name == "CreateIndex1"][0]
        items = [i for i in cfam.column_metadata if i.name == "items"][0]
        stuff = [i for i in cfam.column_metadata if i.name == "stuff"][0]
        self.assertEqual(items.index_name, "namedIndex")
        self.assertEqual(items.index_type, 0, msg="missing index")
        self.assertNotEqual(stuff.index_name, None, msg="index_name should be set")

        # already indexed
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "CREATE INDEX ON CreateIndex1 (stuff)")

    def test_drop_indexes(self):
        "droping indexes on columns"
        cursor = self.cursor
        ksname = self.make_keyspace_name('DropIndexTests')
        cursor.execute("""CREATE KEYSPACE :ks WITH strategy_options:replication_factor = '1'
                                                            AND strategy_class = 'SimpleStrategy';""",
                       {'ks': ksname})
        cursor.execute("USE :ks", {'ks': ksname})
        cursor.execute("CREATE COLUMNFAMILY IndexedCF (KEY text PRIMARY KEY, n text)")
        cursor.execute("CREATE INDEX namedIndex ON IndexedCF (n)")

        ksdef = thrift_client.describe_keyspace(ksname)
        columns = ksdef.cf_defs[0].column_metadata

        self.assertEqual(columns[0].index_name, "namedIndex")
        self.assertEqual(columns[0].index_type, 0)

        # testing "DROP INDEX <INDEX_NAME>"
        cursor.execute("DROP INDEX namedIndex")

        ksdef = thrift_client.describe_keyspace(ksname)
        columns = ksdef.cf_defs[0].column_metadata

        self.assertEqual(columns[0].index_type, None)
        self.assertEqual(columns[0].index_name, None)

        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "DROP INDEX undefIndex")

    def test_time_uuid(self):
        "store and retrieve time-based (type 1) uuids"
        cursor = self.cursor

        # Store and retrieve a timeuuid using it's hex-formatted string
        timeuuid = uuid.uuid1()
        cursor.execute("""
            UPDATE StandardTimeUUID SET '%s' = 10 WHERE KEY = 'uuidtest'
        """ % str(timeuuid))

        cursor.execute("""
            SELECT '%s' FROM StandardTimeUUID WHERE KEY = 'uuidtest'
        """ % str(timeuuid))
        d = cursor.description
        self.assertEqual(d[0][0], timeuuid)

        # Tests a node-side conversion from bigint to UUID.
        ms = uuid1bytes_to_millis(uuid.uuid1().bytes)
        cursor.execute("""
            UPDATE StandardTimeUUIDValues SET 'id' = %d WHERE KEY = 'uuidtest'
        """ % ms)

        cursor.execute("""
            SELECT 'id' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        r = cursor.fetchone()
        self.assertEqual(uuid1bytes_to_millis(r[0].bytes), ms)

        # Tests a node-side conversion from ISO8601 to UUID.
        cursor.execute("""
            UPDATE StandardTimeUUIDValues SET 'id2' = '2011-01-31 17:00:00-0000'
            WHERE KEY = 'uuidtest'
        """)

        cursor.execute("""
            SELECT 'id2' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        # 2011-01-31 17:00:00-0000 == 1296493200000ms
        r = cursor.fetchone()
        ms = uuid1bytes_to_millis(r[0].bytes)
        self.assertEqual(ms, 1296493200000, msg="%d != 1296493200000 (2011-01-31 17:00:00-0000)" % ms)

        # Tests node-side conversion of timeuuid("now") to UUID
        cursor.execute("""
            UPDATE StandardTimeUUIDValues SET 'id3' = 'now'
                    WHERE KEY = 'uuidtest'
        """)

        cursor.execute("""
            SELECT 'id3' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        r = cursor.fetchone()
        ms = uuid1bytes_to_millis(r[0].bytes)
        assert ((time.time() * 1e3) - ms) < 100, \
            "new timeuuid not within 100ms of now (UPDATE vs. SELECT)"

        uuid_range = []
        update = "UPDATE StandardTimeUUID SET :name = :val WHERE KEY = slicetest"
        for i in range(5):
            uuid_range.append(uuid.uuid1())
            cursor.execute(update, dict(name=uuid_range[i], val=i))

        cursor.execute("""
            SELECT :start..:finish FROM StandardTimeUUID WHERE KEY = slicetest
            """, dict(start=uuid_range[0], finish=uuid_range[len(uuid_range)-1]))
        d = cursor.description
        for (i, col_d) in enumerate(d):
            self.assertEqual(uuid_range[i], col_d[0])


    def test_lexical_uuid(self):
        "store and retrieve lexical uuids"
        cursor = self.cursor
        uid = uuid.uuid4()
        cursor.execute("UPDATE StandardUUID SET :name = 10 WHERE KEY = 'uuidtest'",
                       dict(name=uid))

        cursor.execute("SELECT :name FROM StandardUUID WHERE KEY = 'uuidtest'",
                       dict(name=uid))
        d = cursor.description
        self.assertEqual(d[0][0], uid, msg="expected %s, got %s (%s)" % (uid, d[0][0], d[0][1]))

        # TODO: slices of uuids from cf w/ LexicalUUIDType comparator

    def test_utf8_read_write(self):
        "reading and writing utf8 values"
        cursor = self.cursor
        # Sorting: ¢ (u00a2) < © (u00a9) < ® (u00ae) < ¿ (u00bf)
        cursor.execute("UPDATE StandardUtf82 SET :name = v1 WHERE KEY = k1", dict(name="¿"))
        cursor.execute("UPDATE StandardUtf82 SET :name = v1 WHERE KEY = k1", dict(name="©"))
        cursor.execute("UPDATE StandardUtf82 SET :name = v1 WHERE KEY = k1", dict(name="®"))
        cursor.execute("UPDATE StandardUtf82 SET :name = v1 WHERE KEY = k1", dict(name="¢"))

        cursor.execute("SELECT * FROM StandardUtf82 WHERE KEY = k1")
        d = cursor.description
        self.assertEqual(d[0][0], 'KEY')
        self.assertEqual(d[1][0], u"¢")
        self.assertEqual(d[2][0], u"©")
        self.assertEqual(d[3][0], u"®")
        self.assertEqual(d[4][0], u"¿")

        cursor.execute("SELECT :start..'' FROM StandardUtf82 WHERE KEY = k1", dict(start="©"))
        row = cursor.fetchone()
        self.assertEqual(len(row), 3, msg='expected length 3, but got %r' % (row,))
        d = cursor.description
        self.assertEqual(d[0][0], u"©")
        self.assertEqual(d[1][0], u"®")
        self.assertEqual(d[2][0], u"¿")

    def test_read_write_negative_numerics(self):
        "reading and writing negative numeric values"
        cursor = self.cursor
        for cf in ("StandardIntegerA", "StandardLongA"):
            for i in range(10):
                cursor.execute("UPDATE :cf SET :name = :val WHERE KEY = negatives;",
                               dict(cf=cf, name=-(i + 1), val=i))

            cursor.execute("SELECT :start..:finish FROM :cf WHERE KEY = negatives;",
                           dict(start=-10, finish=-1, cf=cf))
            r = cursor.fetchone()
            self.assertEqual(len(r), 10)
            d = cursor.description
            self.assertEqual(d[0][0], -10)
            self.assertEqual(d[9][0], -1)

    def test_escaped_quotes(self):
        "reading and writing strings w/ escaped quotes"
        cursor = self.cursor

        cursor.execute("""
                       UPDATE StandardString1 SET 'x''and''y' = z WHERE KEY = :key
                       """, dict(key="test_escaped_quotes"))

        cursor.execute("""
                       SELECT 'x''and''y' FROM StandardString1 WHERE KEY = :key
                       """, dict(key="test_escaped_quotes"))
        self.assertEqual(cursor.rowcount, 1)
        r = cursor.fetchone()
        self.assertEqual(len(r), 1)
        d = cursor.description
        self.assertEqual(d[0][0], "x'and'y")

    def test_newline_strings(self):
        "reading and writing strings w/ newlines"
        cursor = self.cursor

        cursor.execute("""
                       UPDATE StandardString1 SET :name = :val WHERE KEY = :key;
                       """, {"key": "\nkey", "name": "\nname", "val": "\nval"})

        cursor.execute("""
                       SELECT :name FROM StandardString1 WHERE KEY = :key
                       """, {"key": "\nkey", "name": "\nname"})
        self.assertEqual(cursor.rowcount, 1)
        r = cursor.fetchone()
        self.assertEqual(len(r), 1)
        d = cursor.description
        self.assertEqual(d[0][0], "\nname")
        self.assertEqual(r[0], "\nval")

    def test_typed_keys(self):
        "using typed keys"
        cursor = self.cursor
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = :key", dict(key="ka"))
        row = cursor.fetchone()
        assert isinstance(row[0], unicode), \
            "wrong key-type returned, expected unicode, got %s" % type(row[0])

        # FIXME: The above is woefully inadequate, but the test config uses
        # CollatingOrderPreservingPartitioner which only supports UTF8.

    def test_write_using_insert(self):
        "peforming writes using \"insert\""
        cursor = self.cursor
        cursor.execute("INSERT INTO StandardUtf82 (KEY, :c1, :c2) VALUES (:key, :v1, :v2)", 
                       dict(c1="pork", c2="beef", key="meat", v1="bacon", v2="brisket"))

        cursor.execute("SELECT * FROM StandardUtf82 WHERE KEY = :key", dict(key="meat"))
        r = cursor.fetchone()
        d = cursor.description
        self.assertEqual(d[1][0], "beef")
        self.assertEqual(r[1], "brisket")

        self.assertEqual(d[2][0], "pork")
        self.assertEqual(r[2], "bacon")

        # Bad writes.

        # Too many column values
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "INSERT INTO StandardUtf82 (KEY, :c1) VALUES (:key, :v1, :v2)",
                          dict(c1="name1", key="key0", v1="value1", v2="value2"))

        # Too many column names, (not enough column values)
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "INSERT INTO StandardUtf82 (KEY, :c1, :c2) VALUES (:key, :v1)",
                          dict(c1="name1", c2="name2", key="key0", v1="value1"))

    def test_compression_disabled(self):
        "reading and writing w/ compression disabled"
        cursor = self.cursor
        cursor.compression = 'NONE'
        cursor.execute("UPDATE StandardString1 SET :name = :val WHERE KEY = :key",
                        dict(name="some_name", val="some_value", key="compression_test"))

        cursor.execute("SELECT :name FROM StandardString1 WHERE KEY = :key",
                       dict(name="some_name", key="compression_test"))

        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]
        self.assertEqual(['some_name'], colnames)
        row = cursor.fetchone()
        self.assertEqual(['some_value'], row)

    def test_batch_with_mixed_statements(self):
        "handle BATCH with INSERT/UPDATE/DELETE statements mixed in it"
        cursor = self.cursor
        cursor.compression = 'NONE'
        cursor.execute("""
          BEGIN BATCH USING CONSISTENCY ONE
            UPDATE StandardString1 SET :name = :val WHERE KEY = :key1
            INSERT INTO StandardString1 (KEY, :col1) VALUES (:key2, :val)
            INSERT INTO StandardString1 (KEY, :col2) VALUES (:key3, :val)
            DELETE :col2 FROM StandardString1 WHERE key = :key3
          APPLY BATCH
        """,
        dict(key1="bKey1", key2="bKey2", key3="bKey3", name="bName", col1="bCol1", col2="bCol2", val="bVal"))

        cursor.execute("SELECT :name FROM StandardString1 WHERE KEY = :key",
                       dict(name="bName", key="bKey1"))

        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]
        self.assertEqual(['bName'], colnames)
        r = cursor.fetchone()
        self.assertEqual(['bVal'], r)

        cursor.execute("SELECT :name FROM StandardString1 WHERE KEY = :key",
                       dict(name="bCol2", key="bKey3"))

        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]
        self.assertEqual(['bCol2'], colnames)
        # was deleted by DELETE statement
        r = cursor.fetchone()
        self.assertEqual([None], r)

        cursor.execute("SELECT :name FROM StandardString1 WHERE KEY = :key",
                       dict(name="bCol1", key="bKey2"))

        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]
        self.assertEqual(['bCol1'], colnames)
        r = cursor.fetchone()
        self.assertEqual(['bVal'], r)

        # using all 3 types of statements allowed in batch to test timestamp
        cursor.execute("""
          BEGIN BATCH USING CONSISTENCY ONE AND TIMESTAMP 1303743619771318
            INSERT INTO StandardString1 (KEY, name) VALUES ('TimestampedUser4', 'sname')
            UPDATE StandardString1 SET name = 'name here' WHERE KEY = 'TimestampedUser4'
            DELETE name FROM StandardString1 WHERE KEY = 'TimestampedUser4'
          APPLY BATCH
        """)

        # BATCH should not allow setting individual timestamp when global timestamp is set
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          """
                            BEGIN BATCH USING TIMESTAMP 1303743619771456
                              UPDATE USING TIMESTAMP 1303743619771318 StandardString1
                                     SET name = 'name here'
                                     WHERE KEY = 'TimestampedUser4'
                            APPLY BATCH
                          """)

        # BATCH should not allow setting global TTL
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          """
                            BEGIN BATCH USING TTL 130374
                              UPDATE StandardString1 SET name = 'name here' WHERE KEY = 'TimestampedUser4'
                            APPLY BATCH
                          """)

        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          """
                          BEGIN BATCH USING CONSISTENCY ONE
                              UPDATE USING CONSISTENCY QUORUM StandardString1 SET 'name' = 'value' WHERE KEY = 'bKey4'
                              DELETE 'name' FROM StandardString1 WHERE KEY = 'bKey4'
                          APPLY BATCH
                          """)

    def test_multiple_keys_on_select_and_update(self):
        "select/update statements should support multiple keys by KEY IN construction"
        cursor = self.cursor
        cursor.compression = 'NONE'

        # inserting the same data to the multiple keys
        cursor.execute("""
          UPDATE StandardString1 USING CONSISTENCY ONE SET password = 'p4ssw0rd', login = 'same' WHERE KEY IN ('mUser1', 'mUser2')
        """)

        cursor.execute("SELECT * FROM StandardString1 WHERE KEY IN ('mUser1', 'mUser2')")
        self.assertEqual(cursor.rowcount, 2)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "login")
        self.assertEqual(colnames[2], "password")

        for i in range(2):
            r = cursor.fetchone()
            self.assertEqual(r[1], "same")
            self.assertEqual(r[2], "p4ssw0rd")

        # select with same KEY AND'ed (see CASSANDRA-2717)
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'mUser1' AND KEY = 'mUser1'")
        self.assertEqual(cursor.rowcount, 1)

        # select with different KEYs AND'ed
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "SELECT * FROM StandardString1 WHERE KEY = 'mUser1' AND KEY = 'mUser2'")

        # select with same KEY repeated in IN
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY IN ('mUser1', 'mUser1')")
        self.assertEqual(cursor.rowcount, 1)

    def test_insert_with_timestamp_and_ttl(self):
        "insert statement should support setting timestamp"
        cursor = self.cursor
        cursor.compression = 'NONE'

        # insert to the StandardString1
        cursor.execute("INSERT INTO StandardString1 (KEY, name) VALUES ('TimestampedUser', 'name here') USING TIMESTAMP 1303743619771318")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # and INSERT with CONSISTENCY and TIMESTAMP together
        cursor.execute("INSERT INTO StandardString1 (KEY, name) VALUES ('TimestampedUser1', 'name here') USING TIMESTAMP 1303743619771318 AND CONSISTENCY ONE")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser1'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # and INSERT with TTL
        cursor.execute("INSERT INTO StandardString1 (KEY, name) VALUES ('TimestampedUser2', 'name here') USING TTL 5678")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser2'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # and INSERT with CONSISTENCY, TIMESTAMP and TTL together
        cursor.execute("INSERT INTO StandardString1 (KEY, name) VALUES ('TimestampedUser3', 'name here') USING TTL 4587 AND TIMESTAMP 1303743619771318 AND CONSISTENCY ONE")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser3'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # and INSERT with TTL
        cursor.execute("INSERT INTO StandardString1 (KEY, name) VALUES ('TimestampedUser14', 'name here') USING TTL 1 AND CONSISTENCY ONE")

        # wait for column to expire
        time.sleep(5)

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser14'")
        self.assertEqual(cursor.rowcount, 1)

        r = cursor.fetchone()
        self.assertEqual(len(r), 1)

    def test_update_with_timestamp_and_ttl(self):
        "update statement should support setting timestamp"
        cursor = self.cursor
        cursor.compression = 'NONE'

        # insert to the StandardString1
        cursor.execute("UPDATE StandardString1 USING TIMESTAMP 1303743619771318 SET name = 'name here' WHERE KEY = 'TimestampedUser2'")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser2'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # and UPDATE with CONSISTENCY and TIMESTAMP together
        cursor.execute("UPDATE StandardString1 USING CONSISTENCY ONE AND TIMESTAMP 1303743619771318 SET name = 'name here' WHERE KEY = 'TimestampedUser3'")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser3'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # UPDATE with TTL
        cursor.execute("UPDATE StandardString1 USING TTL 13030 SET name = 'name here' WHERE KEY = 'TimestampedUser4'")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser4'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # UPDATE with CONSISTENCY, TIMESTAMP and TTL together
        cursor.execute("UPDATE StandardString1 USING CONSISTENCY ONE AND TIMESTAMP 1303743619771318 AND TTL 13037 SET name = 'name here' WHERE KEY = 'TimestampedUser5'")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser5'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # UPDATE with TTL
        cursor.execute("UPDATE StandardString1 USING CONSISTENCY ONE TTL 1 SET name = 'name here' WHERE KEY = 'TimestampedUser6'")

        # wait for column to expire
        time.sleep(5)

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser6'")
        self.assertEqual(cursor.rowcount, 1)

        r = cursor.fetchone()
        self.assertEqual(len(r), 1)

    def test_delete_with_timestamp(self):
        "delete statement should support setting timestamp"
        cursor = self.cursor
        cursor.compression = 'NONE'

        # insert to the StandardString1
        cursor.execute("UPDATE StandardString1 USING TIMESTAMP 10 SET name = 'name here' WHERE KEY = 'TimestampedUser3'")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser3'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # DELETE with a lower TIMESTAMP
        cursor.execute("DELETE 'name here' FROM StandardString1 USING TIMESTAMP 3 WHERE KEY = 'TimestampedUser3'")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser3'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(len(colnames), 2)
        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # now DELETE the whole row with a lower TIMESTAMP
        cursor.execute("DELETE FROM StandardString1 USING TIMESTAMP 3 WHERE KEY = 'TimestampedUser3'")

        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser3'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(len(colnames), 2)
        self.assertEqual(colnames[1], "name")

        r = cursor.fetchone()
        self.assertEqual(r[1], "name here")

        # now DELETE the row with a greater TIMESTAMP
        cursor.execute("DELETE FROM StandardString1 USING TIMESTAMP 15 WHERE KEY = 'TimestampedUser3'")
        # try to read it
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'TimestampedUser3'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(len(colnames), 1, msg="expected only the KEY column, got %d columns" % len(colnames))
        self.assertEqual(colnames[0], "KEY")

    def test_alter_table_statement(self):
        "test ALTER statement"
        cursor = self.cursor
        ksname = self.make_keyspace_name('AlterTableKS')
        cursor.execute("""
               CREATE KEYSPACE :ks WITH strategy_options:replication_factor = '1'
                   AND strategy_class = 'SimpleStrategy';
        """, {'ks': ksname})
        cursor.execute("USE :ks;", {'ks': ksname})

        cursor.execute("""
            CREATE COLUMNFAMILY NewCf1 (id_key varint PRIMARY KEY) WITH default_validation = ascii;
        """)

        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace(ksname)
        self.assertEqual(len(ksdef.cf_defs), 1)
        cfam = ksdef.cf_defs[0]

        self.assertEqual(len(cfam.column_metadata), 0)

        # testing "add a new column"
        cursor.execute("ALTER COLUMNFAMILY NewCf1 ADD name varchar")

        ksdef = thrift_client.describe_keyspace(ksname)
        self.assertEqual(len(ksdef.cf_defs), 1)
        columns = ksdef.cf_defs[0].column_metadata

        self.assertEqual(len(columns), 1)
        self.assertEqual(columns[0].name, 'name')
        self.assertEqual(columns[0].validation_class, 'org.apache.cassandra.db.marshal.UTF8Type')

        # testing "alter a column type"
        cursor.execute("ALTER COLUMNFAMILY NewCf1 ALTER name TYPE ascii")

        ksdef = thrift_client.describe_keyspace(ksname)
        self.assertEqual(len(ksdef.cf_defs), 1)
        columns = ksdef.cf_defs[0].column_metadata

        self.assertEqual(len(columns), 1)
        self.assertEqual(columns[0].name, 'name')
        self.assertEqual(columns[0].validation_class, 'org.apache.cassandra.db.marshal.AsciiType')

        # alter column with unknown validator
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "ALTER COLUMNFAMILY NewCf1 ADD name utf8")

        # testing 'drop an existing column'
        cursor.execute("ALTER COLUMNFAMILY NewCf1 DROP name")

        ksdef = thrift_client.describe_keyspace(ksname)
        self.assertEqual(len(ksdef.cf_defs), 1)
        columns = ksdef.cf_defs[0].column_metadata

        self.assertEqual(len(columns), 0)

        # add column with unknown validator
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "ALTER COLUMNFAMILY NewCf1 ADD name utf8")

        # alter not existing column
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "ALTER COLUMNFAMILY NewCf1 ALTER name TYPE uuid")

        # drop not existing column
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "ALTER COLUMNFAMILY NewCf1 DROP name")

        # should raise error when column name equals key alias
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "ALTER COLUMNFAMILY NewCf1 ADD id_key utf8")

    
    def test_counter_column_support(self):
        "update statement should be able to work with counter columns"
        cursor = self.cursor

        # increment counter
        cursor.execute("UPDATE CounterCF SET count_me = count_me + 2 WHERE key = 'counter1'")
        cursor.execute("SELECT * FROM CounterCF WHERE KEY = 'counter1'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "count_me")

        r = cursor.fetchone()
        self.assertEqual(r[1], 2)

        cursor.execute("UPDATE CounterCF SET count_me = count_me + 2 WHERE key = 'counter1'")
        cursor.execute("SELECT * FROM CounterCF WHERE KEY = 'counter1'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "count_me")

        r = cursor.fetchone()
        self.assertEqual(r[1], 4)

        # decrement counter
        cursor.execute("UPDATE CounterCF SET count_me = count_me - 4 WHERE key = 'counter1'")
        cursor.execute("SELECT * FROM CounterCF WHERE KEY = 'counter1'")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "count_me")

        r = cursor.fetchone()
        self.assertEqual(r[1], 0)

        cursor.execute("SELECT * FROM CounterCF")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]

        self.assertEqual(colnames[1], "count_me")

        r = cursor.fetchone()
        self.assertEqual(r[1], 0)

        # deleting a counter column
        cursor.execute("DELETE count_me FROM CounterCF WHERE KEY = 'counter1'")
        cursor.execute("SELECT * FROM CounterCF")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]
        self.assertEqual(len(colnames), 1)

        r = cursor.fetchone()
        self.assertEqual(len(r), 1)

        # can't mix counter and normal statements
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "UPDATE CounterCF SET count_me = count_me + 2, x = 'aa' WHERE key = 'counter1'")

        # column names must match
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "UPDATE CounterCF SET count_me = count_not_me + 2 WHERE key = 'counter1'")

        # counters can't do ANY
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "UPDATE CounterCF USING CONSISTENCY ANY SET count_me = count_me + 2 WHERE key = 'counter1'")

    def test_key_alias_support(self):
        "should be possible to use alias instead of KEY keyword"
        cursor = self.cursor
        ksname = self.make_keyspace_name('KeyAliasKeyspace')

        cursor.execute("""
               CREATE SCHEMA :ks WITH strategy_options:replication_factor = '1'
                   AND strategy_class = 'SimpleStrategy';
        """, {'ks': ksname})
        cursor.execute("USE :ks;", {'ks': ksname})

        # create a Column Family with key alias
        cursor.execute("""
            CREATE COLUMNFAMILY KeyAliasCF (
                'id' varint PRIMARY KEY,
                'username' text
            ) WITH comment = 'shiny, new, cf' AND default_validation = ascii;
        """)

        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace(ksname)
        cfdef = ksdef.cf_defs[0]

        self.assertEqual(len(ksdef.cf_defs), 1)
        self.assertEqual(cfdef.key_alias, 'id')

        # try do insert/update
        cursor.execute("INSERT INTO KeyAliasCF (id, username) VALUES (1, jbellis)")

        # check if we actually stored anything
        cursor.execute("SELECT * FROM KeyAliasCF WHERE id = 1")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]
        self.assertEqual(len(colnames), 2)

        r = cursor.fetchone()
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 1)
        self.assertEqual(r[1], 'jbellis')

        cursor.execute("UPDATE KeyAliasCF SET username = 'xedin' WHERE id = 2")

        # check if we actually stored anything
        cursor.execute("SELECT * FROM KeyAliasCF WHERE id = 2")
        self.assertEqual(cursor.rowcount, 1)
        colnames = [col_d[0] for col_d in cursor.description]
        self.assertEqual(len(colnames), 2)

        r = cursor.fetchone()
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 2)
        self.assertEqual(r[1], 'xedin')

        # delete with key alias
        cursor.execute("DELETE FROM KeyAliasCF WHERE id = 2")
        # check if we actually stored anything
        cursor.execute("SELECT * FROM KeyAliasCF WHERE id = 2")
        self.assertEqual(cursor.rowcount, 1)

        r = cursor.fetchone()
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0], 2)

        # if alias was set you can't use KEY keyword anymore
        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "INSERT INTO KeyAliasCF (KEY, username) VALUES (6, jbellis)")

        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "UPDATE KeyAliasCF SET username = 'xedin' WHERE KEY = 7")

        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "DELETE FROM KeyAliasCF WHERE KEY = 2")

        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "SELECT * FROM KeyAliasCF WHERE KEY = 2")

        self.assertRaises(cql.ProgrammingError,
                          cursor.execute,
                          "SELECT * FROM KeyAliasCF WHERE KEY IN (1, 2)")

        cursor.execute("USE :ks", {'ks': self.keyspace})
        cursor.execute("DROP KEYSPACE :ks", {'ks': ksname})

    def test_key_in_projection_semantics(self):
        "selecting on key always returns at least one result"
        # See: https://issues.apache.org/jira/browse/CASSANDRA-3424
        cursor = self.cursor

        # Key exists, column does not
        cursor.execute("SELECT ka, t0t4llyb0gus FROM StandardString1 WHERE KEY = ka")
        assert cursor.rowcount == 1, "expected exactly 1 result, got %d" % cursor.rowcount

        # Key does not exist
        cursor.execute("SELECT t0t4llyb0gus FROM StandardString1 WHERE KEY = t0t4llyb0gus")
        assert cursor.rowcount == 1, "expected exactly 1 result, got %d" % cursor.rowcount

        # Key does not exist
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = t0t4llyb0gus")
        assert cursor.rowcount == 1, "expected exactly 1 result, got %d" % cursor.rowcount

        # Without explicit key, No Means No (results)
        cursor.execute("TRUNCATE StandardString1")
        cursor.execute("SELECT * FROM StandardString1")
        assert cursor.rowcount == 0, "expected zero results, got %d" % cursor.rowcount

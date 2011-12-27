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
from cql.cursor import Cursor

class TestRegex(unittest.TestCase):

    def single_match(self, match, string):
        groups = match.groups()
        self.assertEquals(groups, (string, ))

    def test_cfamily_regex(self):
        cf_re = Cursor._cfamily_re

        m = cf_re.match("SELECT key FROM column_family WHERE key = 'foo'")
        self.single_match(m, "column_family")

        m = cf_re.match("SELECT key FROM 'column_family' WHERE key = 'foo'")
        self.single_match(m, "column_family")

        m = cf_re.match("SELECT key FROM column_family WHERE key = 'break from chores'")
        self.single_match(m, "column_family")

        m = cf_re.match("SELECT key FROM 'from_cf' WHERE key = 'break from chores'")
        self.single_match(m, "from_cf")

        m = cf_re.match("SELECT '\nkey' FROM 'column_family' WHERE key = 'break \nfrom chores'")
        self.single_match(m, "column_family")

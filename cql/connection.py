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

from cql.apivalues import ProgrammingError, NotSupportedError

class Connection(object):
    cql_major_version = 2

    def __init__(self, host, port, keyspace, user=None, password=None, cql_version=None,
                 compression=None):
        """
        Params:
        * host .........: hostname of Cassandra node.
        * port .........: port number to connect to.
        * keyspace .....: keyspace to connect to.
        * user .........: username used in authentication (optional).
        * password .....: password used in authentication (optional).
        * cql_version...: CQL version to use (optional).
        * compression...: the sort of compression to use by default;
        *                 overrideable per Cursor object. (optional).
        """
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.cql_version = cql_version
        self.compression = compression
        self.open_socket = False

        self.credentials = None
        if user or password:
            self.credentials = {"username": user, "password": password}

        self.establish_connection()
        self.open_socket = True

    def __str__(self):
        return ("%s(host=%r, port=%r, keyspace=%r, %s)"
                % (self.__class__.__name__, self.host, self.port, self.keyspace,
                   self.open_socket and 'conn open' or 'conn closed'))

    def keyspace_changed(self, keyspace):
        self.keyspace = keyspace

    ###
    # Connection API
    ###

    def close(self):
        if not self.open_socket:
            return
        self.terminate_connection()
        self.open_socket = False

    def commit(self):
        """
        'Database modules that do not support transactions should
          implement this method with void functionality.'
        """
        return

    def rollback(self):
        raise NotSupportedError("Rollback functionality not present in Cassandra.")

    def cursor(self):
        if not self.open_socket:
            raise ProgrammingError("Connection has been closed.")
        curs = self.cursorclass(self)
        curs.compression = self.compression
        return curs

# TODO: Pull connections out of a pool instead.
def connect(host, port=9160, keyspace=None, user=None, password=None,
            cql_version=None, native=False):
    if native:
        from native import NativeConnection
        connclass = NativeConnection
    else:
        from thrifteries import ThriftConnection
        connclass = ThriftConnection
    return connclass(host, port, keyspace, user, password, cql_version)

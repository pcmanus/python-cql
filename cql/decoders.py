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

from cql.apivalues import ProgrammingError
from cql import cqltypes

class SchemaDecoder(object):
    """
    Decode binary column names/values according to schema.
    """
    def __init__(self, schema):
        self.schema = schema

    def name_decode_error(self, err, namebytes, expectedtype):
        raise ProgrammingError("column name %r can't be deserialized as %s: %s"
                               % (namebytes, expectedtype, err))

    def value_decode_error(self, err, namebytes, valuebytes, expectedtype):
        raise ProgrammingError("value %r (in col %r) can't be deserialized as %s: %s"
                               % (valuebytes, namebytes, expectedtype, err))

    def decode_description(self, row):
        return self.decode_metadata(row)[0]

    def decode_metadata(self, row):
        return self.decode_metadata_and_types(row)[:2]

    def decode_metadata_and_types(self, row):
        schema = self.schema
        description = []
        column_types = []
        name_info = []
        for column in row.columns:
            namebytes = column.name
            comparator = schema.name_types.get(namebytes, schema.default_name_type)
            comptype = cqltypes.lookup_casstype(comparator)
            validator = schema.value_types.get(namebytes, schema.default_value_type)
            valdtype = cqltypes.lookup_casstype(validator)

            try:
                name = comptype.from_binary(namebytes)
            except Exception, e:
                name = self.name_decode_error(e, namebytes, comparator)
            column_types.append(valdtype)
            description.append((name, validator, None, None, None, None, True))
            name_info.append((namebytes, comptype))

        return description, name_info, column_types

    def decode_row(self, row, column_types=None):
        schema = self.schema
        values = []
        if column_types is None:
            column_types = self.decode_metadata_and_types(row)[2]
        for (column, vtype) in zip(row.columns, column_types):
            try:
                value = vtype.from_binary(column.value)
            except Exception, e:
                value = self.value_decode_error(e, column.name, column.value,
                                                vtype.cass_parameterized_type(full=True))
            values.append(value)

        return values

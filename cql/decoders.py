
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

import cql
from cql.marshal import (unmarshallers, unmarshal_noop)

class SchemaDecoder(object):
    """
    Decode binary column names/values according to schema.
    """
    def __init__(self, schema):
        self.schema = schema

    def name_decode_error(self, err, namebytes, expectedtype):
        raise cql.ProgrammingError("column name %r can't be deserialized as %s: %s"
                                   % (namebytes, expectedtype, err))

    def value_decode_error(self, err, namebytes, valuebytes, expectedtype):
        raise cql.ProgrammingError("value %r (in col %r) can't be deserialized as %s: %s"
                                   % (valuebytes, namebytes, expectedtype, err))

    def decode_description(self, row):
        schema = self.schema
        description = []
        for column in row.columns:
            namebytes = column.name
            comparator = schema.name_types.get(namebytes, schema.default_name_type)
            unmarshal = unmarshallers.get(comparator, unmarshal_noop)
            validator = schema.value_types.get(namebytes, schema.default_value_type)
            try:
                name = unmarshal(namebytes)
            except Exception, e:
                name = self.name_decode_error(e, namebytes, validator)
            description.append((name, validator, None, None, None, None, True))

        return description

    def decode_row(self, row):
        schema = self.schema
        values = []
        for column in row.columns:
            if column.value is None:
                values.append(None)
                continue

            namebytes = column.name
            validator = schema.value_types.get(namebytes, schema.default_value_type)
            unmarshal = unmarshallers.get(validator, unmarshal_noop)
            try:
                value = unmarshal(column.value)
            except Exception, e:
                value = self.value_decode_error(e, namebytes, column.value, validator)
            values.append(value)

        return values

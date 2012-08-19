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

from marshal import int32_pack, int32_unpack, uint16_pack, uint16_unpack
from cqltypes import lookup_casstype
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


PROTOCOL_VERSION             = 0x01
PROTOCOL_VERSION_MASK        = 0x7f

# XXX: should these be called request/response instead? unclear which one will
# apply if/when the server initiates streams in the other direction.
HEADER_DIRECTION_FROM_CLIENT = 0x00
HEADER_DIRECTION_TO_CLIENT   = 0x80
HEADER_DIRECTION_MASK        = 0x80


class CqlResult:
    def __init__(self, column_metadata, rows):
        self.column_metadata = column_metadata
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def __str__(self):
        return '<CqlResult: column_metadata=%r, rows=%r>' \
               % (self.column_metadata, self.rows)
    __repr__ = __str__

class PreparedResult:
    def __init__(self, queryid, param_metadata):
        self.queryid = queryid
        self.param_metadata = param_metadata

    def __str__(self):
        return '<PreparedResult: queryid=%r, column_metadata=%r>' \
               % (self.queryid, self.column_metadata)
    __repr__ = __str__


_message_types_by_name = {}
_message_types_by_opcode = {}

class _register_msg_type(type):
    def __init__(cls, name, bases, dct):
        if not name.startswith('_'):
            _message_types_by_name[cls.name] = cls
            _message_types_by_opcode[cls.opcode] = cls

class _MessageType(object):
    __metaclass__ = _register_msg_type
    params = ()

    def __init__(self, **kwargs):
        for pname in self.params:
            try:
                pval = kwargs[pname]
            except KeyError:
                raise ValueError("%s instances need the %s keyword parameter"
                                 % (self.__class__.__name__, pname))
            setattr(self, pname, pval)

    def send(self, f, streamid, compression=False):
        body = StringIO()
        self.send_body(body)
        body = body.getvalue()
        write_byte(f, PROTOCOL_VERSION | HEADER_DIRECTION_FROM_CLIENT)
        write_byte(f, 0) # no compression supported yet
        write_byte(f, streamid)
        write_byte(f, self.opcode)
        write_int(f, len(body))
        f.write(body)

    def __str__(self):
        paramstrs = ['%s=%r' % (pname, getattr(self, pname)) for pname in self.params]
        return '<%s(%s)>' % (self.__class__.__name__, ', '.join(paramstrs))
    __repr__ = __str__

def read_frame(f):
    version = read_byte(f)
    flags = read_byte(f)
    stream = read_byte(f)
    opcode = read_byte(f)
    body_len = read_int(f)
    assert version & PROTOCOL_VERSION_MASK == PROTOCOL_VERSION, \
            "Unsupported CQL protocol version %d" % version
    assert version & HEADER_DIRECTION_MASK == HEADER_DIRECTION_TO_CLIENT, \
            "Unexpected request from server with opcode %04x, stream id %r" % (opcode, stream)
    assert body_len >= 0, "Invalid CQL protocol body_len %r" % body_len
    if flags:
        warn("Unknown protocol flags set: %02x. May cause problems." % flags)
    body = f.read(body_len)
    msgclass = _message_types_by_opcode[opcode]
    msg = msgclass.recv_body(StringIO(body))
    msg.stream_id = stream
    return msg

def do_request(f, msg):
    msg.send(f, 0)
    f.flush()
    return read_frame(f)

class ErrorMessage(_MessageType):
    opcode = 0x00
    name = 'ERROR'
    params = ('code', 'message')

    error_codes = {
        0x0000: 'Server error',
        0x0001: 'Protocol error',
        0x0002: 'Authentication error',
        0x0100: 'Unavailable exception',
        0x0101: 'Timeout exception',
        0x0102: 'Schema disagreement exception',
        0x0200: 'Request exception',
    }

    @classmethod
    def recv_body(cls, f):
        code = read_int(f)
        msg = read_string(f)
        return cls(code=code, message=msg)

    def __str__(self):
        return '<ErrorMessage code=%04x [%s] message=%r>' \
               % (self.code, self.error_codes.get(self.code, '(Unknown)'), self.message)
    __repr__ = __str__

class StartupMessage(_MessageType):
    opcode = 0x01
    name = 'STARTUP'
    params = ('cqlversion', 'options')

    STARTUP_USE_COMPRESSION = 0x0001

    def send_body(self, f):
        write_string(f, self.cqlversion)
        for key, value in self.options:
            write_short(f, key)
            if key == STARTUP_USE_COMPRESSION:
                write_string(f, value)
            elif isinstance(value, str): # not unicode
                # should be a safe guess
                write_string(f, value)
            else:
                raise NotImplemented("Startup option 0x%04x not known; can't send "
                                     "value to server" % key)

class ReadyMessage(_MessageType):
    opcode = 0x02
    name = 'READY'
    params = ()

    @classmethod
    def recv_body(cls, f):
        return cls()

class AuthenticateMessage(_MessageType):
    opcode = 0x03
    name = 'AUTHENTICATE'
    params = ('authenticator',)

    @classmethod
    def recv_body(cls, f):
        authname = read_string(f)
        return cls(authenticator=authname)

class CredentialsMessage(_MessageType):
    opcode = 0x04
    name = 'CREDENTIALS'
    params = ('creds',)

    def send_body(self, f):
        write_short(f, len(self.creds))
        for credkey, credval in self.creds:
            write_string(f, credkey)
            write_string(f, credval)

class OptionsMessage(_MessageType):
    opcode = 0x05
    name = 'OPTIONS'
    params = ()

    def send_body(self, f):
        pass

class SupportedMessage(_MessageType):
    opcode = 0x06
    name = 'SUPPORTED'
    params = ('cql_versions', 'compressions')

    @classmethod
    def recv_body(cls, f):
        cqlvers = read_stringlist(f)
        compressions = read_stringlist(f)
        return cls(cql_versions=cqlvers, compressions=compressions)

class QueryMessage(_MessageType):
    opcode = 0x07
    name = 'QUERY'
    params = ('query',)

    def send_body(self, f):
        write_longstring(f, self.query)

class ResultMessage(_MessageType):
    opcode = 0x08
    name = 'RESULT'
    params = ('kind', 'results')

    KIND_VOID     = 0x0001
    KIND_ROWS     = 0x0002
    KIND_SET_KS   = 0x0003
    KIND_PREPARED = 0x0004

    type_codes = {
        0x0001: 'ascii',
        0x0002: 'bigint',
        0x0003: 'blob',
        0x0004: 'boolean',
        0x0005: 'counter',
        0x0006: 'decimal',
        0x0007: 'double',
        0x0008: 'float',
        0x0009: 'int',
        0x000A: 'text',
        0x000B: 'timestamp',
        0x000C: 'uuid',
        0x000D: 'varchar',
        0x000E: 'varint',
        0x000F: 'timeuuid',
        0x0020: 'list',
        0x0021: 'map',
        0x0022: 'set',
    }

    FLAGS_GLOBAL_TABLES_SPEC = 0x0001

    @classmethod
    def recv_body(cls, f):
        kind = read_int(f)
        if kind == cls.KIND_VOID:
            results = None
        elif kind == cls.KIND_ROWS:
            results = cls.recv_results_rows(f)
        elif kind == cls.KIND_SET_KS:
            ksname = read_string(f)
            results = ksname
        elif kind == cls.KIND_PREPARED:
            results = cls.recv_results_prepared(f)
        return cls(kind=kind, results=results)

    @classmethod
    def recv_results_rows(cls, f):
        colspecs = cls.recv_results_metadata(f)
        rowcount = read_int(f)
        rows = [cls.recv_row(f, len(colspecs)) for x in xrange(rowcount)]
        return CqlResult(column_metadata=colspecs, rows=rows)

    @classmethod
    def recv_results_prepared(self, f):
        queryid = read_int(f)
        colspecs = cls.recv_results_metadata(f)
        return PreparedResult(queryid, colspecs)

    @classmethod
    def recv_results_metadata(cls, f):
        flags = read_int(f)
        glob_tblspec = bool(flags & cls.FLAGS_GLOBAL_TABLES_SPEC)
        colcount = read_int(f)
        if glob_tblspec:
            ksname = read_string(f)
            cfname = read_string(f)
        colspecs = []
        for x in xrange(colcount):
            if glob_tblspec:
                colksname = ksname
                colcfname = cfname
            else:
                colksname = read_string(f)
                colcfname = read_string(f)
            colname = read_string(f)
            coltype = cls.read_type(f)
            colspecs.append((colksname, colcfname, colname, coltype))
        return colspecs

    @classmethod
    def read_type(cls, f):
        # XXX: stubbed out. should really return more useful 'type' objects.
        optid = read_short(f)
        cqltype = lookup_cqltype(cls.type_codes.get(optid))
        if cqltypename in ('list', 'set'):
            subtype = cls.read_type(f)
            cqltype = cqltype.apply_parameters(subtype)
        elif cqltypename == 'map':
            keysubtype = cls.read_type(f)
            valsubtype = cls.read_type(f)
            cqltype = cqltype.apply_parameters(keysubtype, valsubtype)
        return cqltype

    @staticmethod
    def recv_row(f, colcount):
        return [read_value(f) for x in xrange(colcount)]

class PrepareMessage(_MessageType):
    opcode = 0x09
    name = 'PREPARE'
    params = ('query',)

    def send_body(self, f):
        write_longstring(f, self.query)

class ExecuteMessage(_MessageType):
    opcode = 0x0A
    name = 'EXECUTE'
    params = ('queryid', 'queryparams')

    def send_body(self, f):
        write_int(f, self.queryid)
        write_short(f, len(self.queryparams))
        for param in self.queryparams:
            write_value(f, param)


def read_byte(f):
    return ord(f.read(1))

def write_byte(f, b):
    f.write(chr(b))

def read_int(f):
    return int32_unpack(f.read(4))

def write_int(f, i):
    f.write(int32_pack(i))

def read_short(f):
    return uint16_unpack(f.read(2))

def write_short(f, s):
    f.write(uint16_pack(s))

def read_string(f):
    size = read_short(f)
    contents = f.read(size)
    return contents.decode('utf8')

def write_string(f, s):
    if isinstance(s, unicode):
        s = s.encode('utf8')
    write_short(f, len(s))
    f.write(s)

def read_longstring(f):
    size = read_int(f)
    contents = f.read(size)
    return contents.decode('utf8')

def write_longstring(f, s):
    if isinstance(s, unicode):
        s = s.encode('utf8')
    write_int(f, len(s))
    f.write(s)

def read_stringlist(f):
    numstrs = read_short(f)
    return [read_string(f) for x in xrange(numstrs)]

def write_stringlist(f, stringlist):
    write_short(f, len(stringlist))
    for s in stringlist:
        write_string(f, s)

def read_value(f):
    size = read_int(f)
    if size < 0:
        return None
    return f.read(size)

def write_value(f, v):
    if v is None:
        write_int(f, -1)
    else:
        write_int(f, len(v))
        f.write(v)

# won't work, unless the change from CASSANDRA-4539 is implemented
#def read_option(f):
#    optid = read_short(f)
#    value = read_value(f)
#    return (optid, value)
#
#def write_option(f, optid, value):
#    write_short(f, optid)
#    write_value(f, value)

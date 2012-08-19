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
import cql

__all__ = ['prepare_inline', 'prepare_query', 'cql_quote',
           'cql_marshal', 'PreparedQuery']

def _make_packer(format_string):
    try:
        packer = struct.Struct(format_string) # new in Python 2.5
    except AttributeError:
        pack = lambda x: struct.pack(format_string, x)
        unpack = lambda s: struct.unpack(format_string, s)
    else:
        pack = packer.pack
        unpack = lambda s: packer.unpack(s)[0]
    return pack, unpack

int64_pack, int64_unpack = _make_packer('>q')
int32_pack, int32_unpack = _make_packer('>i')
int16_pack, int16_unpack = _make_packer('>h')
int8_pack, int8_unpack = _make_packer('>b')
uint64_pack, uint64_unpack = _make_packer('>Q')
uint32_pack, uint32_unpack = _make_packer('>I')
uint16_pack, uint16_unpack = _make_packer('>H')
uint8_pack, uint8_unpack = _make_packer('>B')
float_pack, float_unpack = _make_packer('>f')
double_pack, double_unpack = _make_packer('>d')

stringlit_re = re.compile(r"""('[^']*'|"[^"]*")""")
comments_re = re.compile(r'(/\*(?:[^*]|\*[^/])*\*/|//.*$|--.*$)', re.MULTILINE)
param_re = re.compile(r'''
    ( \W )            # don't match : at the beginning of the text (meaning it
                      # immediately follows a comment or string literal) or
                      # right after an identifier character
    : ( \w+ )
    (?= \W )          # and don't match a param that is immediately followed by
                      # a comment or string literal either
''', re.IGNORECASE | re.VERBOSE)

def replace_param_substitutions(query, replacer):
    split_strings = stringlit_re.split(' ' + query + ' ')
    split_str_and_cmt = []
    for p in split_strings:
        if p[:1] in '\'"':
            split_str_and_cmt.append(p)
        else:
            split_str_and_cmt.extend(comments_re.split(p))
    output = []
    for p in split_str_and_cmt:
        if p[:1] in '\'"' or p[:2] in ('--', '//', '/*'):
            output.append(p)
        else:
            output.append(param_re.sub(replacer, p))
    assert output[0][0] == ' ' and output[-1][-1] == ' '
    return ''.join(output)[1:-1]

class PreparedQuery(object):
    def __init__(self, querytext, itemid, vartypes, paramnames):
        self.querytext = querytext
        self.itemid = itemid
        self.vartypes = vartypes
        self.paramnames = paramnames
        if len(self.vartypes) != len(self.paramnames):
            raise cql.ProgrammingError("Length of variable types list is not the same"
                                       " length as the list of parameter names")

    def encode_params(self, params):
        return [cql_marshal(params[n], t) for (n, t) in zip(self.paramnames, self.vartypes)]

def prepare_inline(query, params):
    """
    For every match of the form ":param_name", call cql_quote
    on kwargs['param_name'] and replace that section of the query
    with the result
    """

    def param_replacer(match):
        return match.group(1) + cql_quote(params[match.group(2)])
    return replace_param_substitutions(query, param_replacer)

def prepare_query(querytext):
    paramnames = []
    def found_param(match):
        pre_param_text = match.group(1)
        paramname = match.group(2)
        paramnames.append(paramname)
        return pre_param_text + '?'
    transformed_query = replace_param_substitutions(querytext, found_param)
    return transformed_query, paramnames

def cql_quote(term):
    if isinstance(term, unicode):
        return "'%s'" % __escape_quotes(term.encode('utf8'))
    elif isinstance(term, (str, bool)):
        return "'%s'" % __escape_quotes(str(term))
    else:
        return str(term)

def varint_unpack(term):
    val = int(term.encode('hex'), 16)
    if (ord(term[0]) & 128) != 0:
        val = val - (1 << (len(term) * 8))
    return val

def bitlength(n):
    bitlen = 0
    while n > 0:
        n >>= 1
        bitlen += 1
    return bitlen

def varint_pack(big):
    pos = True
    if big < 0:
        bytelength = bitlength(abs(big) - 1) / 8 + 1
        big = (1 << bytelength * 8) + big
        pos = False
    revbytes = []
    while big > 0:
        revbytes.append(chr(big & 0xff))
        big >>= 8
    if pos and ord(revbytes[-1]) & 0x80:
        revbytes.append('\x00')
    revbytes.reverse()
    return ''.join(revbytes)

def __escape_quotes(term):
    assert isinstance(term, basestring)
    return term.replace("'", "''")

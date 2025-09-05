import cdblib

entries = [
    (b'key', b'value'),                          # from your example
    (b'alpha', b'first'),
    (b'beta', b'second'),
    (b'gamma', b'third'),
    (b'counter:1', b'1'),
    (b'counter:2', b'2'),
    (b'empty', b''),                             # empty value
    (b'binary', b'\x00\x01\x02\xff\xfe'),       # binary value
    (b'newline', b'line1\nline2\n'),
    (b'json', b'{"ok":true,"n":42}'),
    (b'path:/var/log/syslog', b'/var/log/syslog'),
    (b'user:1001', b'per'),
    (b'user:1002', b'anna'),
    (b'duplicate', b'v1'),                       # duplicate key (multiple values)
    (b'duplicate', b'v2'),
    (b'null-in-key:\x00suffix', b'works'),       # key containing a NUL byte
    (b'long:value', b'A' * 1024),                # 1 KiB value
    (b'kv:small', b's'),
    (b'kv:medium', b'm' * 128),
    (b'utf8:key', 'norsk: \u00f8 \u00e6 \u00e5'.encode('utf-8')),  # UTF-8 value
]

with open('test.cdb64', 'wb') as f:
    with cdblib.Writer64(f) as writer:
        for k, v in entries:
            writer.put(k, v)


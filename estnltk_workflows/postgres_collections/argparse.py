import argparse


def get_arg_parser(*args, **kwargs):

    defaults = {'epilog': 'Options can be abbreviated to a prefix and stored in a @conf file.',
                'fromfile_prefix_chars': '@'}
    defaults.update(kwargs)

    parser = argparse.ArgumentParser(**defaults)

    args = set(args)
    known_args = {'pgpass', 'host', 'port', 'dbname', 'user', 'schema', 'role', 'collection', 'mode',
                  'role', 'logging', 'progressbar', 'input_layers', 'output_layer'}
    assert args <= known_args, args - known_args

    if 'collection' in args:
        parser.add_argument('--collection', dest='collection', action='store', required=True,
                            help='name of the collection (required)')
    if 'pgpass' in args:
        parser.add_argument('--pgpass', dest='pgpass', action='store', nargs='?',
                            default='~/.pgpass',
                            help='name of the PostgreSQL password file (default: ~/.pgpass)')
    if 'host' in args:
        parser.add_argument('--host', dest='host', action='store', nargs='?',
                            help='database server host (by default parsed from pgpass file)')
    if 'port' in args:
        parser.add_argument('--port', dest='port', action='store', nargs='?',
                            help='database server port (by default parsed from pgpass file)')
    if 'dbname' in args:
        parser.add_argument('--dbname', dest='dbname', action='store', nargs='?',
                            help='database name to connect to (by default parsed from pgpass file)')
    if 'schema' in args:
        parser.add_argument('--schema', dest='schema', action='store', nargs='?',
                            help='name of the database schema')
    if 'user' in args:
        parser.add_argument('--user', dest='user', action='store', nargs='?',
                            help='database user name (by default parsed from pgpass file)')
    if 'role' in args:
        parser.add_argument('--role', dest='role', action='store', nargs='?',
                            help='database user role (default: user)')
    if 'mode' in args:
        parser.add_argument('--mode', dest='mode', action='store', choices=['extend', 'overwrite'], nargs='?',
                            help='required if the output object already exists in the collection ')
    if 'logging' in args:
        parser.add_argument('--logging', dest='logging', action='store', default='INFO',
                            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                            help='logging level (default: INFO)')
    if 'progressbar' in args:
        parser.add_argument('--progressbar', dest='progressbar', action='store', nargs='?',
                            choices=['ascii', 'unicode', 'notebook'],
                            help='progressbar type (default: no progressbar)')
    if 'input_layers' in args:
        parser.add_argument('--input_layers', dest='input_layers', action='store', nargs='*',
                            help='list of the input layer names')
    if 'output_layer' in args:
        parser.add_argument('--output_layer', dest='output_layer', action='store', nargs='?',
                            help='name of the output layer')
    return parser


def parse_args(*args, **kwargs):
    return get_arg_parser(*args, **kwargs).parse_args()

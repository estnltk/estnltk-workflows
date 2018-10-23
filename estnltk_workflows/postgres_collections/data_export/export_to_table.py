import argparse


parser = argparse.ArgumentParser(description='Exports a layer of EstNltk PgCollection '
                                             'to PostgreSQL table in the same schema.',
                                 epilog='Options can be abbreviated to a prefix and stored in a @conf file.',
                                 fromfile_prefix_chars='@')

parser.add_argument('--pgpass', dest='pgpass', action='store',
                    default='~/.pgpass',
                    help='name of the PostgreSQL password file (default: ~/.pgpass)')
parser.add_argument('--database', dest='database', action='store',
                    help='name of the PostgreSQL database (default: first match in the pgpass file)')
parser.add_argument('--schema', dest='schema', action='store',
                    default='public',
                    help='name of the collection schema (default: public)')
parser.add_argument('--role', dest='role', action='store',
                    help='collection creator role (default: current user)')
parser.add_argument('--mode', dest='mode', action='store', choices=['overwrite', 'append'], nargs='?',
                    help='required if the table already exists')
parser.add_argument('--collection', dest='collection', action='store',
                    help='name of the collection', required=True)
parser.add_argument('--layer', dest='layer', action='store',
                    help='name of layer to be exported')
parser.add_argument('--attributes', dest='attributes', action='store', nargs='*', metavar='ATTR',
                    help='list of attributes of the layer')
parser.add_argument('--logging', dest='logging', action='store', default='INFO',
                    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                    help='logging level (default: INFO)')

args = parser.parse_args()


from estnltk.storage import PostgresStorage
from estnltk import logger

logger.setLevel(args.logging)
logger.info('start script')

storage = PostgresStorage(pgpass_file=args.pgpass,
                          schema=args.schema,
                          dbname=args.database,
                          role=args.role)
collection = storage.get_collection(args.collection)


collection.export_layer(layer=args.layer,
                        attributes=args.attributes,
                        progressbar='unicode')

storage.close()

logger.info('end script')
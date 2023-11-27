from estnltk_workflows.postgres_collections import get_arg_parser

parser = get_arg_parser('collection', 'pgpass', 'host', 'port', 'user', 'dbname', 'role', 'schema',
                        'mode', 'progressbar', 'logging',
                        description='Exports a layer of EstNltk PgCollection '
                                    'to PostgreSQL table in the same schema.')

parser.add_argument('--layer', dest='layer', action='store',
                    help='name of layer to be exported')
parser.add_argument('--attributes', dest='attributes', action='store', nargs='*', metavar='ATTR',
                    help='list of attributes of the layer')

args = parser.parse_args()


from estnltk.storage import PostgresStorage
from estnltk import logger

logger.setLevel(args.logging)
logger.info('start script')

storage = PostgresStorage(pgpass_file=args.pgpass,
                          schema=args.schema,
                          dbname=args.dbname,
                          role=args.role)
collection = storage.get_collection(args.collection)


collection.export_layer(layer=args.layer,
                        attributes=args.attributes,
                        progressbar=args.progressbar)

storage.close()

logger.info('end script')

from estnltk_workflows.postgres_collections import get_arg_parser

parser = get_arg_parser('collection', 'pgpass', 'host', 'port', 'user', 'dbname', 'role', 'schema',
                        'mode', 'progressbar', 'logging',
                        description='Export Estnltk PostgreSQL collection into TEXTA.')

parser.add_argument('--fact_mapping', dest='fact_mapping', action='store', nargs='?',
                    help='name of the csv file that contains fact mapping instructions')
parser.add_argument('--sessionpass', dest='sessionpass', action='store',
                    default=None,
                    help='name of the session password file (default: None). File format: <username>\\n<password>')
parser.add_argument('--textaurl', dest='textaurl', action='store',
                    help='TEXTA server URL with port, for example: http://localhost:8000')
parser.add_argument('--textapass', dest='textapass', action='store',
                    default='~/.textapass',
                    help='name of the file that contains TEXTA access data (default: ~/.textapass). '
                         'File format: <username>\\n<password>')
parser.add_argument('--textaindex', dest='textaindex', action='store',
                    default=None,
                    help='name of the TEXTA index (default: schema name)')
parser.add_argument('--textamapping', dest='textamapping', action='store',
                    default=None,
                    help='name of the TEXTA mapping (default: collection name)')
parser.add_argument('--collection_meta', dest='collection_meta', action='store', nargs='*',
                    help='list of collection meta data columns to include')
args = parser.parse_args()


from estnltk.storage.postgres import PostgresStorage
from estnltk.converters import TextaExporter
from estnltk import logger


logger.info('start script')

storage = PostgresStorage(dbname=args.dbname,
                          user=None,
                          pgpass_file=args.pgpass,
                          schema=args.schema,
                          role=args.role
                          )

collection = storage.get_collection(args.collection)

exporter = TextaExporter(index=args.textaindex or args.schema,
                         doc_type=args.textamapping or args.collection,
                         fact_mapping=args.fact_mapping,
                         textaurl=args.textaurl,
                         textapass=args.textapass,
                         sessionpass=args.sessionpass)

try:
    with exporter.buffered_export() as buffered_export:
        for collection_id, text, meta in collection.select(layers=exporter.fact_layers, progressbar='unicode',
                                                           collection_meta=args.collection_meta):
            meta['collection_id'] = collection_id
            response = buffered_export(text, meta=meta)

    logger.info('end script')
except Exception as e:
    logger.error(e)
finally:
    storage.close()

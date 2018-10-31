import argparse


parser = argparse.ArgumentParser(description='Export Estnltk postgres collection into TEXTA.',
                                 epilog='Options can be abbreviated to a prefix and saved in a config file: '
                                        '$export_to_texta @conf',
                                 fromfile_prefix_chars='@')

parser.add_argument('--database', dest='database', action='store',
                    help='name of the PostgreSQL database (default: first in the pgpass file)')
parser.add_argument('--input-layers', dest='input_layers', action='store', nargs='+', metavar='LAYER',
                    default=[],
                    help='names of the detached layers in the collection that need to be selected '
                         'but are not listed in the fact-mapping file')
parser.add_argument('--fact-mapping', dest='fact_mapping', action='store', default=None, nargs='?',
                    help='name of the csv file that contains fact mapping instructions (default: None)')
parser.add_argument('--sessionpass', dest='sessionpass', action='store',
                    default=None,
                    help='name of the session password file (default: None). File format: <username>\\n<password>')
parser.add_argument('--pgpass', dest='pgpass', action='store',
                    default='~/.pgpass',
                    help='name of the PostgreSQL password file (default: ~/.pgpass). '
                         'File format: hostname:port:database:username:password')
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
parser.add_argument('--schema', dest='schema', action='store', nargs='?',
                    help='name of the collection schema')
parser.add_argument('--collection', dest='collection', action='store',
                    default='collection',
                    help='name of the collection (default: collection)')
parser.add_argument('--collection_meta', dest='collection_meta', action='store', nargs='*',
                    help='list of collection meta data columns')
parser.add_argument('--role', dest='role', action='store', nargs='?',
                    help='collection owner')
parser.add_argument('--mode', dest='mode', action='store', choices=['overwrite', 'append'],
                    help='required if the TEXTA object already exists')
parser.add_argument('--logging', dest='logging', action='store', default='INFO',
                    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                    help='logging level (default: INFO)')
args = parser.parse_args()


from estnltk.storage.postgres import PostgresStorage
from estnltk.converters import TextaExporter
from estnltk import logger


logger.info('start script')

storage = PostgresStorage(dbname=args.database,
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

layers = args.input_layers + exporter.fact_layers

try:
    with exporter.buffered_export() as buffered_export:
        for collection_id, text, meta in collection.select(layers=layers, progressbar='unicode',
                                                     collection_meta=args.collection_meta):
            meta['collection_id'] = collection_id
            response = buffered_export(text, meta=meta)

    logger.info('end script')
except Exception as e:
    logger.error(e)
finally:
    storage.close()

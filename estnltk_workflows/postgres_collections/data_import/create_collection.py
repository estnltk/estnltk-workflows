import argparse


parser = argparse.ArgumentParser(description='Create a collection of estnltk texts '
                                             'with segmentation and morphology layers.',
                                 epilog='Options can be abbreviated to a prefix.',
                                 fromfile_prefix_chars='@')

parser.add_argument('--splittype', dest='splittype', action='store',
                    default='no_splitting', choices=['no_splitting', 'sentences', 'paragraphs'],
                    help='split source texts (default: no_splitting)')
parser.add_argument('--pgpass', dest='pgpass', action='store',
                    default='~/.pgpass',
                    help='name of the PostgreSQL password file (default: ~/.pgpass)')
parser.add_argument('--schema', dest='schema', action='store',
                    default='public',
                    help='name of the collection schema (default: public)')
parser.add_argument('--collection', dest='collection', action='store',
                    default='collection',
                    help='name of the collection (default: collection)')
parser.add_argument('--role', dest='role', action='store',
                    help='collection owner (default: None)')
parser.add_argument('--mode', dest='mode', action='store', choices=['overwrite', 'append'],
                    help='required if the collection already exists')
parser.add_argument('--source', dest='source', action='store', nargs=4, metavar=('SCHEMA', 'TABLE', 'ID', 'TEXT'),
                    default=('public', 'texts', 'id', 'text'),
                    help='schema of the source table, name of the source table, unique id column of the source table, '
                         'plain text column of the source table (default: public, texts, id, text)')
parser.add_argument('--logging', dest='logging', action='store', default='INFO',
                    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                    help='logging level (default: INFO)')
args = parser.parse_args()


from collections import OrderedDict
from psycopg2.sql import SQL, Identifier
import tqdm
from estnltk.storage.postgres import PostgresStorage
from estnltk import Text
from estnltk.layer_operations import split_by
from estnltk_workflows.logger import logger

log = logger(args.logging, __name__)


storage = PostgresStorage(pgpass_file=args.pgpass,
                          schema=args.schema,
                          role=args.role)

source_schema, source_table, source_id_column, source_text_column = args.source
if not storage.table_exists(table=source_table, schema=source_schema):
    raise ValueError('source does not exist: {}.{}'.format(source_schema, source_table))

collection = storage.get_collection(args.collection)

if collection.exists():
    if args.mode is None:
        log.error('Collection {!r} already exists, use --mode {{overwrite,append}}.'.format(args.collection))
        exit(1)
    if args.mode == 'overwrite':
        log.info('Collection {!r} exists. Overwriting.'.format(args.collection))
        collection.delete()
    elif args.mode == 'append':
        log.info('Collection {!r} exists. Appending.'.format(args.collection))


if not collection.exists():
    meta_fields = OrderedDict([('source_id', 'bigint'),
                               ('paragraph_nr', 'int'),
                               ('sentence_nr', 'int')])
    collection = storage.get_collection(args.collection, meta_fields=meta_fields)
    collection.create('collection of estnltk texts with segmentation and morphology layers')
    log.info('New collection {!r} created.'.format(args.collection))


def to_text(text):
    yield text, None, None


def to_paragraphs(text):
    for para_nr, para in enumerate(split_by(text, layer='paragraphs',
                                            layers_to_keep=['words', 'sentences', 'morph_analysis'])):
        yield para, para_nr, None


def to_sentences(text):
    sent_nr = 0
    for para_nr, para in enumerate(split_by(text, layer='paragraphs',
                                            layers_to_keep=['words', 'sentences', 'morph_analysis'])):
        for sent in split_by(para, layer='sentences', layers_to_keep=['words', 'morph_analysis']):
            sent_nr += 1
            yield sent, para_nr, sent_nr


with storage.conn as conn:
    with conn.cursor() as c:
        c.execute(SQL('SELECT count(*) FROM {}.{}').format(Identifier(source_schema),
                                                           Identifier(source_table)))
        total = c.fetchone()[0]

    with conn.cursor('read') as read_cursor:
        # by the documentation named cursor fetches itersize records at time from the backend reducing overhead
        conn.autocommit = False
        read_cursor.execute(SQL('SELECT {}, {} FROM {}.{}').format(Identifier(source_id_column),
                                                                   Identifier(source_text_column),
                                                                   Identifier(source_schema),
                                                                   Identifier(source_table)))
        iter_source = tqdm.tqdm(read_cursor,
                                total=total,
                                unit='doc',
                                disable=args.logging not in {'DEBUG', 'INFO'})

        split = to_text
        if args.splittype == 'no_splitting':
            split = to_text
            log.info('Source texts will not be splitted.')
        elif args.splittype == 'sentences':
            split = to_sentences
            log.info('Source texts will be splitted by sentences.')
        elif args.splittype == 'paragraphs':
            split = to_paragraphs
            log.info('Source texts will be splitted by paragraphs.')

        for source_id, source_text in iter_source:
            iter_source.set_description('source_id: {}'.format(source_id))
            text = Text(source_text).tag_layer(['morph_analysis', 'paragraphs'])
            del text.tokens
            log.debug('source_id: {}, text length: {}, paragraphs: {}, sentences: {}'.format(
                      source_id, len(text.text), len(text.paragraphs), len(text.sentences)))

            for fragment, para_nr, sent_nr in split(text):
                meta = {'source_id': source_id, 'para_nr': para_nr, 'sent_nr': sent_nr}
                collection_id = collection.insert(fragment, meta_data=meta)

        read_cursor.autocommit = True

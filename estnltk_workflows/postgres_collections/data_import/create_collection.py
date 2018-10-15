import argparse


parser = argparse.ArgumentParser(description='Create a collection of estnltk texts '
                                             'with segmentation and morphology layers.',
                                 epilog='Options can be abbreviated to a prefix and stored in a @conf file.',
                                 fromfile_prefix_chars='@')

parser.add_argument('--splittype', dest='splittype', action='store',
                    default='no_splitting', choices=['no_splitting', 'sentences', 'paragraphs'],
                    help='split source texts (default: no_splitting)')
parser.add_argument('--pgpass', dest='pgpass', action='store',
                    default='~/.pgpass',
                    help='name of the PostgreSQL password file (default: ~/.pgpass)')
parser.add_argument('--database', dest='database', action='store',
                    help='name of the PostgreSQL database (default: first in the pgpass file)')
parser.add_argument('--schema', dest='schema', action='store',
                    default='public',
                    help='name of the collection schema (default: public)')
parser.add_argument('--collection', dest='collection', action='store',
                    default='collection',
                    help='name of the collection (default: collection)')
parser.add_argument('--role', dest='role', action='store',
                    help='collection creator role (default: current user)')
parser.add_argument('--mode', dest='mode', action='store', choices=['overwrite', 'append'],
                    help='required if the collection already exists')
parser.add_argument('--source_schema', dest='source_schema', action='store',
                    default='public',
                    help='schema of the source table, (default: public)')
parser.add_argument('--source_table', dest='source_table', action='store',
                    default='texts',
                    help='name of the source table, (default: texts)')
parser.add_argument('--source_id', dest='source_id', action='store',
                    default='id',
                    help='name of the unique id column of the source table (default: id)')
parser.add_argument('--source_text', dest='source_text', action='store', nargs='?', default=None,
                    help='name of the plain text column of the source table; '
                         'exactly one of --source_text or --source_data must be given (default: None)')
parser.add_argument('--source_data', dest='source_data', action='store', nargs='?', default=None,
                    help='name of the column of the source table that contains EstNltk Text objects '
                         'in the EstNltk json format; '
                         'exactly one of --source_text or --source_data must be given (default: None)')
parser.add_argument('--source_columns', dest='source_columns', action='store', nargs='*', metavar='COLUMN_NAME',
                    help='names of the source columns to be copied into the collection table; '
                         'can not include id, data, source_id, start, text, paragraph_nr or sentence_nr '
                         '(default: None)')
parser.add_argument('--logging', dest='logging', action='store', default='INFO',
                    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                    help='logging level (default: INFO)')

args = parser.parse_args()


from collections import OrderedDict
from psycopg2.sql import SQL, Identifier
import tqdm
from estnltk import Text
from estnltk import logger
from estnltk.converters import dict_to_text
from estnltk.storage.postgres import PostgresStorage
from estnltk.layer_operations import split_by


logger.setLevel(args.logging)

schema = args.schema
source_schema = args.source_schema
source_table = args.source_table
source_id = args.source_id
source_text_column = args.source_text
source_columns = [c.strip() for c in args.source_columns]
source_data = args.source_data

if (source_text_column is None) is (source_data is None):
    logger.error('exactly one of --source_text (given: {}) or --source_data (given: {}) expected'.format(
                  source_text_column, source_data))
    exit(1)

storage = PostgresStorage(dbname=args.database,
                          pgpass_file=args.pgpass,
                          schema=schema,
                          role=args.role)

if not storage.table_exists(table=source_table, schema=source_schema):
    logger.error('source does not exist: {}.{}'.format(source_schema, source_table))
    exit(1)

table_name = args.collection
collection = storage.get_collection(table_name=table_name)

if collection.exists():
    if args.mode is None:
        logger.error('Collection {!r} already exists, use --mode {{overwrite,append}}.'.format(table_name))
        exit(1)
    if args.mode == 'overwrite':
        logger.info('Collection {!r} exists. Overwriting.'.format(table_name))
        collection.delete()
    elif args.mode == 'append':
        logger.info('Collection {!r} exists. Appending.'.format(table_name))


if not collection.exists():
    meta_fields = OrderedDict([('source_id', 'bigint'),
                               ('start', 'int'),
                               ('paragraph_nr', 'int'),
                               ('sentence_nr', 'int')])
    collection = storage.get_collection(table_name, meta_fields=meta_fields)
    collection.create('collection of estnltk texts with segmentation and morphology layers')
    logger.info('New collection {!r} created.'.format(table_name))


def to_text(text):
    yield text, 0, None, None


def to_paragraphs(text):
    starts = (s.start for s in text.paragraphs)
    for paragraph_nr, para in enumerate(split_by(text, layer='paragraphs',
                                            layers_to_keep=['words', 'sentences', 'morph_analysis'])):
        yield para, next(starts), paragraph_nr, None


def to_sentences(text):
    starts = (s.start for s in text.sentences)
    sentence_nr = 0
    for paragraph_nr, para in enumerate(split_by(text, layer='paragraphs',
                                            layers_to_keep=['words', 'sentences', 'morph_analysis'])):
        for sent in split_by(para, layer='sentences', layers_to_keep=['words', 'morph_analysis']):
            sentence_nr += 1
            yield sent, next(starts), paragraph_nr, sentence_nr


split = to_text
if args.splittype == 'no_splitting':
    split = to_text
    logger.info('Source texts will not be splitted.')
elif args.splittype == 'sentences':
    split = to_sentences
    logger.info('Source texts will be splitted by sentences.')
elif args.splittype == 'paragraphs':
    split = to_paragraphs
    logger.info('Source texts will be splitted by paragraphs.')

with storage.conn as conn:
    with conn.cursor() as c:
        c.execute(SQL('SELECT count(*) FROM {}.{}').format(Identifier(source_schema),
                                                           Identifier(source_table)))
        total = c.fetchone()[0]

    conn.autocommit = False
    with conn.cursor('read', withhold=True) as read_cursor:
        # by the documentation named cursor fetches itersize records at time from the backend reducing overhead

        read_cursor.execute(SQL('SELECT {}, {} FROM {}.{}').format(
                                                     Identifier(source_id),
                                                     Identifier(source_text_column or source_data),
                                                     Identifier(source_schema),
                                                     Identifier(source_table))
                            )
        iter_source = tqdm.tqdm(read_cursor,
                                total=total,
                                unit='doc',
                                disable=args.logging not in {'DEBUG', 'INFO'})

        commit_interval = 2000
        fragment_counter = 1
        for s_id, source in iter_source:
            iter_source.set_description('source_id: {}'.format(s_id))

            if source_data:
                text = dict_to_text(source)
            else:
                text = Text(source).tag_layer(['morph_analysis', 'paragraphs'])
                del text.tokens
                logger.debug('source_id: {}, text length: {}, paragraphs: {}, sentences: {}'.format(
                    s_id, len(text.text), len(text.paragraphs), len(text.sentences)))

            for fragment, start, paragraph_nr, sentence_nr in split(text):
                meta = {'source_id': s_id, 'start': start, 'paragraph_nr': paragraph_nr, 'sentence_nr': sentence_nr}
                collection_id = collection.insert(fragment, meta_data=meta)

                if fragment_counter == commit_interval:
                    conn.commit()
                    fragment_counter = 1
                else:
                    fragment_counter += 1
        conn.commit()

    conn.autocommit = True

    columns = []
    for c in ['id', 'data', 'source_id', 'start', 'paragraph_nr', 'sentence_nr']:
        columns.append(SQL('\t{}.{}.{}').format(Identifier(schema), Identifier(table_name), Identifier(c)))
    for c in source_columns:
        columns.append(SQL('\t{}.{}.{}').format(Identifier(source_schema), Identifier(source_table), Identifier(c)))
    logger.info('add source columns to the collection: {}'.format(source_columns))
    with conn.cursor() as c:
        temp_table_name = table_name + '_temp_1'
        c.execute(SQL('CREATE TABLE {schema}.{temp_table} \n'
                      'AS SELECT\n{columns} \n'
                      'FROM {schema}.{table}, {source_schema}.{source_table} \n'
                      'WHERE {schema}.{table}."source_id"={source_schema}.{source_table}.{source_id};'
                      ).format(schema=Identifier(schema),
                               temp_table=Identifier(temp_table_name),
                               columns=SQL(',\n').join(columns),
                               table=Identifier(table_name),
                               source_schema=Identifier(source_schema),
                               source_table=Identifier(source_table),
                               source_id=Identifier(source_id)
                               )
                  )
        logger.debug('successful query:\n' + c.query.decode())
        c.execute(SQL('DROP TABLE {schema}.{table};').format(schema=Identifier(schema),
                                                             table=Identifier(table_name)))
        logger.debug('successful query: ' + c.query.decode())
        c.execute(SQL('ALTER TABLE {schema}.{temp_table} RENAME TO {table};'
                      ).format(schema=Identifier(schema),
                               temp_table=Identifier(temp_table_name),
                               table=Identifier(table_name)))
        logger.debug('successful query: ' + c.query.decode())

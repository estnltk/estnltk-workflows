from estnltk_workflows.postgres_collections import get_arg_parser

parser = get_arg_parser('collection', 'pgpass', 'host', 'port', 'user', 'dbname', 'role', 'schema',
                        'mode', 'progressbar', 'logging',
                        description='Create an EstNltk PostgreSQL collection of texts '
                                    'with segmentation and morphology layers.')

parser.add_argument('--splittype', dest='splittype', action='store', nargs='?',
                    default='no_splitting', choices=['no_splitting', 'sentences', 'paragraphs'],
                    help='split source texts (default: no_splitting)')
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
                         'can not include id, data, source_id, start, paragraph_nr or sentence_nr '
                         '(default: None)')
parser.add_argument('--layers', dest='layers', action='store', nargs='*',
                    choices=['words', 'morph_analysis', 'sentences', 'paragraphs'],
                    help='list of layers to be tagged on the texts (default: None)')
parser.add_argument('--chunk_column', dest='chunk_column', action='store', nargs='?',
                    help='name of the chunk column, expects "chunk_value" not None')
parser.add_argument('--chunk_value', dest='chunk_value', action='store', nargs='?',
                    help='only rows with this value in the chunk column are selected')

args = parser.parse_args()


from collections import OrderedDict
from psycopg2.sql import SQL, Identifier, Literal
import tqdm
from estnltk import Text
from estnltk import logger
from estnltk.converters import dict_to_text
from estnltk.storage.postgres import PostgresStorage
from estnltk.layer_operations import split_by
from estnltk.storage.postgres import table_exists


logger.setLevel(args.logging)

logger.info('start script')

schema = args.schema
source_schema = args.source_schema
source_table = args.source_table
source_id = args.source_id
source_text_column = args.source_text
source_columns = [c.strip() for c in args.source_columns or []]
source_data = args.source_data

assert (args.chunk_column is None) is (args.chunk_value is None), (args.chunk_column, args.chunk_value)

collection_columns = ['id', 'data', 'source_id', 'start', 'paragraph_nr', 'sentence_nr']
if set(source_columns) & set(collection_columns):
    logger.error('source_columns can not include: {}'.format(', '.join(set(source_columns) & set(collection_columns))))
    exit(1)

if (source_text_column is None) is (source_data is None):
    logger.error('exactly one of --source_text (given: {}) or --source_data (given: {}) expected'.format(
                  source_text_column, source_data))
    exit(1)

storage = PostgresStorage(dbname=args.dbname,
                          user=args.user,
                          host=args.host,
                          pgpass_file=args.pgpass,
                          schema=schema,
                          role=args.role)

condition = SQL('')
if args.chunk_column:
    condition = SQL('where {}={}').format(Identifier(args.chunk_column),
                                          Literal(args.chunk_value))

with storage.conn.cursor() as c:
    c.execute(SQL('SELECT count({}) FROM {}.{}').format(Identifier(source_id),
                                                        Identifier(source_schema),
                                                        Identifier(source_table)
                                                        ))
    total = c.fetchone()[0]
    logger.debug('total number of rows in the source table: {}'.format(total))
    c.execute(SQL('SELECT count(DISTINCT {}) FROM {}.{}').format(Identifier(source_id),
                                                                 Identifier(source_schema),
                                                                 Identifier(source_table)
                                                                 ))
    distinct = c.fetchone()[0]
    if total != distinct:
        logger.error('values in the source table column {!r} are not unique, {} distinct values in total'.format(
                     source_id, distinct
                     )
                     )
        exit(1)

    if args.chunk_column is not None:
        c.execute(SQL('SELECT count({}) FROM {}.{} {}').format(
                      Identifier(source_id),
                      Identifier(source_schema),
                      Identifier(source_table),
                      condition))
        total = c.fetchone()[0]
        logger.debug('total number of rows in the chunk: {}'.format(total))


if not table_exists(storage, source_table):
    logger.error('source table does not exist: "{}"."{}"'.format(source_schema, source_table))
    exit(1)

table_name = args.collection
collection = storage.get_collection(table_name=table_name)

if collection.exists():
    if args.mode is None:
        logger.error('collection {!r} already exists, use --mode {{overwrite,append}}'.format(table_name))
        exit(1)
    if args.mode == 'overwrite':
        logger.info('collection {!r} exists; overwriting'.format(table_name))
        collection.delete()
    elif args.mode == 'append':
        logger.info('collection {!r} exists; appending.'.format(table_name))


if not collection.exists():
    meta_fields = OrderedDict([('source_id', 'bigint'),
                               ('start', 'int'),
                               ('paragraph_nr', 'int'),
                               ('sentence_nr', 'int')])
    collection = storage.get_collection(table_name, meta_fields=meta_fields)
    collection.create('collection of estnltk texts with segmentation and morphology layers')


def to_text(text):
    yield text, 0, None, None


def to_paragraphs(text):
    starts = (s.start for s in text.paragraphs)
    for paragraph_nr, para in enumerate(split_by(text, layer='paragraphs',
                                                 layers_to_keep=layers_to_keep)):
        yield para, next(starts), paragraph_nr, None


def to_sentences(text):
    starts = (s.start for s in text.sentences)
    sentence_nr = 0
    for paragraph_nr, para in enumerate(split_by(text, layer='paragraphs',
                                                 layers_to_keep=layers_to_keep + ['words', 'sentences'])):
        for sent in split_by(para, layer='sentences', layers_to_keep=layers_to_keep):
            sentence_nr += 1
            yield sent, next(starts), paragraph_nr, sentence_nr


layers_to_tag = set(args.layers or set())
layers_to_keep = layers_to_tag.copy()

split = to_text
if args.splittype == 'no_splitting':
    split = to_text
    logger.info('source texts will not be splitted')
elif args.splittype == 'sentences':
    split = to_sentences
    logger.info('source texts will be splitted by sentences')
    # paragraphs are needed for paragraph numbers
    layers_to_tag.add('paragraphs')

    layers_to_keep.discard('sentences')
    layers_to_keep.discard('paragraphs')
elif args.splittype == 'paragraphs':
    split = to_paragraphs
    logger.info('source texts will be splitted by paragraphs')
    layers_to_tag.add('paragraphs')
    layers_to_keep.discard('paragraphs')

layers_to_tag = sorted(layers_to_tag)
logger.info('layers to tag: {}'.format(layers_to_tag))

if 'paragraphs' in layers_to_keep:
    layers_to_keep.add('sentences')
if 'sentences' in layers_to_keep:
    layers_to_keep.add('words')
if 'morph_analysis' in layers_to_keep:
    layers_to_keep.add('words')
if 'compound_tokens' in layers_to_keep:
    layers_to_keep.add('tokens')
layers_to_keep = sorted(layers_to_keep)

logger.info('layers to keep: {}'.format(layers_to_keep))

with storage.conn as conn:
    conn.autocommit = False
    with conn.cursor('read', withhold=True) as read_cursor:
        # by the documentation named cursor fetches itersize records at time from the backend reducing overhead
        try:
            read_cursor.execute(SQL('SELECT {}, {} FROM {}.{} {};').format(
                Identifier(source_id),
                Identifier(source_text_column or source_data),
                Identifier(source_schema),
                Identifier(source_table),
                condition)
            )
        except Exception as e:
            logger.error(e)
            raise
        finally:
            logger.debug(read_cursor.query.decode())
        iter_source = tqdm.tqdm(read_cursor,
                                total=total,
                                unit='doc',
                                ascii=(args.progressbar == 'ascii'),
                                disable=(args.progressbar not in {'ascii', 'unicode'}),
                                smoothing=0)

        commit_interval = 2000
        fragment_counter = 0
        with collection.insert(buffer_size=1000) as buffered_insert:
            for s_id, source in iter_source:
                iter_source.set_description('source_id: {}'.format(s_id), refresh=False)

                if source_data:
                    text = dict_to_text(source)
                else:
                    text = Text(source).tag_layer(layers_to_tag)
                    if 'tokens' in text.layers:
                        del text.tokens

                for fragment, start, paragraph_nr, sentence_nr in split(text):
                    meta = {'source_id': s_id, 'start': start, 'paragraph_nr': paragraph_nr, 'sentence_nr': sentence_nr}
                    collection_id = buffered_insert(text=fragment, meta_data=meta)

                    fragment_counter += 1
                    if fragment_counter % commit_interval == 0:
                        conn.commit()
        conn.commit()
        logger.info('size of the new collection: {}'.format(fragment_counter))

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
        c.execute(SQL('ALTER TABLE {schema}.{table} ADD PRIMARY KEY ("id");'
                      ).format(schema=Identifier(schema),
                               table=Identifier(table_name)))
        logger.debug(c.query.decode())

logger.info('end script')

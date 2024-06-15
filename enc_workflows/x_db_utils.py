#
#   Various helpful utilities for EstNLTK's Postgres database interface.
#

from typing import List, Union

import re, sys
import os, os.path
import time

from collections import OrderedDict

import psycopg2
from psycopg2 import Error as psycopg2_Error
from psycopg2.extensions import STATUS_BEGIN
from psycopg2.sql import DEFAULT as SQL_DEFAULT
from psycopg2.sql import SQL, Identifier, Literal, Composed

from estnltk import Text
from estnltk import logger
from estnltk.converters import text_to_json
from estnltk.converters import layer_to_json

from estnltk.storage import postgres as pg
from estnltk.storage.postgres import structure_table_identifier
from estnltk.storage.postgres import collection_table_identifier
from estnltk.storage.postgres import collection_table_name
from estnltk.storage.postgres import layer_table_identifier
from estnltk.storage.postgres import layer_table_exists
from estnltk.storage.postgres import layer_table_name

from estnltk.storage.postgres.context_managers.buffered_table_insert import BufferedTableInsert


from x_utils import MetaFieldsCollector
from x_utils import load_collection_layer_templates
from x_utils import normalize_src
from x_utils import SentenceHashRemover
from x_utils import rename_layer


# ===================================================================
#    Collection's layer tables
# ===================================================================

def create_collection_layer_tables( configuration: dict,  collection: 'pg.PgCollection',  layer_type: str = 'detached',
                                    remove_sentences_hash_attr=False, sentences_layer='sentences', sentences_hash_attr='sha256' ):
    '''
    Creates layer tables to the `collection` based on layer templates loaded from collection's JSON files. 
    The `configuration` is used to find collection's subdirectories containing JSON files. 
    
    This layer creation function is a stripped down version of PgCollection.add_layer (
    https://github.com/estnltk/estnltk/blob/ab676f28df06cabee3b7e1f17c9eeaa1f635831d/estnltk/estnltk/storage/postgres/collection.py#L757-L763 ), 
    which allows to add new layers to an empty collection. 
    
    Currently only 'detached' layers can be created.
    '''
    # Validate inputs
    assert 'collection' in configuration, \
        f'(!) Configuration is missing "collection" parameter.'
    collection_dir = configuration['collection']
    assert os.path.exists(collection_dir), \
        f'(!) Missing collection subdirectory {collection_dir!r}'
    if layer_type not in pg.PostgresStorage.TABLED_LAYER_TYPES:
        raise Exception("Unexpected layer type {!r}. Supported layer types are: {!r}".format(layer_type, \
                                                     pg.PostgresStorage.TABLED_LAYER_TYPES))
    if layer_type != 'detached':
        raise NotImplementedError(f"Creating {layer_type} layers is currently not implemented.")
    is_sparse = False
    meta = None
    # Load layer templates
    layer_templates = load_collection_layer_templates(configuration)

    if remove_sentences_hash_attr:
        # Remove hash attribute from 'sentences' layer template
        for template in layer_templates:
            if template.name == sentences_layer and sentences_hash_attr in template.attributes:
                template.attributes = \
                    tuple( [a for a in template.attributes if a != sentences_hash_attr] )
                assert sentences_hash_attr not in template.attributes

    if configuration['layer_renaming_map'] is not None:
        # Rename layers
        for template in layer_templates:
            rename_layer(template, renaming_map=configuration['layer_renaming_map'])
    
    # Create layer tables. Note we need to bypass the standard layer table 
    # creation mechanism, which forbids layer creation on empty collection
    for template in layer_templates:
        # Check for the existence of the layer
        if collection.layers is not None and template.name in collection.layers:
            raise Exception("The {!r} layer already exists in the collection {!r}.".format(template.name, collection.name))
        conn = collection.storage.conn
        conn.commit()
        conn.autocommit = False
        with conn.cursor() as cur:
            try:
                # A) insert the layer to the structure table
                structure_table_id = structure_table_identifier(collection.storage, collection.name)
                # EXCLUSIVE locking -- this mode allows only reads from the table 
                # can proceed in parallel with a transaction holding this lock mode.
                # Prohibit all other modification operations such as delete, insert, 
                # update, create index.
                # (https://www.postgresql.org/docs/9.4/explicit-locking.html)
                cur.execute(SQL('LOCK TABLE ONLY {} IN EXCLUSIVE MODE').format(structure_table_id))

                # Refresh the structure
                collection.refresh(omit_commit=True, omit_rollback=True)
                
                if layer_table_exists(collection.storage, collection.name, template.name, layer_type=layer_type, 
                                                                           omit_commit=True, omit_rollback=True):
                    raise Exception("The table for the {} layer {!r} already exists.".format(layer_type, template.name))
                
                collection._structure.insert(layer=template, layer_type=layer_type, meta=meta, is_sparse=is_sparse)

                # B) create layer table and required indexes
                # The following logic is from former self._create_layer_table method
                layer_table = layer_table_name(collection.name, template.name)
                # create layer table and index
                q = ('CREATE TABLE {layer_identifier} ('
                     'id SERIAL PRIMARY KEY, '
                     'text_id int NOT NULL, '
                     'data jsonb );')
                layer_identifier = \
                    pg.table_identifier(collection.storage, layer_table_name(collection.name, template.name))
                q = SQL(q).format(layer_identifier=layer_identifier)
                cur.execute(q)
                logger.debug(cur.query.decode())

                # Add comment to the layer table
                q = SQL("COMMENT ON TABLE {} IS {};").format(
                        layer_identifier,
                        Literal('created by {} on {}'.format(collection.storage.user, time.asctime())))
                cur.execute(q)
                logger.debug(cur.query.decode())

                cur.execute(SQL(
                    "CREATE INDEX {index} ON {layer_table} (text_id);").format(
                    index=Identifier('idx_%s__text_id' % layer_table),
                    layer_table=layer_identifier))
                logger.debug(cur.query.decode())

            except Exception as layer_adding_error:
                conn.rollback()
                raise PgCollectionException("can't add layer {!r}".format(template.name)) from layer_adding_error
            finally:
                if conn.status == STATUS_BEGIN:
                    # no exception, transaction in progress
                    conn.commit()

        logger.info('{} layer {!r} created from template'.format(layer_type, template.name))


# ===================================================================
#  Collection's metadata table
#  ( contains metadata from <doc>-tags in the original .vert files )
# ===================================================================

def metadata_table_name(collection_name):
    return collection_name + '__metadata'


def metadata_table_identifier(storage, collection_name):
    table_name = metadata_table_name(collection_name)
    return pg.table_identifier(storage, table_name)


def metadata_table_exists( collection: 'pg.PgCollection' ):
    metadata_table = metadata_table_name(collection.name)
    return pg.table_exists(collection.storage, metadata_table, omit_commit=True, omit_rollback=True)


def create_collection_metadata_table( configuration: dict, collection: 'pg.PgCollection' ):
    '''
    Creates collection's metadata table. 
    The `configuration` is used to locate collection's subdirectory, which must also 
    contain file 'meta_fields.txt' that has the names of all metadata fields. 
    '''
    # Validate inputs
    assert 'collection' in configuration, \
        f'(!) Configuration is missing "collection" parameter.'
    collection_dir = configuration['collection']
    assert os.path.exists(collection_dir), \
        f'(!) Missing collection subdirectory {collection_dir!r}'
    metadata_file = os.path.join(collection_dir, 'meta_fields.txt')
    assert os.path.exists(metadata_file), \
        f'(!) Missing collection metadata file {metadata_file!r}'
    # Forbidden metadata column names
    forbidden_field_names = ['id', 'text_id', 'initial_id']
    if configuration['add_vert_indexing_info']:
        forbidden_field_names.extend( ['_vert_file', '_vert_doc_id', \
                                       '_vert_doc_start_line', '_vert_doc_end_line'] )
    # Load collection's metadata fields
    meta_fields = MetaFieldsCollector.load_meta_fields( metadata_file )
    # Rename 'id' -> 'initial_id' and check for forbidden column names
    new_meta_fields = []
    for field in meta_fields:
        if field == 'id':
            if not configuration['remove_initial_id']:
                new_meta_fields.append('initial_id')
        else:
            if field in forbidden_field_names:
                raise ValueError( f'(!) Cannot used {field!r} as a metadata table column '+\
                                  f'name, because name {field!r} is already reserved for '+\
                                  'system purposes. Modify x_db_utils.create_collection_metadata_table() '+\
                                  'and rename the metadata field.')
            new_meta_fields.append(field)
    # Construct metadata table name/identifier
    metadata_table = metadata_table_name(collection.name)
    if metadata_table_exists( collection ):
        raise Exception("The metadata table for the collection {!r} already exists.".format(collection.name))
    table_identifier = metadata_table_identifier(collection.storage, collection.name)
    # Prepare columns
    columns = [SQL('id BIGSERIAL PRIMARY KEY'),
               SQL('text_id INT NOT NULL')]
    for field in new_meta_fields:
        # Add all meta fields as string fields
        columns.append( SQL(f'{field} TEXT') )
    # Add information about document location in the original vert file
    if configuration['add_vert_indexing_info']:
        columns.append( SQL(f'_vert_file TEXT') )
        columns.append( SQL(f'_vert_doc_id TEXT') )
        columns.append( SQL(f'_vert_doc_start_line TEXT') )
        columns.append( SQL(f'_vert_doc_end_line TEXT') )
    conn = collection.storage.conn
    with conn.cursor() as cur:
        try:
            # Create table
            cur.execute(SQL("CREATE TABLE {} ({});").format(table_identifier, SQL(', ').join(columns)))
            logger.debug(cur.query.decode())
            # Add table's comment
            comment = Literal('created by {} on {}'.format(collection.storage.user, time.asctime()))
            if isinstance(configuration['metadata_description'], str):
                comment = Literal(configuration['metadata_description'])
            q = SQL("COMMENT ON TABLE {} IS {};").format( table_identifier, comment )
            cur.execute(q)
            logger.debug(cur.query.decode())
        except Exception as table_creation_error:
            conn.rollback()
            raise PgCollectionException("can't create metadata table {!r}".format(metadata_table)) from table_creation_error
        finally:
            if conn.status == STATUS_BEGIN:
                # no exception, transaction in progress
                conn.commit()
    logger.info('created collection metadata table with meta fields {}'.format(new_meta_fields))


def retrieve_collection_meta_fields( collection: 'pg.PgCollection', exclude_system_fields:bool=False ):
    '''
    Retrieves names and types of columns in the collection metadata table. 
    If `exclude_system_fields` is set, then removes columns 'id' and 'text_id' 
    which are used only for system purposes. 
    '''
    if not metadata_table_exists(collection):
        raise Exception(f'(!) Collection {collection.name!r} has no metadata table.')
    with collection.storage.conn:
        with collection.storage.conn.cursor() as c:
            c.execute(SQL('SELECT column_name, data_type from information_schema.columns '
                          'WHERE table_schema={} and table_name={} '
                          'ORDER BY ordinal_position'
                          ).format(Literal(collection.storage.schema), 
                                   Literal(metadata_table_name(collection.name))))
            meta_table_fields = OrderedDict(c.fetchall())
            if exclude_system_fields:
                del meta_table_fields['id']
                del meta_table_fields['text_id']
            return meta_table_fields


def drop_collection_metadata_table( collection: 'pg.PgCollection', cascade: bool = False):
    metadata_table = metadata_table_name(collection.name)
    pg.drop_table(collection.storage, metadata_table, cascade=cascade)


# ===================================================================
#  Collection's sentence hash table
#  ( contains hash fingerprints of all sentences in the collection )
# ===================================================================

def sentence_hash_table_name(collection_name, layer_name:str='sentences'):
    return collection_name + f'__{layer_name}__hash'


def sentence_hash_table_identifier(storage, collection_name, layer_name:str='sentences'):
    table_name = sentence_hash_table_name(collection_name, layer_name=layer_name)
    return pg.table_identifier(storage, table_name)


def sentence_hash_table_exists( collection: 'pg.PgCollection', layer_name:str='sentences' ):
    table_name = sentence_hash_table_name(collection.name, layer_name=layer_name)
    return pg.table_exists(collection.storage, table_name, omit_commit=True, omit_rollback=True)


def retrieve_collection_hash_table_names( collection: 'pg.PgCollection', return_layer_names:bool=False ):
    '''
    Retrieves and returns names of all (sentences) hash tables of the given collection. 
    If `return_layer_names` is set, then extracts layer names from table names 
    and returns layer names instead.
    '''
    table_prefix = f'{collection.name}__%'
    table_suffix = '%__hash'
    with collection.storage.conn:
        with collection.storage.conn.cursor() as c:
            c.execute(SQL('SELECT table_name from information_schema.tables '
                          'WHERE table_schema={} and table_name LIKE {} and '
                          'table_name LIKE {} '
                          ).format(Literal(collection.storage.schema), 
                                   Literal(table_prefix), Literal(table_suffix)))
            table_names = [row[0] for row in c.fetchall()]
            if return_layer_names:
                # Extract layer names from table names
                table_prefix = f'{collection.name}__'
                table_suffix = '__hash'
                table_names = \
                    [ t[len(table_prefix):-1*len(table_suffix)] for t in table_names ]
            return table_names


def create_sentence_hash_table( configuration: dict, collection: 'pg.PgCollection', validate:bool=True ):
    '''
    Creates collection's sentence hash table. 
    If `validate` is set (default), then the `configuration` is used to find the layer templates (from 
    collection's JSON files), and it is checked that hashable layer exists in layer templates and has 
    the required hash attribute. 
    '''
    layer_name = 'sentences'
    hash_attr  = 'sha256'
    if validate:
        # Load layer templates
        layer_templates = load_collection_layer_templates(configuration)
        # Validate that hashable layer exists in the templates and has the required hash attribute
        template_found = False
        for layer_obj in layer_templates:
            if layer_name in layer_obj.name and hash_attr in layer_obj.attributes:
                template_found = True
                # TODO: if layer_name != layer_obj.name then change layer_name to layer_obj.name (?)
                break
        if not template_found:
            template_names = [layer_obj.name for layer_obj in layer_templates]
            raise Exception(f'(!) Could not find {layer_name!r} layer with attribute {hash_attr!r} '+\
                            f'among the layer templates {template_names!r}. Make sure add_sentence_hashes '+\
                            'was switched on while exporting documents to JSON. ')
    # Construct sentence hash table name/identifier
    if configuration['layer_renaming_map'] is not None:
        assert isinstance( configuration['layer_renaming_map'], dict )
        # Rename sentences layer (if requested)
        layer_name = configuration['layer_renaming_map'].get( layer_name, layer_name )
    sentence_hash_table = sentence_hash_table_name(collection.name, layer_name=layer_name )
    if sentence_hash_table_exists( collection, layer_name=layer_name ):
        raise Exception( "The sentence hash table {!r} already exists in the collection {!r}.".format( \
                         sentence_hash_table, collection.name) )
    table_identifier = sentence_hash_table_identifier(collection.storage, collection.name, layer_name=layer_name)
    # Prepare columns
    columns = [SQL('id BIGSERIAL PRIMARY KEY'),
               SQL('text_id INT NOT NULL'),
               SQL('sentence_id INT NOT NULL'),
               SQL(f'{hash_attr} TEXT')]
    conn = collection.storage.conn
    with conn.cursor() as cur:
        try:
            # Create table
            cur.execute(SQL("CREATE TABLE {} ({});").format(table_identifier, SQL(', ').join(columns)))
            logger.debug(cur.query.decode())
            # Add table's comment
            comment = Literal('created by {} on {}'.format(collection.storage.user, time.asctime()))
            q = SQL("COMMENT ON TABLE {} IS {};").format( table_identifier, comment )
            cur.execute(q)
            logger.debug(cur.query.decode())
        except Exception as table_creation_error:
            conn.rollback()
            raise PgCollectionException("can't create sentence hash table {!r}".format(sentence_hash_table)) from table_creation_error
        finally:
            if conn.status == STATUS_BEGIN:
                # no exception, transaction in progress
                conn.commit()
    logger.info('created collection\'s {} hash table'.format(layer_name))


def drop_sentence_hash_table( collection: 'pg.PgCollection', layer_name:str='sentences', \
                              cascade: bool = False):
    table_name = sentence_hash_table_name(collection.name, layer_name=layer_name)
    pg.drop_table(collection.storage, table_name, cascade=cascade)


# ===================================================================
#    Buffered insertion into all tables of the collection
# ===================================================================


class BufferedMultiTableInsert():
    '''Buffered inserter that maintains insertion buffers over multiple tables.
    
       Builds upon: 
       https://github.com/estnltk/estnltk/blob/main/estnltk/estnltk/storage/postgres/context_managers/buffered_table_insert.py
       https://github.com/estnltk/estnltk/blob/ab676f28df06cabee3b7e1f17c9eeaa1f635831d/estnltk/estnltk/storage/postgres/context_managers/buffered_table_insert.py 
    '''

    def __init__(self, storage, tables_columns, buffer_size=10000, query_length_limit=5000000, \
                       log_doc_completions=False):
        """Initializes context manager for buffered insertions.
        
        Parameters:
        
        :param storage: pg.PostgresStorage
            Postgres Storage into which insertions will be made.
        :param tables_columns:  List[Tuple[str, psycopg2.sql.SQL, List[str]]]
            List with table names, SQL identifiers and corresponding table column
            names into which insertions will be made. 
            Note: tables must already exist when the BufferedMultiTableInsert 
            object is created.
        :param buffer_size: int
            Maximum buffer size (in table rows) for the insert query. 
            If the insertion buffer of any of the tables meets or exceeds this 
            size, then the insert buffer will be flushed. 
            (Default: 10000)
        :param query_length_limit: int
            Soft approximate insert query length limit in unicode characters. 
            If the limit is met or exceeded, the insert buffer will be flushed.
            (Default: 5000000)
        :param log_doc_completions: bool
            Whether completed insertions of documents will be explicitly logged.
            (Default: False)
        """
        self.conn = storage.conn
        self.storage = storage
        self.tables_columns = OrderedDict()
        for items in tables_columns:
            assert len(items) == 3, f'(!) Unexpected values {items!r} for tables_columns row. '+\
                                    'Expected: [table_name:str, SQL_table_identifier:Union[SQL,Composed], list_of_column_names:List[str]]'
            assert isinstance(items[0], str), \
                f'(!) Unexpected type {type(items[0])} for table_name: str'
            table_name = items[0]
            # Check for the existence of the table
            if not pg.table_exists( storage, table_name, omit_commit=True, omit_rollback=True ):
                raise ValueError(f'(!) Table {table_name!r} does not exist. '+\
                                  'Please use script "d_create_collection_tables.py" to create '+\
                                  'tables of the collection.')
            assert isinstance(items[1], (SQL, Composed)), \
                f'(!) Unexpected type {type(items[1])} for SQL_table_identifier: Union[SQL,Composed].'
            table_sql_id = items[1]
            assert isinstance(items[2], list), \
                f'(!) Unexpected type {type(items[2])} for list_of_column_names: List[str]'
            column_identifiers = SQL(', ').join(map(Identifier, items[2]))
            self.tables_columns[table_name] = (table_sql_id, column_identifiers, items[2])
        self.buffer_size = buffer_size
        self.query_length_limit = query_length_limit
        self.log_doc_completions = log_doc_completions
        # Make new cursor for the insertion
        self.cursor = self.conn.cursor()
        # Initialize buffers -- each table has its own buffer
        self._buffered_insert_query_length = 0
        self.table_buffer = {}
        self.completion_markers = {}
        for table in self.tables_columns.keys():
            self.table_buffer[table] = []
            self.completion_markers[table] = []
            column_identifiers = self.tables_columns[table][1]
            self._buffered_insert_query_length += BufferedTableInsert.get_query_length(column_identifiers)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        '''Flushes the buffer and closes this insertion manager. 
           If you are initializing BufferedMultiTableInsert 
           outside the with statement, you should call this method 
           after all insertions have been done.'''
        # Final flushing of the buffer
        self._flush_insert_buffer()
        if self.cursor is not None:
            # Close the cursor
            self.cursor.close()

    def insert(self, table_name, values, doc_completed:int=None):
        """Inserts given values into the table via buffer. 
           Before the insertion, all values will be converted to 
           literals.
           Exceptionally, a value can also be psycopg2.sql.DEFAULT, 
           in which case it will not be converted.
           Optionally, if completion marker `doc_completed` is not 
           `None`, but points to a document id, then records that 
           the current insertion completes the data of the document 
           in all tables. This is used for book-keeping about which 
           of the documents have been completely inserted.
           Note: this method assumes that the table, where values
           will be inserted, has already been created.
        """
        assert self.cursor is not None
        assert self.conn.autocommit == False
        if table_name not in self.tables_columns.keys():
            raise KeyError(f'(!) Unexpected table {table_name!r}: no instructions '+\
                           'available on how to insert into that table.')
        column_names = self.tables_columns[table_name][2]
        assert len( values ) == len( column_names ), \
            f'(!) Number of insertable values: {len(values)} != number of table {table_name!r} columns: {len(column_names)}'
        # Convert values to literals
        converted = []
        for val in values:
            if val == SQL_DEFAULT:
                # Skip value that has already been converted
                converted.append( val )
            else:
                converted.append( Literal(val) )
        q_vals = SQL('({})').format(SQL(', ').join( converted ))
        # Find out how much the query length and the buffer size will increase
        added_query_length = BufferedTableInsert.get_query_length( q_vals )
        cur_buffer = self.table_buffer[table_name]
        # Completion marker: after this insertion, all should be completed for the given document
        if doc_completed is not None:
            self.completion_markers[table_name].append( doc_completed )
        # Do we need to flush the buffer before appending?
        if len(cur_buffer) + 1 >= self.buffer_size or \
           self._buffered_insert_query_length + added_query_length >= self.query_length_limit:
            self._flush_insert_buffer()
        # Add to the buffer
        self.table_buffer[table_name].append( q_vals )
        self._buffered_insert_query_length += added_query_length

    def has_unflushed_buffers(self):
        return any([ len(self.table_buffer[k]) > 0 for k in self.table_buffer.keys() ])

    def incomplete_documents(self):
        return [v for t in self.completion_markers.keys() for v in self.completion_markers[t]]

    def _flush_insert_buffer(self):
        """Flushes the insert buffer, i.e. attempts to execute and commit 
           insert queries of all the tables.
        """
        if not self.has_unflushed_buffers():
            return
        # Flush buffers of all tables
        rows_flushed = 0
        bytes_flushed = 0
        for table in self.tables_columns.keys():
            table_identifier = self.tables_columns[table][0]
            column_identifiers = self.tables_columns[table][1]
            buffer = self.table_buffer[table]
            if len( buffer ) > 0:
                try:
                    self.cursor.execute(SQL('INSERT INTO {} ({}) VALUES {};').format(
                                   table_identifier,
                                   column_identifiers,
                                   SQL(', ').join(buffer)))
                    rows_flushed += len(buffer)
                    bytes_flushed += len(self.cursor.query)
                    if len( self.completion_markers[table] ) > 0:
                        for doc_id in self.completion_markers[table]:
                            if self.log_doc_completions:
                                logger.info('completed insertion of document {}'.format(doc_id))
                        self.completion_markers[table].clear()
                except Exception as ex:
                    if issubclass(type(ex), psycopg2_Error):
                        # Log more information about psycopg2_Error
                        if ex.diag.message_primary is not None:
                            logger.error('{}: {}'.format( ex.__class__.__name__, \
                                                          ex.diag.message_primary ))
                        if ex.diag.message_detail is not None:
                            logger.error('DETAIL: {}'.format( ex.diag.message_detail ))
                        if ex.diag.message_hint is not None:
                            logger.error('HINT: {}'.format( ex.diag.message_hint ))
                        if ex.diag.context is not None:
                            logger.error('CONTEXT: {}'.format( ex.diag.context ))
                    logger.error(f'flush insert buffer failed at table {table}')
                    if rows_flushed > 0:
                        logger.error('number of rows inserted: {}'.format(rows_flushed))
                    logger.error('number of rows still in the buffer: {}'.format(len(buffer)))
                    incomplete_docs = self.incomplete_documents()
                    if incomplete_docs:
                        logger.error('partially inserted documents: {}'.format(incomplete_docs))
                    logger.error('estimated total insert query length: {}'.format(self._buffered_insert_query_length))
                    self.cursor.connection.rollback()
                    raise
                finally:
                    if self.cursor.connection.status == STATUS_BEGIN:
                        # no exception, transaction in progress
                        self.cursor.connection.commit()
        # Log progress
        logger.debug('flush buffer: {} rows, {} bytes, {} estimated characters'.format(
                     rows_flushed, bytes_flushed, self._buffered_insert_query_length))
        # Clear / reset buffer
        self._buffered_insert_query_length = 0
        for table in self.tables_columns.keys():
            self.table_buffer[table].clear()
            column_identifiers = self.tables_columns[table][1]
            self._buffered_insert_query_length += BufferedTableInsert.get_query_length(column_identifiers)




class CollectionMultiTableInserter():
    '''A version of CollectionTextObjectInserter that allows to insert a Text object into all tables of 
       the collection. 
       Updates simultaneously collection base table, collection detached layer tables, metadata table, 
       and sentences hash table.
       
       Builds upon: 
       https://github.com/estnltk/estnltk/blob/ab676f28df06cabee3b7e1f17c9eeaa1f635831d/estnltk/estnltk/storage/postgres/context_managers/collection_text_object_inserter.py
       https://github.com/estnltk/estnltk/blob/ab676f28df06cabee3b7e1f17c9eeaa1f635831d/estnltk/estnltk/storage/postgres/context_managers/collection_detached_layer_inserter.py
    '''

    def __init__(self, collection, buffer_size=10000, query_length_limit=5000000, 
                       remove_sentences_hash_attr=False, sentences_layer='sentences', 
                       sentences_hash_attr='sha256', layer_renaming_map:dict=None, 
                       log_doc_completions:bool=False ):
        """Initializes context manager for Text object insertions.
        
        Parameters:
         
        :param collection: PgCollection
            Collection where Text objects will be inserted.
        :param buffer_size: int
            Maximum buffer size (in table rows) for the insert query. 
            If the size is met or exceeded, the insert buffer will be flushed. 
            (Default: 10000)
        :param query_length_limit: int
            Soft approximate insert query length limit in unicode characters. 
            If the limit is met or exceeded, the insert buffer will be flushed.
            (Default: 5000000)
        :param remove_sentences_hash_attr: bool
            Whether `sentences_hash_attr` will be removed from `sentences_layer` 
            before inserting the layer into the layer table.
            Default: False
        :param sentences_layer: str
            Name of the sentences layer which also contains hash fingerprints 
            in the layer attribute `sentences_hash_attr`.
            Default: 'sentences'
        :param sentences_hash_attr: str
            Name of the hash fingerprint attribute in the `sentences_layer`.
            Default: 'sha256'
        :param layer_renaming_map:dict
            A dictionary specifying how to rename layers, mapping from old layer 
            names (strings) to new ones (strings).
            Default: None (no layers will be renamed);
        :param log_doc_completions: bool
            Whether completed insertions of documents will be explicitly logged.
            (Default: False)
        """
        self.collection = collection
        if self.collection.version < '4.0':
            raise Exception( ("Cannot use this CollectionMultiTableInserter with collection version {!r}. "+\
                              "PgCollection version 4.0+ is required.").format(self.collection.version) )
        self.buffer_size = buffer_size
        self.query_length_limit = query_length_limit
        assert layer_renaming_map is None or isinstance(layer_renaming_map, dict)
        self.layer_renaming_map = layer_renaming_map
        self.log_doc_completions = log_doc_completions
        # Make mapping from insertion phases to table names and columns
        self.insertion_phase_map = OrderedDict()
        insertable_tables = []
        # Collection table
        collection_table = collection_table_name(self.collection.name)
        collection_identifier = collection_table_identifier( self.collection.storage, self.collection.name )
        collection_table_columns = self.collection.column_names
        insertable_tables.append( [collection_table, collection_identifier, collection_table_columns] )
        self.insertion_phase_map['_collection'] = (collection_table, collection_table_columns)
        self.add_meta_src  = 'src' in (self.collection).meta.columns
        assert len((self.collection).meta.columns) <= 1, \
            f'(!) Unexpected meta columns in collection: {(self.collection).meta.columns!r}'
        # Metadata table
        metadata_table     = metadata_table_name(self.collection.name)
        meta_identifier    = metadata_table_identifier(self.collection.storage, self.collection.name)
        meta_table_columns_and_types = \
            retrieve_collection_meta_fields(self.collection, exclude_system_fields=False)
        meta_table_columns = [col for col in meta_table_columns_and_types.keys()]
        insertable_tables.append( [metadata_table, meta_identifier, meta_table_columns] )
        self.insertion_phase_map['_metadata'] = (metadata_table, insertable_tables[-1][-1]) 
        # Sentence hash table
        # (must come before layer insertion because sentence hashes can be removed during layer insertion)
        self.sentences_layer = sentences_layer
        self.sentences_hash_attr = sentences_hash_attr
        self.remove_sentences_hash_attr = remove_sentences_hash_attr
        self.sentences_hash_remover = None
        if self.remove_sentences_hash_attr:
            self.sentences_hash_remover = SentenceHashRemover( output_layer=self.sentences_layer, \
                                                               attrib = self.sentences_hash_attr )
        # Construct sentence hash table name/identifier
        sentences_name_for_hash_table = self.sentences_layer
        if self.layer_renaming_map is not None:
            # Add prefix/suffix to layer name
            sentences_name_for_hash_table = (self.layer_renaming_map).get( sentences_name_for_hash_table, \
                                                                           sentences_name_for_hash_table ) 
            assert isinstance(sentences_name_for_hash_table, str)
        sentence_hash_table = sentence_hash_table_name(self.collection.name, \
                                                       layer_name=sentences_name_for_hash_table)
        sentence_hash_id    = sentence_hash_table_identifier(self.collection.storage, \
                                                             self.collection.name, \
                                                             layer_name=sentences_name_for_hash_table)
        sentence_hash_columns = ['id', 'text_id', 'sentence_id', f'{self.sentences_hash_attr}']
        insertable_tables.append( [sentence_hash_table, sentence_hash_id, sentence_hash_columns] )
        self.insertion_phase_map['_hashes'] = (sentence_hash_table, insertable_tables[-1][-1])
        # Layer tables
        layers = list(self.collection.structure)
        for layer_name in layers:
            layer_table = layer_table_name(collection.name, layer_name)
            table_identifier = \
                layer_table_identifier(self.collection.storage, self.collection.name, layer_name)
            insertable_tables.append( [layer_table, table_identifier, ["id", "text_id", "data"]] )
            self.insertion_phase_map[f'_layer_{layer_name}'] = (layer_table, insertable_tables[-1][-1])
        self.insertable_tables = insertable_tables
        self.buffered_inserter = None
        self.text_insert_counter = 0
        # TODO: count complete vs incomplete insertions


    def __enter__(self):
        """ Initializes the insertion buffer. Assumes collection structure & tables have already been created. """
        self.collection.storage.conn.commit()
        self.collection.storage.conn.autocommit = False
        assert self.insertable_tables is not None and len(self.insertable_tables) > 0
        # Make new buffered inserter
        self.buffered_inserter = BufferedMultiTableInsert( self.collection.storage, 
                                                           self.insertable_tables,
                                                           query_length_limit = self.query_length_limit,
                                                           buffer_size = self.buffer_size,
                                                           log_doc_completions = self.log_doc_completions)
        cursor = self.buffered_inserter.cursor
        assert cursor is not None
        return self

    def __exit__(self, type, value, traceback):
        """ Closes the insertion buffer. """
        if self.buffered_inserter is not None:
            self.buffered_inserter.close()
            logger.info('inserted {} texts into the collection {!r}'.format(self.text_insert_counter, self.collection.name))

    def __call__(self, text, key): 
        self.insert(text, key=key)


    def insert(self, text, key):
        """Inserts given Text object with the given key into the collection.
           Optionally, metadata of the insertable Text object can be specified. 
        """
        assert self.buffered_inserter is not None
        #
        # Divide Text obj insertion into different phases
        #
        last_phase = list( self.insertion_phase_map.keys() )[-1]
        for phase in self.insertion_phase_map.keys():
            table_name    = self.insertion_phase_map[phase][0]
            table_columns = self.insertion_phase_map[phase][1]
            if phase == '_collection':
                # Insert Text object without annotations and metadata
                new_text_meta = None
                if self.add_meta_src:
                    new_text, new_text_meta = \
                        CollectionMultiTableInserter._insertable_text_object(text, add_src=self.add_meta_src)
                    row = [ key, text_to_json(new_text), False, new_text_meta.get('src', SQL_DEFAULT) ]
                else:
                    new_text = \
                        CollectionMultiTableInserter._insertable_text_object(text, add_src=self.add_meta_src)
                    row = [ key, text_to_json(new_text), False ]
                assert len(table_columns) == len(row)
                self.buffered_inserter.insert( table_name, row )
            elif phase == '_metadata':
                # Insert Text's metadata
                row = [ SQL_DEFAULT, key ]
                new_text_meta = CollectionMultiTableInserter._insertable_metadata(text, table_columns)
                row.extend(new_text_meta)
                assert len(table_columns) == len(row)
                self.buffered_inserter.insert( table_name, row )
            elif phase == '_hashes':
                # Insert Text's sentence hashes
                sent_hashes = CollectionMultiTableInserter._insertable_hashes(text, 
                                                                              layer=self.sentences_layer, 
                                                                              hash_attr=self.sentences_hash_attr)
                for [sent_id, sent_hash] in sent_hashes:
                    row = [SQL_DEFAULT, key, sent_id, sent_hash]
                    assert len(table_columns) == len(row)
                    self.buffered_inserter.insert( table_name, row )
            elif phase.startswith('_layer_'):
                # Insert Text's layer
                layer_name = phase[7:]
                cur_layer_new_name = None
                if self.layer_renaming_map is not None:
                    # Fetch the old name of the layer (before it was renamed)
                    for old_name, new_name in (self.layer_renaming_map).items():
                        if new_name == layer_name:
                            layer_name = old_name
                            cur_layer_new_name = new_name
                            break
                    # This layer was not renamed
                    if cur_layer_new_name is None:
                        cur_layer_new_name = layer_name
                assert layer_name in text.layers, \
                    f'(!) Text object is missing insertable layer {layer_name!r}. Available layers: {text.layers}'
                if self.remove_sentences_hash_attr and layer_name == self.sentences_layer:
                    # Remove hash attribute from the sentences layer
                    self.sentences_hash_remover.retag(text)
                    assert self.sentences_hash_attr not in text[layer_name]
                layer_object = text[layer_name]
                if self.layer_renaming_map is not None:
                    # Rename Layer object
                    rename_layer( layer_object, self.layer_renaming_map )
                    assert layer_object.name == cur_layer_new_name
                row = [ SQL_DEFAULT, key, layer_to_json( layer_object ) ]
                assert len(table_columns) == len(row)
                # If this the last phase of the insertion, then 
                # mark this document as completed
                doc_completed = None
                if last_phase == phase:
                    doc_completed = key
                self.buffered_inserter.insert( table_name, row, doc_completed=doc_completed )
            else:
                raise NotImplementedError(f'(!) Unimplemented phase: {phase!r}')
        # Mark document insertion completed
        self.text_insert_counter += 1


    @staticmethod 
    def _insertable_text_object( text, add_src=True ):
        # Make new insertable Text that does not have any metadata
        assert isinstance(text, Text)
        new_text = Text(text.text)
        if add_src:
            new_text.meta['src'] = normalize_src((text.meta).get('src', None))
        return new_text, new_text.meta if add_src else new_text

    @staticmethod
    def _insertable_metadata( text, metadata_columns, initial_id='initial_id' ):
        # Extract metadata of the document
        text_meta = []
        for mid, meta_key in enumerate(metadata_columns):
            if meta_key in ['id', 'text_id']:
                continue
            elif meta_key == initial_id:
                text_meta.append( (text.meta).get("id", SQL_DEFAULT) )
            elif meta_key == '_vert_file':
                text_meta.append( (text.meta).get("_doc_vert_file", SQL_DEFAULT) )
            elif meta_key == '_vert_doc_id':
                text_meta.append( (text.meta).get("_doc_id", SQL_DEFAULT) )
            elif meta_key == '_vert_doc_start_line':
                text_meta.append( (text.meta).get("_doc_start_line", SQL_DEFAULT) )
            elif meta_key == '_vert_doc_end_line':
                text_meta.append( (text.meta).get("_doc_end_line", SQL_DEFAULT) )
            else:
                text_meta.append( (text.meta).get(meta_key, SQL_DEFAULT) )
        return text_meta

    @staticmethod
    def _insertable_hashes( text, layer='sentences', hash_attr='sha256' ):
        # Extract sentence hashes of the document
        assert layer in text.layers
        assert hash_attr in text[layer].attributes, \
            f'(!) Text layer\'s {layer!r} is missing hash attribute {hash_attr!r}'
        sentence_hashes = []
        for sent_id, sentence in enumerate( text[layer] ):
            sent_hash = sentence.annotations[0][hash_attr]
            assert isinstance(sent_hash, str)
            sentence_hashes.append( [sent_id, sent_hash] )
        return sentence_hashes


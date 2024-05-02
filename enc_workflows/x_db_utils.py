#
#   Various helpful utilities for EstNLTK's Postgres database interface.
#

from typing import List, Union

import re, sys
import os, os.path
import time

import psycopg2
from psycopg2.extensions import STATUS_BEGIN
from psycopg2.sql import SQL, Identifier, Literal, DEFAULT

from estnltk import logger
from estnltk.converters import json_to_text
from estnltk.storage import postgres as pg
from estnltk.storage.postgres import structure_table_identifier
from estnltk.storage.postgres import layer_table_exists
from estnltk.storage.postgres import layer_table_name

from x_utils import MetaFieldsCollector
from x_utils import load_collection_layer_templates


def create_collection_layer_tables( configuration: dict, collection: 'pg.PgCollection', layer_type: str = 'detached' ):
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
    
    # TODO: normalize templates:
    # * remove 'sha256' from 'sentences' ?
    # * add prefixes to layer names, e.g. 'v173_' ?
    
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


def metadata_table_name(collection_name):
    return collection_name + '__metadata'


def metadata_table_identifier(storage, collection_name):
    table_name = metadata_table_name(collection_name)
    return pg.table_identifier(storage, table_name)


def metadata_table_exists( collection: 'pg.PgCollection' ):
    metadata_table = metadata_table_name(collection.name)
    return pg.table_exists(collection.storage, metadata_table, omit_commit=True, omit_rollback=True)


def create_collection_metadata_table( configuration: dict, collection: 'pg.PgCollection', description:str = None ):
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
    # Load collection's metadata fields
    meta_fields = MetaFieldsCollector.load_meta_fields( metadata_file )
    # Rename 'id' -> 'vert_id'
    new_meta_fields = []
    for field in meta_fields:
        if field == 'id':
            new_meta_fields.append('vert_id')
        else:
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
        # All meta fields are string fields
        columns.append( SQL(f'{field} TEXT') )
    conn = collection.storage.conn
    with conn.cursor() as cur:
        try:
            # Create table
            cur.execute(SQL("CREATE TABLE {} ({});").format(table_identifier, SQL(', ').join(columns)))
            logger.debug(cur.query.decode())
            # Add table's comment
            comment = Literal('created by {} on {}'.format(collection.storage.user, time.asctime()))
            if isinstance(description, str):
                comment = Literal(description)
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
    logger.info('created collection metadata table with fields {}'.format(new_meta_fields))



def drop_collection_metadata_table( collection: 'pg.PgCollection', cascade: bool = False):
    metadata_table = metadata_table_name(collection.name)
    pg.drop_table(collection.storage, metadata_table)
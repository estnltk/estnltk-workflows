#
#   Creates table indexes for the given Postgres' collection. 
#   Indexes are necessary to speed up quering in the database. 
#
#   Requires name of a configuration INI file as an input argument. 
#   
#   Requires EstNLTK version 1.7.5+.
#

import json
import time
import re, sys
import os, os.path

from datetime import datetime

import warnings

from packaging.version import Version as pkg_Version
from packaging.version import parse as parse_version

from estnltk.storage import postgres as pg
from estnltk.storage.postgres import layer_table_name
from estnltk import __version__ as estnltk_version

from x_configparser import parse_configuration
from x_configparser import validate_database_access_parameters
from x_db_utils import metadata_table_name
from x_db_utils import metadata_table_identifier
from x_db_utils import sentence_hash_table_name
from x_db_utils import sentence_hash_table_identifier
from x_db_utils import sentence_hash_table_exists
from x_db_utils import get_collection_indexes
from x_db_utils import drop_layer_index
from x_db_utils import create_text_id_index
from x_logging import get_logger_with_tqdm_handler

if parse_version( estnltk_version ) < pkg_Version('1.7.5'):
    print(f'(!) Estnltk version 1.7.5+ is required for running this code. Please update the package.')
    sys.exit()

from estnltk.storage.postgres import get_index_name_hash
from estnltk.storage.postgres import index_exists

logger = get_logger_with_tqdm_handler()

# Overwrite existing indexes
overwrite_existing = False

if len(sys.argv) > 1:
    input_fname = sys.argv[1]
    for s_arg in sys.argv[1:]:
        if s_arg.lower() in ['-r', '--overwrite']:
            overwrite_existing = True
    if os.path.isfile(input_fname):
        # Get & validate configuration parameters
        configuration = None
        if (input_fname.lower()).endswith('.ini'):
            configuration = parse_configuration( input_fname, load_db_conf=True, ignore_missing_vert_file=True )
        else:
            raise Exception('(!) Input file {!r} with unexpected extension, expected a configuration INI file.'.format(input_fname))
        if configuration is not None:
            # Get collection's parameters
            logger.setLevel( configuration['db_insertion_log_level'] )
            collection_name = configuration.get('db_collection_name', None)
            if collection_name is None:
                collection_name = configuration['collection']
            else:
                logger.info( f'Local collection name: {configuration["collection"]!r} | Database collection name: {collection_name!r}' )
            validate_database_access_parameters( configuration )
            #print( configuration )
            # Connect to the storage
            storage = pg.PostgresStorage(host=configuration.get('db_host', None),
                                         port=configuration.get('db_port', None),
                                         dbname=configuration.get('db_name', None),
                                         user=configuration.get('db_username', None),
                                         password=configuration.get('db_password', None),
                                         pgpass_file=configuration.get('db_pgpass_file', None),
                                         schema=configuration.get('db_schema', None),
                                         role=configuration.get('db_role', None),
                                         create_schema_if_missing=configuration.get('create_schema_if_missing', False))
            # Check for the existence of the collection
            if collection_name in storage.collections:
                collection = storage[collection_name]
                if collection._is_empty:
                    print(f'(!) Collection {collection_name!r} is empty. Please add data before creating layer indexes. ')
                else:
                    total_start_time = datetime.now()
                    collection.refresh()
                    indexes_created = {}
                    # Retrieve detached layers
                    detached_layers = []
                    is_relation_layer = {}
                    existing_filled_layers = collection.layers or []
                    if len(existing_filled_layers) > 0:
                        for layer in collection.layers:
                            layer_type = collection.structure[layer]['layer_type']
                            if layer_type == 'detached':
                                detached_layers.append( layer )
                                is_relation_layer[layer] = \
                                    collection.structure[layer].get('relation_layer', False)
                    if detached_layers:
                        logger.info( f'Collection {collection_name!r} has detached layers: {detached_layers!r}. ')
                    #
                    # Retrieve existing indexes
                    #
                    all_existing_indexes = get_collection_indexes( collection )
                    detached_layer_indexes = {}
                    metadata_textid_index = None
                    sentences_hash_textid_index = None
                    # Detect existing text_id, spans and relations indexes
                    for (tablename, indexname, indexdef) in all_existing_indexes:
                        tablename_parts = tablename.split('__')
                        # Metadata indexes
                        if len(tablename_parts) > 1 and tablename_parts[-1]  == 'metadata' and \
                           indexdef.endswith('(text_id)'):
                            metadata_textid_index = indexname
                        # Sentences hash indexes
                        if len(tablename_parts) > 2 and tablename_parts[-1]  == 'hash' and \
                           tablename_parts[1] == 'sentences' and indexdef.endswith('(text_id)'):
                            sentences_hash_textid_index = indexname
                        # Layer indexes
                        if len(tablename_parts) > 2 and tablename_parts[-1] == 'layer':
                            layer = tablename_parts[1]
                            detached_layer_indexes.setdefault(layer, {'text_id':None, 
                                                                      'spans':None,
                                                                      'relations':None})
                            if indexdef.endswith('(text_id)'):
                                # Found an index over text_id
                                detached_layer_indexes[layer]['text_id'] = indexname
                            elif " gin " in indexdef.lower() and \
                                 ("data->'spans'" in indexdef.lower() or "data -> 'spans'" in indexdef.lower()) and \
                                 'jsonb_path_ops' in indexdef.lower():
                                detached_layer_indexes[layer]['spans'] = indexname
                            elif " gin " in indexdef.lower() and \
                                 ("data->'relations'" in indexdef.lower() or "data -> 'relations'" in indexdef.lower()) and \
                                 'jsonb_path_ops' in indexdef.lower():
                                detached_layer_indexes[layer]['relations'] = indexname
                    #
                    # Create missing text_id indexes
                    #
                    if metadata_textid_index is None:
                        metadata_table = metadata_table_name(collection.name)
                        metadata_table_id = metadata_table_identifier(collection.storage, collection.name)
                        logger.info(f'Creating text_id index for {metadata_table!r} table ...')
                        create_text_id_index( collection, \
                                              get_index_name_hash('%s__text_id' % metadata_table), \
                                              metadata_table_id )
                        indexes_created.setdefault('text_id', 0)
                        indexes_created['text_id'] += 1
                    if sentences_hash_textid_index is None:
                        sentence_hash_table = sentence_hash_table_name(collection.name, layer_name='sentences' )
                        if sentence_hash_table_exists( collection, layer_name='sentences' ):
                            sentence_hash_table_id = \
                                sentence_hash_table_identifier(collection.storage, collection.name, layer_name='sentences')
                            logger.info(f'Creating text_id index for {sentence_hash_table!r} table ...')
                            create_text_id_index( collection, \
                                                  get_index_name_hash('%s__text_id' % sentence_hash_table), \
                                                  sentence_hash_table_id )
                            indexes_created.setdefault('text_id', 0)
                            indexes_created['text_id'] += 1
                    for layer in detached_layer_indexes.keys():
                        if layer in detached_layers:
                            old_textid_index = detached_layer_indexes[layer]['text_id']
                            if old_textid_index is None:
                                logger.info(f'Creating text_id index for layer {layer!r} ...')
                                layer_table = layer_table_name(collection.name, layer)
                                layer_table_id = pg.table_identifier(collection.storage, layer_table)
                                create_text_id_index( collection, \
                                                      get_index_name_hash('%s__text_id' % layer_table), \
                                                      layer_table_id )
                                indexes_created.setdefault('text_id', 0)
                                indexes_created['text_id'] += 1
                    #
                    # Create layer tables spans/relations indexes
                    #
                    for layer in detached_layer_indexes.keys():
                        if layer in detached_layers:
                            if is_relation_layer[layer]:
                                # Relation layer
                                old_layer_index = detached_layer_indexes[layer]['relations']
                                if overwrite_existing and old_layer_index is not None:
                                    # Remove old index
                                    logger.info(f'Removing old index from the layer {layer!r} ...')
                                    drop_layer_index(collection, layer)
                                    old_layer_index = None
                                if old_layer_index is None:
                                    logger.info(f'Creating index for relation layer {layer!r} ...')
                                    collection.create_layer_index(layer, index_type='data')
                                    indexes_created.setdefault('relation_layer', 0)
                                    indexes_created['relation_layer'] += 1
                            else:
                                # Span layer
                                old_layer_index = detached_layer_indexes[layer]['spans']
                                if overwrite_existing and old_layer_index is not None:
                                    # Remove old index
                                    logger.info(f'Removing old index from the layer {layer!r} ...')
                                    drop_layer_index(collection, layer)
                                    old_layer_index = None
                                if old_layer_index is None:
                                    logger.info(f'Creating index for span layer {layer!r} ...')
                                    collection.create_layer_index(layer, index_type='data')
                                    indexes_created.setdefault('span_layer', 0)
                                    indexes_created['span_layer'] += 1
                    if len(indexes_created.keys()) > 0:
                        indexes_created_info_str = \
                            ', '.join([f'{v} {k}' for k,v in indexes_created.items()])
                        logger.info('')
                        logger.info(f'  Created {indexes_created_info_str} indexes.')
                        logger.info(f'  Total time elapsed:  {datetime.now()-total_start_time}')
                        logger.info('')
                    else:
                        logger.info(f'Did not find any tables with missing indexes. '+\
                                     'Use the flag -r to overwrite existing indexes with new ones. ')
            else:
                print(f'(!) Cannot update collection {collection_name!r}: collection does not exist!')
            # Close connection
            storage.close()
        else:
            print(f'Missing or bad configuration in {input_fname!r}. Unable to get configuration parameters.')
    else:
        print(f'(!) {input_fname!r} is not an existing file. Config INI file name required as the first input argument.')
else:
    print('Config INI file name required as an input argument.')


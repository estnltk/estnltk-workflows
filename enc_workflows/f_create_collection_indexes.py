#
#   Creates layer table indexes for the given Postgres' collection. 
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

from estnltk.storage import postgres as pg

from x_configparser import parse_configuration
from x_configparser import validate_database_access_parameters
from x_db_utils import get_collection_indexes
from x_db_utils import drop_layer_index
from x_logging import get_logger_with_tqdm_handler

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
                    # Check whether the method create_layer_index is available (requires v1.7.5)
                    if hasattr(collection, "create_layer_index") and callable(getattr(collection, "create_layer_index")):
                        total_start_time = datetime.now()
                        collection.refresh()
                        existing_filled_layers = collection.layers or []
                        if len(existing_filled_layers) > 0:
                            # Retrieve detached layers
                            detached_layers = []
                            is_relation_layer = {}
                            for layer in collection.layers:
                                layer_type = collection.structure[layer]['layer_type']
                                if layer_type == 'detached':
                                    detached_layers.append( layer )
                                    is_relation_layer[layer] = \
                                        collection.structure[layer].get('relation_layer', False)
                            if detached_layers:
                                logger.info( f'Collection {collection_name!r} has detached layers: {detached_layers!r}. ')
                            # Retrieve existing indexes
                            all_existing_indexes = get_collection_indexes( collection )
                            detached_layer_indexes = {}
                            # Detect existing text_id, spans and relations indexes
                            for (tablename, indexname, indexdef) in all_existing_indexes:
                                tablename_parts = tablename.split('__')
                                if len(tablename_parts) > 1 and tablename_parts[1] in detached_layers and \
                                   tablename_parts[2] == 'layer':
                                    layer = tablename_parts[1]
                                    detached_layer_indexes.setdefault(layer, {'text_id':None, 
                                                                              'spans':None,
                                                                              'relations':None})
                                    if indexname.endswith('__text_id') or indexdef.endswith('(text_id)'):
                                        # Found an index over text_id
                                        detached_layer_indexes[layer]['text_id'] = indexname
                                    elif indexname.endswith('__layer_spans') or \
                                         "gin ((data->'spans'::text) jsonb_path_ops)" in indexdef.lower():
                                        detached_layer_indexes[layer]['spans'] = indexname
                                    elif indexname.endswith('__layer_relations') or \
                                         "gin ((data->'relations'::text) jsonb_path_ops)" in indexdef.lower():
                                        detached_layer_indexes[layer]['relations'] = indexname
                            # Create missing indexes
                            indexes_created_for = []
                            for layer in detached_layer_indexes.keys():
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
                                        indexes_created_for.append(layer)
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
                                        indexes_created_for.append(layer)
                            if indexes_created_for:
                                logger.info('')
                                logger.info(f'  Created indexes {len(indexes_created_for)!r} layers.')
                                logger.info(f'  Total time elapsed:  {datetime.now()-total_start_time}')
                                logger.info('')
                            else:
                                logger.info(f'Did not find any layer tables with missing indexes. '+\
                                             'Use the flag -r to overwrite existing indexes with new ones. ')
                        else:
                            print(f'(!) Cannot update collection {collection_name!r}: no existing layers!')
                    else:
                        print('(!) EstNLTK v1.7.5+ is required for indexing layers of the collection.')
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


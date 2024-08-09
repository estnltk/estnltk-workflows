#
#   Creates Postgres database tables for the given collection.
#   
#   Requires name of a configuration INI file as an input argument. 
#   
#   Requires EstNLTK version 1.7.3+.
#

import json
import time
import re, sys
import os, os.path

from datetime import datetime

import warnings

from estnltk import logger
from estnltk.storage import postgres as pg

from x_configparser import parse_configuration
from x_configparser import validate_database_access_parameters
from x_db_utils import create_collection_layer_tables
from x_db_utils import create_collection_metadata_table
from x_db_utils import metadata_table_exists
from x_db_utils import drop_collection_metadata_table
from x_db_utils import sentence_hash_table_exists
from x_db_utils import create_sentence_hash_table
from x_db_utils import drop_sentence_hash_table
from x_db_utils import retrieve_collection_hash_table_names

# Overwrite existing collection
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
            collection_name = configuration['collection']
            collection_description = configuration.get('collection_description', None)
            logger.setLevel( configuration['db_insertion_log_level'] )
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
                if not overwrite_existing:
                    storage_exists_error_msg = \
                        f'(!) Collection {collection_name!r} already exists in the database. '+\
                        f'Use flag -r to remove the existing collection and start from the scratch.'
                    logger.error( storage_exists_error_msg )
                    raise Exception(storage_exists_error_msg)
                else:
                    logger.info( f'Removing existing collection {collection_name!r}.' )
                    collection = storage[collection_name]
                    if metadata_table_exists(collection):
                        drop_collection_metadata_table(collection)
                    # Retrieve hash table layer names
                    hash_table_layer_names = \
                        retrieve_collection_hash_table_names(collection, return_layer_names=True)
                    for layer in hash_table_layer_names:
                        if sentence_hash_table_exists(collection, layer_name=layer):
                            drop_sentence_hash_table(collection, layer_name=layer)
                    storage.delete_collection(collection_name)
            
            # Add new collection
            meta = {'src': 'str'} if configuration['add_src_as_meta'] else None
            if collection_description is None:
                collection_description = 'created by {} on {}'.format(storage.user, time.asctime())
            if configuration['add_estnltk_version_to_description']:
                from estnltk import __version__ as estnltk_version
                collection_description = f'{collection_description} (estnltk v{estnltk_version})'
            storage.add_collection( collection_name, description=collection_description, meta=meta )

            # Create collection layer tables
            collection = storage[collection_name]
            remove_sentences_hash_attr = configuration['remove_sentences_hash_attr']
            create_collection_layer_tables(configuration, collection, 
                                           remove_sentences_hash_attr=remove_sentences_hash_attr, 
                                           sentences_layer='sentences', 
                                           sentences_hash_attr='sha256' )
            
            # Create collection's sentences hash table
            create_sentence_hash_table(configuration, collection)
            
            # Create collection metadata table
            create_collection_metadata_table(configuration, collection)
            
            # Close connection
            storage.close()
        else:
            print(f'Missing or bad configuration in {input_fname!r}. Unable to get configuration parameters.')
    else:
        print(f'(!) {input_fname!r} is not an existing file. Config INI file name required as the first input argument.')
else:
    print('Config INI file name required as an input argument.')


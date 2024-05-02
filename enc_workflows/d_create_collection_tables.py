#
#   Creates Postgres database tables for given collection.
#   ( work in progress )
#   
#   Requires name of a configuration INI file as an input argument. 
#   
#   Requires EstNLTK version 1.7.3+.
#

import json
import re, sys
import os, os.path

from datetime import datetime

import warnings

from estnltk import logger
from estnltk.storage import postgres as pg

from x_configparser import parse_configuration

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
            configuration = parse_configuration( input_fname, load_db_conf=True )
        else:
            raise Exception('(!) Input file {!r} with unexpected extension, expected a configuration INI file.'.format(input_fname))
        if configuration is not None:
            # Get collection's parameters
            collection_name = configuration['collection']
            collection_description = configuration.get('collection_description', None)
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
                        f'Use flag -w to remove the existing collection and start from the scratch.'
                    logger.error( storage_exists_error_msg )
                    raise Exception(storage_exists_error_msg)
                else:
                    logger.info( f'Removing existing collection {collection_name!r}.' )
                    storage.delete_collection(collection_name)
                    # TODO: remove other collection tables
            
            # Add new collection
            meta = {'src': 'str'} if configuration['src_as_meta'] else None
            storage.add_collection( collection_name, description=collection_description, meta=meta )
            
            # TODO: create collection tables
            
            # Close connection
            storage.close()
        else:
            print(f'Missing or bad configuration in {input_fname!r}. Unable to get configuration parameters.')
else:
    print('Config INI name required as an input argument.')

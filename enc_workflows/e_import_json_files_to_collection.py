#
#   Reads annotated documents from EstNLTK .json files and writes into Postgres collection.
#   Assumes collection tables have already been created with script "d_create_collection_tables.py".
#   
#   Requires name of a configuration INI file as an input argument. 
#

import json
import re, sys
import os, os.path

from datetime import datetime

import warnings

from tqdm import tqdm

from estnltk import logger
from estnltk.converters import json_to_text
from estnltk.storage import postgres as pg

from x_utils import collect_collection_subdirs
from x_utils import find_processing_speed

from x_configparser import parse_configuration
from x_configparser import validate_database_access_parameters

from x_db_utils import CollectionMultiTableInserter

# Insert only first N documents [for debugging]
insert_only_first = 5

if len(sys.argv) > 1:
    input_fname = sys.argv[1]
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
            validate_database_access_parameters( configuration )
            remove_sentences_hash_attr = configuration['remove_sentences_hash_attr']
            # Iterate over all vert subdirs and all document subdirs within these subdirs
            vert_subdirs = collect_collection_subdirs(configuration['collection'], only_first_level=True, full_paths=False)
            if len(vert_subdirs) == 0:
                warnings.warn(f'(!) No document subdirectories found from collection dir {configuration["collection"]!r}')
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
                total_start_time = datetime.now()
                processed_docs = 0
                processed_words = 0
                processed_sentences = 0
                global_doc_id = 0
                words_layer = 'words'
                sentences_layer = 'sentences'
                with CollectionMultiTableInserter( collection,
                                                   buffer_size=10000, 
                                                   query_length_limit=5000000, 
                                                   remove_sentences_hash_attr=remove_sentences_hash_attr, 
                                                   sentences_layer=sentences_layer, 
                                                   sentences_hash_attr='sha256' ) as text_inserter:
                    for vert_subdir in vert_subdirs:
                        # Start processing one vert_file / vert_subdir
                        subdir_start_time = datetime.now()
                        print(f'Importing files from {vert_subdir} ...')
                        full_subdir = os.path.join(configuration['collection'], vert_subdir)
                        # Find vert file corresponding to the vert_subdir
                        vert_file = [v_file for v_file in configuration['vert_files'] if vert_subdir in v_file]
                        if len(vert_file) == 0: # sanity check
                            raise ValueError(f'(!) Unable to find .vert file corresponding to vert_subdir {vert_subdir!r}. '+\
                                             f'Configuration lists only following vert files: {configuration["vert_files"]!r}.')
                        else:
                            vert_file = vert_file[0]
                            # Just in case: remove directory name from vert file
                            _, vert_file = os.path.split(vert_file)
                        # Fetch all the document subdirs
                        document_subdirs = collect_collection_subdirs(full_subdir, only_first_level=False, full_paths=True)
                        for doc_subdir in tqdm( document_subdirs, ascii=True ):
                            document_id = int( doc_subdir.split(os.path.sep)[-1] )
                            # Collect document json files
                            found_doc_files = []
                            for fname in os.listdir(doc_subdir):
                                if fname.startswith('doc') and fname.endswith('.json'):
                                    found_doc_files.append( fname )
                            if len( found_doc_files ) == 0:
                                warnings.warn( f'(!) No document json files found from {doc_subdir!r}' )
                            else:
                                for fname in found_doc_files:
                                    fpath = os.path.join(doc_subdir, fname)
                                    text_obj = json_to_text(file = fpath)
                                    assert "_doc_vert_file" not in text_obj.meta.keys()
                                    text_obj.meta["_doc_vert_file"] = vert_file
                                    assert words_layer in text_obj.layers
                                    assert sentences_layer in text_obj.layers
                                    text_inserter.insert(text_obj, global_doc_id)
                                    #try:
                                    #    text_obj = json_to_text(file = fpath)
                                    #    text_inserter.insert(text_obj, global_doc_id)
                                    #except Exception as err:
                                    #    raise Exception(f'Failed at processing document {fpath!r} due to an error: ') from err
                                    processed_words += len(text_obj[words_layer])
                                    processed_sentences += len(text_obj[sentences_layer])
                                processed_docs += 1
                            if insert_only_first > 0 and insert_only_first <= processed_docs:
                                break
                            global_doc_id += 1
                        print(f'Processing {vert_subdir} took {datetime.now()-subdir_start_time}.')
                # Complete the whole collection
                if processed_docs > 0:
                    print()
                    print(f' =={collection_name}==')
                    print(f' Inserted documents:  {processed_docs}')
                    print(f'          sentences:  {processed_sentences}')
                    print(f'              words:  {processed_words}')
                    print()
                    print(f'  Total time elapsed:  {datetime.now()-total_start_time}')
                    if processed_words > 0:
                        speed_str = find_processing_speed(datetime.now()-total_start_time, processed_words)
                        print(f'    Processing speed:  ~{speed_str} words/sec')
                else:
                    warnings.warn(f'(!) No document JSON files found from subdirectories of the collection dir {configuration["collection"]!r}')
            else:
                warnings.warn(f'(!) Collection {configuration["collection"]!r} does not exist in the Postgres storage. '+\
                              'Please use script "d_create_collection_tables.py" to create tables of the collection.')
            # Close db connection
            storage.close()

        else:
            print(f'Missing or bad configuration in {input_fname!r}. Unable to get configuration parameters.')
else:
    print('Config INI name required as an input argument.')


#
#   Reads annotated documents from EstNLTK .json files and writes into Postgres collection.
#   Assumes collection tables have already been created with script "d_create_collection_tables.py".
#   
#   Requires name of a configuration INI file as an input argument. 
#
#   In order to update an existing collection, pass the flag -u on launching the script. 
#   Then the configuration must have at least one update section, starting with prefix 
#   'database_update', and defining a parameter "add_layers" (a list with new layers to 
#   be added to the collection). If there are multiple update sections, then the last 
#   update section will be used. 
#
#   This script supports data parallelization: you can launch multiple instances 
#   of the script and give each instance a (non-overlapping) sub set of data for 
#   processing. Use command line parameters `divisor,remainder` to process only 
#   texts for which holds `text_id % divisor == remainder`. 
#   For instance: 
#
#      >> python e_import_json_files_to_collection.py  confs\balanced_and_reference_corpus.ini   2,0
#      ... processes texts 0, 2, 4, 6, 8, ...
#
#      >> python e_import_json_files_to_collection.py  confs\balanced_and_reference_corpus.ini   2,1
#      ... processes texts 1, 3, 5, 7, 9, ...
#

import json
import re, sys
import os, os.path

from datetime import datetime

import warnings

from tqdm import tqdm

from estnltk.converters import json_to_text
from estnltk.storage import postgres as pg

from x_utils import collect_collection_subdirs
from x_utils import find_processing_speed

from x_configparser import parse_configuration
from x_configparser import validate_database_access_parameters

from x_db_utils import CollectionTextMultiTableInserter
from x_db_utils import CollectionLayerMultiTableInserter
from x_logging import get_logger_with_tqdm_handler

logger = get_logger_with_tqdm_handler()

# Insert only first N documents [for debugging]
insert_only_first = 0

# Insert only last N documents [for debugging]
insert_only_last = 0

# Update existing collection
update_existing = False

# Sanity check: check that the first insertable document
# is not in the database yet
validate_first = True


def sorted_vert_subdirs( configuration, vert_subdirs ):
    '''Sorts vert subdirs into the order in which vert files appear in the configuration file.'''
    sorted_subdirs = []
    for conf_vert_file in configuration['vert_files']:
        subdir_found = False
        for subdir in vert_subdirs:
            if subdir in conf_vert_file:
                assert subdir not in sorted_subdirs
                sorted_subdirs.append(subdir)
                subdir_found = True
                break
        if not subdir_found:
            raise Exception(f'(!) Missing vert subdir corresponding to the vert file {conf_vert_file!r} '+\
                            f'listed in the configuration. \n Available subdirs: {vert_subdirs!r}')
    return sorted_subdirs


if len(sys.argv) > 1:
    input_fname = sys.argv[1]
    focus_block = None
    for s_arg in sys.argv[1:]:
        # Get divisor & reminder for data parallelization
        m = re.match(r'(\d+)[,:;](\d+)', s_arg)
        if m:
            divisor = int(m.group(1))
            assert divisor > 0
            remainder = int(m.group(2))
            assert remainder < divisor
            focus_block = (divisor, remainder)
            print(f'Data parallelization: focus on block {focus_block}.')
            break
        # Insert only N first documents
        if s_arg.isdigit():
            insert_only_first = int(s_arg)
        # Insert only N last documents
        elif s_arg[0]=='-' and s_arg[1:].isdigit():
            insert_only_last = int(s_arg)
            assert insert_only_last < 0
        elif s_arg.lower() in ['-u', '--update']:
            update_existing = True
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
            remove_sentences_hash_attr = configuration['remove_sentences_hash_attr']
            log_doc_completions = configuration.get('db_log_doc_completion', False)
            layer_renaming_map = configuration['layer_renaming_map']
            db_insert_buffer_size = configuration['db_insert_buffer_size']
            db_insert_query_length_limit = configuration['db_insert_query_length_limit']
            # Iterate over all vert subdirs and all document subdirs within these subdirs
            vert_subdirs = collect_collection_subdirs(configuration['collection'], only_first_level=True, full_paths=False)
            if len(vert_subdirs) == 0:
                warnings.warn(f'(!) No document subdirectories found from collection dir {configuration["collection"]!r}')
            target_layers = None
            if update_existing:
                # Get database updates
                db_updates_conf = configuration.get('db_updates', None)
                if db_updates_conf is None or len(db_updates_conf.keys()) == 0:
                    raise Exception(f'(!) Configuration file {input_fname!r} does not define any database updates.')
                # Take the last / latest update
                # TODO: make it possible to pass name of the target update as a command line parameter
                last_update = next(reversed(db_updates_conf.keys()))
                target_layers = db_updates_conf[last_update].get('add_layers', None)
                assert target_layers is not None and len(target_layers) > 0
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
                if insert_only_first > 0:
                    print(f'[Debugging] Inserting only first {insert_only_first} documents.')
                if insert_only_last < 0:
                    print(f'[Debugging] Inserting only last {insert_only_last*-1} documents.')
                collection = storage[collection_name]
                total_start_time = datetime.now()
                processed_docs = 0
                processed_words = 0
                processed_sentences = 0
                global_doc_id = 0   # keeps track of unique doc indexes over the whole collection
                words_layer = 'words'
                sentences_layer = 'sentences'
                if not update_existing:
                    # Insert new Text objects and new base layers
                    logger.info('Working in NEW mode: inserting new documents and base layers to the collection.')
                    text_inserter = CollectionTextMultiTableInserter( collection,
                                                                      buffer_size=db_insert_buffer_size, 
                                                                      query_length_limit=db_insert_query_length_limit, 
                                                                      remove_sentences_hash_attr=remove_sentences_hash_attr, 
                                                                      sentences_layer=sentences_layer, 
                                                                      sentences_hash_attr='sha256', 
                                                                      layer_renaming_map=layer_renaming_map,
                                                                      log_doc_completions=log_doc_completions )
                else:
                    # Add new layers to existing Text objects
                    logger.info(f'Working in UPDATE mode: inserting layers {target_layers} to existing documents.')
                    text_inserter = CollectionLayerMultiTableInserter( collection, 
                                                                       target_layers, 
                                                                       buffer_size=db_insert_buffer_size, 
                                                                       query_length_limit=db_insert_query_length_limit, 
                                                                       layer_renaming_map=layer_renaming_map,
                                                                       log_doc_completions=log_doc_completions )
                with text_inserter:
                    for vert_subdir in sorted_vert_subdirs( configuration, vert_subdirs ):
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
                        # Fetch all the document subdirs and sort by document id-s
                        document_subdirs = collect_collection_subdirs(full_subdir, only_first_level=False, full_paths=True, sort=True)
                        debug_insertion_goals = None
                        if insert_only_first > 0 or insert_only_last < 0:
                            # Debugging: insert only N first/last documents
                            debug_insertion_goals = dict()
                            first_to_insert = []
                            last_to_insert = []
                            if insert_only_first > 0:
                                first_to_insert = document_subdirs[:insert_only_first]
                            if insert_only_last < 0:
                                last_to_insert = document_subdirs[insert_only_last:]
                            for i in first_to_insert:
                                debug_insertion_goals[i] = 1
                            for i in last_to_insert:
                                debug_insertion_goals[i] = 1
                            assert len(debug_insertion_goals.keys()) > 0
                        subdir_id = 0
                        for doc_subdir in tqdm( document_subdirs, ascii=True ):
                            subdir_id += 1
                            if debug_insertion_goals is not None:
                                # Debugging: skip (majority of) documents
                                if doc_subdir not in debug_insertion_goals.keys():
                                    global_doc_id += 1
                                    continue
                            document_id = int( doc_subdir.split(os.path.sep)[-1] )
                            # Apply block filter
                            if focus_block is not None and document_id % focus_block[0] != focus_block[1]:
                                # Skip the document (wrong block)
                                global_doc_id += 1
                                continue
                            # Collect document json files
                            found_doc_files = []
                            for fname in os.listdir(doc_subdir):
                                if fname.startswith('doc') and fname.endswith('.json'):
                                    found_doc_files.append( fname )
                            if len( found_doc_files ) == 0:
                                warnings.warn( f'(!) No document json files found from {doc_subdir!r}' )
                            else:
                                if len( found_doc_files ) > 1:
                                    raise NotImplementedError( f'(!) Insertion of split documents not implemented. '+\
                                                               f'Unexpectedly, multiple document files encountered in {doc_subdir!r}' )
                                for fname in found_doc_files:
                                    fpath = os.path.join(doc_subdir, fname)
                                    try:
                                        text_obj = json_to_text(file = fpath)
                                        assert "_doc_vert_file" not in text_obj.meta.keys()
                                        text_obj.meta["_doc_vert_file"] = vert_file
                                        assert words_layer in text_obj.layers
                                        assert sentences_layer in text_obj.layers
                                        if validate_first:
                                            insertion_status = \
                                                text_inserter.is_inserted(global_doc_id, detailed=False)
                                            if insertion_status:
                                                error_msg = \
                                                    f'(!) Document with id={global_doc_id!r} has already been inserted into the collection.'
                                                if update_existing:
                                                    error_msg = \
                                                        f'(!) Layers {target_layers} have already been inserted for the document with id={global_doc_id!r}.'
                                                raise ValueError(error_msg)
                                            validate_first = False
                                        text_inserter.insert(text_obj, global_doc_id)
                                    except Exception as err:
                                        raise Exception(f'Failed at processing document {fpath!r} due to an error: ') from err
                                    processed_words += len(text_obj[words_layer])
                                    processed_sentences += len(text_obj[sentences_layer])
                                processed_docs += 1
                            global_doc_id += 1
                        print(f'Processing {vert_subdir} took {datetime.now()-subdir_start_time}.')
                # Complete the whole collection
                if processed_docs > 0:
                    activity = 'Inserted' if not update_existing else ' Updated'
                    print()
                    print(f' =={collection_name}==')
                    print(f' {activity} documents:  {processed_docs}')
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
        print(f'(!) {input_fname!r} is not an existing file. Config INI file name required as the first input argument.')
else:
    print('Config INI file name required as an input argument.')


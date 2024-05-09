#
#   Parses configuration INI files and extracts ENC processing configuration parameters.
# 

import re
import os, os.path
import configparser
import logging

def parse_configuration( conf_file:str, load_db_conf:bool=False ):
    '''Parses ENC processing configuration parameters from the given INI file.'''
    # Parse configuration file
    config = configparser.ConfigParser()
    if conf_file is None or not os.path.exists(conf_file):
        raise FileNotFoundError("Config file {} does not exist".format(conf_file))
    if len(config.read(conf_file)) != 1:
        raise ValueError("File {} is not accessible or is not in valid INI format".format(conf_file))
    clean_conf = {}
    collection_info_found = False
    for section in config.sections():
        if section.startswith('collection'):
            #
            # Load collection's configuration from the section
            #
            collection_info_found = True
            if not config.has_option(section, 'name'):
                raise ValueError(f'Error in {conf_file}: section {section!r} is missing "name" parameter.')
            collection_name = str(config[section]['name'])
            if not collection_name.isidentifier():
                raise ValueError(f'Error in {conf_file}: section {section!r} invalid value {collection_name!r} for parameter "name". '+
                                  'Expected a legitimate identifier.')
            if not config.has_option(section, 'vert_files'):
                raise ValueError(f'Error in {conf_file}: section {section} is missing "vert_files" parameter.')
            vert_files_raw = config[section]['vert_files']
            vert_files = [f.strip() for f in re.split('[;,]', vert_files_raw) if len(f.strip()) > 0]
            # Check for existence of the vert files
            if len(vert_files) == 0:
                raise ValueError(f'Error in {conf_file}: section {section} has empty "vert_files" parameter.')
            else:
                for vert_file in vert_files:
                    if not os.path.exists( vert_file ):
                        raise ValueError(f'Error in {conf_file}: missing file {vert_file!r} listed in "vert_files".')
            # Rename morph layer to the given name
            rename_morph_layer = config[section].get('rename_morph_layer', 'morph_analysis_ext')
            if isinstance(rename_morph_layer, str) and not rename_morph_layer.isidentifier():
                raise ValueError(f'Error in {conf_file}: section {section!r} invalid value {rename_morph_layer!r} for parameter "rename_morph_layer". '+
                                  'Expected a legitimate identifier for a layer name.')
            # Logging parameters
            log_json_conversion = config[section].getboolean('log_json_conversion', False)
            json_conversion_log_level = logging.getLevelName( config[section].get('json_conversion_log_level', 'INFO') )
            # Flag parameters
            add_sentence_hashes = config[section].getboolean('add_sentence_hashes', False)
            collect_meta_fields = config[section].getboolean('collect_meta_fields', True)
            # Debugging parameter:  focus_doc_ids
            focus_doc_ids = None
            if config.has_option(section, 'focus_doc_ids'):
                focus_doc_ids_raw = config[section]['focus_doc_ids']
                if not isinstance(focus_doc_ids_raw, str):
                    raise ValueError(f'Error in {conf_file}: section {section} parameter "focus_doc_ids" should be string.')
                focus_doc_ids = [f.strip() for f in re.split('[;,]', focus_doc_ids_raw) if len(f.strip()) > 0]
                focus_doc_ids = set(focus_doc_ids)
            clean_conf['collection'] = collection_name
            clean_conf['vert_files'] = vert_files
            clean_conf['add_sentence_hashes'] = add_sentence_hashes
            clean_conf['collect_meta_fields'] = collect_meta_fields
            clean_conf['focus_doc_ids'] = focus_doc_ids
            clean_conf['log_json_conversion'] = log_json_conversion
            clean_conf['json_conversion_log_level'] = json_conversion_log_level
            clean_conf['rename_morph_layer'] = rename_morph_layer
        if section.startswith('add_syntax_layer'):
            #
            # Load configuration for adding the syntax layer to the collection
            #
            if not config.has_option(section, 'name'):
                raise ValueError(f'Error in {conf_file}: section {section!r} is missing "name" parameter.')
            # output layer name
            syntax_layer_name = str(config[section]['name'])
            if not syntax_layer_name.isidentifier():
                raise ValueError(f'Error in {conf_file}: section {section!r} invalid value {syntax_layer_name!r} for parameter "name". '+
                                  'Expected a legitimate identifier for a layer name.')
            clean_conf['output_syntax_layer'] = syntax_layer_name
            # names of the input layer
            clean_conf['input_morph_layer'] = \
                config[section].get('input_morph_layer', clean_conf.get('rename_morph_layer', 'morph_analysis_ext'))
            clean_conf['input_words_layer'] = config[section].get('input_words_layer', 'words')
            clean_conf['input_sentences_layer'] = config[section].get('input_sentences_layer', "sentences")
            #
            # output_mode
            # NEW_FILE  -- creates a new json file by adding `output_file_infix` to the old file name;
            # OVERWRITE -- overwrites the old json file with new content;
            # Applies to both NEW_FILE and OVERWRITE:
            # if `output_remove_morph` is set, then removes the input morph layer from the output document;
            #
            clean_conf['output_mode'] = config[section].get('output_mode', 'NEW_FILE')
            clean_conf['output_file_infix'] = config[section].get('output_file_infix', '_syntax')
            clean_conf['output_remove_morph'] = config[section].getboolean('output_remove_morph', True)
            if not isinstance(clean_conf['output_mode'], str) or not clean_conf['output_mode'].upper() in ['NEW_FILE', 'OVERWRITE']:
                raise ValueError(f'Error in {conf_file}: section {section!r} invalid value {clean_conf["output_mode"]!r} for '+\
                                  'parameter "output_mode". Expected values: NEW_FILE or OVERWRITE.')
            clean_conf['output_mode'] = clean_conf['output_mode'].upper()
            if clean_conf['output_mode'] == 'NEW_FILE':
                if (not isinstance(clean_conf['output_file_infix'], str) or \
                    len(clean_conf['output_file_infix'].strip()) == 0):
                    raise ValueError(f'Error in {conf_file}: section {section!r} invalid value {clean_conf["output_file_infix"]!r} for '+\
                                      'parameter "output_file_infix". Expected a non-empty string.')
            clean_conf['output_file_infix'] = clean_conf['output_file_infix'].strip()
            # parsing parameters
            clean_conf['skip_annotated'] = config[section].getboolean('skip_annotated', True)
            clean_conf['use_gpu'] = config[section].getboolean('use_gpu', False)
            clean_conf['use_cpu_for_long_sentences'] = config[section].getboolean('use_cpu_for_long_sentences', False)
            clean_conf['add_layer_creation_time'] = config[section].getboolean('add_layer_creation_time', False)
        if section.startswith('write_syntax_to_vert'):
            #
            # Load configuration for writing syntactic annotations to (a new) vert file
            #
            if not config.has_option(section, 'vert_output_dir'):
                raise ValueError(f'Error in {conf_file}: section {section!r} is missing "vert_output_dir" parameter.')
            # Output directory for new vert files
            vert_output_dir = str(config[section]['vert_output_dir'])
            if not vert_output_dir.isidentifier():
                raise ValueError(f'Error in {conf_file}: section {section!r} invalid value {vert_output_dir!r} for parameter "vert_output_dir". '+
                                  'Expected a legitimate identifier.')
            clean_conf['vert_output_dir'] = vert_output_dir
            if not config.has_option(section, 'vert_output_suffix'):
                raise ValueError(f'Error in {conf_file}: section {section!r} is missing "vert_output_suffix" parameter.')
            # Suffix to be added to the end of the new vert file name
            vert_output_suffix = str(config[section]['vert_output_suffix']).strip()
            if len(vert_output_suffix) == 0:
                raise ValueError(f'Error in {conf_file}: section {section} has empty "vert_output_suffix" parameter.')
            clean_conf['vert_output_suffix'] = vert_output_suffix
        if load_db_conf and section.startswith('database_conf'):
            #
            # Load Postgres database configuration
            #
            # Get database access parameters from separate file
            if not config.has_option(section, 'conf_file'):
                raise ValueError(f'Error in {conf_file}: section {section!r} is missing "conf_file" parameter.')
            db_conf_file = str(config[section]['conf_file']).strip()
            if len( db_conf_file ) == 0 or not os.path.isfile( db_conf_file ):
                raise FileNotFoundError(f'Error in {conf_file}: section {section!r} parameter "conf_file" '+\
                                        f'points to an invalid or missing conf file: {db_conf_file!r}')
            db_conf_dict = parse_database_access_configuration( db_conf_file )
            if db_conf_dict is None:
                raise ValueError( f'Error in {conf_file}: section {section!r}: '+\
                                  f'could not find any database access parameters from {db_conf_file}.' )
            db_keys  = set(db_conf_dict.keys())
            cur_keys = set(clean_conf.keys())
            intersecting = db_keys.intersection(cur_keys)
            if len(intersecting) > 0:
                # Sanity check: avoid overlapping parameter names
                raise ValueError(f'Error in {conf_file}: unexpectedly, database conf and main conf contain parameters '+\
                                 f'with overlapping names: {intersecting}')
            # Merge db access conf into main conf
            clean_conf.update( db_conf_dict )
            #
            # Parse parameters required for collection table creation
            #
            # Description of the collection table. 
            # If not provided, then the description will be 'created by {user} on {creation_time}'.
            clean_conf['collection_description'] = config[section].get('collection_description', None)
            #
            # Description of the collection metadata table. 
            # If not provided, then the description will be 'created by {user} on {creation_time}'.
            clean_conf['metadata_description'] = config[section].get('metadata_description', None)
            #
            # Add information about document's location in the original vert file to the metadata table. 
            # This adds columns ('_vert_file', '_vert_doc_id', '_vert_doc_start_line', '_vert_doc_end_line') to 
            # the collection metadata table.
            clean_conf['add_vert_indexing_info'] = config[section].getboolean('add_vert_indexing_info', True)
            #
            # Remove 'initial_id' from the metadata table. The removal is justified in collections 
            # where there was no 'id' in the original .vert files and 'id' was generated by EstNLTK 
            clean_conf['remove_initial_id'] = config[section].getboolean('remove_initial_id', False)
            #
            # Remove sentence_hash attribute from sentence Layer objects. Note that even after the removal, 
            # sentence hash information is still stored in the sentences hash table, only sentence Layer 
            # objects do not have their hash fingerprints explicitly marked.
            clean_conf['remove_sentences_hash_attr'] = config[section].getboolean('remove_sentences_hash_attr', True)
            #
            # Add src as a meta field of the collection base table 
            clean_conf['add_src_as_meta'] = config[section].getboolean('add_src_as_meta', True)
    if 'collection' in clean_conf.keys():
        # Return collected configuration
        return clean_conf
    if not collection_info_found:
        print(f'No section starting with "collection" in {conf_file}. Unable to collect collection information.')
    return None



def parse_database_access_configuration( conf_file:str ):
    '''Parses Postgres' database access configuration parameters from the given INI file.'''
    # Parse configuration file
    config = configparser.ConfigParser()
    if conf_file is None or not os.path.exists(conf_file):
        raise FileNotFoundError("Config file {} does not exist".format(conf_file))
    if len(config.read(conf_file)) != 1:
        raise ValueError("File {} is not accessible or is not in valid INI format".format(conf_file))
    db_conf = {}
    for section in config.sections():
        if section.startswith('database_access'):
            #
            # Parse Postgres database access configuration
            #
            # A) user provided only pgpass_file (that contains host:port:dbname:user:password ), schema and role;
            #
            pgpass_file = None
            if config.has_option(section, 'pgpass_file'):
                pgpass_file = str(config[section]['pgpass_file']).strip()
                if len( pgpass_file ) == 0 or os.path.isfile( pgpass_file ):
                    raise FileNotFoundError(f'Error in {conf_file}: section {section!r} parameter "pgpass_file" '+\
                                            f'points to an invalid or missing file: {pgpass_file!r}')
            db_conf['db_pgpass_file'] = pgpass_file
            schema = None
            if config.has_option(section, 'schema'):
                schema_raw = str(config[section]['schema']).strip()
                if len(schema_raw) > 0:  # only take non-empty string
                    schema = schema_raw
            db_conf['db_schema'] = schema
            role = None
            if config.has_option(section, 'role'):
                role_raw = str(config[section]['role']).strip()
                if len(role_raw) > 0:  # only take non-empty string
                    role = role_raw
            db_conf['db_role'] = role
            #
            # B) user provided host, port, database, username, password, schema and role explicitly;
            #
            host = None
            if config.has_option(section, 'host'):
                str_raw = str(config[section]['host']).strip()
                if len(str_raw) > 0:  # only take non-empty string
                    host = str_raw
            db_conf['db_host'] = host
            port = None
            if config.has_option(section, 'port'):
                str_raw = str(config[section]['port']).strip()
                if len(str_raw) > 0:  # only take non-empty string
                    try:
                        port = int(str_raw)
                    except ValueError:
                        port = str_raw
            db_conf['db_port'] = port
            database = None
            if config.has_option(section, 'database'):
                str_raw = str(config[section]['database']).strip()
                if len(str_raw) > 0:  # only take non-empty string
                    database = str_raw
            db_conf['db_name'] = database
            username = None
            if config.has_option(section, 'username'):
                str_raw = str(config[section]['username']).strip()
                if len(str_raw) > 0:  # only take non-empty string
                    username = str_raw
            db_conf['db_username'] = username
            password = None
            if config.has_option(section, 'password'):
                password = config[section]['password']
            db_conf['db_password'] = password
            #
            #  Other optional parameters
            #
            db_conf['create_schema_if_missing'] = config[section].getboolean('create_schema_if_missing', False)
            return db_conf
    return None


def validate_database_access_parameters( configuration:dict ):
    '''
    Validates that the given configuration contains a complete database access configuration.
    If the configuration appears incomplete, throws an informing exception.
    '''
    host       = configuration.get('db_host', None)
    port       = configuration.get('db_port', None)
    dbname     = configuration.get('db_name', None)
    user       = configuration.get('db_username', None)
    password   = configuration.get('db_password', None)
    pgpass_file= configuration.get('db_pgpass_file', None)
    schema     = configuration.get('db_schema', None)
    role       = configuration.get('db_role', None)
    first_unsatisfied = pgpass_file is None or schema is None
    second_unsatisfied = host is None or port is None or \
                         dbname is None or user is None or \
                         schema is None
    if first_unsatisfied and second_unsatisfied:
        raise Exception('(!) Incomplete database access configuration. '+\
                        'Database access configuration must provide either '+\
                        '1) pgpass_file (that contains host:port:dbname:user:password ), '+\
                        'schema and role; or 2) explicit values for host, port, database, '+\
                        'username, password, schema and role;')

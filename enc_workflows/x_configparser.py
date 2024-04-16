#
#   Parses configuration INI files and extracts ENC processing configuration parameters.
# 

import re
import os, os.path
import configparser
import logging

def parse_configuration( conf_file:str ):
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
            collection_info_found = True
            # Load collection's configuration from the section
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
        if section.startswith('syntax_layer'):
            # Load collection's syntax annotation configuration from the section
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
            # parsing parameters
            clean_conf['use_gpu'] = config[section].getboolean('use_gpu', False)
            clean_conf['add_layer_creation_time'] = config[section].getboolean('add_layer_creation_time', False)
    if 'collection' in clean_conf.keys():
        # Return collected configuration
        return clean_conf
    if not collection_info_found:
        print(f'No section starting with "collection" in {conf_file}. Unable to collect collection information.')
    return None
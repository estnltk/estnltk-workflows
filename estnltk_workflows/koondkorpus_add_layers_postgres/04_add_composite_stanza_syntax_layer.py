#
#  Add Stanza(Ensemble)SyntaxCompositeTagger's layer to collection. 
#  Note: this tagger creates two layers: the morph_extended layer, which is an 
#  input layer required by the syntax tagger, and the stanza's syntax layer (which 
#  will be the final output layer). The morph_extended layer will be disposed after 
#  it is no longer needed. 
#
#  Command line arguments:
#  * CONFIGURATION_INI -- name of the configuration INI file, provides info about: 
#                         [database] -> (schema, role, pgpass_file(optional)), 
#                         [tagger]   -> (collection, output_layer, *_layer) 
#  * PGPASS_FILE(optional) -- pgpass file providing "host:port:dbname:user:password". 
#                             this will be overridden by pgpass_file from INI file.
#  * MODULE,REMAINDER(optional) -- processes only block/subset of documents, e.g. 
#                                  if command line argument "2,0" is provided, then 
#                                  processes only documents which text_id divided to 
#                                  2 gives module 0;
#  * "APPEND"(optional) -- to continue tagging already existing layer;
#
#

import logging
import re, sys
import os, os.path
import configparser
from datetime import datetime

from estnltk import logger
from estnltk.storage.postgres import PostgresStorage

from composite_stanza_syntax_taggers import StanzaSyntaxCompositeTagger
from composite_stanza_syntax_taggers import StanzaEnsembleSyntaxCompositeTagger
from composite_stanza_syntax_taggers import StanzaPOSbasedSyntaxCompositeTagger


def init_logger( level, block ):
    logger.setLevel( level )
    if block is None:
        logfile = sys.argv[0]+'.log'
    else:
        logfile = sys.argv[0]+'__block'+str(block[0])+'_'+str(block[1])+'.log'
    f_handler = logging.FileHandler( logfile, mode='w', encoding='utf-8' )
    f_handler.setLevel( level )
    f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    return logger


if __name__ == '__main__':
    # =============================================
    #   Parse configuration
    # =============================================
    # Get settings/configuration files from command line
    pgpass_file = None
    conf_file   = None
    block = None
    mode  = None
    for item in sys.argv:
        # Get name of the pgpass.txt file
        if (item.lower()).endswith('.txt') and \
           (os.path.split(item)[1]).startswith('pg'):
            # provides info:  host:port:dbname:user:password
            pgpass_file = item
            if not os.path.exists(pgpass_file):
                raise FileNotFoundError('Non-existent pgpass file: {}'.format(pgpass_file))
        # Get name of the configuration.ini file
        elif (item.lower()).endswith('.ini'):
            # provides info:  database: (schema, role, pgpass_file(optional)),  
            #                 tagger: (collection, output_layer, *_layer)
            conf_file = item
            if not os.path.exists(conf_file):
                raise FileNotFoundError('Non-existent conf file: {}'.format(conf_file))
        # Get multi-blocked processing instructions
        m = re.match('(\d+)[,:;](\d+)', item)
        if m and block is None:
            first = int(m.group(1))
            assert first > 0
            second = int(m.group(2))
            assert second < first
            block = (first, second)
        # Get appending instruction
        m_append = re.match('append', item, re.IGNORECASE)
        if m_append:
            mode = 'append'
    # Check if required arguments have been provided
    if conf_file is None:
        raise ValueError('Missing command line argument: name of the configuration file, e.g. "configuration.ini"')
    # Parse configuration
    config = configparser.ConfigParser()
    config_file_full = os.path.abspath(os.path.expanduser(os.path.expandvars(str(conf_file))))
    if len(config.read(config_file_full)) != 1:
        raise ValueError("File {file} is not accessible or is not in valid INI format".format(file=config_file_full))
    # Check if the database configuration is provided
    section_name = 'database'
    if not config.has_section(section_name):
        prelude = "Error in file {}\n".format(config_file_full) if len(config_file_full) > 0 else ""
        raise ValueError("{prelude}Missing a section [{section}]".format(prelude=prelude, section=section_name))
    for option in ["schema", "role"]:
        if not config.has_option(section_name, option):
            prelude = "Error in file {}\n".format(config_file_full) if len(config_file_full) > 0 else ""
            raise ValueError(
                "{prelude}Missing option {option} in the section [{section}]".format(
                    prelude=prelude, option=option, section=section_name
                )
            )
    # Get extra configuration
    pgpass_extra = config[section_name].get('pgpass_file', None)
    if pgpass_extra is not None:
        if not os.path.exists(pgpass_extra):
            raise FileNotFoundError('Non-existent pgpass file: {}'.format(pgpass_extra))
        else:
            # Override pgpass file with the file from configuration
            pgpass_file = pgpass_extra
    if pgpass_file is None:
        raise ValueError('Missing parameter: name of the pgpass file, e.g. "pgpass.txt"')
    # Check if the tagger configuration is provided
    section_name = 'tagger'
    if not config.has_section(section_name):
        prelude = "Error in file {}\n".format(config_file_full) if len(config_file_full) > 0 else ""
        raise ValueError("{prelude}Missing a section [{section}]".format(prelude=prelude, section=section_name))
    for option in ["collection", "output_layer", 'tagger_class']:
        if not config.has_option(section_name, option):
            prelude = "Error in file {}\n".format(config_file_full) if len(config_file_full) > 0 else ""
            raise ValueError(
                "{prelude}Missing option {option} in the section [{section}]".format(
                    prelude=prelude, option=option, section=section_name
                )
            )
    # Get tagger class
    composite_classes = ['StanzaSyntaxCompositeTagger', \
                         'StanzaEnsembleSyntaxCompositeTagger', \
                         'StanzaPOSbasedSyntaxCompositeTagger']
    tagger_class_name = config[section_name].get('tagger_class')
    if tagger_class_name not in composite_classes:
        raise ValueError(f'Invalid value tagger_class={tagger_class_name}. Expected a value from: {composite_classes!r}')
    # Get model location
    resources_path = config[section_name].get('resources_path', None)
    depparse_path = config[section_name].get('depparse_path', None)
    model_paths = config[section_name].get('model_paths', None)
    if tagger_class_name == 'StanzaSyntaxCompositeTagger':
        if resources_path is None and depparse_path is None:
            raise ValueError('Invalid model configuration: either resources_path or depparse_path must be defined.')
    else:
        if model_paths is None:
            raise ValueError('Invalid model configuration: either model_paths must be defined.')
        model_paths = [fpath for fpath in model_paths.split() if len(fpath) > 0]
    # Get extra configuration
    input_morph_analysis_layer = config[section_name].get('morph_analysis_layer', 'morph_analysis')
    input_words_layer = config[section_name].get('words_layer', 'words')
    input_sentences_layer = config[section_name].get('sentences_layer', 'sentences')
    add_parent_and_children = config[section_name].getboolean('add_parent_and_children', True)
    dry_run = config[section_name].getboolean('dry_run', False)  # Just initialize, no tagging
    use_gpu = config[section_name].getboolean('use_gpu', False)

    # =============================================
    #  Initialize tagger
    # =============================================
    if tagger_class_name == 'StanzaSyntaxCompositeTagger':
        tagger = StanzaSyntaxCompositeTagger( output_layer=config['tagger']["output_layer"], 
                                              input_morph_layer=input_morph_analysis_layer, 
                                              sentences_layer=input_sentences_layer, 
                                              words_layer=input_words_layer, 
                                              resources_path=resources_path, 
                                              depparse_path =depparse_path, 
                                              add_parent_and_children=add_parent_and_children, 
                                              use_gpu=use_gpu )
    elif tagger_class_name == 'StanzaEnsembleSyntaxCompositeTagger':
        tagger = StanzaEnsembleSyntaxCompositeTagger( output_layer=config['tagger']["output_layer"], 
                                                      input_morph_layer=input_morph_analysis_layer, 
                                                      sentences_layer=input_sentences_layer, 
                                                      words_layer=input_words_layer, 
                                                      model_paths=model_paths, 
                                                      add_parent_and_children=add_parent_and_children, 
                                                      use_gpu=use_gpu )
    elif tagger_class_name == 'StanzaPOSbasedSyntaxCompositeTagger':
        tagger = StanzaPOSbasedSyntaxCompositeTagger( output_layer=config['tagger']["output_layer"], 
                                                      input_morph_layer=input_morph_analysis_layer, 
                                                      sentences_layer=input_sentences_layer, 
                                                      words_layer=input_words_layer, 
                                                      model_paths=model_paths,
                                                      add_parent_and_children=add_parent_and_children, 
                                                      use_gpu=use_gpu )

    # =============================================
    #  Logging 
    # =============================================
    log = init_logger( logging.DEBUG, block )
    
    # =============================================
    #  Connect DB
    # =============================================
    storage = PostgresStorage(pgpass_file=pgpass_file, 
                              schema=config['database']["schema"], 
                              role=config['database']["role"])

    if storage.collections:
        log.info(' All available collections: {!r} '.format( storage.collections ) )

        target_collection = config['tagger']["collection"]
        collection = storage[target_collection]
        if collection.exists():
            log.info(' Collection {!r} exists. '.format( target_collection ))
            log.info(' Collection {!r} has layers: {!r} '.format( target_collection, 
                                                                  collection.layers ))

            # Check that input layers exist
            missing_input_layers = []
            for layer in tagger.input_layers:
                if layer not in collection.layers:
                    missing_input_layers.append(layer)
            if missing_input_layers:
                log.error(' (!) Collection {!r} misses input layers {!r} required by the tagger.'.format(target_collection, missing_input_layers))
                storage.close()
                exit(1)

            total_docs = len(collection)
            if block is not None:
                log.info(' Preparing to tag {!r} the block {!r} from the {} documents...'.format(tagger.output_layer, block, total_docs) )
            else:
                log.info(' Preparing to tag {!r} all of {} documents...'.format(tagger.output_layer, total_docs) )

            startTime = datetime.now()
            if not dry_run:
                if tagger.output_layer not in collection.layers and (block is not None or mode == 'append'):
                    log.info( f" Initializing collection's layer" )
                    collection.add_layer( tagger.get_layer_template() )
                startTime = datetime.now()
                if block is not None:
                    assert tagger.output_layer in collection.layers
                    if mode is not None and mode == 'append':
                        log.info( ' Continuing existing block {!r} ...'.format( block ) )
                    else:
                        total_items_in_block = int( total_docs / block[0] )
                        log.info( ' Tagging new block {!r} with an estimated size {} ...'.format( block, total_items_in_block ) )
                    collection.create_layer_block( tagger, block, mode=mode )
                else:
                    collection.create_layer( tagger=tagger, progressbar='ascii', mode=mode )
            else:
                log.info(' [dry_run mode]: just testing, no tagging.')

            time_diff = datetime.now() - startTime
            log.info( 'Total processing time: {}'.format(time_diff) )
            
        elif not collection.exists():
            log.error(' (!) Collection {!r} does not exist...'.format(target_collection))
            storage.close()
            exit(1)

    storage.close()

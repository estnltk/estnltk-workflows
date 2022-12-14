#
#   Runs detect_clause_errors on a large corpus.
#   Outputs detected clause errors and error statistics.
#   Optionally, saves erroneous sentences as Estnltk json objects.
#
#   Use INI file to provide detailed processing settings.
#
import os, os.path, sys
import logging
import configparser

from collections import defaultdict

from tqdm import tqdm

from estnltk_core.layer_operations import rebase
from estnltk_core.layer_operations import extract_section, extract_sections

from estnltk import Layer
from estnltk.converters import text_to_json
from estnltk.converters import json_to_text

from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import IndexQuery

# Use local code:
#from clauses_and_syntax_consistency import yield_clauses_and_syntax_words_sentence_wise
#from clauses_and_syntax_consistency import detect_clause_errors
#from clauses_and_syntax_consistency import _extract_sentences_with_clause_errors

# Use package code:
from estnltk.consistency.clauses_and_syntax_consistency import yield_clauses_and_syntax_words_sentence_wise
from estnltk.consistency.clauses_and_syntax_consistency import detect_clause_errors
from estnltk.consistency.clauses_and_syntax_consistency import _extract_sentences_with_clause_errors


def init_logger(log_file_name=None):
    '''
    Initializes stdout logger and optionally file logger.
    About flushing logger, see: https://stackoverflow.com/a/46065766 
    '''
    logging.basicConfig( level='INFO' )
    logger = logging.getLogger(__name__)
    logger.handlers.clear()
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.INFO)
    c_handler.flush = sys.stdout.flush
    c_format = logging.Formatter('%(message)s')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)
    if log_file_name is not None:
        f_handler = logging.FileHandler(log_file_name, mode='w', encoding='utf-8')
        f_handler.setLevel(logging.INFO)
        f_format = logging.Formatter('%(message)s')
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)
    return logger

def load_in_doc_ids_from_file(fnm, logger, sort=True):
    '''Loads processable document ids from a text file. 
       In the text file, each document id should be on a separate line.
       Returns a list with document ids.
    '''
    if not os.path.isfile( fnm ):
        log.error('Error at loading document index: invalid index file {!r}. '.format( fnm ))
        exit(1)
    ids = []
    with open( fnm, 'r', encoding='utf-8' ) as f:
       for line in f:
           line = line.strip()
           if len(line) == 0:
              continue
           ids.append( int(line) )
    if len(ids) == 0:
        log.error('No valid document ids were found from the index file {!r}.'.format( fnm ))
        exit(1)
    if sort:
        ids = sorted( ids )
    return ids

def _extract_non_erroneous_sents( text, erroneous_sents, 
                                        clauses_layer='clauses', 
                                        syntax_layer='syntax', 
                                        sentences_layer='sentences',
                                        logger=None,
                                        copy_metadata=True ):
    '''
    Extracts sentences with embedded clauses starting with kes/mis, but without detected clause errors.
    Returns a list of sentence Text objects.
    (for debugging only)
    '''
    assert len(erroneous_sents) > 0
    non_erroneous_sents = []
    for sent_id, sent_clauses, sent_cl_syntax_words in yield_clauses_and_syntax_words_sentence_wise( text, \
                                                                                clauses_layer=clauses_layer, \
                                                                                syntax_layer=syntax_layer, \
                                                                                sentences_layer=sentences_layer ):
        sentence = text[sentences_layer][sent_id]
        # Skip erroneous sents
        skip_sentence = False
        for err_sent in erroneous_sents:
            if err_sent.meta['_original_sentence_start'] == sentence.start and \
               err_sent.meta['_original_sentence_end'] == sentence.end:
                skip_sentence = True
                break
        if skip_sentence:
            continue
        # No error in this sentence -- see, if it is valid
        embedded = False
        embedded_mis_kes = False
        for cid, cl in enumerate(sent_clauses):
            if cl.annotations[0]['clause_type'] == 'embedded':
                embedded = True
            cl_syn_words = sent_cl_syntax_words[cid]
            if len(cl_syn_words) > 2 and cl_syn_words[1].annotations[0]['lemma'] in ['kes', 'mis']:
                if embedded:
                    embedded_mis_kes = True
        if embedded_mis_kes:
            # Extract sentence with embedded kes/mis
            extracted_text = extract_section( text = text, 
                start = sentence.start,
                end = sentence.end,
                layers_to_keep = text.layers,
                trim_overlapping=False
            )
            # Carry over metadata
            if copy_metadata:
                for key in text.meta.keys():
                    extracted_text.meta[key] = text.meta[key]
            extracted_text.meta['_original_sentence_start'] = sentence.start
            extracted_text.meta['_original_sentence_end'] = sentence.end
            if logger is not None:
                output_str = []
                output_str.append( '\n' )
                output_str.append( '='*50 )
                output_str.append( '\n' )
                for clause2, cl_syntax_words2 in zip(sent_clauses, sent_cl_syntax_words):
                    cl_word_idx = 0
                    for w in cl_syntax_words2:
                        output_str.append( f"{w.text} {w.annotations[0]['id']} {w.annotations[0]['head']} {w.annotations[0]['deprel']}" )
                        output_str.append( '\n' )
                        cl_word_idx += 1
                    output_str.append( '' )
                    output_str.append( '\n' )
                output_str.append( '' )
                output_str.append( '\n' )
                logger.info( ''.join(output_str) )
            non_erroneous_sents.append( extracted_text )
    return non_erroneous_sents


if __name__ == "__main__":
    # Get configuration from INI file
    config = configparser.ConfigParser()
    if len(sys.argv) < 2:
        raise Exception('(!) Missing input argument: name of the configuration INI file.')
    conf_file = sys.argv[1]
    if not os.path.exists(conf_file):
        raise FileNotFoundError("Config file {} does not exist".format(conf_file))
    if len(config.read(conf_file)) != 1:
        raise ValueError("File {} is not accessible or is not in valid INI format".format(conf_file))

    config_sections = {'db-connection': ['pgpass_file', 'schema', 'role'], 
                       'collection':['collection', 'clauses_layer', 'sentences_layer', 'syntax_layer'], 
                       'subset':['selected_indexes_file'], 
                       'output':['target_error_prefixes', 'log_file_name', 'errors_json_file_name'] }
    ids = []
    ids_file = ''
    target_collection = ''
    clauses_layer = ''
    sentences_layer = ''
    syntax_layer = ''
    log_file = None
    save_to_json = False
    target_error_prefixes = []
    errors_json_file_name = None
    for section in config_sections.keys():
        if not config.has_section(section) and section == 'subset':
            # Subsection 'subset' can be skipped
            continue
        if not config.has_section(section):
            raise ValueError("Error in config file {!r}: missing a section {!r}".format(conf_file, section))
        for option in config_sections[section]:
            if not config.has_option(section, option):
                if section == 'subset' and option == 'selected_indexes_file':
                    # Index selection can be skipped
                    continue
                if section == 'output' and option == 'log_file_name':
                    # Log file name can be skipped
                    continue
                if section == 'output' and option == 'target_error_prefixes':
                    # Error filtering can be skipped
                    continue
                if section == 'output' and option == 'errors_json_file_name':
                    # Saving errors as json can be skipped
                    continue
            if not config.has_option(section, option):
                raise ValueError("Error in config file {!r}: missing option {!r} in section {!r}".format(conf_file, option, section))
            if section == 'subset' and option == 'selected_indexes_file':
                ids_file = (str(config[section][option])).strip()
            if section == 'collection':
                if option == 'collection':
                    target_collection = config['collection']['collection']
                if option == 'syntax_layer':
                    syntax_layer = config['collection']['syntax_layer']
                if option == 'clauses_layer':
                    clauses_layer = config['collection']['clauses_layer']
                if option == 'sentences_layer':
                    sentences_layer = config['collection']['sentences_layer']
            if section == 'output':
                if option == 'target_error_prefixes':
                    # Comma-separated list of allowed error types. Can be empty
                    _prefixes = config['output']['target_error_prefixes']
                    for prefix in _prefixes.split(','):
                        prefix = prefix.strip()
                        if len(prefix) > 0:
                            target_error_prefixes.append(prefix)
                if option == 'log_file_name':
                    log_file = config['output']['log_file_name']
                if option == 'errors_json_file_name':
                    errors_json_file_name = str(config['output']['errors_json_file_name']).strip()
                    if len(errors_json_file_name) > 0:
                        save_to_json = True

    log = init_logger(log_file_name=log_file)
    
    storage = PostgresStorage(pgpass_file=config['db-connection']['pgpass_file'], 
                              schema=config['db-connection']['schema'], 
                              role=config['db-connection']['role'])
    if len(ids_file) > 0:
        ids = load_in_doc_ids_from_file(ids_file, log, sort=True)
    
    # For debugging only: collect sentences without clause errors
    collect_non_errors_json = False
    non_errors_outfname = f'{target_collection}_non_errors.jsonl'
    if collect_non_errors_json:
        # Clear non_errors output file
        with open(non_errors_outfname, 'w', encoding='utf-8') as out_f:
            pass
    
    if storage.collections:
        log.info(' All available collections: {!r} '.format(storage.collections) )

        if target_collection in storage.collections:
            collection = storage[target_collection]
            log.info(' Collection {!r} exists. '.format( target_collection ))
            log.info(' Collection {!r} has layers: {!r} '.format( target_collection, 
                                                                  collection.layers ))
        else:
            raise Exception( '(!) Missing collection {!r}.'.format(collection) )

        if len(ids) > 0:
            selection = collection.select( IndexQuery(keys=ids), 
                                           layers=[clauses_layer, sentences_layer, syntax_layer], 
                                           progressbar='ascii' )
        else:
            selection = collection.select( layers=[clauses_layer, sentences_layer, syntax_layer], 
                                           progressbar='ascii' )
        
        problems_found = defaultdict(int)
        collected_sentence_texts = []
        for text_id, text_obj in selection:
            #
            # Collect regular errors 
            #
            output_layer, output = detect_clause_errors( text_obj, clauses_layer=clauses_layer, 
                                                                   syntax_layer=syntax_layer,
                                                                   sentences_layer=sentences_layer,
                                                                   debug_output=True,
                                                                   status=problems_found )
            assert isinstance(output_layer, Layer)
            #
            # Check target error prefixes
            #
            has_target_error_prefixes = True
            if len(target_error_prefixes) > 0:
                has_target_error_prefixes = False
                for prefix in target_error_prefixes:
                    for span in output_layer:
                        if span['err_type'].startswith(prefix):
                            has_target_error_prefixes = True
                            break
            if has_target_error_prefixes and len(output_layer) > 0 :
                msg = f'(!) clauses_errors in text with id {text_id} ({text_obj.meta["file"]})'
                log.info( msg )
                log.info( output )
                log.info( '\n' )
                for span in output_layer:
                    log.info(f' > {span.text!r} {span.err_type!r} {span.correction_description!r} ')
            #
            # Collect json sentences (if needed)
            #
            if has_target_error_prefixes and save_to_json:
                # Collect and save sentences with errors
                erroneous_sents = []
                for err_sent_text_obj in _extract_sentences_with_clause_errors( text_obj, clauses_layer=clauses_layer, 
                                                                                          syntax_layer=syntax_layer, 
                                                                                          sentences_layer=sentences_layer):
                    # Reduce size of the Text object: remove morph_analysis layer
                    syntax_parent = err_sent_text_obj[syntax_layer].parent
                    if syntax_parent is not None:
                        syntax_parent_parent = err_sent_text_obj[syntax_parent].parent
                        if syntax_parent_parent is not None:
                            rebase(err_sent_text_obj, syntax_layer, syntax_parent_parent)
                            err_sent_text_obj.pop_layer( syntax_parent )
                    collected_sentence_texts.append( err_sent_text_obj )
                    erroneous_sents.append( err_sent_text_obj )
                if collect_non_errors_json and erroneous_sents:
                    # Collect and save non-erroneous sentences
                    non_erroneous_sents = _extract_non_erroneous_sents( text_obj, erroneous_sents, 
                                                                                  clauses_layer=clauses_layer, 
                                                                                  syntax_layer=syntax_layer, 
                                                                                  sentences_layer=sentences_layer,
                                                                                  logger=log )
                    if non_erroneous_sents:
                        with open(non_errors_outfname, 'a', encoding='utf-8') as out_f:
                            for sid, sent_text_obj in enumerate(non_erroneous_sents):
                                # Reduce size of the Text object: remove morph_analysis layer
                                syntax_parent = sent_text_obj[syntax_layer].parent
                                if syntax_parent is not None:
                                    syntax_parent_parent = sent_text_obj[syntax_parent].parent
                                    if syntax_parent_parent is not None:
                                        rebase(sent_text_obj, syntax_layer, syntax_parent_parent)
                                        sent_text_obj.pop_layer( syntax_parent )
                                sent_text_obj_json = text_to_json(sent_text_obj)
                                out_f.write(sent_text_obj_json)
                                out_f.write('\n')
                        log.info( f' Extracted {len(non_erroneous_sents)} non-erroneous sentence(s). ' )
        #
        # After processing:
        # * Output final statistics
        # * Save collected erroneous sentences (if needed)
        if len(problems_found.keys()) > 0:
            log.info('\n')
            log.info('============    Final statistics    ============')
            total = 0
            for k in sorted( problems_found.keys(), key=problems_found.get, reverse=True ):
                log.info( f' {k} -- #{problems_found[k]} ' )
                total += problems_found[k]
            log.info( f' TOTAL -- #{total} ' )
        if collected_sentence_texts:
            log.info( f' TOTAL of #{len(collected_sentence_texts)} sentences collected. ' )
            outfname = errors_json_file_name
            if len(target_error_prefixes) > 0:
                outfname = f'{target_error_prefixes[0]}_{outfname}'
            log.info( f' Writing extracted sentences into {outfname} ... ' )
            with open(outfname, 'w', encoding='utf-8') as out_f:
                for sid, sent_text_obj in enumerate(collected_sentence_texts):
                    sent_text_obj_json = text_to_json(sent_text_obj)
                    out_f.write(sent_text_obj_json)
                    if sid+1 < len(collected_sentence_texts):
                        out_f.write('\n')

    elif not collection.exists():
        log.error(' (!) Collection {!r} does not exist...'.format(target_collection))
        exit(1)

    storage.close()




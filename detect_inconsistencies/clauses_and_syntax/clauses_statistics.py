#
#   Finds statistics of clauses based on a rough classification of clauses.
#   Use INI file to provide detailed processing settings.
#
#   Clause classes give information about:
#   1) clause_type: regular, embedded, simple_sentence
#   2) verb containment: has_verb, no_verb
#   3) whether the clause is parenthesised or quoted
#   4) whether the clause starts with words:
#      a) ja-ning-ega-või
#      b) aga-kuigi
#      c) et, kui, millal, kus, kuhu, kust, sest, kuid, nagu, ehkki,
#         siis, kuni, otsekui, justkui, kuna, kuidas, kas, siis,
#      d) or lemmas: mis, kes, milline, see, missugune;
#
#  Outputs statistics to screen and to a log file.
#
import os, os.path, sys
import logging
import configparser

from collections import defaultdict

from tqdm import tqdm

from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import IndexQuery

from estnltk.consistency.clauses_and_syntax_consistency import yield_clauses_and_syntax_words_sentence_wise

def init_logger(log_file_name):
    '''
    Initializes stdout logger and file logger.
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
    f_handler = logging.FileHandler(log_file_name, mode='w', encoding='utf-8')
    f_handler.setLevel(logging.INFO)
    f_format = logging.Formatter('%(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    return logger

def load_in_doc_ids_from_file(fnm, logger, sort=True):
    '''Loads processable document ids from a text file. 
       In the text file, each document id should be on a separate line.
       Returns a list with document ids. '''
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

start_quotes = ['"', "'", '«', '“', '”', u'\u00AB', u'\u02EE', u'\u030B', u'\u201C', u'\u201D', u'\u201E']
end_quotes   = ['"', "'", '»', '“', '”', r'\u00BB', r'\u02EE', r'\u030B', r'\u201C', r'\u201D', r'\u201E']

def classify_clause( clause, cl_syntax_words, is_simple_clause=False ):
    '''Provides a rough classification for the given clause.'''
    clause_type = clause.annotations[0]['clause_type']
    if is_simple_clause:
        clause_type = 'simple_sentence'
    clause_class = f'{clause_type}'
    # Determine if the clause has verb
    verbs = [w for w in cl_syntax_words if 'V' in list(w['xpostag'])]
    verb_status = 'has_verb' if len(verbs) > 0 else 'no_verb'
    parenthesised = ''
    quoted = ''
    starts_with = ''
    if len(cl_syntax_words) > 0:
        first_word = cl_syntax_words[0]
        if len(cl_syntax_words) > 1:
            last_word  = cl_syntax_words[-1]
            if first_word.annotations[0]['lemma'] in ['(', '['] and \
               last_word.annotations[0]['lemma']  in [')', ']']:
                parenthesised = '|parenthesised'
            
            if first_word.annotations[0]['lemma'] in start_quotes and \
               last_word.annotations[0]['lemma']  in end_quotes:
                quoted = '|quoted'
        if not first_word.text.istitle():
            first_word_text  = first_word.text.lower()
            first_word_lemma = first_word.annotations[0]['lemma'].lower()
            first_word_lemma = first_word_lemma.replace('mis_sugune', 'missugune')
            if first_word_text in ['ja','ning','ega','või']:
                starts_with = '|ja-ning-ega-või'
            elif first_word_text in ['aga','kuigi']:
                starts_with = '|aga-kuigi'
            elif first_word_text in ['et','kui', 'millal', 'kus', 'kuhu', \
                                     'kust', 'sest', 'kuid', 'nagu', 'ehkki', \
                                     'siis', 'kuni', 'otsekui', 'justkui', \
                                     'kuna', 'kuidas', 'kas', 'siis']:
                starts_with = f'|{first_word.text.lower()}'
            elif first_word_lemma in ['mis', 'kes', 'milline', 'see', 'missugune']:
                starts_with = f"|{first_word.annotations[0]['lemma'].lower()}"
    clause_class = f'{clause_type}|{verb_status}{parenthesised}{quoted}{starts_with}'
    return clause_class


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
                       'output':['log_file_name'] }
    ids = []
    ids_file = ''
    target_collection = ''
    clauses_layer = ''
    sentences_layer = ''
    syntax_layer = ''
    log_file = sys.argv[0]+'.log'
    for section in config_sections.keys():
        if not config.has_section(section) and section == 'subset':
            # Subsection 'subset' can be skipped
            continue
        if not config.has_section(section):
            raise ValueError("Error in config file {!r}: missing a section {!r}".format(conf_file, section))
        for option in config_sections[section]:
            if not config.has_option(section, option) and section == 'subset' and option == 'selected_indexes_file':
                # Index selection can be skipped
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
                if option == 'log_file_name':
                    log_file = config['output']['log_file_name']

    log = init_logger(log_file)

    storage = PostgresStorage(pgpass_file=config['db-connection']['pgpass_file'], 
                              schema=config['db-connection']['schema'], 
                              role=config['db-connection']['role'])

    if len(ids_file) > 0:
        ids = load_in_doc_ids_from_file(ids_file, log, sort=True)
    
    # For internal debbuging only
    debug_output = False
    
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

        clause_stats = defaultdict(int)
        documents = 0
        for text_id, text_obj in selection:
            for sent_id, sent_clauses, sent_cl_syntax_words in yield_clauses_and_syntax_words_sentence_wise( text_obj, \
                                                                                        clauses_layer=clauses_layer, \
                                                                                        syntax_layer=syntax_layer, \
                                                                                        sentences_layer=sentences_layer ):
                simple_clause = len(sent_clauses) == 1
                for clause, cl_syntax_words in zip(sent_clauses, sent_cl_syntax_words):
                    cl_class = classify_clause( clause, cl_syntax_words, is_simple_clause=simple_clause)
                    clause_stats[cl_class] += 1
                    
                    # Construct debug output
                    if cl_class in ['regular|no_verb'] and debug_output:
                        output_str = []
                        output_str.append( '\n' )
                        output_str.append( '='*50 )
                        output_str.append( '\n' )
                        output_str.append( f"{text_id}::{text_obj.meta['file']}::sent={sent_id}" )
                        output_str.append( '\n' )
                        output_str.append( '='*50 )
                        output_str.append( '\n' )
                        for clause2, cl_syntax_words2 in zip(sent_clauses, sent_cl_syntax_words):
                            cl_word_idx = 0
                            if clause2.annotations[0]['clause_type'] == 'embedded':
                                output_str.append( '<EMBEDDED>' )
                                output_str.append( '\n' )
                            for w in cl_syntax_words2:
                                marking = ''
                                output_str.append( f"{w.text} {w.annotations[0]['id']} {w.annotations[0]['head']} {'|'.join(w['xpostag'])} {w.annotations[0]['deprel']} {marking}" )
                                output_str.append( '\n' )
                                cl_word_idx += 1
                            output_str.append( '' )
                            output_str.append( '\n' )
                        output_str.append( '' )
                        output_str.append( '\n' )
                        log.info( ''.join(output_str) )
                        log.info('\n')
            documents += 1
        log.info('\n')
        log.info('============    Final statistics    ============')
        log.info(f'{documents} docs from {target_collection}::{ids_file.replace(".txt", "")}')
        log.info('\n')
        log.info('============       All clauses      ============')
        total = sum( [clause_stats[k] for k in clause_stats.keys()] )
        for k in sorted( clause_stats.keys(), key=clause_stats.get, reverse=True ):
            percentage = f'{clause_stats[k]*100/total:.3f}%'
            log.info( f' #{clause_stats[k]} ({percentage})  --  {k}' )
        log.info( f'#{total} (100%)  --  TOTAL clauses' )
        log.info('\n')
        log.info('============    Regular clauses    ============')
        for k in sorted( clause_stats.keys(), key=clause_stats.get, reverse=True ):
            if k.startswith('regular'):
                percentage = f'{clause_stats[k]*100/total:.3f}%'
                log.info( f' #{clause_stats[k]} ({percentage})  --  {k}' )
        log.info('\n')
        log.info('============    Embedded clauses    ============')
        for k in sorted( clause_stats.keys(), key=clause_stats.get, reverse=True ):
            if k.startswith('embedded'):
                percentage = f'{clause_stats[k]*100/total:.3f}%'
                log.info( f' #{clause_stats[k]} ({percentage})  --  {k}' )
        log.info('\n')
        log.info('============    Simple sentences    ============')
        for k in sorted( clause_stats.keys(), key=clause_stats.get, reverse=True ):
            if k.startswith('simple'):
                percentage = f'{clause_stats[k]*100/total:.3f}%'
                log.info( f' #{clause_stats[k]} ({percentage})  --  {k}' )

    elif not collection.exists():
        log.error(' (!) Collection {!r} does not exist...'.format(target_collection))
        exit(1)

    storage.close()




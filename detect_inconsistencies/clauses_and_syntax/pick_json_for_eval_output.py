#
#   Selects Estnltk JSON sentences corresponding to manual evaluations.
#   Basically: merges outputs of scripts pick_randomly_from_errs.py
#   (applied on a log file) and detect_clause_errors.py (extracted 
#   erroneous sentences).
#
#   Partly based on:
#      https://github.com/estnltk/eval_experiments_lrec_2020
#

import re
import argparse
import os, os.path
import warnings

from math import floor

from collections import defaultdict

from random import randint, seed

from estnltk import logger

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Selects Estnltk JSON sentences corresponding to manual evaluations.")
    parser.add_argument('input_eval_file', type=str, \
                         help="File containing manually evaluated sentences. "+\
                              "Assumingly file that was initially created by "+\
                              "script pick_randomly_from_errs.py. ")
    parser.add_argument('input_json_file', type=str, \
                         help="File containing all extracted erroneous sentences, which "+\
                              "should also include sentences used for manual evaluation in "+\
                              "estnltk JSON format.")
    parser.add_argument('-j', '--judgements', dest='add_judgements', default=False, action='store_true', \
                        help="If set, attempts to pick manual judgements from the file of manual "+
                             "evaluations, and add to the output as document metadata. "+
                             "(default: False)" )
    parser.add_argument('-d', '--debug', dest='debug_output', default=False, action='store_true', \
                        help="If set, then prints additional debug output. "+
                             "(default: False)" )
    args = parser.parse_args()
    
   
    errors_file    = args.input_eval_file
    json_file      = args.input_json_file
    add_judgements = args.add_judgements
    debug_output   = args.debug_output
    assert os.path.exists( errors_file ), '(!) Input file {} not found!'.format( errors_file )
    assert os.path.exists( json_file ), '(!) Input file {} not found!'.format( json_file )

    logger.setLevel( 'INFO' )
    if debug_output:
        logger.setLevel( 'DEBUG' )
    log = logger

    # =====================================================================
    #  Collect sentences from the file of manual evaluations
    # =====================================================================
    
    # Collect all errs gaps, e.g.
    # ==================================================
    pattern_separator = re.compile('^\s*={30,}\s*$')
    # ==================================================
    # in text with id 700909 (delfi9.xml)
    pattern_text_id   = re.compile('^\s*in text with id\s+(\d+)\s+(\S+)\s*$')
    # ==================================================
    # attributive_embedded_clause_wrong_end::150 (?)
    pattern_err_index_1 = re.compile('^\s*([^:]+)::(\d+)\s+(\S+)\s*$')  # with judgement
    pattern_err_index_2 = re.compile('^\s*([^:]+)::(\d+)\s*$')          # without judgement
    # ==================================================
    # ei 7 8 aux
    # panusta 8 1 acl:relcl
    # , 15 8 punct <--- NEW CLAUSE END / EMBEDDING
    # need 16 17 nsubj
    pattern_inside_sent = re.compile('^(\S+)\s(\d+)\s(\d+)\s(\S+).*$')
    log.info('Collecting manually evaluated errors ...')
    all_collected_err_sentences = []
    all_collected_judgements = []
    all_empty_judgements = []
    with open(errors_file, 'r', encoding='utf-8') as in_f:
        last_was_err_indx = False
        last_was_sep   = False
        collect_now    = False
        last_text_id   = None
        last_fname     = None
        last_judgement = None
        collected = []
        for line in in_f:
            line = line.strip()
            sep_indx_match = pattern_separator.match( line )
            text_id_match = pattern_text_id.match( line )
            if text_id_match:
                last_text_id = text_id_match.group(1)
                last_fname = text_id_match.group(2)
            err_index_match_1 = pattern_err_index_1.match( line )
            if err_index_match_1 and len(collected)==0:
                collect_now = True
                last_judgement = err_index_match_1.group(3)
                continue
            err_index_match_2 = pattern_err_index_2.match( line )
            if err_index_match_2 and len(collected)==0:
                collect_now = True
                last_judgement = None
                continue
            if collect_now:
                inside_sent_match = pattern_inside_sent.match( line )
                if inside_sent_match:
                    word = inside_sent_match.group(1)
                    collected.append(word)
                elif len(line) == 0:
                    if len(collected) > 0 and collected[-1] != '|':
                        collected.append('|')
            if len(collected) > 0:
                # check stopping criteria
                if text_id_match is not None or sep_indx_match is not None:
                    # empty collected buffer
                    all_collected_err_sentences.append( collected )
                    all_collected_judgements.append(last_judgement)
                    if last_judgement is None:
                        all_empty_judgements.append(last_judgement)
                    log.debug(' '.join(collected))
                    collected = []
                    collect_now = False
                    last_judgement = None
            last_line = line
            last_was_err_indx = err_index_match_1 is not None or \
                                err_index_match_2 is not None
            last_was_sep  = sep_indx_match is not None
    if len(collected) > 0:
        # empty collected buffer
        all_collected_err_sentences.append( collected )
        all_collected_judgements.append(last_judgement)
        if last_judgement is None:
            all_empty_judgements.append(last_judgement)
        log.debug(' '.join(collected))
    non_empty_judgements = len(all_collected_judgements) - len(all_empty_judgements)
    if non_empty_judgements > 0:
        log.info(f'Collected {len(all_collected_err_sentences)} manually evaluated sentences and {len(all_collected_judgements)} manual judgements. {len(all_empty_judgements)}')
    else:
        log.info(f'Collected {len(all_collected_err_sentences)} manually evaluated sentences.')
    if add_judgements:
        assert len(all_collected_err_sentences) == len(all_collected_judgements)
        assert len(all_collected_judgements) == non_empty_judgements, \
            '(!) Cannot add judgements because {} of manual evaluation judgements are missing.'.format(len(all_empty_judgements))

    # =====================================================================
    #  Collect all json sentences
    # =====================================================================
    assert json_file.endswith('.jsonl')
    from estnltk.converters import json_to_text
    from estnltk.converters import text_to_json
    log.info('Collecting annotated sentences ...')
    all_text_objs = []
    with open(json_file, 'r', encoding='utf-8') as in_f:
        for line in in_f:
            line = line.strip()
            text_obj = json_to_text( line )
            # find whether given Text obj is inside 
            all_text_objs.append( text_obj )
    if len(all_text_objs) == 0:
        log.error('(!) No sentences found from the given file. Invalid file, perhaps?')
        exit(1)
    log.info(f'Collected {len(all_text_objs)} sentences from jsonl file.')
    
    # =====================================================================
    #  Find corresponding json sentences
    # =====================================================================
    collected = []
    found_text_objects = dict()
    for text_obj in all_text_objs:
        text = text_obj.text
        words_layer = [layer for layer in text_obj.layers if 'words' in layer]
        assert len(words_layer) > 0, f'No words layer detected in {text_obj.layers}'
        words_layer = words_layer[0]
        text_words_set = set(list(text_obj[words_layer].text))
        text_words_set_len = len(text_words_set)
        candidates = []
        for esid, err_sent_words in enumerate(all_collected_err_sentences):
            err_words_set = set(err_sent_words)
            err_words_set_len = len(err_words_set)
            common = text_words_set.intersection(err_words_set)
            if len(common) > 0 and len(common)+2 >= min(text_words_set_len, err_words_set_len):
                candidates.append( (esid, len(common)) )
        if candidates:
            candidates = sorted(candidates, key=lambda x:x[1], reverse=True)
            first_key = candidates[0][0]
            found_text_objects[ first_key ] = text_obj
            log.debug( '' )
            first_len  = len(set(all_collected_err_sentences[first_key]))
            second_len = len(set(text_obj[words_layer].text))
            log.debug( f'{all_collected_err_sentences[first_key]!r} --> {text_obj.text!r} || {first_len} {second_len} || {candidates!r}' )
            log.debug( '' )
    # Check for missing matches
    missing = 0
    for esid, err_sentence in enumerate( all_collected_err_sentences ):
        if esid not in found_text_objects.keys():
            log.error(f'(!) No JSON match found for sentence: {err_sentence!r}')
            missing += 1
    if missing > 0:
        log.error(f'(!) No JSON match found for {missing} sentences.')
    
    # =====================================================================
    #  Write out results
    # =====================================================================
    in_f_head, in_f_tail = os.path.split(errors_file)
    in_f_root, in_f_ext = os.path.splitext(in_f_tail)
    assert in_f_ext == '' or in_f_ext.startswith('.')
    assert in_f_ext != '.jsonl'
    in_f_ext = '.jsonl'
    judgements_suffix = '_with_judgements' if add_judgements else ''
    out_fname = \
        os.path.join(in_f_head, f'{in_f_root}{judgements_suffix}{in_f_ext}')
    log.info('Saving into  {} ...'.format(out_fname) )
    with open(out_fname, 'w', encoding='utf-8') as out_f:
        for esid, err_sentence in enumerate(all_collected_err_sentences):
            if esid in found_text_objects.keys():
                text_obj = found_text_objects[esid]
                if add_judgements:
                    text_obj.meta['_manual_evaluation'] = \
                        all_collected_judgements[esid]
                out_f.write( text_to_json(text_obj) )
            if esid + 1 < len(all_collected_err_sentences):
                out_f.write('\n')
    log.info( 'Done.')
#
#   Script for picking a random subset of errors from log or json file.
#   Output file will be named as:
#        {input_file}_x{sample_size}.{input_file_extension}
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

from estnltk.converters import json_to_text
from estnltk.converters import text_to_json

from estnltk import logger

def pick_randomly_from_errors_text_file(input_file, rand_pick, random_seed_value, log, error_prefix=None):
    '''Picks random subset of erroneous sentences from the log text file.'''
    # Collect all errs gaps, e.g.
    # ==================================================
    # mis_kes_embedded_clause_wrong_end::1
    pattern_separator = re.compile('^\s*={30,}\s*$')
    pattern_final_separator = re.compile('^\s*={10,}\s+Final statistics\s+={10,}$')
    pattern_log_info  = re.compile('^\s*INFO:([^:]+):(\d+):\s*$')
    pattern_err_index = re.compile('^\s*([^:]+)::(\d+)\s*$')
    pattern_layer_debug_output = re.compile("^>\s*'([^']+)'\s'([^']+)'\s'([^']+)'\s*$")
    # console output
    pattern_log_info_text_id_1 = \
        re.compile('^\s*INFO:([^:]+):(\d+):\s*\(\!\) clauses_errors in (text with id .+)$')
    # log file output
    pattern_log_info_text_id_2 = \
        re.compile('^\s*\(\!\) clauses_errors in (text with id .+)$')
    # INFO:clauses_vs_syntax_consistency_in_koondkorpus.py:116: (!) clauses_errors in text with id 684 (aja_kr_2001_12_18.xml)
    log.info('Collecting error indexes ...')
    errs = []
    total_errs = 0
    with open(input_file, 'r', encoding='utf-8') as in_f:
        last_was_log_match = False
        last_was_err_indx = False
        last_was_sep = False
        last_text_id = None
        collected = []
        for line in in_f:
            line = line.strip()
            sep_indx_match = pattern_separator.match( line )
            log_info_match = pattern_log_info.match( line )
            final_sep_match = pattern_final_separator.match( line )
            layer_debug_match = pattern_layer_debug_output.match( line )
            # console output
            log_info_match_text_id_1 = \
                pattern_log_info_text_id_1.match( line )
            if log_info_match_text_id_1:
                last_text_id = log_info_match_text_id_1.group(3)
            # log file output
            log_info_match_text_id_2 = \
                pattern_log_info_text_id_2.match( line )
            if log_info_match_text_id_2:
                last_text_id = log_info_match_text_id_2.group(1)
            err_index_match = pattern_err_index.match( line )
            if err_index_match and last_was_sep and len(collected)==0:
                can_be_collected = True
                if error_prefix is not None:
                    err_type = err_index_match.group(1)
                    can_be_collected = \
                        err_type.startswith(error_prefix)
                if can_be_collected:
                    total_errs += 1
                    collected = [last_line]
                    if last_text_id is not None:
                        collected = [last_line, 'in '+last_text_id, last_line]
            if len(collected) > 0:
                # check stopping criteria
                if log_info_match is not None or \
                   sep_indx_match is not None or \
                   final_sep_match is not None:
                    # empty collected buffer
                    errs.append( collected )
                    collected = []
                else:
                    # continue
                    if layer_debug_match is None and \
                       log_info_match_text_id_1 is None and \
                       log_info_match_text_id_2 is None:
                        collected.append(line)
            last_line = line
            last_was_log_match = log_info_match is not None
            last_was_err_indx  = err_index_match is not None
            last_was_sep       = sep_indx_match is not None
    assert len(errs) == total_errs
    if total_errs == 0:
        log.error('(!) No errors found from the given file. Invalid file, perhaps?')
        exit(1)
    if total_errs <= rand_pick:
        warnings.warn('(!) Unreasonable rand_pick value {}: the file contains only {} errors. Picking all.'.format(rand_pick, total_errs))
    if total_errs > rand_pick:
        # Make a pick over the whole data
        seed( random_seed_value )
        _picks = set()
        failed_attempts = 0
        while len( _picks ) < rand_pick:
            if len(errs) > 1:
                i = randint(0, len(errs) - 1)
            else:
                i = 0
            gap_ind = i
            if gap_ind not in _picks:
                _picks.add(gap_ind)
                failed_attempts = 0
            else:
                failed_attempts += 1
                if failed_attempts >= 20:
                    log.error('(!) 20 unsuccessful random picks in a row: terminating ...')
                    break
    else:
        # Select all available
        _picks = [ i for i in range(0, len(errs)-1) ]
    # Sort
    _picks_flat = sorted( list(_picks) )
    # Write output file
    in_f_head, in_f_tail = os.path.split(input_file)
    in_f_root, in_f_ext = os.path.splitext(in_f_tail)
    assert in_f_ext == '' or in_f_ext.startswith('.')
    out_fname = os.path.join(in_f_head, f'{in_f_root}_x{len(_picks_flat)}{in_f_ext}')
    log.info('Saving into  {} ...'.format(out_fname) )
    with open(out_fname, 'w', encoding='utf-8') as out_f:
        for k in sorted( list(_picks) ):
            content = '\n'.join(errs[k])
            out_f.write(content)
            out_f.write('\n')
    log.info( 'Done.')


def pick_randomly_from_errors_jsonl_file(input_file, rand_pick, random_seed_value, log, debug=True):
    '''Picks random subset of erroneous sentences from the input jsonl file.'''
    assert input_file.endswith('.jsonl')
    log.info('Collecting annotated sentences ...')
    collected = []
    with open(input_file, 'r', encoding='utf-8') as in_f:
        for line in in_f:
            line = line.strip()
            text_obj = json_to_text( line )
            collected.append( text_obj )
    if len(collected) == 0:
        log.error('(!) No sentences found from the given file. Invalid file, perhaps?')
        exit(1)
    if len(collected) <= rand_pick:
        warnings.warn('(!) Unreasonable rand_pick value {}: the file contains only {} sentences. Picking all.'.format(rand_pick, len(collected)))
    if len(collected) > rand_pick:
        # Make a pick over the whole data
        seed( random_seed_value )
        _picks = set()
        failed_attempts = 0
        while len( _picks ) < rand_pick:
            if len(collected) > 1:
                i = randint(0, len(collected) - 1)
            else:
                i = 0
            gap_ind = i
            if gap_ind not in _picks:
                _picks.add(gap_ind)
                failed_attempts = 0
            else:
                failed_attempts += 1
                if failed_attempts >= 20:
                    log.error('(!) 20 unsuccessful random picks in a row: terminating ...')
                    break
    else:
        # Select all available
        _picks = [ i for i in range(0,len(collected)-1) ]
    # Sort
    _picks_flat = sorted( list(_picks) )
    # Write output file
    in_f_head, in_f_tail = os.path.split(input_file)
    in_f_root, in_f_ext = os.path.splitext(in_f_tail)
    assert in_f_ext == '' or in_f_ext.startswith('.')
    out_fname = \
        os.path.join(in_f_head, f'{in_f_root}_x{len(_picks_flat)}{in_f_ext}')
    log.info('Saving into  {} ...'.format(out_fname) )
    with open(out_fname, 'w', encoding='utf-8') as out_f:
        _picks = list(_picks)
        for k in sorted( _picks ):
            if debug:
                print( k, f'>> {collected[k].text}' )
            out_f.write( text_to_json(collected[k]) )
            if k + 1 < len( _picks ):
                out_f.write('\n')
    log.info( 'Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Selects a random subset of inconsistencies/errors from input file. "+
       "By default, the chosen subset of inconsistencies will be written into a "+
       "file named as: the original file name + '_x' + amount of randomly "+
       "chosen differences. ")
    parser.add_argument('input_file', type=str, \
                        help="file containing inconsistencies/errors. ")
    parser.add_argument('rand_pick', type=int, \
                        help="integer value specifying the amount of differences "+\
                             "to be randomly chosen from the input file." )
    parser.add_argument('--seed', dest='random_seed_value', action='store', type=int, default=1,\
                        help="seed value used in making the random selection (default: 1).")
    parser.add_argument('--prefix', dest='pick_prefix', action='store', type=str, default=None,\
                        help="prefix for filtering errors: only errors with the given prefix "+\
                             "will be selected (default: None).")
    args = parser.parse_args()
    
    logger.setLevel( 'INFO' )
    log = logger
    
    rand_pick = args.rand_pick
    errors_file = args.input_file
    random_seed_value = args.random_seed_value
    error_prefix = args.pick_prefix
    assert os.path.exists( errors_file ), '(!) Input file {} not found!'.format( errors_file )
    assert 0 < rand_pick, '(!) rand_pick must be a positive integer'
    
    if not errors_file.endswith('.jsonl'):
        pick_randomly_from_errors_text_file(errors_file, rand_pick, random_seed_value, log, error_prefix=error_prefix)
    else:
        if error_prefix:
            raise NotImplementedError('error_prefix not supported for jsonl files')
        pick_randomly_from_errors_jsonl_file(errors_file, rand_pick, random_seed_value, log)


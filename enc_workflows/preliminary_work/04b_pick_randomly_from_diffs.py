#
#   Script for making a random selection from *_diffs.txt file.
#
#   Partly based on:
#      https://github.com/estnltk/eval_experiments_lrec_2020
#

import re
import argparse
import os, os.path
import warnings

from collections import defaultdict

from random import randint, seed

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Selects a random subset of differences from *_diffs.txt file. "+
       "By default, the chosen subset of differences will be written into a "+
       "file named as: the original file name + '_x' + amount of randomly "+
       "chosen differences. ")
    parser.add_argument('ann_diffs_file', type=str, \
                        help="file containing annotation differences. "+\
                             "must be *_diffs.txt file created by the script "+\
                             "that finds annotation differences ( 04a_find_literature_old_unnormalized_normalized_diff.py ).")
    parser.add_argument('rand_pick', type=int, \
                        help="integer value specifying the amount of differences "+\
                             "to be randomly chosen from the ann_diffs_file." )
    parser.add_argument('--seed', dest='random_seed_value', action='store', type=int, default=1,\
                        help="seed value used in making the random selection (default: 1).")
    args = parser.parse_args()
    
    rand_pick = args.rand_pick
    diff_file = args.ann_diffs_file
    random_seed_value = args.random_seed_value
    assert os.path.exists( diff_file ), '(!) Input file {} not found!'.format( diff_file )
    if '_diffs.txt' not in diff_file:
        warnings.warn( f'The input file  {diff_file}  does not contain substring "_diffs.txt". Is it correct file?' )
    assert 0 < rand_pick, '(!) rand_pick must be a positive integer'
    #
    # Collect all diff gaps, e.g.
    #
    #   literature_old\nc23_Literature_Old\0\0\doc.json::2
    #
    pattern_diff_index = re.compile('^\s*([^:]+)::(\d+)\s*$')
    print('Collecting difference indexes ...')
    total_differences = []
    unique_diff_numbers = set()
    with open(diff_file, 'r', encoding='utf-8') as in_f:
        for line in in_f:
            line = line.strip()
            diff_ind_match = pattern_diff_index.match( line )
            if diff_ind_match:
                diff_number = diff_ind_match.group(2)
                assert diff_number not in unique_diff_numbers
                total_differences.append( diff_number )
                unique_diff_numbers.add( diff_number )
    if len(total_differences) == 0:
        print('(!) No differences found from the given ann_diffs_file. Invalid file, perhaps?')
        exit(1)
    if len(total_differences) <= rand_pick:
        print('(!) Unreasonable rand_pick value {}: the ann_diffs_file contains only {} differences.'.format(rand_pick, total_differences))
        exit(1)
    print('')
    print( f' {len(total_differences)}  (100.0%)  TOTAL')
    print('')
    seed( random_seed_value )

    # Make a pick over the whole corpus
    print(f'Picking randomly {rand_pick} differences from the whole corpus ...')
    picked_differences = set()
    failed_attempts = 0
    while len(picked_differences) < rand_pick:
        i = randint(0, len(total_differences) - 1)
        diff_str = total_differences[i]
        if diff_str not in picked_differences:
            picked_differences.add( diff_str )
            failed_attempts = 0
        else:
            failed_attempts += 1
            if failed_attempts >= 20:
                log.error('(!) 20 unsuccessful random picks in a row: terminating ...')
                break
    print('Collecting randomly picked differences ...')

    all_collected_lines = []
    pattern_separator = '=========='
    with open(diff_file, 'r', encoding='utf-8') as in_f:
        collected_lines = []
        pickable_line   = False
        for line in in_f:
            line = line.strip()
            diff_ind_match = pattern_diff_index.match( line )
            if diff_ind_match:
                diff_number = diff_ind_match.group(2)
                if diff_number in picked_differences:
                    pickable_line = True
            collected_lines.append(line)
            if pattern_separator in line:
                if pickable_line:
                    all_collected_lines.extend( collected_lines )
                collected_lines = []
                pickable_line = False

    in_f_head, in_f_tail = os.path.split(diff_file)
    in_f_root, in_f_ext = os.path.splitext(in_f_tail)
    assert in_f_ext == '' or in_f_ext.startswith('.')
    out_fname = os.path.join(in_f_head, f'{in_f_root}_x{rand_pick}{in_f_ext}')
    print('Saving into  {} ...'.format(out_fname) )
    with open(out_fname, 'w', encoding='utf-8') as out_f:
        for line in all_collected_lines:
            out_f.write(line.rstrip())
            out_f.write('\n')
    print( 'Done.')

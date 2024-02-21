#
#  Selects a random subset of documents from a word count index. 
#  The subset is constrained by the number of documents, and, 
#  optionally, by minimum/maximum amount of words each document 
#  must have.
#  The result is a file with selected document indexes.
#
#  For the reference, here are the average, minimun and maximum word counts 
#  by Koondkorpus subcorpora and text types:
#
#   avg words: 198112.57 (76 docs) rkogu_stenogramm  (words min: 1022, max: 376863)
#   avg words: 74948.55 (64 docs) uudisgrupi_salvestus_uudisgrupi_salvestus  (words min: 88, max: 814446)
#   avg words: 41897.49 (301 docs) jututoavestlus_jututoavestlus  (words min: 271, max: 130466)
#   avg words: 41814.76 (66 docs) tea_dissertatsioon  (words min: 3630, max: 127225)
#   avg words: 35517.71 (202 docs) ilu_tervikteos  (words min: 773, max: 214423)
#   avg words: 28210.48 (77 docs) netikommentaarid_kommentaarid  (words min: 3448, max: 163524)
#   avg words: 23977.66 (35 docs) tea_ajakirjanumber  (words min: 1245, max: 36184)
#   avg words: 3105.71 (1006 docs) tea_artikkel  (words min: 3, max: 129915)
#   avg words: 2042.52 (5823 docs) seadus_seadus  (words min: 52, max: 148665)
#   avg words: 980.78 (2297 docs) aja_luup_artikkel  (words min: 12, max: 5294)
#   avg words: 668.59 (1062 docs) aja_kr_artikkel  (words min: 16, max: 2695)
#   avg words: 609.77 (6974 docs) aja_ml_artikkel  (words min: 4, max: 6529)
#   avg words: 499.09 (19255 docs) netifoorum_teema  (words min: 1, max: 32579)
#   avg words: 466.48 (18273 docs) aja_ee_artikkel  (words min: 1, max: 10413)
#   avg words: 432.45 (88606 docs) aja_pm_artikkel  (words min: 1, max: 18294)
#   avg words: 311.53 (6407 docs) aja_le_artikkel  (words min: 2, max: 3636)
#   avg words: 294.91 (177422 docs) aja_sloleht_artikkel  (words min: 1, max: 23946)
#   avg words: 270.16 (10571 docs) aja_vm_artikkel  (words min: 1, max: 2918)
#   avg words: 269.10 (366839 docs) aja_EPL_artikkel  (words min: 1, max: 11634)
#
#   (Note, however, that these word counts depend on the tokenization. EstNLTK's 
#    tokenization 'v166_words' was used for obtaining the counts above.)
#

import os, os.path
import argparse
import sys

from collections import defaultdict

import logging

from random import randint, seed


INDEX_FIELD_DELIMITER = '|||'

def collect_entries_for_selection( index_file, word_counts_field ):
    '''Collects index entries.'''
    entries = []
    index_heading = None
    with open(index_file, 'r', encoding='utf-8') as in_f:
        first = True
        for line in in_f:
            line=line.strip()
            if len(line) > 0:
                if first:
                    index_fields = line.split( INDEX_FIELD_DELIMITER )
                    index_heading = line
                    assert word_counts_field in index_fields
                else:
                    items = line.split( INDEX_FIELD_DELIMITER )
                    assert len(items) == len(index_fields)
                    entry = { k:items[id] for id, k in enumerate(index_fields) }
                    entry[word_counts_field] = int(entry[word_counts_field])
                    entries.append( (line, entry[word_counts_field]) )
            first = False
    return index_heading, entries


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Selects a random subset of documents from a word count "+
       "index. The subset is constrained by the number of documents, "+
       "and, optionally, by minimum/maximum amount of words each "+
       "document must have.")
    parser.add_argument('index_file', type=str, \
                        help='name of the index file containing document word counts.')
    parser.add_argument('size', type=int, \
                        help='total size of the randomly chosen subset (in documents).')
    parser.add_argument('--word_counts', dest='word_counts_field', action='store', type=str, default='v166_words',\
                        help="name of the field in the index file which stores word counts information "+
                             '(default: "v166_words").')
    parser.add_argument('--words_min', dest='words_min', action='store', type=int, default=None,\
                        help="minimum amount of words that each selected document must have. "+
                             '(default: None).')
    parser.add_argument('--words_max', dest='words_max', action='store', type=int, default=None,\
                        help="maximum amount of words that each selected document must have. "+
                             '(default: None).')
    parser.add_argument('--seed', dest='random_seed_value', action='store', type=int, default=1,\
                        help="random seed value used in making the random selection (default: 1).")
    parser.add_argument('-o', '--out_index_file', dest='out_index_file', action='store', type=str, default='random_selection_index.txt',\
                        help="name of the output file where randomly selected document indexes will be written. "+\
                             "(default: 'random_selection_index.txt')" )
    parser.add_argument('--out_word_count_file', dest='out_word_count_file', action='store', type=str, \
                                            default='random_selection_word_count_index.txt', \
                        help="name of the output file where word count index of the random selection will be written. "+\
                             "(default: 'random_selection_word_count_index.txt')" )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    log = logging
    
    index_file        = args.index_file
    target_selection_size = args.size
    word_counts_field = args.word_counts_field
    out_index_file      = args.out_index_file
    out_word_count_file = args.out_word_count_file
    random_seed_value = args.random_seed_value
    words_min = args.words_min
    words_max = args.words_max

    index_heading, entries = collect_entries_for_selection( index_file, word_counts_field )    
    if target_selection_size > len(entries):
        raise ValueError(f'(!) Target selection size {target_selection_size} exceeds total documents of the corpus: {len(entries)}')
    log.info(f' Picking randomly {target_selection_size} documents:')
    seed( random_seed_value )

    current_selection_size = list()
    current_selection_docs = set()
    failed_attempts = 0
    while len(current_selection_docs) < target_selection_size:
        i = randint(0, len(entries) - 1)
        entry, word_count = entries[i]
        words_size_check_passed = True
        if words_min is not None and word_count < words_min:
            words_size_check_passed = False
        if words_max is not None and word_count > words_max:
            words_size_check_passed = False
        if words_size_check_passed and entry not in current_selection_docs:
            current_selection_size.append(word_count)
            current_selection_docs.add( entry )
            failed_attempts = 0
        else:
            failed_attempts += 1
            if failed_attempts >= 20:
                log.info('(!) 20 unsuccessful random picks in a row: terminating ...')
                break
    log.info('')
    log.info(' Selected total docs:  '+str(len(current_selection_docs)))
    log.info('          total words: '+str(sum(current_selection_size)))
    log.info(f'         (words min size: {min(current_selection_size)}, max size: {max(current_selection_size)})')
    log.info('')
    # Sort selection by document indexes
    all_selected_docs = sorted(current_selection_docs, key=lambda x: int(x.split(INDEX_FIELD_DELIMITER)[0]) )
    # Write out selection's word count index
    with open(out_word_count_file, 'w', encoding='utf-8') as out_f:
        out_f.write( index_heading + '\n' )
        for doc_str in all_selected_docs:
            out_f.write( doc_str + '\n' )
    log.info(f' Selection word count index written to: {out_word_count_file} ')
    # Write out selection's (plain) document index
    with open(out_index_file, 'w', encoding='utf-8') as out_f:
        for doc_str in all_selected_docs:
            doc_id = doc_str.split(INDEX_FIELD_DELIMITER)[0]
            out_f.write( doc_id + '\n' )
    log.info(f' Selection document index written to: {out_index_file} ')
    

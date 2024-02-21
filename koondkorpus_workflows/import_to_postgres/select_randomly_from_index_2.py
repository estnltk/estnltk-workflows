#
#  Selects a random subset of documents from a word count index (version 2).
#  * The subset preserves the proportional distribution of documents 
#    with respect to a target category.
#  * The subset is constrained by the number of words in the whole selection, 
#    and, optionally, by minimum/maximum amount of words that each document 
#    must have. 
#  The result is a file with selected document indexes.
#

import os, os.path
import argparse
import sys

from datetime import datetime
from collections import defaultdict

import logging

from random import randint, seed


INDEX_FIELD_DELIMITER = '|||'

def collect_category_counts_proportions( index_file, category, word_counts_field ):
    '''Collects word counts per category, word count proportions with respect to 
       given category, and total word count.'''
    # 1) Collect total word count for each category
    total_words = 0
    wordcount_by_cat = defaultdict(int)
    with open(index_file, 'r', encoding='utf-8') as in_f:
        first = True
        for line in in_f:
            line=line.strip()
            if len(line) > 0:
                if first:
                    index_fields = line.split( INDEX_FIELD_DELIMITER )
                    assert category in index_fields
                    assert word_counts_field in index_fields
                else:
                    items = line.split( INDEX_FIELD_DELIMITER )
                    assert len(items) == len(index_fields)
                    entry = { k:items[id] for id, k in enumerate(index_fields) }
                    entry[word_counts_field] = int(entry[word_counts_field])
                    total_words += entry[word_counts_field]
                    wordcount_by_cat[entry[category]] += entry[word_counts_field]
            first = False
    # 2) Find proportions of categories
    category_proportions = defaultdict(float)
    for cat in sorted(wordcount_by_cat.keys(), key=wordcount_by_cat.get, reverse=True):
        category_proportions[cat] = wordcount_by_cat.get(cat)/total_words
    return category_proportions, wordcount_by_cat, total_words


def collect_entries_for_selection( index_file, category, word_counts_field ):
    '''Collects index entries distributed by the given category.'''
    entries_by_cat = defaultdict(list)
    index_heading = None
    with open(index_file, 'r', encoding='utf-8') as in_f:
        first = True
        for line in in_f:
            line=line.strip()
            if len(line) > 0:
                if first:
                    index_fields = line.split( INDEX_FIELD_DELIMITER )
                    index_heading = line
                    assert category in index_fields
                    assert word_counts_field in index_fields
                else:
                    items = line.split( INDEX_FIELD_DELIMITER )
                    assert len(items) == len(index_fields)
                    entry = { k:items[id] for id, k in enumerate(index_fields) }
                    entry[word_counts_field] = int(entry[word_counts_field])
                    entries_by_cat[ entry[category] ].append( (line, entry[word_counts_field]) )
            first = False
    return index_heading, entries_by_cat


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Selects a random subset of documents from a word count index (version 2). "+
       "The subset preserves the proportional distribution of documents "+
       "with respect to a target category. "+
       "The subset is constrained by the number of words in the whole selection, "+
       "and, optionally, by minimum/maximum amount of words in each document. ")
    # Specification of the selection #1
    parser.add_argument('index_file', type=str, \
                        help='name of the index file containing document word counts.')
    parser.add_argument('category', type=str, \
                        help='target category which proportional distribution must be '+
                             'preserved while making the random selection.')
    parser.add_argument('size', type=int, \
                        help='total size of the randomly chosen subset (in words).')
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
    parser.add_argument('-o', '--out_index_file', dest='out_index_file', action='store', type=str, default='random_selection_v2_index.txt',\
                        help="name of the output file where randomly selected document indexes will be written. "+\
                             "(default: 'random_selection_v2_index.txt')" )
    parser.add_argument('--out_word_count_file', dest='out_word_count_file', action='store', type=str, \
                                            default='random_selection_v2_word_count_index.txt', \
                        help="name of the output file where word count index of the random selection will be written. "+\
                             "(default: 'random_selection_v2_word_count_index.txt')" )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    log = logging
    
    index_file        = args.index_file
    target_selection_size = args.size
    target_category   = args.category
    word_counts_field = args.word_counts_field
    out_index_file      = args.out_index_file
    out_word_count_file = args.out_word_count_file
    random_seed_value = args.random_seed_value
    words_min = args.words_min
    words_max = args.words_max
    
    category_proportions, wordcounts, total_words = collect_category_counts_proportions( index_file, target_category, word_counts_field )
    if target_selection_size > total_words:
        raise ValueError(f'(!) Target selection size {target_selection_size} exceeds total word count of the corpus: {total_words}')
    selection_percentage = (target_selection_size/total_words)*100.0
    log.info(f' Random subset goal: select {target_selection_size} of {total_words} words ({selection_percentage:.2f}%)')
    log.info('')
    log.info(' Current category proportions and word selection goals:')
    category_selection_goals = defaultdict(int)
    for cat in sorted(category_proportions.keys(), key=category_proportions.get, reverse=True):
        category_selection_goals[cat] = int( category_proportions[cat]*target_selection_size )
        log.info( f'   proportion: {category_proportions[cat]:.3f} '+
               f' selection_goal: {category_selection_goals[cat]:.0f} / {wordcounts[cat]}  {cat}  ' )
    log.info('')
    index_heading, entries_by_cat = collect_entries_for_selection( index_file, target_category, word_counts_field )
    log.info(f' Picking randomly {target_selection_size} words:')
    seed( random_seed_value )
    current_selection_size = defaultdict(int)
    current_selection_docs = defaultdict(set)
    selection_total_words = 0
    selection_total_docs  = 0
    for cat in sorted(category_proportions.keys(), key=category_proportions.get, reverse=True):
        target_amount = category_selection_goals[cat]
        if int(target_amount) == 0:
            continue
        current_selection_size[cat] = 0
        current_selection_docs[cat] = set()
        failed_attempts = 0
        while current_selection_size[cat] < target_amount:
            i = randint(0, len(entries_by_cat[cat]) - 1)
            entry, word_count = entries_by_cat[cat][i]
            words_size_check_passed = True
            if words_min is not None and word_count < words_min:
                words_size_check_passed = False
            if words_max is not None and word_count > words_max:
                words_size_check_passed = False
            if words_size_check_passed and entry not in current_selection_docs[cat]:
                current_selection_size[cat] += word_count
                current_selection_docs[cat].add( entry )
                failed_attempts = 0
            else:
                failed_attempts += 1
                if failed_attempts >= 20:
                    log.info('(!) 20 unsuccessful random picks in a row: terminating ...')
                    break
        log.info(f'   words: {current_selection_size[cat]}  docs: {len(current_selection_docs[cat])}   {cat}')
        selection_total_words += current_selection_size[cat]
        selection_total_docs += len(current_selection_docs[cat])
    log.info('')
    log.info(' Selected total docs:  '+str(selection_total_docs))
    log.info('          total words: '+str(selection_total_words))
    log.info('')
    # Flatten the selection
    all_selected_docs = [doc for cat in current_selection_docs.keys() for doc in current_selection_docs[cat]]
    # Sort selection by document indexes
    all_selected_docs = sorted(all_selected_docs, key=lambda x: int(x.split(INDEX_FIELD_DELIMITER)[0]) )
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
    

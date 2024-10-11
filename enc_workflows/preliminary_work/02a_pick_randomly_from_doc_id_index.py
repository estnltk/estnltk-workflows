#
#  Picks randomly given number of documents (doc_id-s) from 
#  'vert_document_index.csv' file in the root directory. 
#  Saves results into f'random_pick_x{pick_number}_from_vert.csv'.
#
import json
import re, sys
import os, os.path

from datetime import datetime
from random import randint, seed

from collections import defaultdict

doc_id_index_file = 'vert_document_index.csv'

random_seed_value = 1

def split(a, n):
    """
    Splits list `a` into `n` roughly equal-sized subsets.
    If `a` is not exactly divisible by `n`, then finds the
    reminder `r` of the division and enlarges sizes of first 
    `r` subsets by 1. 
    Returns a generator of the split. 
    
    Examples:
    
    >>> sp1 = split([1,1,2,2,3,3], 3)
    >>> list(sp1)
    [[1, 1], [2, 2], [3, 3]]
    >>> sp2 = split([1,2,2,3,3,3,4,4,4,4,5,5,5,5,5], 6)
    >>> list(sp2)
    [[1, 2, 2], [3, 3, 3], [4, 4, 4], [4, 5], [5, 5], [5, 5]]
    >> sp3 = split([[1], [2,2], [3,3,3], [4,4,4,4]], 3)
    >> list(sp3)
    [[[1], [2, 2]], [[3, 3, 3]], [[4, 4, 4, 4]]]
    
    Original source code from:
    https://github.com/estnltk/syntax_experiments/blob/ablation_experiments/02_split_data.py
    """
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


if len(sys.argv) > 1:
    pick_number = sys.argv[1]
    assert str(pick_number).isnumeric(), f'Expected int, but got: {pick_number!r}'
    pick_number = int(pick_number)
    assert os.path.isfile(doc_id_index_file), \
        f'(!) Missing doc_id index file {doc_id_index_file!r}'
    print('Loading index ...')
    start = datetime.now()
    doc_index = defaultdict(list)
    with open(doc_id_index_file, 'r', encoding='utf-8') as in_f:
        for line in in_f:
            line = line.strip()
            if len(line) > 0:
                if 'vert_file,doc_index' not in line:
                    vert_file, doc_id = line.split(',')
                    doc_index[vert_file].append(doc_id)
    
    for fname in sorted(doc_index.keys()):
        print(fname)
        print(' Indexed docs:  ', len(doc_index[fname]) )
    print()
    indexed_v_files = sorted( list(doc_index.keys()) )
    print(f' Total indexing time:        {datetime.now()-start}')
    print(' Total indexed vert files:  ', len(indexed_v_files) )
    print(' Total indexed docs:        ', sum([len(d) for d in doc_index.values()]) )
    print()
    # Distribute picks among files
    random_pick_indexes = [i for i in range(pick_number)]
    split_subsets = list(split(random_pick_indexes, len(indexed_v_files)))
    random_pick_goals = {}
    for subset, vert_file in zip(split_subsets, indexed_v_files):
        random_pick_goals[vert_file] = len(subset)
    print('Random pick goals:')
    print(random_pick_goals)
    print()
    print('Making random picks:')
    seed( random_seed_value )
    random_picks = []
    for vert_file in sorted( doc_index.keys() ):
        target_amount = random_pick_goals[vert_file]
        available_docs = doc_index[vert_file]
        if int(target_amount) == 0:
            continue
        failed_attempts = 0
        current_selection_docs = set()
        while len(current_selection_docs) < target_amount:
            i = randint(0, len(available_docs) - 1)
            entry = available_docs[i]
            entry_id = str(entry)
            if entry_id not in current_selection_docs:
                current_selection_docs.add( entry_id )
                failed_attempts = 0
            else:
                failed_attempts += 1
                if failed_attempts >= 20:
                    print('(!) 20 unsuccessful random picks in a row: terminating ...')
                    break
        for id_str in sorted(list(current_selection_docs)):
            random_picks.append( (vert_file, id_str))
    print()
    output_fname = f'random_pick_x{pick_number}_from_vert.csv'
    print(f'Saving {pick_number} random pick document indexes to file: {output_fname!r} ...')
    with open(output_fname, 'w', encoding='utf-8') as out_f:
        for (fname, id_str) in random_picks:
            out_f.write( f'{fname},{id_str}\n' )
else:
    print('Number of documents to be picked is required as an input argument.')

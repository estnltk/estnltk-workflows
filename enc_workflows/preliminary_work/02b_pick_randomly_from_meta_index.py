#
#  Picks randomly given number of documents (doc_id-s) from 
#  meta_indx_*.jl files in the root directory. 
#  Saves results into f'random_pick_x{pick_number}_from_vert.csv'.
#
import json
import re, sys
import os, os.path

from datetime import datetime
from random import randint, seed

from collections import defaultdict

random_seed_value = 1

# Skip these files (nothing to index)
skip_list = ['meta_indx_nc23_Literature_Old.jl',
             'meta_indx_nc23_Timestamped.jl',
             'meta_indx_nc23_Literature_Contemporary.jl']

def index_documents_by_meta_file(fname:str, index:dict):
    assert os.path.isfile(fname), f'(!) Invalid file name: {fname}'
    indexed_docs = 0
    original_fname = (fname[:]).replace('meta_indx_', '')
    original_fname = original_fname.replace('.jl', '.vert')
    assert original_fname.endswith('.vert')
    with open(fname, 'r', encoding='utf-8') as in_f:
        for line in in_f:
            line = line.strip()
            if len(line) > 0:
                #
                # Example json line:
                # {"__id": "nc19_Balanced_Corpus__1", "id": "2184", "src": "Balanced Corpus 1990–2008", "genre": "periodicals", "genre_src": "source", "filename": "aja_EPL_2002_02_12.tasak.ma", "texttype_nc": "periodicals", "newspaperNumber": "Eesti Päevaleht 12.02.2002", "heading": "Majandus", "article": "Mustamäe ühiselamute üks omanik on USAs registreeritud firma", "autocorrected_paragraphs": true, "__words": 252, "__sentences": 11}
                #
                line_js = json.loads(line)
                assert "__id" in line_js
                id_str = line_js.get('id', '')
                if str(id_str).isnumeric():
                    # index only documents with numeric id
                    if fname not in index.keys():
                        index[fname] = []
                    entry = { 'file': original_fname, 
                              'id': id_str, 
                              'words': line_js["__words"] }
                    index[fname].append( entry )
                    indexed_docs += 1
    return indexed_docs

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
    print('Indexing documents ...')
    start = datetime.now()
    doc_index = {}
    for fname in os.listdir('.'):
        if fname in skip_list:
            continue
        if fname.startswith('meta_indx_') and fname.endswith('.jl'):
            print(fname)
            current_docs = \
                index_documents_by_meta_file(fname, doc_index)
            print()
            print(' Indexed docs:  ', current_docs )
            print()
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
    random_picks_word_count = 0
    for meta_file in sorted(doc_index.keys()):
        target_amount = random_pick_goals[meta_file]
        available_docs = doc_index[meta_file]
        if int(target_amount) == 0:
            continue
        failed_attempts = 0
        current_selection_docs = set()
        current_selection_original_fname = None
        while len(current_selection_docs) < target_amount:
            i = randint(0, len(available_docs) - 1)
            entry = available_docs[i]
            entry_id = str(entry['id'])
            if entry_id not in current_selection_docs:
                current_selection_docs.add( entry_id )
                current_selection_original_fname = entry['file']
                random_picks_word_count += int(entry['words'])
                failed_attempts = 0
            else:
                failed_attempts += 1
                if failed_attempts >= 20:
                    print('(!) 20 unsuccessful random picks in a row: terminating ...')
                    break
        assert current_selection_original_fname is not None
        for id_str in sorted(list(current_selection_docs)):
            random_picks.append( (current_selection_original_fname, id_str))
    print()
    print(f'Picked total {pick_number} documents containing {random_picks_word_count} words.')
    print()
    output_fname = f'random_pick_x{pick_number}_from_vert.csv'
    print(f'Saving {pick_number} random pick document indexes to file: {output_fname!r} ...')
    with open(output_fname, 'w', encoding='utf-8') as out_f:
        for (fname, id_str) in random_picks:
            out_f.write( f'{fname},{id_str}\n' )
else:
    print('Number of documents to be picked is required as an input argument.')

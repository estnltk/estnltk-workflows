#
#  Extracts EstNLTK's Text objects of selected documents (given by 
#  doc_id-s) from *.vert files and saves into path:
#   '{output_dir}/{vert_file_name_without_ext}_doc_{doc_id}.json'
#  The input file must be a random selection index file CSV.
#  The {output_dir} is constructed by removing '.csv' from the 
#  input file and creating a directory with corresponding name.
#
import json
import re, sys
import os, os.path

from datetime import datetime

from collections import defaultdict

from estnltk.corpus_processing.parse_enc import parse_enc_file_iterator
from estnltk.converters import text_to_json

skip_list = []

if len(sys.argv) > 1:
    pick_indexes_file = sys.argv[1]
    assert os.path.exists(pick_indexes_file), f'(!) Missing file {pick_indexes_file!r}'
    output_folder = pick_indexes_file.replace('.csv', '')
    assert output_folder != pick_indexes_file
    os.makedirs(output_folder, exist_ok=True)
    target_docs = defaultdict(list)
    targets = 0
    with open(pick_indexes_file, mode='r', encoding='utf-8') as in_f:
        for line in in_f:
            line = line.strip()
            if len(line) > 0:
                # Example format:
                # nc19_Balanced_Corpus.vert,142135
                # nc19_Reference_Corpus.vert,408199
                fname, index = line.split(',')
                target_docs[fname].append(index)
                targets += 1
    print(f'Extracting {targets} documents from vert files ...')
    start = datetime.now()
    found_documents = defaultdict(set)
    for fname in sorted( os.listdir('.') ):
        if fname in skip_list:
            continue
        if fname not in target_docs.keys():
            continue
        if fname.endswith('.vert'):
            for text_obj in parse_enc_file_iterator( fname, \
                                                     focus_doc_ids=set(target_docs[fname]), \
                                                     line_progressbar='ascii', \
                                                     tokenization='preserve', \
                                                     restore_morph_analysis=True, \
                                                     extended_morph_form=True ):
                assert 'id' in text_obj.meta.keys(), f'(!) Unexpected meta: {text_obj.meta}'
                #print(f'Found doc with id {text_obj.meta["id"]} from {fname}.')
                output_fname = f'{fname.replace(".vert", "")}_doc_{text_obj.meta["id"]}.json'
                found_documents[fname].add( str(text_obj.meta["id"]) )
                output_fpath = os.path.join(output_folder, output_fname)
                text_to_json(text_obj, file=output_fpath)
    # Sanity check: how many targets were found?
    missed = 0
    for fname in sorted(target_docs.keys()):
        for doc_id in target_docs[fname]:
            if str(doc_id) not in found_documents[fname]:
                print(f'(!) Missed document {doc_id} from {fname}.')
                missed += 1
    if missed > 0:
        print(f' Missed {missed} / {targets} documents at total.')
else:
    print('File containing pickable document indexes is required as an input argument.')

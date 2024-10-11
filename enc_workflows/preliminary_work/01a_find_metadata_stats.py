#
#  Finds document metadata fields and metadata value examples based 
#  on meta_indx_*.jl files in the root directory. 
#  
#  If the input is a single meta_indx_*.jl file, then counts and 
#  outputs meta fields in the file. In addition, outputs the TOP 10 
#  most common values of each meta field. 
#
#  If the input is '.', then processes all meta_indx_*.jl files in 
#  the root directory and counts and outputs meta fields / examples 
#  of all files. In addition, for each meta field, counts in how many 
#  files it is used, sorts meta fields by commonness, and outputs.
#  
#
import json
import re, sys
import os, os.path
from collections import defaultdict

import numpy as np

def collect_meta_field_counts_from_meta_file(fname:str):
    assert os.path.isfile(fname), f'(!) Invalid file name: {fname}'
    total_docs = 0
    meta_fields = dict()
    skip_meta_fields = ["__id", "__words", "__sentences"]
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
                for k,v in line_js.items():
                    if k not in skip_meta_fields:
                        if k not in meta_fields.keys():
                            meta_fields[k] = dict()
                        if v not in meta_fields[k].keys():
                            meta_fields[k][v] = 0
                        meta_fields[k][v] += 1
                total_docs += 1
    return meta_fields, total_docs


def print_meta_fields(meta_fields:dict):
    print(' Meta fields: ' )
    longest_field_name = max([len(_key) for _key in meta_fields.keys()])
    for k,v in meta_fields.items():
        field_name_spec = ('{:'+str(longest_field_name+3)+'}').format(k)
        example_keys = list(v.keys())[:10]
        print(f'{len(v.items()):10}  {field_name_spec}  {str(example_keys):.120}...')
    print()


def count_and_percent(items, items_total):
    assert items_total > 0
    return f'{items} / {items_total} ({(items/items_total)*100.0:.2f}%)'


if len(sys.argv) > 1:
    fname = sys.argv[1]
    if os.path.isfile(fname):
        meta_fields, total_docs = \
            collect_meta_field_counts_from_meta_file(fname)
        print()
        print(' Total docs:  ', total_docs  )
        print()
        print_meta_fields(meta_fields)
    elif fname == '.':
        corpus_files_total = 0
        common_meta_fields = dict()
        for fname in os.listdir('.'):
            if fname.startswith('meta_indx_') and fname.endswith('.jl'):
                print(fname)
                meta_fields, total_docs = \
                    collect_meta_field_counts_from_meta_file(fname)
                print()
                print(' Total docs:  ', total_docs  )
                print()
                print_meta_fields(meta_fields)
                # Record common meta fields
                for k,v in meta_fields.items():
                    if k not in common_meta_fields.keys():
                        common_meta_fields[k] = []
                    if fname not in common_meta_fields[k]:
                        common_meta_fields[k].append(fname)
                corpus_files_total += 1
        # Output common meta fields
        if len( common_meta_fields.keys() ) > 0:
            print()
            print(' Common meta fields: ')
            for k in sorted( common_meta_fields.keys(), key=lambda x : len(common_meta_fields[x]), reverse=True ):
                cp = count_and_percent( len(common_meta_fields[k]), corpus_files_total )
                print( f' {cp}   {k} ' )
                for i in range(0, len(common_meta_fields[k]), 2):
                    field1 = common_meta_fields[k][i] if i < len(common_meta_fields[k]) else ''
                    field2 = common_meta_fields[k][i+1] if i+1 < len(common_meta_fields[k]) else ''
                    print( f'      {field1}   {field2}   ' )
            print()
else:
    print('Index file name required as an input argument.')





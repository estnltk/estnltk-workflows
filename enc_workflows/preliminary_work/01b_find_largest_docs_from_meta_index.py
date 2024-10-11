#
#  Finds largest documents based on meta_indx_*.jl files in the root 
#  directory. 
#  
#  If koondkorpus_index_file is available, also estimates numbers of 
#  characters in text (and text size in bytes) based on the number of 
#  word counts in meta index files.
#

import json
import re, sys
import os, os.path
from collections import defaultdict
import warnings

import numpy as np

def collect_word_counts_from_meta_file(fname:str):
    assert os.path.isfile(fname), f'(!) Invalid file name: {fname}'
    docs_by_words = defaultdict(int)
    total_docs = 0
    total_words = 0
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
                assert "__words" in line_js
                doc_id = line_js["__id"]
                _, doc_id_number = doc_id.split('__')
                doc_file = line_js.get("filename", None)
                doc_url = line_js.get("url", None)
                doc_key = None
                if doc_file is not None:
                    if 'nc23_Literature' in fname:
                        title = line_js.get("title", None)
                        if title is not None:
                            doc_file = title.replace(' ', '_')
                    doc_key = f'{doc_file}__{doc_id_number}'
                elif doc_url is not None:
                    doc_key = f'{doc_url}__{doc_id_number}'
                else:
                    doc_key = doc_id
                docs_by_words[doc_key] = int(line_js["__words"])
                total_words += int(line_js["__words"])
                total_docs += 1
    return docs_by_words, total_words, total_docs

def sizeof_fmt(num, suffix="B"):
    # Source: https://stackoverflow.com/a/1094933
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Yi{suffix}"

def create_chars_prediction_model():
    # Create LM model for predicting number of characters based 
    # on text word count. 
    # ~~ the model overestimates with word counts larger than 
    # 600k, and underestimates with word counts less than 370k.
    INDEX_FIELD_DELIMITER = '|||'
    #
    # the following koondkorpus_index_file should be a koondkorpus index file created 
    # with the script "build_pgcollection_index.py" from this repository:
    #  https://github.com/estnltk/estnltk-workflows/tree/master/koondkorpus_workflows/import_to_postgres
    #
    koondkorpus_index_file = 'koondkorpus_base_v2_index.txt'
    if os.path.exists( koondkorpus_index_file ):
        # File exists: build the model
        print(f'Loading data ...')
        X, y = [], []
        with open(koondkorpus_index_file, 'r', encoding='utf-8') as in_f:
            first = True
            for line in in_f:
                line=line.strip()
                if len(line) > 0:
                    if first:
                        index_fields = line.split( INDEX_FIELD_DELIMITER )
                        first = False
                    else:
                        items = line.split( INDEX_FIELD_DELIMITER )
                        assert len(items) == len(index_fields)
                        entry = { k:items[id] for id, k in enumerate(index_fields) }
                        X.append(int(entry['v166_words']))
                        y.append(int(entry['chars']))
        print(f'Fitting model ...')
        from sklearn.linear_model import LinearRegression
        lm = LinearRegression()
        X = np.array(X).reshape(-1, 1)
        y = np.array(y).reshape(-1, 1)
        lm.fit(X, y)
        return lm
    else:
        warnings.warn(f'(!) Missing koondkorpus words index file {koondkorpus_index_file!r} required '+\
                       'for building char prediction model. Cannot make document char size estimations.')
        return None

def predict(model, x):
    return int(lm.predict(np.array([[x]]))[0][0])


if len(sys.argv) > 1:
    fname = sys.argv[1]
    if os.path.isfile(fname):
        lm = create_chars_prediction_model()
        docs_by_words, total_words, total_docs = \
            collect_word_counts_from_meta_file(fname)
        print()
        print(' Total docs:  ', total_docs  )
        print(' Total words: ', total_words )
        print()
        print(' Largest docs by word count: ' )
        for doc in list(sorted(docs_by_words.keys(), key=docs_by_words.get, reverse=True))[:10]:
            print('   ', doc, '| words:', docs_by_words[doc], end = '')
            if lm is not None:
                print(f'| estimated_chars: ~{predict(lm, docs_by_words[doc])}'+\
                      f' ({sizeof_fmt(predict(lm, docs_by_words[doc]))})')
            else:
                print()
        print()
    elif fname == '.':
        lm = create_chars_prediction_model()
        for fname in os.listdir('.'):
            if fname.startswith('meta_indx_') and fname.endswith('.jl'):
                print(fname)
                docs_by_words, total_words, total_docs = \
                    collect_word_counts_from_meta_file(fname)
                print()
                print(' Total docs:  ', total_docs  )
                print(' Total words: ', total_words )
                print()
                print(' Largest docs by word count: ' )
                for doc in list(sorted(docs_by_words.keys(), key=docs_by_words.get, reverse=True))[:10]:
                    print('   ', doc, '| words:', docs_by_words[doc], end = '')
                    if lm is not None:
                        print(f'| estimated_chars: ~{predict(lm, docs_by_words[doc])}'+\
                              f' ({sizeof_fmt(predict(lm, docs_by_words[doc]))})')
                    else:
                        print()
                print()
                print()
else:
    print('Meta index file name required as an input argument.')




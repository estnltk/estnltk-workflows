#
#  Creates two indexes from *.vert files in the root dir:
#  1) Count index 'vert_counts.csv' recording docs, sentences, words 
#     counts in each *.vert file;
#  2) Meta index, recording document metadata (including words and 
#     sentences counts) of each document in a vert file.
#     Meta index is saved in json format, into file with name pattern 
#     f'meta_indx_{corpus_name}.jl', e.g. 'nc19_Web_2013.vert' meta 
#     index will be saved into 'meta_indx_nc19_Web_2013.jl'.
#  Note, this is a long process, expected processing time ~6 days.
#

import sys
import json
import os, os.path
from datetime import datetime

from estnltk import Text
from estnltk.corpus_processing.parse_enc import parse_enc_file_iterator
from estnltk.corpus_processing.parse_enc import parse_tag_attributes
from estnltk.corpus_processing.parse_enc import extract_doc_ids_from_corpus_file

def meta_without_lang(meta:dict) -> dict:
    # Leave lang_scores and other lang detection info out of metadata
    return {k:v for (k,v) in meta.items() if k not in ['lang_old2', 'lang_scores', 'lang_scores2']}

def reorder_meta_keys(meta:dict) -> dict:
    # Reorder keys in a way that '__id' comes first
    if '__id' in meta:
        new_meta = {'__id':meta['__id']}
        for (k,v) in meta.items():
            if k != '__id':
                new_meta[k] = v
        return new_meta
    else:
        return meta

target_files = []
if len(sys.argv) > 1:
    # Parse only specified target files, not all files as default
    new_target_files = None
    target_files = []
    for farg in sys.argv[1:]:
        if farg.endswith('.vert') and farg not in target_files:
            target_files.append( farg )

skip_list = []

output_counts_csv = 'vert_counts.csv'

start = datetime.now()
total_docs = 0
total_words = 0
total_sentences = 0
for fname in sorted(os.listdir('.')):
    if fname in skip_list:
        continue
    if len(target_files) > 0 and fname not in target_files:
        continue
    if fname.endswith('.vert'):
        local_start = datetime.now()
        print(fname)
        vert_docs = 0
        vert_words = 0
        vert_sentences = 0
        vert_meta = []
        corpus_name = fname.replace(".vert", "")
        # TODO future: use add_document_index=True to record exact location of each document
        for text_obj in parse_enc_file_iterator(fname, line_progressbar='ascii', restore_morph_analysis=True):
            meta_stripped = meta_without_lang(text_obj.meta)
            assert '__id' not in meta_stripped, f'(!) Unexpected meta: {meta_stripped}'
            meta_stripped['__id'] = f'{fname.replace(".vert", "")}__{vert_docs+1}'
            assert '__words' not in meta_stripped, f'(!) Unexpected meta: {meta_stripped}'
            meta_stripped['__words'] = len(text_obj['original_morph_analysis'])
            assert '__sentences' not in meta_stripped, f'(!) Unexpected meta: {meta_stripped}'
            meta_stripped['__sentences'] = len(text_obj['original_sentences'])
            # TODO: record textual content's length
            #print( meta_without_lang(text_obj.meta) )
            vert_docs += 1
            vert_words += len(text_obj['original_morph_analysis'])
            vert_sentences += len(text_obj['original_sentences'])
            vert_meta.append( meta_stripped )
            #if vert_docs > 10:
            #    break
        print(f'   docs:      {vert_docs}')
        print(f'   words:     {vert_words}')
        print(f'   sentences: {vert_sentences}')
        print(f'{fname} processing time: {datetime.now() - local_start}')
        total_docs += vert_docs
        total_words += vert_words
        total_sentences += vert_sentences
        if vert_words > 0:
            add_header = not os.path.exists(output_counts_csv)
            with open(output_counts_csv, mode='a', encoding='utf-8') as out_f:
                if add_header:
                    out_f.write(f'vert_file,docs,sentences,words\n')
                out_f.write(f'{fname},{vert_docs},{vert_sentences},{vert_words}\n')
        if vert_meta:
            output_meta_jl = f'meta_indx_{corpus_name}.jl'
            with open(output_meta_jl, mode='w', encoding='utf-8') as out_f_2:
                for meta_dict in vert_meta:
                    meta_dict = reorder_meta_keys(meta_dict)
                    out_f_2.write(f'{json.dumps(meta_dict, ensure_ascii=False)}\n')
        #break
print()
print(f'Total:')
print(f'   docs:      {total_docs}')
print(f'   words:     {total_words}')
print(f'   sentences: {total_sentences}')
print()
print(f'Total processing time: {datetime.now() - start}')

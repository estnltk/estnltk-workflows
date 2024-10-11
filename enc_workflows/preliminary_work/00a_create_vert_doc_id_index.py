#
#  Creates document id-s index from *.vert files in the root dir. 
#  In other words, collects all <doc> 'id' values from *.vert files 
#  and records into 'vert_document_index.csv' file. 
#  Note: if a vert file doesn not have 'id' values in <doc> tags,
#  nothing will be extracted. 
#
#  Note, this process takes a couple of hours, at maximum.
#

import sys
import json
import os, os.path
from datetime import datetime

from estnltk.corpus_processing.parse_enc import extract_doc_ids_from_corpus_file

target_files = []
if len(sys.argv) > 1:
    # Parse only specified target files, not all files as default
    new_target_files = None
    target_files = []
    for farg in sys.argv[1:]:
        if farg.endswith('.vert') and farg not in target_files:
            target_files.append( farg )

skip_list = []

output_index_csv = 'vert_document_index.csv'

start = datetime.now()
total_docs = 0
for fname in sorted(os.listdir('.')):
    if fname in skip_list:
        continue
    if len(target_files) > 0 and fname not in target_files:
        continue
    if fname.endswith('.vert'):
        local_start = datetime.now()
        print(fname)
        doc_ids = extract_doc_ids_from_corpus_file(fname)
        print(f'   docs_ids:      {len(doc_ids)}')
        if len(doc_ids) > 0:
            add_header = not os.path.exists(output_index_csv)
            with open(output_index_csv, mode='a', encoding='utf-8') as out_f:
                if add_header:
                    out_f.write(f'vert_file,doc_index\n')
                for doc_id in doc_ids:
                    out_f.write(f'{fname},{doc_id}\n')
                    total_docs += 1
            doc_ids = []
        #break
print()
print(f'Total:')
print(f'   docs:      {total_docs}')
print()
print(f'Total processing time: {datetime.now() - start}')

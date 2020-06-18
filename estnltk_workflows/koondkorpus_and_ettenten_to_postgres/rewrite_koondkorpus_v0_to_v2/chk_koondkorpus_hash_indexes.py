#
#    Compares two hash indexes (corresponding to 
#   pgcollections that should be copies of one
#   another) and verifies that all the keys are
#   represented in both collections.
#   

import os.path
from collections import defaultdict

COLLECTION_1 = 'koondkorpus'
COLLECTION_2 = 'koondkorpus_v2'

hash_index_file_1 = COLLECTION_1+'__hash_index.txt'
hash_index_file_2 = COLLECTION_2+'__hash_index.txt'

def load_hash_index( fname ):
    '''Loads koondkorpus document hash index created by the 
       script create_index_and_detect_duplicates.'''
    assert os.path.exists(fname), '(!) hash file {!r} not found.'.format(fname)
    hash_index = defaultdict( list )
    with open( fname, 'r', encoding='utf-8' ) as in_f:
        for line in in_f:
            line = line.rstrip()
            if len(line) > 0:
                key, doc_hash = line.split('\t')
                hash_index[doc_hash].append( key )
    return hash_index

print('Loading hash index for {!r}...'.format(COLLECTION_1))
hash_index_1 = load_hash_index( hash_index_file_1 )
print('Loading hash index for {!r}...'.format(COLLECTION_2))
hash_index_2 = load_hash_index( hash_index_file_2 )
print('Check mapping from {!r} to {!r}...'.format(COLLECTION_1,COLLECTION_2))
for key in hash_index_1.keys():
    assert key in hash_index_2.keys()
print('Check mapping from {!r} to {!r}...'.format(COLLECTION_2,COLLECTION_1))
for key in hash_index_2.keys():
    assert key in hash_index_1.keys()
print('Ok.')

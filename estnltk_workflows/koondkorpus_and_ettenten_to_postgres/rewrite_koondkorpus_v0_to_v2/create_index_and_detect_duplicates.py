#
#   Creates an index of koondkorpus documents, 
#   and detects duplicates along the way.
#   
#   Duplicates will only be searched among 
#   texts originating from the same XML file. 
#   
#   Most important output is the "hash index", 
#   which will be in the following form:
#      db_key -> text_original_XML_file + '||' + md5_digest(text_content)
#   

import difflib
import hashlib

import os, sys
import os.path
import logging
import re

from collections import defaultdict
from datetime import datetime

from estnltk import Text
from estnltk.storage.postgres import PostgresStorage

from pgpass_utils import read_info_from_pgpass_file

PGPASS_FILE = 'pgpass.txt'

# Input collection:
TARGET_COLLECTION = 'koondkorpus_v2'
TARGET_SCHEMA = ''
TARGET_ROLE   = ''

# Output files:
out_index_file = TARGET_COLLECTION+'__snippet_index.txt'
out_hash_index_file = TARGET_COLLECTION+'__hash_index.txt'
out_duplicates_index_file = TARGET_COLLECTION+'__duplicates_index.txt'

def create_doc_snippet_key( text ):
    '''Attempts to create a unique doc key based on snippets of the 
       text:  the original file name, the start of doc title and 
       start of the text. 
       Note: in reality, these keys can collide (and quite often). '''
    doc_fname = text.meta.get('file', '--')
    doc_title = (text.meta.get('title', '--'))[:20].replace('\n','{N}')
    doc_text_start = (text.text[:10]).replace('\n','{N}')
    doc_str_key = doc_fname+'||'+doc_title+'||'+doc_text_start
    doc_str_key = doc_str_key.replace('\t','{T}')
    return doc_str_key

def create_doc_hash_key( text ):
    '''Creates a unique doc key based on the original file name, 
       and MD5 hash of the document's text.
       This should be collision-free in most cases.'''
    doc_fname     = text.meta.get('file', '--')
    doc_text_hash = hashlib.md5( (text.text).encode() ).hexdigest()
    doc_str_key = doc_fname+'||'+doc_text_hash
    doc_str_key = doc_str_key.replace('\t','{T}')
    return doc_str_key

def measure_diff_in_lines( text_a, text_b ):
    '''Measures difference in lines between two texts.'''
    diff = difflib.ndiff(text_a.splitlines(keepends=True), text_b.splitlines(keepends=True))
    matching  = 0
    differing = 0
    for d in diff:
        if d.startswith(('-', '+')):
            differing += 1
        elif d.startswith(('?')):
            pass
        else:
            matching += 1
    return matching, matching+differing, round(matching / (matching+differing), 2)

logging_level = 'info'
logging.basicConfig( level=(logging_level).upper() )
log = logging.getLogger(__name__)

# Load the access info & create access
access_info = read_info_from_pgpass_file( PGPASS_FILE )
storage = PostgresStorage(dbname   = access_info['dbname'],
                          user     = access_info['user'], 
                          password = access_info['passwd'], 
                          host     = access_info['host'], 
                          port     = access_info['port'],
                          schema   = TARGET_SCHEMA, 
                          role     = TARGET_ROLE)

collection = storage.get_collection( TARGET_COLLECTION )
assert collection.exists(), '(!) Collection {!r} does not exist.'.format( TARGET_COLLECTION )
log.info(' Collection {!r} exists. '.format(collection.name))

try:
    startTime = datetime.now()
    doc_count = 0
    doc_snip_index   = {}
    doc_hash_index   = defaultdict(list)
    duplicates_index = {}
    near_duplicates_index = {}
    duplicate_docs      = 0
    near_duplicate_docs = 0
    far_duplicate_docs  = 0
    #for key, text in collection.select( keys=[1,500,1000,1621,1623,1622,1624,5000,10000,50000,100000,400000,500000,600000,700000,702000,705000], progressbar='ascii', layers=[] ):
    for key, text in collection.select( progressbar='ascii', layers=[] ):
        doc_snip_key = create_doc_snippet_key( text )
        doc_hash_key = create_doc_hash_key( text )
        if doc_snip_key in doc_snip_index:
            prev_doc = collection[ doc_snip_index[doc_snip_key] ]
            if prev_doc.text == text.text:
                # Full duplicate
                if prev_doc.meta != text.meta:
                    log.warn('(!) Duplicate texts, but differences in metadata {!r} vs {!r}'.format( prev_doc.meta, text.meta ))
                if doc_snip_key not in near_duplicates_index:
                    near_duplicates_index[doc_snip_key] = []
                near_duplicates_index[doc_snip_key].append( (doc_snip_index[doc_snip_key], key, 1.0) )
                duplicate_docs += 1
            else:
                # Near duplicate: find the degree of difference
                matching, total, ratio = measure_diff_in_lines( prev_doc.text, text.text )
                if ratio >= 0.1:
                    if doc_snip_key not in near_duplicates_index:
                        near_duplicates_index[doc_snip_key] = []
                    near_duplicates_index[doc_snip_key].append( (doc_snip_index[doc_snip_key], key, ratio) )
                    if doc_hash_key in doc_hash_index: 
                        log.warn('(!) #1.1 Warning! Unexpectedly colliding hash key {} for {}'.format( doc_hash_key, key ))
                    if ratio < 0.7:
                        far_duplicate_docs += 1
                    else:
                        near_duplicate_docs += 1
            doc_hash_index[doc_hash_key].append( key )
            continue
        else:
            if doc_hash_key in doc_hash_index: 
                # Collision: check for duplicates
                low_similarity = 0
                for prev_key in doc_hash_index[doc_hash_key]:
                    prev_doc = collection[ prev_key ]
                    if prev_doc.text == text.text:
                        # Exact duplicate
                        if prev_doc.meta != text.meta:
                            log.warn('(!) Duplicate texts, but differences in metadata {!r} vs {!r}'.format( prev_doc.meta, text.meta ))
                        if doc_snip_key not in near_duplicates_index:
                            near_duplicates_index[doc_snip_key] = []
                        near_duplicates_index[doc_snip_key].append( (prev_key, key, 1.0) )
                        duplicate_docs += 1
                    else:
                        # Near duplicate: find the degree of difference
                        matching, total, ratio = measure_diff_in_lines( prev_doc.text, text.text )
                        if ratio >= 0.1:
                            if doc_snip_key not in near_duplicates_index:
                                near_duplicates_index[doc_snip_key] = []
                            near_duplicates_index[doc_snip_key].append( (prev_key, key, ratio) )
                            if doc_hash_key in doc_hash_index: 
                                log.warn('(!) #1.1 Warning! Unexpectedly colliding hash key {} for {}'.format( doc_hash_key, key ))
                            if ratio < 0.7:
                                far_duplicate_docs += 1
                            else:
                                near_duplicate_docs += 1
                        else:
                            low_similarity += 1
                if low_similarity == len( doc_hash_index[doc_hash_key] ): 
                    log.warn('(!) #2 Warning! Unexpectedly colliding hash key {} for {}'.format( doc_hash_key, key ))
        doc_snip_index[doc_snip_key] = key
        doc_hash_index[doc_hash_key].append( key )
        doc_count += 1
        #if doc_count > 4500:
        #    break

    time_diff = datetime.now() - startTime
    print()
    print(' Number of Texts processed:      ', doc_count)
    print('     # of full duplicates found: ', duplicate_docs)
    print('     # of near duplicates found: ', near_duplicate_docs)
    print('     # of far duplicates found:  ', far_duplicate_docs)
    print(' Total processing time:          {}'.format(time_diff))
    print()
    print('Writing duplicates index to ',out_duplicates_index_file,'...')
    with open(out_duplicates_index_file, 'w', encoding='utf-8') as out_f:
        for key in sorted( near_duplicates_index.keys(), key=lambda x: near_duplicates_index[x][0][0] ):
            nd_tuples = [str(a) for a in near_duplicates_index[key]]
            out_f.write( key + '\t' + str( ';'.join( nd_tuples ) ) )
            out_f.write( '\n' )
    print()
    print('Writing snippet index to ',out_index_file,'...')
    with open(out_index_file, 'w', encoding='utf-8') as out_f:
        for key in sorted( doc_snip_index.keys(), key=doc_snip_index.get ):
            out_f.write( str(doc_snip_index[key])+'\t'+key )
            out_f.write( '\n' )
    print()
    print('Writing hash index to ',out_hash_index_file,'...')
    with open(out_hash_index_file, 'w', encoding='utf-8') as out_f:
        for hash_key in sorted( doc_hash_index.keys(), key = lambda x : doc_hash_index[x][0] ):
            for key in doc_hash_index[hash_key]:
                out_f.write( str(key)+'\t'+hash_key )
                out_f.write( '\n' )
    print()
finally:
    storage.close()

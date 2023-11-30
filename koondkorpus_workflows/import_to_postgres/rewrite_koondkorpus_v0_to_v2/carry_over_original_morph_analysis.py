#
#  Carries over 'ot_morph_analysis' layer from 
#  koondkorpus v0 to v2 

import hashlib

import os, sys
import os.path
import logging
import re

from collections import defaultdict
from datetime import datetime

from estnltk import Text, Layer, Annotation, Span
from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import RowMapperRecord

from pgpass_utils import read_info_from_pgpass_file

PGPASS_FILE = 'pgpass.txt'

SOURCE_COLLECTION = 'koondkorpus'
SOURCE_SCHEMA     = ''
SOURCE_ROLE       = ''

TARGET_COLLECTION = 'koondkorpus_subset_of_5000_v2'
TARGET_SCHEMA     = ''
TARGET_ROLE       = ''

def create_doc_hash_key( text ):
    '''Creates a unique doc key based on the original file name, 
       and MD5 hash of the document's text.
       This should be collision-free in most cases.'''
    doc_fname     = text.meta.get('file', '--')
    doc_text_hash = hashlib.md5( (text.text).encode() ).hexdigest()
    doc_str_key = doc_fname+'||'+doc_text_hash
    doc_str_key = doc_str_key.replace('\t','{T}')
    return doc_str_key

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

def fetch_src_documents( src_keys, src_collection, target_text=None, layers=[], filter_by_text_match=True ):
    ''' Fetches documents with given id-s from the given source collection.
        Adds specific layers, when required.
    '''
    src_keys = [int(k) for k in src_keys]
    src_texts = []
    for skey, src_text in src_collection.select( keys=src_keys, progressbar=None, layers=layers ):
        if filter_by_text_match and target_text is not None:
            if target_text.text != src_text.text:
                continue
        src_texts.append( (skey, src_text) )
    return src_texts

def make_new_ot_morph_layer( old_text_obj, new_text_obj, new_layer='original_words_morph_analysis', 
                             old_layer='ot_morph_analysis',
                             new_parent_layer='original_words' ):
    '''Creates new 'original_words_morph_analysis' layer based on the old morph analysis layer.'''
    assert old_layer in old_text_obj.layers
    assert new_parent_layer in new_text_obj.layers
    assert new_layer not in new_text_obj.layers
    original_layer = old_text_obj[old_layer]
    assert 'normalized_text' not in original_layer.attributes
    parent_layer   = new_text_obj[new_parent_layer]
    layer = Layer(name        = new_layer,
                  text_object = new_text_obj,
                  attributes  = ('normalized_text',) + original_layer.attributes,
                  parent      = new_parent_layer,
                  ambiguous   = True)
    assert len(parent_layer) == len(original_layer)
    for wid, parent_span in enumerate(parent_layer):
        old_morph_span = original_layer[wid]
        new_span = Span(base_span=parent_span.base_span, layer=layer)
        for ann in old_morph_span.annotations:
            new_annotation = { 'normalized_text' : parent_span.text }
            for a in layer.attributes:
                if a in ['start', 'end', 'text', 'normalized_text']:
                    continue
                new_annotation[a] = ann[a]
            new_span.add_annotation( Annotation(new_span, **new_annotation ) )
        layer.add_span( new_span )
    return layer

def carry_over_ot_morph_row_mapper( row ):
    new_text_id, new_text = row[0], row[1]
    status = {}
    target_hash = create_doc_hash_key( new_text )
    assert target_hash in src_hash_index, '(!) Doc hash {!r} cannot be mapped to {!r}'.format(target_hash, SOURCE_COLLECTION)
    src_keys = src_hash_index[target_hash]
    src_texts = fetch_src_documents( src_keys, src_collection, target_text=new_text, layers=['ot_morph_analysis'] )
    assert len(src_texts) > 0, '(!) No matching text found for doc hash {!r}'.format(target_hash)
    src_key, src_text_obj = src_texts[0]
    layer = make_new_ot_morph_layer( src_text_obj, new_text, new_layer='original_words_morph_analysis', old_layer='ot_morph_analysis' )
    return RowMapperRecord( layer=layer, meta=status )

logging_level = 'info'
logging.basicConfig( level=(logging_level).upper() )
log = logging.getLogger(__name__)

print('Loading hash index for {!r}...'.format(SOURCE_COLLECTION))
src_hash_index_file = SOURCE_COLLECTION+'__hash_index.txt'
src_hash_index = load_hash_index( src_hash_index_file )
print('Done.')

# Load the access info
access_info = read_info_from_pgpass_file( PGPASS_FILE )

src_storage = PostgresStorage(dbname   = access_info['dbname'],
                              user     = access_info['user'], 
                              password = access_info['passwd'], 
                              host     = access_info['host'], 
                              port     = access_info['port'],
                              schema   = SOURCE_SCHEMA, 
                              role     = SOURCE_ROLE)

src_collection = src_storage.get_collection( SOURCE_COLLECTION )
assert src_collection.exists(), '(!) Collection {!r} does not exist.'.format( SOURCE_COLLECTION )
log.info(' Source collection {!r} exists. '.format(src_collection.name))

trg_storage = PostgresStorage(dbname   = access_info['dbname'],
                              user     = access_info['user'], 
                              password = access_info['passwd'], 
                              host     = access_info['host'], 
                              port     = access_info['port'],
                              schema   = TARGET_SCHEMA, 
                              role     = TARGET_ROLE)

trg_collection = trg_storage.get_collection( TARGET_COLLECTION )
assert trg_collection.exists(), '(!) Collection {!r} does not exist.'.format( TARGET_COLLECTION )
log.info(' Target collection {!r} exists. '.format(trg_collection.name))

try:
    #data_iterator = trg_collection.select( keys=[1,2,3,4,5,6,7,8,9,10], progressbar='ascii', layers=['original_words'] )
    data_iterator = trg_collection.select( progressbar='ascii', layers=['original_words'] )
    trg_collection.create_layer(layer_name='original_words_morph_analysis', 
                                data_iterator=data_iterator, 
                                row_mapper=carry_over_ot_morph_row_mapper, 
                                tagger=None,
                                progressbar='ascii',
                                mode='overwrite')
    '''
    # experiment with dry run
    for tkey, target_text  in  data_iterator:
        target_hash = create_doc_hash_key( target_text )
        assert target_hash in src_hash_index, '(!) Doc hash {!r} cannot be mapped to {!r}'.format(target_hash, SOURCE_COLLECTION)
        src_keys = src_hash_index[target_hash]
        src_texts = fetch_src_documents( src_keys, src_collection, target_text=target_text, layers=['ot_morph_analysis'] )
        assert len(src_texts) > 0, '(!) No matching text found for doc hash {!r}'.format(target_hash)
        new_layer = make_new_ot_morph_layer( src_texts[0][1], target_text, new_layer='original_words_morph_analysis', old_layer='ot_morph_analysis' )
    '''
finally:
    src_storage.close()
    trg_storage.close()
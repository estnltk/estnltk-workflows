#
#   Various helpful utilities for processing ENC .vert files,
#   and corresponding estnltk Text object json files.
#

from typing import List, Union

import re, sys
import os, os.path
import hashlib

from datetime import datetime, timedelta

from estnltk import Text, Layer, Annotation
from estnltk.taggers import Retagger
from estnltk_core import EnvelopingSpan
from estnltk.converters import layer_to_json
from estnltk.converters import text_to_json
from estnltk.converters import json_to_text
from estnltk.converters import layer_to_dict
from estnltk.converters import dict_to_layer

from estnltk_core.layer_operations import extract_sections

from estnltk.converters.serialisation_modules import syntax_v0
from estnltk.taggers.standard.syntax.syntax_dependency_retagger import SyntaxDependencyRetagger


# ======================================================================
#  JSON file/directory path handling
# ======================================================================

def get_doc_file_path(collection_dir:str, vert_fname:str, doc_id:int, max_files:int=30000, 
                                          remove_vert_prefix:bool=False, create_path:bool=True):
    '''Computes sub-directory path for the document with the given id. 
       This path is used for storing JSON files of the given document. 
       Returns the path (string).
       
       The sub-directory path will be in the format: 
       `{collection_dir}/{stripped_vert_fname}/{doc_group_dir}/{doc_id}`, 
       where `stripped_vert_fname` is `vert_fname` without directory 
       path and file extension, and `doc_group_dir` is a numeric group 
       name where the document with the given `doc_id` belongs to. 
       Documents are grouped following the `max_files` limit: once 
       a group contains `max_files` files, a new group will created. 
       More specifically, `doc_group_dir` is computed as 
       `int(doc_id / max_files)`. 
       
       If create_path is True (default), then all the missing sub-directories 
       on the path are also created. 
    '''
    assert max_files > 0, f'(!) max_files must be a positive value'
    # Strip path and extension from the .vert file name
    vert_fpath, vert_fname_with_ext = os.path.split(vert_fname)
    vert_fname, vert_ext = os.path.splitext( vert_fname_with_ext )
    if remove_vert_prefix:
        # Remove vert file prefixes 'nc19_', 'nc21_', 'nc23_'
        vert_fname = re.sub('^nc[0-9]{2}_', '', vert_fname)
    # Compute document group subdirectory based on doc_id & maximum allowed 
    # files in a subdirectory (max_files)
    doc_group_dir = int(doc_id / max_files)
    full_path = os.path.join(collection_dir, vert_fname, f'{doc_group_dir}', f'{doc_id}')
    if create_path:
        os.makedirs(full_path, exist_ok=True)
    return full_path


# Smoke-test function get_doc_file_path()
assert get_doc_file_path("balanced_and_reference_corpus", 'nc19_Reference_Corpus.vert', 0, create_path=False).split(os.path.sep) == \
        ['balanced_and_reference_corpus', 'nc19_Reference_Corpus', '0', '0']
assert get_doc_file_path("balanced_and_reference_corpus", 'nc19_Reference_Corpus.vert', 30001, create_path=False).split(os.path.sep)  == \
        ['balanced_and_reference_corpus', 'nc19_Reference_Corpus', '1', '30001']
assert get_doc_file_path("balanced_and_reference_corpus", 'nc19_Reference_Corpus.vert', 60001, create_path=False).split(os.path.sep)  == \
        ['balanced_and_reference_corpus', 'nc19_Reference_Corpus', '2', '60001']
assert get_doc_file_path("balanced_and_reference_corpus", 'nc19_Reference_Corpus.vert', 99999, create_path=False).split(os.path.sep)  == \
        ['balanced_and_reference_corpus', 'nc19_Reference_Corpus', '3', '99999']


def is_document_subdir(dirpath: str):
    '''Determines heuristically whether `dirpath` is a document subdirectory created by `get_doc_file_path()` function. 
       A document subdirectory stores document's files in JSON format. 
       Returns True only if `dirpath` has more than 2 sub-directories and names of the last 2 sub-directories are numeric strings. 
    '''
    dirpath_parts = dirpath.split(os.path.sep)
    return len(dirpath_parts) > 2 and dirpath_parts[-1].isnumeric() and dirpath_parts[-2].isnumeric()


# Smoke-test function is_document_subdir()
assert is_document_subdir(os.path.join('balanced_and_reference_corpus', 'nc19_Reference_Corpus', '0', '0'))
assert is_document_subdir(os.path.join('balanced_and_reference_corpus', 'nc19_Reference_Corpus', '2', '60001'))
assert is_document_subdir(os.path.join('nc19_Reference_Corpus', '3', '99999'))
assert not is_document_subdir(os.path.join('nc19_Reference_Corpus'))
assert not is_document_subdir(os.path.join('nc19_Reference_Corpus', '4'))


def collect_collection_subdirs(collection_dir:str, 
                               full_paths:bool=True, 
                               only_first_level:bool=False, 
                               sort:bool=True):
    '''Collects all subdirectories of `collection_dir`. 
       Returns a list of subdirectory paths (or names).
       
       If `only_first_level` is False (default), then traverses the whole directory tree starting from 
       `collection_dir` and collects only those subdirectories which are document subdirectories 
       (see functions `get_doc_file_path()` and `is_document_subdir` for details).
       If `only_first_level` is True, then only collects first level subdirectories of the `collection_dir`, 
       regardless whether these subdirectories are document subdirectories or not. 
       
       If `sort` is True (default), then collected subdirectories will be sorted. If all collected 
       subdirectories are document subdirectories, then they will be sorted by document numbers/id-s 
       (increasingly). Otherwise, collected subdirectories will be sorted alphabetically.
       
       If `full_paths` is True (default), then collected subdirectories will have full paths (with 
       respect to `collection_dir`). Otherwise, only subdirectory names will be returned.
    '''
    assert os.path.exists(collection_dir), f'(!) Directory path {collection_dir!r} does not exist. '+\
                                           'Cannot collect sub-directories.'
    subdirs = []
    all_document_subdirs = True
    if only_first_level:
        # Collect only first level subdirectories
        for fname in os.listdir(collection_dir):
            fpath = os.path.join(collection_dir, fname)
            if os.path.isdir(fpath):
                if full_paths:
                    subdirs.append(fpath)
                else:
                    subdirs.append(fname)
                if not is_document_subdir(subdirs[-1]):
                    all_document_subdirs = False 
    else:
        # Collect all directories from any depth
        for root, dirs, files in os.walk(collection_dir, topdown=False):
            if is_document_subdir(root):
                if full_paths:
                    subdirs.append(root)
                else:
                    _, subdir = os.path.split(root)
                    subdirs.append(subdir)
    if sort:
        if all_document_subdirs:
            # Document subdirs can be sorted by doc_id-s
            subdirs = sorted(subdirs, key=lambda x: int((x.split(os.path.sep))[-1]))
        else:
            # Other directories can only be sorted alphabetically
            subdirs = sorted(subdirs)
    return subdirs


# ======================================================================
#  Metadata collection
# ======================================================================

class MetaFieldsCollector:
    '''Collects vert file metadata fields from given Text object.'''

    def __init__(self):
        self.meta_attributes = {}
        # skip metadata fields that were added by the parse_enc function, 
        # keep only original fields extracted from the vert file
        self.skip_attributes = \
            ['autocorrected_paragraphs', '_doc_id', '_doc_start_line', '_doc_end_line']

    def collect(self, text_obj: Text):
        # Collects metadata fields from the given Text object
        for meta_key, meta_val in text_obj.meta.items():
            if meta_key not in self.skip_attributes:
                if meta_key not in self.meta_attributes:
                    self.meta_attributes[meta_key]={}
                else:
                    pass

    @property
    def meta_fields(self):
        # Returns collected metadata fields
        return list(self.meta_attributes.keys())

    def output_meta_fields(self, collection_dir:str, vert_fname:str=None, 
                                 remove_vert_prefix:bool=False, 
                                 merge_with_existing:bool=True):
        if vert_fname is not None and len(vert_fname) > 0:
            # Strip path and extension from the .vert file name
            vert_fpath, vert_fname_with_ext = os.path.split(vert_fname)
            vert_fname, vert_ext = os.path.splitext( vert_fname_with_ext )
            if remove_vert_prefix:
                # Remove vert file prefixes 'nc19_', 'nc21_', 'nc23_'
                vert_fname = re.sub('^nc[0-9]{2}_', '', vert_fname)
            full_path = os.path.join( collection_dir, vert_fname )
        else:
            full_path = collection_dir
        assert os.path.exists(full_path), f'(!) Directory path {full_path!r} does not exist. '+\
                                           'Cannot create metadata files.'
        fpath = os.path.join( full_path, 'meta_fields.txt' )
        new_meta_fields = self.meta_fields[:]
        if merge_with_existing and os.path.exists( fpath ):
            # Merge newly collected meta fields with existing ones
            existing_meta_fields = \
                MetaFieldsCollector.load_meta_fields( fpath )
            for ex_field in existing_meta_fields:
                if ex_field not in new_meta_fields:
                    new_meta_fields.append( ex_field )
        with open(fpath, mode='w', encoding='utf-8') as out_f:
            for field in new_meta_fields:
                out_f.write(field)
                out_f.write('\n')

    @staticmethod
    def load_meta_fields(fpath:str):
        meta_fields = []
        assert os.path.exists(fpath), f'(!) Missing input file {fpath}'
        with open(fpath, mode='r', encoding='utf-8') as in_f:
            for line in in_f:
                line = line.rstrip()
                if len(line) > 0:
                    meta_fields.append( line )
        return meta_fields


def normalize_src( src:str ):
    '''Normalizes metadata field `src` before database insertion.
       The intention is to remove year (range) suffixes if the 
       year information can change in future.
    '''
    if isinstance(src, str):
        if src.startswith('Balanced Corpus'):
            return 'Balanced Corpus'
        elif src.startswith('Reference Corpus'):
            return 'Reference Corpus'
        elif src.startswith('Academic Texts'):
            return 'Academic Texts'
        elif src.startswith('Timestamped'):
            return 'Timestamped'
        elif src.startswith('Wikipedia'):
            return 'Wikipedia'
        elif src.startswith('Literature Old'):
            return 'Literature Old'
        elif src.startswith('Literature Contemporary'):
            return 'Literature Contemporary'
    return src


# Smoke-tests for function normalize_src()
assert normalize_src('Web 2021') == 'Web 2021'
assert normalize_src('Web 2023') == 'Web 2023'
assert normalize_src('Wikipedia 2023') == 'Wikipedia'
assert normalize_src('Literature Old 1864–1945') == 'Literature Old'
assert normalize_src('Literature Contemporary 2000–2023') == 'Literature Contemporary'


# ======================================================================
#  Document splitting
# ======================================================================

def split_text_into_smaller_texts( large_text:Text, max_size:int, batch_enveloping_layer:str='sentences' ):
    '''Splits given large_text into smaller texts following the `max_size` limit. 
       Each smaller text has roughly the length of `max_size` characters. 
       Splitting follows the boundaries of `batch_enveloping_layer` (assumingly: 
       sentences or paragraphs layer): a smaller text is allowed to exceed `max_size` 
       if it is required for storing a span of `batch_enveloping_layer`.
       Returns a tuple of two items: a list of smaller text objects and a list of 
       string separators between texts.
    '''
    assert isinstance(batch_enveloping_layer, str) and \
           batch_enveloping_layer in large_text.layers
    assert max_size > 0
    chunks = []
    start_new_chunk = True
    # Chunk the layer into pieces considering the boundaries of the enveloping layer
    for env_span in large_text[ batch_enveloping_layer ]:
        # Initialize a new chunk
        if start_new_chunk:
            chunks.append( [env_span.start, -1] )
            start_new_chunk = False
        # Find total size of the chunk strech
        assert chunks[-1][0] > -1
        chunk_total_size = env_span.end - chunks[-1][0]
        # Check if the chunk exceeds the size limit
        if chunk_total_size >= max_size:
            # Complete the last chunk
            chunks[-1][-1] = env_span.end
            start_new_chunk = True
    if chunks[-1][-1] == -1:
        # Complete the last chunk
        chunks[-1][-1] = large_text[batch_enveloping_layer][-1].end
    # Find chunk separators
    chunk_separators = []
    last_chunk = None
    for [c_start, c_end] in chunks:
        assert c_end > -1
        if last_chunk is not None:
            chunk_separators.append( large_text.text[ last_chunk[-1]:c_start ] )
        last_chunk = [c_start, c_end]
    assert len(chunk_separators) == len(chunks) - 1
    # Extract chunks
    return ( extract_sections(text=large_text, sections=chunks, layers_to_keep=large_text.layers, \
                              trim_overlapping=False), chunk_separators )


def _texts_and_layer_names( texts_obj_list: List[Text] ):
    '''Extracts raw texts and layer names from given list of Text objects.
       Only for testing purposes. '''
    return [(text_obj.text, sorted(list(text_obj.layers))) for text_obj in texts_obj_list]



# Smoke-test function split_text_into_smaller_texts()
assert _texts_and_layer_names( 
            split_text_into_smaller_texts( Text('Tere! Võtaks õige siit kohast uuesti.').tag_layer('morph_analysis'),
                                           max_size=5, batch_enveloping_layer='sentences' )[0] ) == \
       [ ('Tere!', ['compound_tokens', 'morph_analysis', 'sentences', 'tokens', 'words']), 
         ('Võtaks õige siit kohast uuesti.', ['compound_tokens', 'morph_analysis', 'sentences', 'tokens', 'words']) ]
assert _texts_and_layer_names( 
            split_text_into_smaller_texts( Text('Tere! Võtaks õige siit kohast uuesti.').tag_layer('morph_analysis'),
                                           max_size=15, batch_enveloping_layer='sentences' )[0] ) == \
       [('Tere! Võtaks õige siit kohast uuesti.', ['compound_tokens', 'morph_analysis', 'sentences', 'tokens', 'words'])]
assert _texts_and_layer_names( 
            split_text_into_smaller_texts( Text('Tere! Võtaks õige siit kohast uuesti.').tag_layer('morph_analysis'),
                                           max_size=100, batch_enveloping_layer='sentences' )[0] ) == \
       [('Tere! Võtaks õige siit kohast uuesti.', ['compound_tokens', 'morph_analysis', 'sentences', 'tokens', 'words'])]


# ======================================================================
#  Document saving (and splitting if needed)
# ======================================================================

def save_text_obj_as_json_file( text: Text, output_dir:str, max_text_size:int=1000000, batch_splitting_layer:str='sentences', 
                                                            max_layer_size:int=175000000, size_validation_layer:str='morph_analysis' ):
    '''Saves given Text object as a json file into the `output_dir`. 
       
       Before saving, the length of the raw text string is checked and if the length exceeds `max_text_size`, 
       then the input Text object is split into smaller Text objects roughly meeting the `max_text_size`, 
       and saved as separate Text objects. 
       Parameter `batch_splitting_layer` specifies the layer which span boundaries will be respected while 
       splitting the input Text object; the default is 'sentences', which means that `max_text_size` is 
       allowed to be exceeded if it is required to store a last sentence of a smaller Text object. 
       
       Parameters `max_layer_size` and `size_validation_layer` specify additional validation steps: 
       the json string size of `size_validation_layer` is checked and if it exceeds `max_layer_size`, 
       then an Exception will be thrown. This check aims to ensure that the layer meets the size 
       constraints of the Postgres database.
    '''
    assert max_text_size > 0
    if size_validation_layer is not None:
        assert size_validation_layer in text.layers, \
               f'(!) Error on saving {output_dir}: Input text object is missing validation layer {size_validation_layer!r}'
    text_chunks = []
    if len(text.text) > max_text_size:
        # Split large text into smaller chunks
        text_chunks, chunk_separators = \
            split_text_into_smaller_texts( text, max_text_size, batch_enveloping_layer=batch_splitting_layer )
    else:
        text_chunks = [text]
    if size_validation_layer is not None:
        # Validate size of the (morphosyntactic) layer in order to
        # ensure we meet the size constraints of the Postgres database
        for text_chunk in text_chunks:
            assert size_validation_layer in text_chunk.layers
            layer_size = len( layer_to_json(text_chunk[size_validation_layer]) )
            if layer_size > max_layer_size:
                raise Exception(f'(!) Error on saving {output_dir}: layer {size_validation_layer!r} json '+\
                                f'size {layer_size} is exceeding max_layer_size {max_layer_size}.')
            else:
                #print(f'Layer size validation passed at: {layer_size} (max allowed: {max_layer_size})')
                pass
    is_split = len(text_chunks) > 1
    for tid, text_chunk in enumerate(text_chunks):
        # Carry over metadata of the original text
        for meta_key, meta_value in text.meta.items():
            text_chunk.meta[meta_key] =  meta_value
        if is_split:
            text_chunk.meta['_split_document'] = 'true'
            text_chunk.meta['_split_document_part'] = tid+1
        # Generate file name & path
        if is_split:
            outfname = f'doc_{(tid+1):02d}.json'
        else:
            outfname = f'doc.json'
        outfpath = os.path.join(output_dir, outfname)
        # Output file
        text_to_json(text_chunk, file=outfpath)

# ======================================================================
#  Processing speed calculation
# ======================================================================

def find_processing_speed( time_delta:timedelta, word_count:int, return_formatted:bool=True ):
    '''Calculates and returns raw processing speed (words per second).'''
    assert isinstance(time_delta, timedelta)
    assert isinstance(word_count, int) and word_count > 0
    total_sec = time_delta.total_seconds()
    if total_sec > 0:
        words_per_second = word_count / total_sec
    else:
        words_per_second = None
    if return_formatted and words_per_second is not None:
        return '{:.0f}'.format(words_per_second)
    return words_per_second


# ======================================================================
#  Sentence fingerprint computation
# ======================================================================

default_hash_func = hashlib.new('sha256')

def get_sentence_hash( sentence_span: Union[EnvelopingSpan, List[str]], hash_function=default_hash_func ):
    '''Calculates hash fingerprint of the given sentence with the given hash_function.'''
    if isinstance(sentence_span, EnvelopingSpan):
        sent_words = [w.text for w in sentence_span]
    elif isinstance(sentence_span, list) and all(isinstance(w, str) for w in sentence_span):
        sent_words = sentence_span
    else:
        raise TypeError(f'(!) Unexpected input sentence_span: {sentence_span!r}.'+\
                         ' Expected type of Union[EnvelopingSpan, List[str]].')
    sent_words_b_str = str(sent_words).encode('utf8')
    hash_func = hash_function.copy()
    hash_func.update(sent_words_b_str)
    return hash_func.hexdigest()


# Smoke-test function get_sentence_hash()
assert get_sentence_hash(["d", "e", "c", '1'], default_hash_func) == get_sentence_hash(["d", "e", "c", '1'], default_hash_func)
assert get_sentence_hash(["a", "b", "c"], default_hash_func) == get_sentence_hash(["a", "b", "c"], default_hash_func)
assert get_sentence_hash(['2', '1'], default_hash_func) == get_sentence_hash(['2', '1'], default_hash_func)


class SentenceHashRetagger(Retagger):
    """Adds a hash fingerprint to each sentence. 
       The main purpose is to track differences/changes in sentence segmentation."""
    
    conf_param = ['algorithm', '_hash_func']
    
    def __init__(self, output_layer:str='sentences', algorithm:str='sha256'):
        self.algorithm = algorithm
        self.output_layer = output_layer
        self.input_layers = [output_layer]
        self.output_attributes =(self.algorithm,)
        self._hash_func = hashlib.new(self.algorithm)

    def _change_layer(self, text, layers, status):
        sentences_layer = layers[self.output_layer]
        if self.algorithm not in sentences_layer.attributes:
            sentences_layer.attributes += (self.algorithm, )
        for sentence in sentences_layer:
            # Compute hash
            sent_hash = get_sentence_hash(sentence, self._hash_func)
            # Collect old annotations
            records = []
            for annotation in sentence.annotations:
                record = {}
                for attr in sentences_layer.attributes:
                    if attr != self.algorithm:
                        record[attr] = annotation[attr]
                records.append(record)
            # Update annotations
            sentence.clear_annotations()
            for record in records:
                record[self.algorithm] = sent_hash
                sentence.add_annotation( Annotation(sentence, **record) )


class SentenceHashRemover(Retagger):
    """Removes a hash fingerprints from each sentence. 
       Before inserting sentence annotation to the database, fingerprints need to be removed."""
    
    conf_param = ['attrib']
    
    def __init__(self, output_layer:str='sentences', attrib:str='sha256'):
        self.attrib = attrib
        self.output_layer = output_layer
        self.input_layers = [output_layer]
        self.output_attributes =()

    def _change_layer(self, text, layers, status):
        sentences_layer = layers[self.output_layer]
        sentences_layer.attributes = \
            tuple( [a for a in sentences_layer.attributes if a != self.attrib] )
        for sentence in sentences_layer:
            # Collect old annotations
            records = []
            for annotation in sentence.annotations:
                record = {}
                for attr in sentences_layer.attributes:
                    if attr != self.attrib:
                        record[attr] = annotation[attr]
                records.append(record)
            # Update annotations
            sentence.clear_annotations()
            for record in records:
                sentence.add_annotation( Annotation(sentence, **record) )


def create_sentences_hash_map( sentences_layer: Layer, hash_attrib:str='sha256' ):
    '''For all sentences in the layer, creates a dictionary mapping from sentence 
       hash fingerprint to corresponding sentence spans. 
       Note the more than one sentence can be associated with one fingerprint, 
       thus a fingerprint maps to a list of sentences.
       This function requires that the sentences_layer has hash_attrib.
    '''
    assert hash_attrib in sentences_layer.attributes, \
        f'(!) Unable to create sentence hash map: sentences layer is missing attribute {hash_attrib}.'
    hash_map = {}
    for sentence in sentences_layer:
        sent_hash = sentence.annotations[0][hash_attrib]
        assert isinstance(sent_hash, str)
        if sent_hash not in hash_map.keys():
            hash_map[sent_hash] = []
        hash_map[sent_hash].append(sentence)
    return hash_map


# ======================================================================
#  Pre- and post-processing for syntax analysis
# ======================================================================

def convert_original_morph_to_stanza_input_morph(morph_layer: Layer):
    '''Converts (extended) morphological analysis layer imported from the 
       ENC corpus to StanzaSyntaxTagger's input format. Or vice versa, 
       if values have already been swapped previously. 
       
       Basically, swaps values of 'form' and 'extended_form' attributes 
       in the layer. The input layer will be modified, and the method 
       returns nothing.
    '''
    assert 'extended_form' in morph_layer.attributes, \
        '(!) Morph layer is missing attribute "extended_form".'
    for morph_span in morph_layer:
        assert len(morph_span.annotations) == 1
        annotations = morph_span.annotations[0]
        annotations_dict = {a:annotations[a] for a in morph_layer.attributes}
        # Swap 'form' and 'extended_form' values
        temp_form = annotations_dict['form']
        # Replace the form with extended_form value for syntax
        annotations_dict['form'] = annotations_dict['extended_form']
        # Replace extended_form with old form to keep the value for later usage
        annotations_dict['extended_form'] = temp_form
        morph_span.clear_annotations()
        assert len(morph_span.annotations) == 0
        morph_span.add_annotation( Annotation(morph_span, annotations_dict) )
        assert len(morph_span.annotations) == 1
    if 'swapped' not in morph_layer.meta:
        morph_layer.meta['swapped'] = True
    else:
        morph_layer.meta['swapped'] = not morph_layer.meta['swapped']


def construct_db_syntax_layer(text_obj: Text, morph_layer: Layer, syntax_layer: Layer, 
                              output_layer:str, words_layer:str='words', 
                              add_parent_and_children:bool=True):
    '''Merges morph_layer and syntax_layer into one morphosyntactic layer to be written into database. 
       Attributes of the new layer will be:
       ('id', 'lemma', 'root_tokens', 'clitic', 'xpostag', 'feats', 'extended_feats', 'head', 'deprel').
       If add_parent_and_children is set (default), also adds attributes 'parent_span' and 'children' 
       to the layer.
       Returns the new layer.
    '''
    assert len(morph_layer) == len(syntax_layer)
    if morph_layer.meta.get('swapped', False):
        # Sanity check
        raise ValueError('(!) Morph layer has swapped "form" and "extended_form" values. '+\
                         "Please swap values back via convert_original_morph_to_stanza_input_morph(...).")
    layer = Layer(name=output_layer,
                  text_object=text_obj,
                  attributes=('id', 'lemma', 'root_tokens', 'clitic', 'xpostag', 'feats', 'extended_feats', 'head', 'deprel'),
                  parent=words_layer,
                  ambiguous=False )
    word_id = 0
    for morph_span, syntax_span in zip(morph_layer, syntax_layer):
        word_span = text_obj[words_layer][word_id]
        assert morph_span.base_span == syntax_span.base_span
        assert word_span.base_span == syntax_span.base_span
        morph_ann = morph_span.annotations[0]
        syntax_ann = syntax_span.annotations[0]
        annotation_dict = {}
        annotation_dict['id'] = syntax_ann['id']
        annotation_dict['lemma'] = syntax_ann['lemma']
        annotation_dict['root_tokens'] = morph_ann['root_tokens']
        annotation_dict['clitic'] = morph_ann['clitic']
        annotation_dict['xpostag'] = syntax_ann['xpostag']
        annotation_dict['feats'] = morph_ann['form']
        annotation_dict['extended_feats'] = morph_ann['extended_form']
        annotation_dict['head'] = syntax_ann['head']
        annotation_dict['deprel'] = syntax_ann['deprel']
        layer.add_annotation(morph_span.base_span, annotation_dict)
        word_id += 1
    if add_parent_and_children:
        syntax_dependency_retagger = SyntaxDependencyRetagger(syntax_layer=output_layer)
        syntax_dependency_retagger.change_layer(text_obj.text, {output_layer:layer}, {})
        layer.serialisation_module = syntax_v0.__version__
    assert len(layer) == len(syntax_layer)
    return layer


# ======================================================================
#  Load/Create layer templates (for database insertion)
# ======================================================================

def load_collection_layer_templates(configuration: dict):
    '''
    Creates collection layer templates based on the first annotated JSON document. 
    Assumes all other JSON documents have the same structure / same layers as the first 
    document. 
    The `configuration` is used to find subdirectories of collection's documents. 
    Raises exceptions if no subdirectories nor documents are found. 
    Returns a list of Layer objects.
    '''
    assert 'collection' in configuration, f'(!) Configuration is missing "collection" parameter.'
    vert_subdirs = collect_collection_subdirs(configuration['collection'], only_first_level=True, full_paths=False)
    if len(vert_subdirs) == 0:
        raise FileNotFoundError(f'(!) No document subdirectories found from collection dir {configuration["collection"]!r}')
    # Collect the first document from JSON files. All other documents should have the same structure / same layers
    first_text = None
    full_subdir = os.path.join( configuration['collection'], vert_subdirs[0] )
    # Optimization: try to find the first JSON document sub-directory without listing all sub-directories
    first_json_subdir = os.path.join( full_subdir, '0', '0' )
    if not os.path.exists(first_json_subdir) or not os.path.isdir(first_json_subdir): 
        # If the first document sub-directory was not available, then list all sub-directories to get the first one
        document_subdirs = collect_collection_subdirs(full_subdir, only_first_level=False, full_paths=True)
        if len(document_subdirs) == 0:
            raise FileNotFoundError(f'(!) No JSON document subdirectories found from collection dir {full_subdir!r}')
        first_json_subdir = document_subdirs[0]
    for fname in sorted( os.listdir(first_json_subdir) ):
        if fname.startswith('doc') and fname.endswith('.json'):
            # Load Text object
            fpath = os.path.join(first_json_subdir, fname)
            first_text = json_to_text(file = fpath)
            # Break, no need to look further
            break
    if first_text is not None:
        # Create layer templates (simply erase annotations)
        templates = []
        for layer_obj in first_text.sorted_layers():
            layer_dict = layer_to_dict(layer_obj)
            # Remove all annotations from the layer
            if 'spans' in layer_dict.keys():
                layer_dict['spans'] = []
            elif 'relations' in layer_dict.keys():
                layer_dict['relations'] = []
            # Remove layer metadata
            has_dct = 'document_creation_time' in layer_dict['meta']
            layer_dict['meta'] = {}
            if has_dct:
                # Set default DCT
                layer_dict['meta']['document_creation_time'] = 'XXXX-XX-XX'
            templates.append( dict_to_layer(layer_dict) )
        return templates
    else:
        raise FileNotFoundError(f'(!) No JSON documents found from dir {first_json_subdir!r}')


def rename_layer(layer: Layer, renaming_map:dict=None):
    '''
    Renames Layer by changing its name to a new name from `renaming_map`. 
    `renaming_map` must be a dictionary mapping from old layer names to 
    new ones.
    If layer has `parent` or it is `enveloping`, and corresponding 
    layers are available in the `renaming_map`, then also renames those 
    layers correspondingly.
    Returnes renamed layer.
    '''
    if renaming_map is not None and isinstance(renaming_map, dict):
        if isinstance(layer, Layer):
            layer.name = renaming_map.get( layer.name, layer.name )
            assert isinstance(layer.name, str)
            if isinstance(layer.parent, str):
                layer.parent = renaming_map.get( layer.parent, layer.parent )
                assert isinstance(layer.parent, str)
            if isinstance(layer.enveloping, str):
                layer.enveloping = renaming_map.get( layer.enveloping, layer.enveloping )
                assert isinstance(layer.enveloping, str)
        else:
            raise NotImplementedError(f'(!) Renaming {type(layer)} not implemented')
        return layer
    else:
        # Nothing to do here
        return layer


# ======================================================================
#  Word normalization (limited)
# ======================================================================

def normalize_words_w_to_v(words_layer: Layer, doc_path: str=None):
    '''
    Normalizes words_layer by changing 'w' -> 'v' 
    (e.g. 'Jüripäew' -> 'Jüripäev', 'wõtavad' -> 'võtavad'). 
    This can be useful for processing Old Estonian 
    (documents written before ~1930). 
    '''
    if 'normalized_form' in words_layer.attributes:
        for word_span in words_layer:
            word_text = word_span.text
            if 'w' in word_text.lower():
                word_span.clear_annotations()
                word_norm = word_text.replace('w', 'v')
                word_norm = word_norm.replace('W', 'V')
                word_span.add_annotation( normalized_form=word_norm )
    else:
        raise Exception(f'(!) Document {doc_path} is missing "normalized_form" in its "words" layer.')

def clear_words_normalized_form(words_layer: Layer):
    '''
    Deletes all 'normalized_form' values from the words_layer.
    '''
    if 'normalized_form' in words_layer.attributes:
        for word_span in words_layer:
            word_span.clear_annotations()
            word_span.add_annotation( normalized_form=None )
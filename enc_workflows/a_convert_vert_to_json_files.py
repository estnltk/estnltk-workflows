#
#   Converts ENC vert files to estnltk Text objects and saves as json files.
#   Optional processing:
#   * Splits large documents into smaller ones if documents exceed maximum length threshold;
#   * Computes sha256 fingerprints for sentence segmentations;
#
#   Requires name of a configuration INI file as an input argument. 
# 

import json
import re, sys
import os, os.path
from collections import defaultdict

from datetime import datetime

import warnings

from estnltk import Text
from estnltk.corpus_processing.parse_enc import parse_enc_file_iterator

from x_utils import get_doc_file_path
from x_utils import MetaFieldsCollector
from x_utils import save_text_obj_as_json_file
from x_utils import SentenceHashRetagger

from x_configparser import parse_configuration

# Maximum allowed sentence length. Throws a warning if exceeded
maximum_sentence_length = 1000

# Maximum text size in characters. 
# If exceeded, then document will be split into smaller documents.
maximum_text_size = 5000000

# Maximum layer json size in characters. 
# If exceeded, then an exception will be thrown.
# Only morph_analysis layer will be checked
max_layer_size = 175000000

if len(sys.argv) > 1:
    input_fname = sys.argv[1]
    if os.path.isfile(input_fname):
        # Get & validate configuration parameters
        configuration = None
        if (input_fname.lower()).endswith('.ini'):
            configuration = parse_configuration( input_fname )
        else:
            raise Exception('(!) Input file {!r} with unexpected extension, expected a configuration INI file.'.format(input_fname))
        if configuration is not None:
            split_docs = 0
            converted_docs = 0
            focus_block = None
            too_long_sentences = 0
            # Get divisor & reminder for data parallelization
            for sys_arg in sys.argv[2:]:
                m = re.match('(\d+)[,:;](\d+)', sys_arg)
                if m:
                    divisor = int(m.group(1))
                    assert divisor > 0
                    remainder = int(m.group(2))
                    assert remainder < divisor
                    focus_block = (divisor, remainder)
                    print(f'Data parallelization: focus on block {focus_block}.')
                    break
            collection_directory = configuration['collection']
            focus_doc_ids = configuration['focus_doc_ids']
            total_start_time = datetime.now()
            logger = None
            if configuration['log_json_conversion']:
                from x_logging import init_logger
                log_fname = f'json_conv_{collection_directory}'
                logger = init_logger( log_fname, configuration['json_conversion_log_level'], focus_block )
            global_meta_collector = MetaFieldsCollector() # collect meta over all vert files
            sentence_hash_retagger = \
                SentenceHashRetagger() if configuration['add_sentence_hashes'] else None
            for input_vert_fname in configuration['vert_files']:
                _, vert_fname = os.path.split(input_vert_fname)
                print(f'Processing {vert_fname} ...')
                meta_collector = MetaFieldsCollector() # collect meta over this vert file
                for text_obj in parse_enc_file_iterator( input_vert_fname, line_progressbar='ascii', 
                                                                           focus_block=focus_block, 
                                                                           focus_doc_ids=focus_doc_ids, 
                                                                           restore_morph_analysis=True, 
                                                                           extended_morph_form=True,
                                                                           add_document_index=True,
                                                                           original_layer_prefix='',
                                                                           logger=logger):
                    assert text_obj.layers == {'words', 'tokens', 'paragraphs', 'morph_analysis', 
                                               'compound_tokens', 'word_chunks', 'sentences'}
                    assert [key in text_obj.meta for key in ['_doc_id', '_doc_start_line', '_doc_end_line']]
                    # Remove redundant layers
                    text_obj.pop_layer('compound_tokens')
                    text_obj.pop_layer('tokens')
                    text_obj.pop_layer('word_chunks')
                    text_obj.pop_layer('paragraphs')
                    # Rename 'morph_analysis' -> 'morph_analysis_ext' to avoid confusion
                    # ( StanzaSyntaxTagger has some hard-coded checks that will raise an 
                    #  alarm if input_type 'morph_extended' goes with layer named 'morph_analysis' )
                    if configuration.get('rename_morph_layer', None) is not None:
                        morph_analysis = text_obj.pop_layer('morph_analysis')
                        morph_analysis.name = configuration['rename_morph_layer']
                        text_obj.add_layer(morph_analysis)
                    # Document id inside the vert file (not to be mistaken with 'id' in <doc> tag) 
                    doc_file_id = text_obj.meta['_doc_id']
                    # Collect document metadata
                    meta_collector.collect(text_obj)
                    global_meta_collector.collect(text_obj)
                    # Validate that sentence lengths do not exceed maximum sentence length and 
                    # throw corresponding warnings
                    bad_sentences = []
                    bad_sentence_ids = []
                    for sid, s in enumerate(text_obj['sentences']):
                        if len(s) > maximum_sentence_length:
                            bad_sentences.append( list(s.text) )
                            bad_sentence_ids.append( sid )
                    if len(bad_sentences) > 0:
                        warnings.warn(f"Document {doc_file_id} contains sentence(s) exceeding maximum sentence length {maximum_sentence_length}:" )
                        for bs_id, bs in enumerate(bad_sentences):
                            warnings.warn(f"{bad_sentence_ids[bs_id]}: {bs[:100]!r}...{bs[-100:]!r}" )
                            too_long_sentences += 1
                    # Calculate sentence fingerprints
                    if sentence_hash_retagger is not None:
                        sentence_hash_retagger.retag( text_obj )
                    # Determine output path and write document into output file:
                    json_file_path = get_doc_file_path(collection_directory, input_vert_fname, int(doc_file_id))
                    save_text_obj_as_json_file(text_obj, json_file_path, max_text_size=maximum_text_size, batch_splitting_layer='sentences', 
                                                                         max_layer_size=max_layer_size, size_validation_layer='morph_analysis_ext' )
                    # Post-check:
                    # 1) validate that the document json file was created
                    # 2) check whether the document was split or not
                    found_doc_files = []
                    for fname in os.listdir(json_file_path):
                        if fname.startswith('doc') and fname.endswith('.json'):
                            found_doc_files.append(fname)
                    if len(found_doc_files) > 0:
                        converted_docs += 1
                    else:
                        raise Exception(f'(!) After-check fail: did not find any document json files from {json_file_path!r}')
                    if len(found_doc_files) > 1:
                        split_docs += 1
                if configuration['collect_meta_fields']:
                    meta_collector.output_meta_fields(collection_directory, vert_fname=input_vert_fname)
            if configuration['collect_meta_fields']:
                global_meta_collector.output_meta_fields(collection_directory)
            if converted_docs > 0:
                print()
                print(f' =={collection_directory}==')
                print(f' Converted documents:  {converted_docs}')
                print(f'     split documents:  {split_docs}')
                if too_long_sentences > 0:
                    print()
                    print(f'(!) too long sentences:  {too_long_sentences}')
                print(f'  Total time elapsed:  {datetime.now()-total_start_time}')
        else:
            print(f'Missing or bad configuration in {input_fname!r}. Unable to get configuration parameters.')
else:
    print('Config INI name required as an input argument.')


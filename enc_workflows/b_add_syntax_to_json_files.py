#
#   Adds syntactic annotations to ENC json files.
#
#   Requires name of a configuration INI file as an input argument. 
# 
#   Requires estnltk & estnltk_neural v1.7.3+
#

import json
import re, sys
import os, os.path

from datetime import datetime

import warnings

from tqdm import tqdm

from estnltk import Text
from estnltk.converters import json_to_text
from estnltk.converters import text_to_json

from estnltk_neural.taggers import StanzaSyntaxTagger

from x_utils import collect_collection_subdirs
from x_utils import convert_original_morph_to_stanza_input_morph
from x_utils import construct_db_syntax_layer

from x_configparser import parse_configuration

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
            annotated_docs = 0
            annotated_split_docs = 0
            focus_block = None
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
            focus_doc_ids = configuration['focus_doc_ids']
            collection_directory = configuration['collection']
            logger = None  # TODO
            syntax_layer_name = configuration.get('syntax_layer_name', None)
            remove_old_document = False   # TODO
            if syntax_layer_name is None:
                raise Exception('(!) Input configuration {} does not list syntax_layer_name. '.format(input_fname) +\
                                'Probably missing section "syntax_layer" with option "name".')
            add_layer_creation_time = configuration.get('add_layer_creation_time', False)
            # Initialize tagger
            # TODO: make more configurable
            input_morph_layer = 'morph_analysis_ext'
            input_words_layer = 'words'
            input_sentences_layer = "sentences"
            syntax_parser = StanzaSyntaxTagger( input_type='morph_extended', \
                                                words_layer=input_words_layer, \
                                                sentences_layer=input_sentences_layer, \
                                                input_morph_layer=input_morph_layer, \
                                                random_pick_seed=1,
                                                use_gpu=configuration.get('use_gpu', False) )
            # Iterate over all vert subdirs and all document subdirs within these subdirs
            vert_subdirs = collect_collection_subdirs(configuration['collection'], only_first_level=True, full_paths=False)
            for vert_subdir in vert_subdirs:
                full_subdir = os.path.join(configuration['collection'], vert_subdir)
                print(f'Processing {vert_subdir} ...')
                # Fetch all the document subdirs
                document_subdirs = collect_collection_subdirs(full_subdir, only_first_level=False, full_paths=True)
                for doc_subdir in tqdm(document_subdirs, ascii=True):
                    document_id = int( doc_subdir.split(os.path.sep)[-1] )
                    # Apply block filter
                    if focus_block is not None and document_id % focus_block[0] != focus_block[1]:
                        # Skip the block
                        continue
                    # Collect document json files
                    found_doc_files = []
                    for fname in os.listdir(doc_subdir):
                        if '_syntax' in fname:
                            # Skip already annotated documents
                            continue
                        if fname.startswith('doc') and fname.endswith('.json'):
                            found_doc_files.append(fname)
                    if len( found_doc_files ) == 0:
                        warnings.warn( f'(!) No document json files found from {doc_subdir!r}' )
                    else:
                        # Process json files
                        for fname in found_doc_files:
                            fpath = os.path.join(doc_subdir, fname)
                            text_obj = json_to_text(file = fpath)
                            if input_morph_layer not in text_obj.layers:
                                raise Exception(f'(!) Input json document {fpath!r} is missing {input_morph_layer!r} layer. '+\
                                                f'Available layers: {text_obj.layers!r}.')
                            assert ('form' in text_obj[input_morph_layer].attributes) and \
                                   ('extended_form' in text_obj[input_morph_layer].attributes)
                            # Swap 'form' and 'extended_form' values for stanza parser
                            convert_original_morph_to_stanza_input_morph( text_obj[input_morph_layer] )
                            # Tag syntax
                            syntax_parser.tag( text_obj )
                            # Swap 'form' and 'extended_form' values back
                            convert_original_morph_to_stanza_input_morph( text_obj[input_morph_layer] )
                            # Construct syntax layer for database
                            db_syntax_layer = construct_db_syntax_layer(text_obj, 
                                                                        text_obj[input_morph_layer], 
                                                                        text_obj[syntax_parser.output_layer], 
                                                                        syntax_layer_name, 
                                                                        words_layer=input_words_layer, 
                                                                        add_parent_and_children=True)
                            text_obj.add_layer( db_syntax_layer )
                            # Remove other layers
                            text_obj.pop_layer( syntax_parser.output_layer )
                            text_obj.pop_layer( input_morph_layer )
                            # Finally, save the results
                            if not remove_old_document:
                                fpath_fname, fpath_ext = os.path.splitext( fpath )
                                new_fpath = f'{fpath_fname}_syntax{fpath_ext}'
                                text_to_json( text_obj, file=new_fpath )
                            else:
                                text_to_json( text_obj, file=fpath )
                            #
                            # TODO: measure processing time in previous steps
                            #
                        annotated_docs += 1
                        if len(found_doc_files) > 1:
                            annotated_split_docs += 1
            if annotated_docs > 0:
                #
                # TODO: report processing times
                #
                print()
                print(f' =={collection_directory}==')
                print(f' Annotated documents:  {annotated_docs}')
                print(f'    incl. split docs:  {annotated_split_docs}')
        else:
            print(f'Missing or bad configuration in {input_fname!r}. Unable to get configuration parameters.')
else:
    print('Config INI name required as an input argument.')


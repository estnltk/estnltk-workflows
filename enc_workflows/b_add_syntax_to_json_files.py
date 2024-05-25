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
from x_stanza_tagger import DualStanzaSyntaxTagger
from x_stanza_tagger import StanzaSyntaxTaggerWithChunking

from x_utils import collect_collection_subdirs
from x_utils import convert_original_morph_to_stanza_input_morph
from x_utils import construct_db_syntax_layer
from x_utils import find_processing_speed

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
            annotated_words = 0
            annotated_sentences = 0
            annotated_split_docs = 0
            skipped_annotated_docs = 0 # documents that were already annotated
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
            # Get collection's parameters
            collection_directory = configuration['collection']
            # Get layer annotation parameters
            total_start_time = datetime.now()
            syntax_layer_name = configuration.get('output_syntax_layer', None)
            if syntax_layer_name is None:
                raise Exception('(!) Input configuration {} does not list syntax_layer_name. '.format(input_fname) +\
                                'Probably missing section "add_syntax_layer" with option "name".')
            skip_annotated = configuration.get('skip_annotated', True)
            add_layer_creation_time = configuration.get('add_layer_creation_time', False)
            #
            # Get output_mode
            # NEW_FILE  -- creates a new json file by adding `output_file_infix` to the old file name;
            # OVERWRITE -- overwrites the old json file with new content;
            # Applies to both NEW_FILE and OVERWRITE:
            # if `output_remove_morph` is set, then removes the input morph layer from the output document;
            #
            output_mode         = configuration['output_mode']
            output_file_infix   = configuration['output_file_infix']
            output_remove_morph = configuration['output_remove_morph']
            logger = None  # TODO
            # Initialize tagger
            input_morph_layer = configuration['input_morph_layer']
            input_words_layer = configuration['input_words_layer']
            input_sentences_layer = configuration['input_sentences_layer']
            max_words_in_sentence = configuration['parsing_max_words_in_sentence']
            if configuration['long_sentences_strategy'] == 'NONE':
                syntax_parser = StanzaSyntaxTagger( input_type='morph_extended', \
                                                    words_layer=input_words_layer, \
                                                    sentences_layer=input_sentences_layer, \
                                                    input_morph_layer=input_morph_layer, \
                                                    random_pick_seed=1,
                                                    use_gpu=configuration.get('use_gpu', False) )
            elif configuration['long_sentences_strategy'] == 'CHUNKING':
                syntax_parser = StanzaSyntaxTaggerWithChunking( input_type='morph_extended', \
                                                    words_layer=input_words_layer, \
                                                    sentences_layer=input_sentences_layer, \
                                                    input_morph_layer=input_morph_layer, \
                                                    random_pick_seed=1,
                                                    max_words_in_sentence=max_words_in_sentence,
                                                    use_gpu=configuration.get('use_gpu', False) )
            elif configuration['long_sentences_strategy'] == 'USE_CPU':
                syntax_parser = DualStanzaSyntaxTagger( input_type='morph_extended', \
                                                        words_layer=input_words_layer, \
                                                        sentences_layer=input_sentences_layer, \
                                                        input_morph_layer=input_morph_layer, \
                                                        random_pick_seed=1,
                                                        use_gpu=configuration.get('use_gpu', False) )
            else:
                raise ValueError(f'(!) Unexpected "long_sentences_strategy" value {configuration["long_sentences_strategy"]}.')
            print(f'Using {syntax_parser.__class__.__name__}.' )
            # Iterate over all vert subdirs and all document subdirs within these subdirs
            vert_subdirs = collect_collection_subdirs(configuration['collection'], only_first_level=True, full_paths=False)
            if len(vert_subdirs) == 0:
                warnings.warn(f'(!) No document subdirectories found from collection dir {configuration["collection"]!r}')
            for vert_subdir in vert_subdirs:
                subdir_start_time = datetime.now()
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
                        if output_file_infix in fname:
                            # Skip already annotated documents
                            skipped_annotated_docs += 1
                            continue
                        if fname.startswith('doc') and fname.endswith('.json'):
                            found_doc_files.append(fname)
                    if len( found_doc_files ) == 0:
                        warnings.warn( f'(!) No document json files found from {doc_subdir!r}' )
                    else:
                        # Process json files
                        local_annotated_docs = 0
                        for fname in found_doc_files:
                            fpath = os.path.join(doc_subdir, fname)
                            try:
                                text_obj = json_to_text(file = fpath)
                                if skip_annotated and syntax_layer_name in text_obj.layers:
                                    # Skip document (already annotated)
                                    skipped_annotated_docs += 1
                                    continue
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
                                if add_layer_creation_time:
                                    # Add layer creation timestamp
                                    db_syntax_layer.meta['created_at'] = \
                                        (datetime.now()).strftime('%Y-%m-%d')
                                # Remove the temporary syntax layer
                                text_obj.pop_layer( syntax_parser.output_layer )
                                # Remove the input morph layer
                                if output_remove_morph:
                                    text_obj.pop_layer( input_morph_layer )
                                # Records statistics
                                annotated_words += len( text_obj[db_syntax_layer.name] )
                                annotated_sentences += len( text_obj[input_sentences_layer] )
                                # Finally, save the results
                                if output_mode == 'NEW_FILE':
                                    fpath_fname, fpath_ext = os.path.splitext( fpath )
                                    new_fpath = f'{fpath_fname}{output_file_infix}{fpath_ext}'
                                    assert new_fpath != fpath
                                    text_to_json( text_obj, file=new_fpath )
                                else:
                                    text_to_json( text_obj, file=fpath )
                                local_annotated_docs += 1
                            except Exception as err:
                                raise Exception(f'Failed at processing document {fpath!r} due to an error: ') from err
                        annotated_docs += 1 if local_annotated_docs > 0 else 0
                        if local_annotated_docs > 1:
                            annotated_split_docs += local_annotated_docs
                print(f'Processing {vert_subdir} took {datetime.now()-subdir_start_time}.')
            if annotated_docs > 0 or skipped_annotated_docs > 0:
                print()
                print(f' =={collection_directory}==')
                if skipped_annotated_docs > 0:
                    print(f'   Skipped documents:  {skipped_annotated_docs} (already annotated)')
                print(f' Annotated documents:  {annotated_docs}')
                print(f'    incl. split docs:  {annotated_split_docs}')
                print(f' Annotated sentences:  {annotated_sentences}')
                print(f'     Annotated words:  {annotated_words}')
                print()
                print(f'  Total time elapsed:  {datetime.now()-total_start_time}')
                if annotated_words > 0:
                    speed_str = find_processing_speed(datetime.now()-total_start_time, annotated_words)
                    with_gpu_str = '(with GPU)' if syntax_parser.use_gpu else ''
                    print(f'    Processing speed:  ~{speed_str} words/sec {with_gpu_str}')
            else:
                warnings.warn(f'(!) No document JSON files found from subdirectories of the collection dir {configuration["collection"]!r}')
        else:
            print(f'Missing or bad configuration in {input_fname!r}. Unable to get configuration parameters.')
else:
    print('Config INI name required as an input argument.')


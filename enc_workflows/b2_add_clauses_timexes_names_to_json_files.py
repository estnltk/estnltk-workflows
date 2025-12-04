#
#   Adds clauses, timexes and named entities layers to ENC json files. 
#
#   Named entities are annotated via the traditional machine learning model 
#   provided by Tkachenko et al (2013) ( https://aclanthology.org/W13-2412/ ).
#
#   Requires name of a configuration INI file as an input argument. 
# 
#   Requires estnltk v1.7.4+
#            Java (for tagging clauses and timexes);
#

import json
import re, sys
import os, os.path

from datetime import datetime, timedelta

import warnings
from collections import defaultdict

from tqdm import tqdm

from estnltk import Text, Layer
from estnltk.converters import json_to_text
from estnltk.converters import text_to_json
from estnltk.converters import layer_to_json

from estnltk_core.layer_operations import flatten

from estnltk.taggers import PretokenizedTextCompoundTokensTagger
from estnltk.taggers import VabamorfTagger

from estnltk.taggers import ClauseSegmenter
from estnltk.taggers import NerTagger
from estnltk.taggers import TimexTagger

from x_utils import collect_collection_subdirs
from x_utils import find_processing_speed

from x_utils import normalize_words_w_to_v
from x_utils import clear_words_normalized_form

from x_dct import detect_reference_time_from_meta
from x_dct import get_reference_time_type

from x_configparser import parse_configuration


def _record_annotated_clause_types(clauses_layer, annotated_types_dict):
    '''Records detailed statistics about clause types.'''
    assert 'clause_type' in clauses_layer.attributes
    for clause in clauses_layer:
        cl_type = clause.annotations[0]['clause_type']
        annotated_types_dict[str(cl_type)] += 1
    return annotated_types_dict


def _record_annotated_timex_types(timexes_layer, annotated_types_dict, debug=False):
    '''Records detailed statistics about timex types.'''
    assert 'type' in timexes_layer.attributes
    assert 'temporal_function' in timexes_layer.attributes
    for timex in timexes_layer:
        t_type = timex.annotations[0]['type']
        t_func = timex.annotations[0]['temporal_function']
        if t_type in ['DATE', 'TIME']:
            if not bool(t_func):
                t_type = f'{t_type}_ABS'
            else:
                t_type = f'{t_type}_REL'
        if debug:
            print(f'> {timex.text} -> {t_type}')
        annotated_types_dict[str(t_type)] += 1
    return annotated_types_dict


def _record_annotated_name_types(ner_layer, annotated_types_dict):
    '''Records detailed statistics about named entity types.'''
    assert 'nertag' in ner_layer.attributes
    for named_entity in ner_layer:
        ne_type = named_entity.annotations[0]['nertag']
        annotated_types_dict[str(ne_type)] += 1
    return annotated_types_dict


def _display_type_statistics(annotated_types_dict):
    '''Creates an onliner string summarizing proportions of different types of annotations.'''
    total = sum([v for v in annotated_types_dict.values()])
    str_to_join = []
    for k in sorted( annotated_types_dict.keys(), key=annotated_types_dict.get, reverse=True ):
        if total > 0:
            proportion = (100.0*annotated_types_dict.get(k))/total
        else:
            proportion = 0.0
        str_to_join.append( f'{k} {proportion:.2f}%' )
    return ', '.join(str_to_join)


def _display_time_delta_statistics(time_deltas_dict):
    '''Creates an onliner string summarizing proportions of time spent on different sub-processes.'''
    total_sec = sum([v.total_seconds() for v in time_deltas_dict.values()])
    str_to_join = []
    for k in sorted( time_deltas_dict.keys(), \
                     key=lambda x : time_deltas_dict[x].total_seconds(), \
                     reverse=True ):
        if total_sec > 0:
            proportion = (100.0*time_deltas_dict[k].total_seconds())/total_sec
        else:
            proportion = 0.0
        str_to_join.append( f'{k} {proportion:.2f}%' )
    return ', '.join(str_to_join)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        input_fname = sys.argv[1]
        if os.path.isfile(input_fname):
            # Get & validate configuration parameters
            configuration = None
            if (input_fname.lower()).endswith('.ini'):
                configuration = parse_configuration( input_fname, ignore_missing_vert_file=True )
            else:
                raise Exception('(!) Input file {!r} with unexpected extension, expected a configuration INI file.'.format(input_fname))
            if configuration is not None:
                # Annotate only first N documents [for debugging]
                annotate_only_first = 0
                annotated_docs = 0
                annotated_words = 0
                annotated_sentences = 0
                annotated_split_docs = 0
                skipped_annotated_docs = 0 # documents that were already annotated
                annotated_clauses = 0
                annotated_timexes = 0
                annotated_names   = 0
                annotated_clause_types = defaultdict(int)
                annotated_timex_types  = defaultdict(int)
                annotated_name_types   = defaultdict(int)
                annotated_dct_types    = defaultdict(int)
                time_deltas = dict()
                for sub_process in ['pre', 'aux', 'clauses', 'timexes', 'names', 'post']:
                    time_deltas[sub_process] = timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, 
                                                         minutes=0, hours=0, weeks=0)
                focus_block = None
                # Get divisor & reminder for data parallelization
                for sys_arg in sys.argv[2:]:
                    m = re.match(r'(\d+)[,:;](\d+)', sys_arg)
                    if m:
                        divisor = int(m.group(1))
                        assert divisor > 0
                        remainder = int(m.group(2))
                        assert remainder < divisor
                        focus_block = (divisor, remainder)
                        print(f'Data parallelization: focus on block {focus_block}.')
                        break
                    # Insert only N first documents
                    if sys_arg.isdigit():
                        annotate_only_first = int(sys_arg)
                # Get collection's parameters
                collection_directory = configuration['collection']
                # Get layer annotation parameters
                total_start_time = datetime.now()
                clauses_layer_name = configuration.get('b2_output_clauses_layer', None)
                timexes_layer_name = configuration.get('b2_output_timexes_layer', None)
                ner_layer_name   = configuration.get('b2_output_ner_layer', None)
                # Validate names
                if clauses_layer_name is None:
                    raise Exception('(!) Input configuration {} does not list clauses_layer_name. '.format(input_fname) +\
                                    'Probably missing section "add_clauses_timexes_names_layers" with option "clauses_layer".')
                if timexes_layer_name is None:
                    raise Exception('(!) Input configuration {} does not list timexes_layer_name. '.format(input_fname) +\
                                    'Probably missing section "add_clauses_timexes_names_layers" with option "timexes_layer".')
                if ner_layer_name is None:
                    raise Exception('(!) Input configuration {} does not list ner_layer_name. '.format(input_fname) +\
                                    'Probably missing section "add_clauses_timexes_names_layers" with option "ner_layer".')
                skip_annotated = configuration.get('b2_skip_annotated', True)
                add_layer_creation_time = configuration.get('b2_add_layer_creation_time', False)
                validate_layer_sizes = configuration.get('b2_validate_layer_sizes', False)
                #
                # Get output_mode
                # NEW_FILE  -- creates a new json file by adding `output_file_infix` to the old file name;
                # OVERWRITE -- overwrites the old json file with new content;
                # Applies to both NEW_FILE and OVERWRITE:
                # if `output_remove_morph` is set, then removes the input morph layer from the output document;
                #
                output_mode         = configuration['b2_output_mode']
                output_file_infix   = configuration['b2_output_file_infix']
                # Use words normalization (Optional)
                normalize_w_to_v    = configuration['b2_normalize_w_to_v']
                if normalize_w_to_v:
                    print(f'Applying words normalization: w -> v.')
                logger = None  # TODO
                # Initialize tagger
                input_words_layer = configuration.get('b2_input_words_layer', 'words')
                input_sentences_layer = configuration.get('b2_input_sentences_layer', 'sentences')
                
                # Auxiliary layers (will be removed afterwards) 
                compound_tokens_tagger = PretokenizedTextCompoundTokensTagger()
                morph_tagger = VabamorfTagger()
                
                # Main layers (must be preserved) 
                clause_segmenter = ClauseSegmenter( output_layer=clauses_layer_name, \
                                                    use_normalized_word_form=normalize_w_to_v )
                if not normalize_w_to_v:
                    # No words normalization
                    timex_tagger = TimexTagger(output_layer=timexes_layer_name)
                else:
                    # Use words normalization
                    from estnltk.taggers.standard.timexes.core_timex_tagger import CoreTimexTagger
                    timex_tagger = CoreTimexTagger(output_layer=timexes_layer_name, \
                                                   use_normalized_word_form=normalize_w_to_v)
                nertagger = NerTagger(output_layer=ner_layer_name)
                
                print(f'Using {clause_segmenter.__class__.__name__}.' )
                print(f'Using {timex_tagger.__class__.__name__}.' )
                print(f'Using {nertagger.__class__.__name__}.' )
                if annotate_only_first > 0:
                    print(f'[Debugging] Annotating only first {annotate_only_first} documents.')
                
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
                            if fname.startswith('doc') and fname.endswith('.json'):
                                found_doc_files.append(fname)
                        if len( found_doc_files ) == 0:
                            warnings.warn( f'(!) No document json files found from {doc_subdir!r}' )
                        else:
                            # Process json files
                            local_annotated_docs = 0
                            for fname in found_doc_files:
                                pre_start_time = datetime.now()
                                fpath = os.path.join(doc_subdir, fname)
                                try:
                                    text_obj = json_to_text(file = fpath)
                                    original_layers = text_obj.layers
                                    
                                    if skip_annotated and \
                                       clause_segmenter.output_layer in original_layers and \
                                       timex_tagger.output_layer in original_layers and \
                                       nertagger.output_layer in original_layers:
                                        # Skip document (already annotated)
                                        skipped_annotated_docs += 1
                                        continue
                                    
                                    if input_words_layer not in text_obj.layers:
                                        raise Exception(f'(!) Input json document {fpath!r} is missing {input_words_layer!r} layer. '+\
                                                        f'Available layers: {text_obj.layers!r}.')
                                    if input_sentences_layer not in text_obj.layers:
                                        raise Exception(f'(!) Input json document {fpath!r} is missing {input_sentences_layer!r} layer. '+\
                                                        f'Available layers: {text_obj.layers!r}.')
                                except Exception as err:
                                    raise Exception(f'Failed at processing document {fpath!r} due to an error: ') from err
                                time_deltas['pre'] += ( datetime.now() - pre_start_time )
                                # Auxiliary layers (will be removed afterwards) 
                                aux_start_time = datetime.now()
                                try:
                                    cp_layer = compound_tokens_tagger._make_layer_template()
                                    cp_layer.text_object = text_obj
                                    cp_layer.enveloping = None  # Hack!
                                    text_obj.add_layer( cp_layer )
                                except Exception as err:
                                    raise Exception(f'{compound_tokens_tagger.__class__.__name__}: Failed at processing document {fpath!r} due to an error: ') from err
                                if normalize_w_to_v:
                                    try:
                                        # Apply words normalization (optional)
                                        normalize_words_w_to_v( text_obj[input_words_layer], doc_path=fpath )
                                    except Exception as err:
                                        raise Exception(f'words normalization: Failed at processing document {fpath!r} due to an error: ') from err
                                try:
                                    morph_tagger.tag(text_obj)
                                except Exception as err:
                                    raise Exception(f'{morph_tagger.__class__.__name__}: Failed at processing document {fpath!r} due to an error: ') from err
                                time_deltas['aux'] += ( datetime.now() - aux_start_time )
                                # Main layers
                                clauses_start_time = datetime.now()
                                try:
                                    clause_segmenter.tag(text_obj)
                                    if add_layer_creation_time:
                                        # Add layer creation timestamp
                                        text_obj[clause_segmenter.output_layer].meta['created_at'] = \
                                            (datetime.now()).strftime('%Y-%m-%d')
                                except Exception as err:
                                    raise Exception(f'{clause_segmenter.__class__.__name__}: Failed at processing document {fpath!r} due to an error: ') from err
                                time_deltas['clauses'] += ( datetime.now() - clauses_start_time )
                                timexes_start_time = datetime.now()
                                try:
                                    # Detect document creation date
                                    dct_str = detect_reference_time_from_meta( text_obj.meta )
                                    dct_type = get_reference_time_type( dct_str )
                                    text_obj.meta['document_creation_time'] = dct_str
                                    timex_tagger.tag(text_obj)
                                    # Record dct in layer for book-keeping
                                    text_obj[timex_tagger.output_layer].meta['document_creation_time'] = dct_str
                                    # Record found document creation time type
                                    annotated_dct_types[str(dct_type)] += 1
                                    if add_layer_creation_time:
                                        # Add layer creation timestamp
                                        text_obj[timex_tagger.output_layer].meta['created_at'] = \
                                            (datetime.now()).strftime('%Y-%m-%d')
                                    if normalize_w_to_v:
                                        # If we've used CoreTimexTagger that envelops the 
                                        # output layer around words layer, then flatten the 
                                        # output layer
                                        assert text_obj[timex_tagger.output_layer].enveloping is not None
                                        enveloping_timexes = text_obj.pop_layer( timex_tagger.output_layer )
                                        flat_timexes = flatten(enveloping_timexes, timex_tagger.output_layer, \
                                                               disambiguation_strategy='pick_first')
                                        flat_timexes.meta['document_creation_time'] = \
                                            enveloping_timexes.meta['document_creation_time']
                                        assert flat_timexes.enveloping is None
                                        assert not flat_timexes.ambiguous
                                        text_obj.add_layer( flat_timexes )
                                except Exception as err:
                                    raise Exception(f'{timex_tagger.__class__.__name__}: Failed at processing document {fpath!r} due to an error: ') from err
                                time_deltas['timexes'] += ( datetime.now() - timexes_start_time )
                                names_start_time = datetime.now()
                                try:
                                    nertagger.tag(text_obj)
                                    if add_layer_creation_time:
                                        # Add layer creation timestamp
                                        text_obj[nertagger.output_layer].meta['created_at'] = \
                                            (datetime.now()).strftime('%Y-%m-%d')
                                except Exception as err:
                                    raise Exception(f'{nertagger.__class__.__name__}: Failed at processing document {fpath!r} due to an error: ') from err
                                time_deltas['names'] += ( datetime.now() - names_start_time )

                                post_start_time = datetime.now()
                                assert clauses_layer_name in text_obj.layers
                                assert timexes_layer_name in text_obj.layers
                                assert ner_layer_name in text_obj.layers

                                # Remove auxiliary layers
                                text_obj.pop_layer( compound_tokens_tagger.output_layer )
                                text_obj.pop_layer( morph_tagger.output_layer )

                                # Validate set of remaining layers
                                assert set(text_obj.layers) == set(original_layers).union(set([ clause_segmenter.output_layer, \
                                                                                                timex_tagger.output_layer, \
                                                                                                nertagger.output_layer ]))

                                # Clear words normalization
                                if normalize_w_to_v:
                                    clear_words_normalized_form( text_obj[input_words_layer] )

                                # Validate layer sizes (Optional)
                                if validate_layer_sizes:
                                    max_layer_size = 175000000
                                    for layer_name in [ clause_segmenter.output_layer, \
                                                        timex_tagger.output_layer, \
                                                        nertagger.output_layer ]:
                                        layer_size = len( layer_to_json(text_obj[layer_name]) )
                                        if layer_size > max_layer_size:
                                            raise Exception(f'(!) Error on saving {fpath}: layer {layer_name!r} json '+\
                                                            f'size {layer_size} is exceeding max_layer_size {max_layer_size}.')
                                        else:
                                            pass
      
                                # Records general statistics
                                annotated_words     += len( text_obj[input_words_layer] )
                                annotated_sentences += len( text_obj[input_sentences_layer] )
                                annotated_clauses   += len( text_obj[clause_segmenter.output_layer] )
                                annotated_timexes   += len( text_obj[timex_tagger.output_layer] )
                                annotated_names     += len( text_obj[nertagger.output_layer] )
                                
                                # Records detailed statistics
                                _record_annotated_clause_types( text_obj[clause_segmenter.output_layer], annotated_clause_types )
                                _record_annotated_timex_types( text_obj[timex_tagger.output_layer], annotated_timex_types )
                                _record_annotated_name_types( text_obj[nertagger.output_layer], annotated_name_types )

                                # Finally, save the results
                                if output_mode == 'NEW_FILE':
                                    fpath_fname, fpath_ext = os.path.splitext( fpath )
                                    new_fpath = f'{fpath_fname}{output_file_infix}{fpath_ext}'
                                    assert new_fpath != fpath
                                    text_to_json( text_obj, file=new_fpath )
                                elif output_mode == 'OVERWRITE':
                                    text_to_json( text_obj, file=fpath )
                                elif output_mode == 'NO_OUTPUT':
                                    # Do not write any output (debugging)
                                    pass

                                local_annotated_docs += 1
                                time_deltas['post'] += ( datetime.now() - post_start_time )
                                
                            annotated_docs += 1 if local_annotated_docs > 0 else 0
                            if local_annotated_docs > 1:
                                annotated_split_docs += local_annotated_docs
                            if annotate_only_first > 0 and \
                               annotate_only_first <= skipped_annotated_docs + annotated_docs:
                                break
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
                    print(f'   Annotated clauses:  {annotated_clauses} | ( {_display_type_statistics(annotated_clause_types)} )')
                    print(f'   Annotated names:    {annotated_names} | ( {_display_type_statistics(annotated_name_types)} )')
                    print(f'   Annotated timexes:  {annotated_timexes} | ( {_display_type_statistics(annotated_timex_types)} )')
                    print(f'     Found DCT types:  {_display_type_statistics(annotated_dct_types)}')
                    print()
                    print(f'  Total time elapsed:  {datetime.now()-total_start_time}')
                    if annotated_words > 0:
                        speed_str = find_processing_speed(datetime.now()-total_start_time, annotated_words)
                        print(f'    Processing speed:  ~{speed_str} words/sec')
                    print(f'  Detailed time spending: {_display_time_delta_statistics(time_deltas)}')
                else:
                    warnings.warn(f'(!) No document JSON files found from subdirectories of the collection dir {configuration["collection"]!r}')
                #
                # Release resources
                #
                clause_segmenter.close()
                timex_tagger.close()
            else:
                print(f'Missing or bad configuration in {input_fname!r}. Unable to get configuration parameters.')
    else:
        print('Config INI name required as an input argument.')


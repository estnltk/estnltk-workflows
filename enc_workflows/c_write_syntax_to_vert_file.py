#
#   Reads syntactic annotations from EstNLTK .json files and writes into ENC .vert files. 
#   
#   The process also involves aligning the textual content and annotations in .vert 
#   and .json files -- in case of any misalignments, exceptions will be thrown. 
#   
#   Requires name of a configuration INI file as an input argument. 
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

from x_utils import collect_collection_subdirs
from x_utils import find_processing_speed
from x_utils import get_sentence_hash
from x_utils import create_sentences_hash_map

from x_configparser import parse_configuration
from x_vert_parser import SimpleVertFileParser
from x_vert_parser import SyntaxVertFileWriter
from x_vert_parser import collect_sentence_tokens

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
            processed_docs = 0
            processed_words = 0
            processed_sentences = 0
            # Get collection's parameters
            collection_directory = configuration['collection']
            syntax_layer_name = configuration.get('output_syntax_layer', None)
            if syntax_layer_name is None:
                raise Exception('(!) Input configuration {} does not list syntax_layer_name. '.format(input_fname) +\
                                'Probably missing section "add_syntax_layer" with option "name".')
            input_sentences_layer = 'sentences'
            # Get output parameters
            vert_output_dir = configuration.get('vert_output_dir', None)
            if vert_output_dir is None:
                raise ValueError( f'(!) Input configuration file {input_fname} does not specify section '+\
                                  f'"write_syntax_to_vert" with the option "vert_output_dir". ' )
            vert_output_suffix = configuration.get('vert_output_suffix', None)
            if vert_output_suffix is None:
                raise ValueError( f'(!) Input configuration file {input_fname} does not specify section '+\
                                  f'"write_syntax_to_vert" with the option "vert_output_suffix". ' )
            total_start_time = datetime.now()
            # Iterate over all vert subdirs and all document subdirs within these subdirs
            vert_subdirs = collect_collection_subdirs(configuration['collection'], only_first_level=True, full_paths=False)
            if len(vert_subdirs) == 0:
                warnings.warn(f'(!) No document subdirectories found from collection dir {configuration["collection"]!r}')
            for vert_subdir in vert_subdirs:
                subdir_start_time = datetime.now()
                print(f'Processing {vert_subdir} ...')
                full_subdir = os.path.join(configuration['collection'], vert_subdir)
                # Find input vert file corresponding to the vert_subdir
                vert_file = [v_file for v_file in configuration['vert_files'] if vert_subdir in v_file]
                if len(vert_file) == 0: # sanity check
                    raise ValueError(f'(!) Unable to find .vert file corresponding to vert_subdir {vert_subdir!r}. '+\
                                     f'Available vert files are: {configuration["vert_files"]!r}.')
                else:
                    vert_file = vert_file[0]
                # Construct output vert file name
                _, vert_fname_with_ext = os.path.split(vert_file)
                vert_fname, vert_ext = os.path.splitext(vert_fname_with_ext)
                vert_output_fname = f'{vert_fname}{vert_output_suffix}{vert_ext}'
                # Launch new vert_file_parser
                vert_file_parser = SimpleVertFileParser(vert_file)
                # Launch new vert file writer
                vert_file_writer = SyntaxVertFileWriter(vert_output_fname, vert_file_dir=vert_output_dir)
                # Fetch all the document subdirs
                document_subdirs = collect_collection_subdirs(full_subdir, only_first_level=False, full_paths=True)
                progress_bar = tqdm( desc="Parsing and writing vert {}".format(vert_subdir), 
                                     total=len(document_subdirs), ascii=True )
                subdir_id = 0
                hold_vert = False
                vert_document = None
                while not vert_file_parser.parsing_finished:
                    if not hold_vert:
                        # Parse next document from the vert file
                        vert_document = next(vert_file_parser)
                    else:
                        # Keep the last vert document
                        hold_vert = False
                    # Get json document subdir
                    json_doc_subdir = document_subdirs[subdir_id]
                    json_doc_id = int( json_doc_subdir.split(os.path.sep)[-1] )
                    # Collect Text objects from json files
                    found_json_texts = []
                    for fname in sorted(os.listdir(json_doc_subdir)):
                        if fname.startswith('doc') and fname.endswith('.json'):
                            fpath = os.path.join(json_doc_subdir, fname)
                            text_obj = json_to_text(file = fpath)
                            text_obj.meta['_json_file'] = fname
                            if syntax_layer_name not in text_obj.layers:
                                raise Exception(f'(!) Input json document {fpath!r} is missing {syntax_layer_name!r} layer. '+\
                                                f'Available layers: {text_obj.layers!r}.')
                            found_json_texts.append(text_obj)
                    if len( found_json_texts ) == 0:
                        warnings.warn( f'(!) No document json files found from {doc_subdir!r}' )
                    # Validate that vert_document id and json document id match
                    if vert_document is not None:
                        if int(vert_document[1]['_doc_id']) == json_doc_id:
                            # Process json files and vert document content
                            id_inside_vert = 0
                            last_vert_sent_start = 0
                            for text_obj in found_json_texts:
                                # Manage sentence-wise alignment 
                                # (can be tricky due to empty sentences/wrong sentence tags)
                                json_sentences_hash_map = \
                                    create_sentences_hash_map(text_obj[input_sentences_layer])
                                # Align vert and json content word by word
                                for wid, syntax_word in enumerate(text_obj[syntax_layer_name]):
                                    vert_token = None
                                    while id_inside_vert < len(vert_document[0]):
                                        vert_token = vert_document[0][id_inside_vert]
                                        if vert_token[0] == '<s>':
                                            last_vert_sent_start = id_inside_vert
                                            # Collect sentence tokens
                                            vert_sent_tokens = \
                                                collect_sentence_tokens(vert_document[0], id_inside_vert)
                                            if len(vert_sent_tokens) > 0:
                                                # Compute vert sentence hash
                                                vert_hash = get_sentence_hash(vert_sent_tokens)
                                                # Find matching json sentence(s)
                                                json_sents = json_sentences_hash_map.get(vert_hash, [])
                                                if json_sents:
                                                    # Found matching sentence
                                                    processed_sentences += 1
                                                    # Write out sentence start
                                                    vert_file_writer.write_sentence_start( vert_token, \
                                                                                           sentence_hash=vert_hash )
                                                else:
                                                    raise Exception(f'(!) Unable to find matching json sentence for '+\
                                                                    f'the vert sentence {vert_sent_tokens!r} at the '+\
                                                                    f'document with id={json_doc_id}.')
                                        elif vert_token[0] != '<s>' and vert_token[0] != 'TOKEN':
                                            # Write out (probably unannotated) tag
                                            vert_file_writer.write_tag( vert_token )
                                        if vert_token[0] == 'TOKEN':
                                            break
                                        # Pass by non-tokens (tags etc.)
                                        id_inside_vert += 1
                                    if vert_token is not None:
                                        vert_token_text = vert_token[1]
                                        if vert_token_text == syntax_word.text:
                                            # Words match
                                            processed_words += 1
                                            # Write out annotated tag
                                            vert_file_writer.write_syntax_token( vert_token, syntax_word )
                                            # Take next vert token
                                            id_inside_vert += 1
                                        else:
                                            # Report mismatch
                                            json_context = [sp.text for sp in text_obj[syntax_layer_name][wid-25:wid+1]]
                                            vert_context = [sp for sp in vert_document[0][id_inside_vert-25:id_inside_vert+1]]
                                            json_doc_loc = os.path.join( json_doc_subdir, text_obj.meta["_json_file"] )
                                            raise Exception(f'(!) Mismatching vert word {vert_token!r} and '+\
                                                            f' json word {syntax_word.text!r} at the vert file position '+\
                                                            f'{id_inside_vert} in json document {json_doc_loc!r} and '+\
                                                            f'vert file {vert_file!r} after {processed_words} matches.\n\n'+\
                                                            f'json_doc_context:\n{json_context}\n\n'+\
                                                            f'vert_doc_context:\n{vert_context}\n\n')
                                    else:
                                        # Report mismatch
                                        json_context = [sp.text for sp in text_obj[syntax_layer_name][wid-25:wid+1]]
                                        vert_context = [sp for sp in vert_document[0][id_inside_vert-25:id_inside_vert+1]]
                                        json_doc_loc = os.path.join( json_doc_subdir, text_obj.meta["_json_file"] )
                                        raise Exception(f'(!) Mismatching vert word {vert_token!r} and '+\
                                                        f' json word {syntax_word.text!r} at the vert file position '+
                                                        f'{id_inside_vert} in json document {json_doc_loc!r} and '+
                                                        f'vert file {vert_file!r} after {processed_words} matches.\n\n'+\
                                                        f'json_doc_context:\n{json_context}\n\n'+\
                                                        f'vert_doc_context:\n{vert_context}\n\n')
                            
                            # After we have exhausted all json documents, we may still 
                            # need to complete tags in the vert file
                            while id_inside_vert < len(vert_document[0]):
                                vert_token = vert_document[0][id_inside_vert]
                                assert vert_token[0] != 'TOKEN'  # no more tokens expected at this point
                                # Write out (probably unannotated) tag
                                vert_file_writer.write_tag( vert_token )
                                id_inside_vert += 1

                            # Finalize & record statistics 
                            processed_docs += 1
                            subdir_id += 1
                            progress_bar.update(1)

                        else:
                            # Determine if vert document is empty, that is, has no word tokens
                            word_tokens = [line for line in vert_document[0] if line[0] == 'TOKEN']
                            if not word_tokens:
                                
                                # No words, no syntax ...
                                # Just write tags of the empty/tokenless vert document
                                for vert_token in vert_document[0]:
                                    vert_file_writer.write_tag( vert_token )
                                # Pick next json document subdir
                                subdir_id += 1
                                # Hold vert document for another iteration
                                hold_vert = True
                                
                            else:
                                
                                # Report mismatch
                                vert_start = vert_document[0][:10]
                                json_start = found_json_texts[0].text[:100] if found_json_texts else None
                                raise Exception(f'(!) Mismatching vert doc id {vert_document[1]["_doc_id"]} and '+\
                                                f' json document id {json_doc_id}.\n\n.vert file start: {vert_start!r}'+
                                                f'\n\njson document start: {json_start}')
                    else:
                        if len( found_json_texts ) > 0:
                            # Report mismatch
                            json_start = found_json_texts[0].text[:100] if found_json_texts else None
                            raise Exception(f'(!) No vert doc available for the json document with id {json_doc_id}.'+\
                                            f'\n\njson document start: {json_start}')
                progress_bar.close()
                vert_file_writer.finish_writing()
                # Output reading and writing statuses
                print( vert_file_parser.status_str() )
                print( vert_file_writer.status_str() )
                print(f'Processing {vert_subdir} took {datetime.now()-subdir_start_time}.')
            if processed_docs > 0:
                print()
                print(f' =={collection_directory}==')
                print(f' Processed documents:  {processed_docs}')
                print(f' Processed sentences:  {processed_sentences}')
                print(f'     Processed words:  {processed_words}')
                print()
                print(f'  Total time elapsed:  {datetime.now()-total_start_time}')
                if processed_words > 0:
                    speed_str = find_processing_speed(datetime.now()-total_start_time, processed_words)
                    print(f'    Processing speed:  ~{speed_str} words/sec')
            else:
                warnings.warn(f'(!) No document JSON files found from subdirectories of the collection dir {configuration["collection"]!r}')
        else:
            print(f'Missing or bad configuration in {input_fname!r}. Unable to get configuration parameters.')
else:
    print('Config INI name required as an input argument.')


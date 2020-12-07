#
#  Runs VabamorfTagger with text-based disambiguation on morphologically annotated 
#  PostgreSQL collection, and finds differences in morphological annotations.
#  
#  Outputs summarized statistics about differences, and writes all differences into 
#  a file. The output will be written into a directory named 'diff_' + collection's 
#  name.
#

import os, sys, re
import os.path
import argparse
import gc

from collections import defaultdict

from datetime import datetime
from datetime import timedelta

from estnltk import logger
from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import KeysQuery

from estnltk.taggers import DiffTagger
from estnltk.taggers import VabamorfTagger
from estnltk.layer_operations import extract_section

from morph_eval_utils import write_formatted_diff_str_to_file
from morph_eval_utils import MorphDiffSummarizer, MorphDiffFinder

from conf_utils import create_vm_tagger
from conf_utils import find_morph_analysis_dependency_layers
from conf_utils import pick_random_doc_ids
from conf_utils import find_division_into_chunks


# Whether large texts should be chunked into smaller texts before processing?
chunk_large_texts = True

# Minimum size for texts to be chunked
chunked_text_min_size = 3750000

# Chunk size 
chunk_size = 300000

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Runs VabamorfTagger with text-based disambiguation on given morphologically annotated "+
       "annotated PostgreSQL collection, and finds differences in morphological annotations. "+
       "Outputs summarized statistics about differences, and writes all differences into a "+
       "file. By default, the output will be written into a directory named 'diff_' + "+
       "collection's name. ")
    # 1) Specification of the evaluation settings #1
    parser.add_argument('collection', type=str, \
                        help='name of the collection on which the evaluation will be performed.')
    parser.add_argument('morph_layer', type=str, \
                        help='name of the morph analysis layer to be compared against. '+\
                             'must be a layer of the input collection.')
    # 2) Database access & logging parameters
    parser.add_argument('--pgpass', dest='pgpass', action='store', \
                        default='~/.pgpass', \
                        help='name of the PostgreSQL password file (default: ~/.pgpass). '+\
                             'the format of the file should be:  hostname:port:database:username:password ')
    parser.add_argument('--schema', dest='schema', action='store',\
                        default='public',\
                        help='name of the collection schema (default: public)')
    parser.add_argument('--role', dest='role', action='store',
                        help='role used for accessing the collection. the role must have a read access. (default: None)')
    parser.add_argument('--logging', dest='logging', action='store', default='info',\
                        choices=['debug', 'info', 'warning', 'error', 'critical'],\
                        help='logging level (default: info)')
    # 3) Specification of the evaluation settings #2
    parser.add_argument('-l', '--tbd_level', dest='disamb_level', action='store', default='both',\
                        choices=['predisambiguate', 'postdisambiguate', 'both'],\
                        help='The level of text-based disambiguation. By default, both predisambiguation and '+
                             'postdisambiguation will be used, but you can use this flag to focus only on one '+
                             'of the settings. (default: "both")')
    parser.add_argument('--new_morph_layer', dest='new_morph_layer', action='store', default='new_morph_analysis',\
                        help="name of the morph analysis layer created during the re-annotation "+
                             "of the collection with VabamorfTagger's text-based disambiguation. "
                             "(default: 'new_morph_analysis')")
    parser.add_argument('--in_prefix', dest='in_prefix', action='store', default='',\
                        help="prefix for filtering collection layers suitable as VabamorfTagger's input layers."+\
                             " if the collection contains multiple candidates for an input layer (e.g. multiple "+\
                             " 'words' layers), then only layers with the given prefix will be used as input layers. "+\
                             "(default: '')" )
    parser.add_argument('--in_suffix', dest='in_suffix', action='store', default='',\
                        help="suffix for filtering collection layers suitable as VabamorfTagger's input layers."+\
                             " if the collection contains multiple candidates for an input layer (e.g. multiple "+\
                             " 'words' layers), then only layers with the given suffix will be used as input layers. "+\
                             "(default: '')" )
    parser.add_argument('--out_dir_prefix', dest='out_dir_prefix', action='store', default='diff_',\
                        help="a prefix that will be added to the output directory name. the output directory "+\
                             " name will be: this prefix concatenated with the name of the collection. "+\
                             "(default: 'diff_')" )
    parser.add_argument('--filename_key', dest='file_name_key', action='store', default='file',\
                        help="name of the key in text object's metadata which conveys the original file "+\
                             "name. if the key is specified and corresponding keys are available in "+\
                             "metadata (of each text object), then each of the collection's document will be "+\
                             "associated with its corresponding file name (that is: the file name will be the "+\
                             "identifier of the document in the output). Otherwise, the identifier of the document "+\
                             "in the output will be 'doc'+ID, where ID is document's numeric index in "+\
                             "the collection. "+\
                             "(default: 'fname')" )
    parser.add_argument('--textcat_key', dest='text_cat_key', action='store', default='subcorpus',\
                        help="name of the key in text object's metadata which conveys subcorpus "+\
                             "or text category name. if the key is specified and corresponding keys are "+\
                             "available in metadata (of each text object), then the evaluation / difference "+\
                             "statistics will be recorded / collected subcorpus wise. Otherwise, no subcorpus "+\
                             "distinction will be made in difference statistics and output. "+\
                             "(default: 'subcorpus')" )
    parser.add_argument('--no_chunking', dest='no_chunking', default=False, action='store_true', \
                        help=f"By default, documents that are too large (string size exceeding: {chunked_text_min_size}) "+
                             "will be divided into chunks (so that the chunks follow sentence boundaries) and will be "+
                             "processed chunk by chunk. However, setting this flag disables the chunking behaviour "+
                             "and then all documents will be processed as whole. "+
                             "(default: False)", \
                        )
    parser.add_argument('-r', '--rand_pick', dest='rand_pick', action='store', type=int, \
                        help="integer value specifying the amount of documents to be randomly chosen for "+\
                             "difference evaluation. if specified, then the given amount of documents will be "+\
                             "processed (instead of processing the whole corpus). if the amount exceeds the "+\
                             "corpus size, then the whole corpus is processed. (default: None)" )
    args = parser.parse_args()

    logger.setLevel( (args.logging).upper() )
    log = logger
    
    chunk_large_texts = not args.no_chunking
    if not chunk_large_texts:
        log.info(' Chunking of large documents disabled.' )
    
    storage = PostgresStorage(pgpass_file=args.pgpass,
                              schema=args.schema,
                              role=args.role)
    try:

        # Check layer names
        if args.morph_layer == args.new_morph_layer:
            log.error("(!) Invalid layer names: morph_layer cannot be identical to new_morph_layer: {!r}".format(args.morph_layer))
            exit(1)
        
        collection = storage.get_collection( args.collection )
        if not collection.exists():
            log.error(' (!) Collection {!r} does not exist...'.format(args.collection))
            exit(1)
        else:
            docs_in_collection = len( collection )
            log.info(' Collection {!r} exists and has {} documents. '.format( args.collection,
                                                                              docs_in_collection ))
            log.debug(' Collection {!r} has layers: {!r} '.format( args.collection, 
                                                                   collection.layers ))
            # Pick a random sample (instead of the whole corpus)
            chosen_doc_ids = []
            if args.rand_pick is not None and args.rand_pick > 0:
                chosen_doc_ids = pick_random_doc_ids( args.rand_pick, storage, args.schema, args.collection, logger )
                log.info(' Random sample of {!r} documents chosen for processing.'.format( len(chosen_doc_ids) ))
            
            # Create new VabamorfTagger with text-based disambiguation switched on
            vmtagger_input_args = dict()
            vmtagger_input_args['predisambiguate']  = True
            vmtagger_input_args['postdisambiguate'] = True
            if args.disamb_level == 'predisambiguate':
                vmtagger_input_args['predisambiguate']  = True
                vmtagger_input_args['postdisambiguate'] = False
            elif args.disamb_level == 'postdisambiguate':
                vmtagger_input_args['predisambiguate']  = False
                vmtagger_input_args['postdisambiguate'] = True
            vm_tagger = create_vm_tagger( args.morph_layer, collection, log, \
                                          args.new_morph_layer, 
                                          incl_prefix=args.in_prefix, \
                                          incl_suffix=args.in_suffix, \
                                          **vmtagger_input_args )
            
            morph_diff_finder = MorphDiffFinder( args.morph_layer, args.new_morph_layer, 
                                                 diff_attribs  = ['root', 'lemma', 'root_tokens', 'ending', 'clitic', 
                                                                  'partofspeech', 'form'],
                                                 focus_attribs = ['root', 'ending', 'clitic', 'partofspeech', 'form'] )
            morph_diff_summarizer = MorphDiffSummarizer( args.morph_layer, args.new_morph_layer )
            
            startTime = datetime.now()
            
            # Create output directory name
            output_dir = args.out_dir_prefix + args.collection
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            
            # Timestamp for output files
            output_file_prefix = os.path.splitext(sys.argv[0])[0]
            assert os.path.sep not in output_file_prefix
            output_file_suffix = startTime.strftime('%Y-%m-%dT%H%M%S')
            
            eval_layers = list(vm_tagger.input_layers) + [args.morph_layer]
            data_iterator = None
            if chosen_doc_ids:
                data_iterator = collection.select( KeysQuery(keys=chosen_doc_ids), progressbar='ascii', layers=eval_layers )
            else:
                data_iterator = collection.select( progressbar='ascii', layers=eval_layers )
            last_was_huge_file = False
            for key, text in data_iterator:
                # *) Garbage collection if previously a huge file was processed
                if last_was_huge_file:
                    gc.collect()  # Clean garbage before processing
                # *) Fetch document and subcorpus' identifiers
                fname_stub = 'doc' + str(key)
                if args.file_name_key is not None:
                    if args.file_name_key in text.meta.keys() and text.meta[args.file_name_key] is not None:
                        fname_stub = text.meta[ args.file_name_key ]+f'({key})'
                text_cat = 'corpus'
                if args.text_cat_key is not None:
                    if args.text_cat_key in text.meta.keys() and text.meta[args.text_cat_key] is not None:
                        text_cat = text.meta[ args.text_cat_key ]
                # *) Does the text need chunk by chunk processing?
                text_sentences_str_len = sum( [len(s.enclosing_text) for s in text[vm_tagger.input_layers[1]] ] )
                if chunk_large_texts and text_sentences_str_len >= chunked_text_min_size:
                    log.info('Document {!r} (id: {!r}) is too large for processing it as a whole (approx. string size: {!r}).'.format( fname_stub, key, text_sentences_str_len ))
                    log.info('It will be divided into chunks and processed chunk by chunk.')
                    document_chunks = find_division_into_chunks( text[vm_tagger.input_layers[1]], chunk_size = chunk_size )
                    first_chunk = True
                    for (chunk_start, chunk_end) in document_chunks:
                        gc.collect()  # Clean garbage before processing
                        log.debug('Processing chunk {!r} from {!r} ...'.format( (chunk_start, chunk_end), fname_stub) )
                        text_chunk = extract_section(text, chunk_start, chunk_end, layers_to_keep=list(text.layers), trim_overlapping=True)
                        # 1) Add new morph analysis annotations
                        vm_tagger.tag( text_chunk )
                        # 2) Find the layer of differences, group differences & format nicely 
                        morph_diff_layer, formatted_diffs_str, total_diff_gaps = \
                             morph_diff_finder.find_difference( text_chunk, fname_stub, text_cat=text_cat, start_new_doc=first_chunk )
                        # 3) Record difference statistics
                        morph_diff_summarizer.record_from_diff_layer( 'morph_analysis', morph_diff_layer, 
                                                                      text_cat, start_new_doc=first_chunk )
                        # 4) Visualize & output words that have differences in annotations
                        if formatted_diffs_str is not None  and  len(formatted_diffs_str) > 0:
                            fpath = os.path.join(output_dir, f'_{output_file_prefix}__ann_diffs_{output_file_suffix}.txt')
                            write_formatted_diff_str_to_file( fpath, formatted_diffs_str )
                        # Set pointers to None ( to help garbage collection )
                        text_chunk          = None
                        formatted_diffs_str = None
                        morph_diff_layer    = None
                        # Next chunk == not first chunk anymore
                        first_chunk = False
                    # Remember that last was a huge file
                    last_was_huge_file = True
                else:
                    #
                    # No chunking was required. Process the document as a whole
                    #
                    # 1) Add new morph analysis annotations
                    vm_tagger.tag( text )
                    # 2) Find the layer of differences, group differences & format nicely 
                    morph_diff_layer, formatted_diffs_str, total_diff_gaps = \
                         morph_diff_finder.find_difference( text, fname_stub, text_cat=text_cat, start_new_doc=True )
                    # 3) Record difference statistics
                    morph_diff_summarizer.record_from_diff_layer( 'morph_analysis', morph_diff_layer, text_cat )
                    # 4) Visualize & output words that have differences in annotations
                    if formatted_diffs_str is not None  and  len(formatted_diffs_str) > 0:
                        fpath = os.path.join(output_dir, f'_{output_file_prefix}__ann_diffs_{output_file_suffix}.txt')
                        write_formatted_diff_str_to_file( fpath, formatted_diffs_str )
                    # Set pointers to None ( to help garbage collection )
                    text                = None
                    morph_diff_layer    = None
                    formatted_diffs_str = None
                    last_was_huge_file  = False
            
            summarizer_result_str = morph_diff_summarizer.get_diffs_summary_output( show_doc_count=True )
            log.info( os.linesep+os.linesep+'TOTAL DIFF STATISTICS:'+os.linesep+summarizer_result_str )
            time_diff = datetime.now() - startTime
            log.info('Total processing time: {}'.format(time_diff))
            # Write summarizer's results to output dir
            fpath = os.path.join(output_dir, f'_{output_file_prefix}__stats_{output_file_suffix}.txt')
            with open(fpath, 'w', encoding='utf-8') as out_f:
                out_f.write( 'TOTAL DIFF STATISTICS:'+os.linesep+summarizer_result_str )
                out_f.write( 'Total processing time: {}'.format(time_diff) )
    except:
        raise
    finally:
        storage.close()

       

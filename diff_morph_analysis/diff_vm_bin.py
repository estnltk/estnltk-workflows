#
#  Runs VabamorfTagger with given Vabamorf's binary lexicons on given morphologically 
#  annotated PostgreSQL collection, and finds differences in morphological annotations.
#  
#  Outputs summarized statistics about differences, and writes all differences into a 
#  file. The output will be written into a directory named 'diff_' + collection's name.
#

import os, sys, re
import os.path
import argparse

from collections import defaultdict

from datetime import datetime
from datetime import timedelta

from estnltk import logger
from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import KeysQuery

from estnltk.taggers import DiffTagger

from morph_eval_utils import create_flat_v1_6_morph_analysis_layer
from morph_eval_utils import get_estnltk_morph_analysis_diff_annotations
from morph_eval_utils import get_estnltk_morph_analysis_annotation_alignments
from morph_eval_utils import get_concise_morph_diff_alignment_str
from morph_eval_utils import format_morph_diffs_string
from morph_eval_utils import write_formatted_diff_str_to_file
from morph_eval_utils import MorphDiffSummarizer

from conf_utils import create_vm_tagger_based_on_vm_instance
from conf_utils import pick_random_doc_ids
from conf_utils import create_vm_tagger


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Runs VabamorfTagger with given Vabamorf's binary lexicons on given morphologically "+
       "annotated PostgreSQL collection, and finds differences in morphological annotations. "+
       "Outputs summarized statistics about differences, and writes all differences into a "+
       "file. By default, the output will be written into a directory named 'diff_' + "+
       "collection's name. ")
    # 1) Specification of the evaluation settings #1
    parser.add_argument('vm_bin_dir', type=str, \
                        help="directory containing Vabamorf's binary lexicons (files 'et.dct' and 'et3.dct') "+\
                             "that will be evaluated on the collection. Note: if the value is string '....', "+\
                             "then VabamorfTagger's default binary lexicons will be evaluated.")
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
    parser.add_argument('--new_morph_layer', dest='new_morph_layer', action='store', default='new_morph_analysis',\
                        help="name of the morph analysis layer created during the re-annotation "+
                             "of the collection with Vabamorf's new binary lexicons. "
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
    parser.add_argument('-r', '--rand_pick', dest='rand_pick', action='store', type=int, \
                        help="integer value specifying the amount of documents to be randomly chosen for "+\
                             "difference evaluation. if specified, then the given amount of documents will be "+\
                             "processed (instead of processing the whole corpus). if the amount exceeds the "+\
                             "corpus size, then the whole corpus is processed. (default: None)" )
    args = parser.parse_args()

    logger.setLevel( (args.logging).upper() )
    log = logger
    
    storage = PostgresStorage(pgpass_file=args.pgpass,
                              schema=args.schema,
                              role=args.role)
    try:

        # Check input arguments:  vm_bin_dir
        new_lex_path        = 'et.dct'
        new_disamb_lex_path = 'et3.dct'
        if not re.match('^\.{3,}$', args.vm_bin_dir):
            if not os.path.exists(args.vm_bin_dir) or not os.path.isdir(args.vm_bin_dir):
                log.error("(!) Invalid Vabamorf's binaries directory (vm_bin_dir): {!r}".format(args.vm_bin_dir))
                parser.print_usage()
                exit(1)
            
            # Check for existence of binary files
            new_lex_path        = os.path.join(args.vm_bin_dir, new_lex_path)
            new_disamb_lex_path = os.path.join(args.vm_bin_dir, new_disamb_lex_path)
            
            if not os.path.isfile( new_lex_path ):
                log.error("(!) Invalid vm_bin_dir {!r}".format(args.vm_bin_dir))
                log.error("(!) Missing Vabamorf's binary lexicon file: {!r}".format(new_lex_path))
                parser.print_usage()
                exit(1)
            if not os.path.isfile( new_disamb_lex_path ):
                log.error("(!) Invalid vm_bin_dir {!r}".format(args.vm_bin_dir))
                log.error("(!) Missing Vabamorf's binary lexicon file: {!r}".format(new_disamb_lex_path))
                parser.print_usage()
                exit(1)
        
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
            
            if not re.match('^\.{3,}$', args.vm_bin_dir):
                # Create VabamorfTagger based on given binary lexicon files
                vm_tagger = create_vm_tagger_based_on_vm_instance( args.morph_layer, collection, log, \
                                                                   args.new_morph_layer, new_lex_path, \
                                                                   new_disamb_lex_path, \
                                                                   incl_prefix=args.in_prefix, \
                                                                   incl_suffix=args.in_suffix )
            else:
                # Create VabamorfTagger with default settings
                vm_tagger = create_vm_tagger( args.morph_layer, collection, log, \
                                              args.new_morph_layer, 
                                              incl_prefix=args.in_prefix, \
                                              incl_suffix=args.in_suffix )
            
            morph_diff_tagger = DiffTagger(layer_a = args.morph_layer+'_flat',
                                           layer_b = args.new_morph_layer+'_flat',
                                           output_layer='morph_diff_layer',
                                           output_attributes=('span_status', 'root', 'lemma', 'root_tokens', 'ending', 'clitic', 'partofspeech', 'form'),
                                           span_status_attribute='span_status')
            morph_diff_summarizer = MorphDiffSummarizer( args.morph_layer, args.new_morph_layer )
            morph_diff_gap_counter = 0
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
            for key, text in data_iterator:
                # 0) Fetch document and subcorpus' identifiers
                fname_stub = 'doc' + str(key)
                if args.file_name_key is not None:
                    if args.file_name_key in text.meta.keys() and text.meta[args.file_name_key] is not None:
                        fname_stub = text.meta[ args.file_name_key ]
                text_cat = 'corpus'
                if args.text_cat_key is not None:
                    if args.text_cat_key in text.meta.keys() and text.meta[args.text_cat_key] is not None:
                        text_cat = text.meta[ args.text_cat_key ]
                # 1) Add new morph analysis annotations
                vm_tagger.tag( text )
                # 2) Create flat v1_6 morph analysis layers
                flat_morph_1 = create_flat_v1_6_morph_analysis_layer( text, args.morph_layer, 
                                                                            args.morph_layer+'_flat', add_layer=True )
                flat_morph_2 = create_flat_v1_6_morph_analysis_layer( text, args.new_morph_layer,
                                                                            args.new_morph_layer+'_flat', add_layer=True )
                # 3) Find differences & alignments
                morph_diff_tagger.tag( text )
                ann_diffs  = get_estnltk_morph_analysis_diff_annotations( text, \
                                                                          args.morph_layer+'_flat', \
                                                                          args.new_morph_layer+'_flat', \
                                                                          'morph_diff_layer' )
                flat_morph_layers = [args.morph_layer+'_flat', args.new_morph_layer+'_flat']
                focus_attributes  = ['root', 'ending', 'clitic', 'partofspeech', 'form']
                alignments = get_estnltk_morph_analysis_annotation_alignments( ann_diffs, flat_morph_layers ,\
                                                                               text['morph_diff_layer'],
                                                                               focus_attributes=focus_attributes )
                # Record difference statistics
                morph_diff_summarizer.record_from_diff_layer( 'morph_analysis', text['morph_diff_layer'], text_cat )
                # 5) Visualize & output words that have differences in annotations
                formatted, morph_diff_gap_counter = \
                     format_morph_diffs_string( fname_stub, text, alignments, args.morph_layer+'_flat', \
                                                                  args.new_morph_layer+'_flat', \
                                                                  gap_counter=morph_diff_gap_counter,
                                                                  text_cat=text_cat, \
                                                                  focus_attributes=focus_attributes )
                if formatted is not None:
                    fpath = os.path.join(output_dir, f'_{output_file_prefix}__ann_diffs_{output_file_suffix}.txt')
                    write_formatted_diff_str_to_file( fpath, formatted )
                
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

       

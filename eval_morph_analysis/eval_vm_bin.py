#
#  Evaluates how changing Vabamorf's binary lexicons alters EstNLTK's morphological 
#  annotations. 
#
#  Runs given Vabamorf's binary lexicons on a morphologically annotated PostgreSQL
#  collection. 
#  Compares collection's morphological annotations against new annotations produced 
#  by VabamorfTagger with given binaries and finds all annotation differences.
#
#  Outputs summarizing statistics and detailed annotation differences.
#
#  Status:
#      work-in-progress
#

import os, sys
import os.path
import argparse

from collections import defaultdict

from datetime import datetime
from datetime import timedelta

from estnltk import logger
from estnltk.storage.postgres import PostgresStorage

from estnltk.resolve_layer_dag import DEFAULT_RESOLVER
from estnltk.vabamorf.morf import Vabamorf as VabamorfInstance
from estnltk.taggers import VabamorfTagger
from estnltk.taggers import DiffTagger

from morph_eval_utils import create_flat_v1_6_morph_analysis_layer
from morph_eval_utils import get_estnltk_morph_analysis_diff_annotations
from morph_eval_utils import get_estnltk_morph_analysis_annotation_alignments
from morph_eval_utils import get_concise_morph_diff_alignment_str
from morph_eval_utils import format_morph_diffs_string
from morph_eval_utils import write_formatted_diff_str_to_file
from morph_eval_utils import MorphDiffSummarizer


def find_morph_analysis_dependency_layers( vabamorftagger, morph_layer, collection, log, 
                                           incl_prefix='', incl_suffix='' ):
    ''' Finds a mapping from VabamorfTagger's input layer names to layers 
        available in the collection. 
        Mapping relies on an assumption that input layer names are substrings
        of the corresponding layer names in the collection.
        If incl_prefix and incl_suffix have been specified (that is: are non-empty
        strings), then they are used to filter collection layers. Only those 
        collection layer names that satisfy the constraint startswith( incl_prefix ) 
        and endswith( incl_suffix ) will be used for the mapping.
    '''
    # 1) Match VabamorfTagger's input argument names to its input_layers
    input_arg_matches = defaultdict(str)
    input_arg_matches['input_words_layer'] = ''
    input_arg_matches['input_sentences_layer'] = ''
    input_arg_matches['input_compound_tokens_layer'] = ''
    for input_layer in vabamorftagger.input_layers:
        for input_arg in input_arg_matches.keys():
            if input_layer in input_arg:
                input_arg_matches[input_arg] = input_layer
    # 2) Match VabamorfTagger's input_layers to collection's layers
    input_layer_matches = defaultdict(list)
    for input_layer in vabamorftagger.input_layers:
        for collection_layer in collection.layers:
            if not collection_layer.startswith(incl_prefix):
                # If the layer name does not have required prefix, skip it
                continue
            if not collection_layer.endswith(incl_suffix):
                # If the layer name does not have required suffix, skip it
                continue
            if input_layer in collection_layer:
                input_layer_matches[input_layer].append( collection_layer )
                if len( input_layer_matches[input_layer] ) > 1:
                    log.error(("(!) VabamorfTagger's input layer {!r} has more than 1 "+\
                               "possible matches in the collection {!r}: {!r}").format(input_layer,
                                                                                       collection.name,
                                                                                       input_layer_matches[input_layer]))
                    log.error(("Please use arguments in_prefix and/or in_suffix to specify, "+
                               "which layers are relevant dependencies of the {!r} layer.").format(morph_layer))
                    exit(1)
        if len( input_layer_matches[input_layer] ) == 0:
            log.error(("(!) VabamorfTagger's input layer {!r} could not be found from "+\
                       "layers of the collection {!r}. Collection's layers are: {!r}").format(input_layer,
                                                                                       collection.name,
                                                                                       collection.layers))
            exit(1)
    # 3) Match input_arg -> input_layers -> collection_layer_name
    input_arg_to_collection_layer = defaultdict(str)
    for input_arg in input_arg_matches.keys():
        input_layer = input_arg_matches[input_arg]
        input_arg_to_collection_layer[input_arg] = input_layer_matches[input_layer]
    # 4) Convert value types from list to string
    for input_arg in input_arg_to_collection_layer.keys():
        val = input_arg_to_collection_layer[input_arg]
        assert isinstance(val, list) and len(val) == 1
        input_arg_to_collection_layer[input_arg] = val[0]
    return input_arg_to_collection_layer



def create_vm_tagger_based_on_vm_instance( old_morph_layer, collection, log, new_morph_layer, \
                                                            new_lex_path, new_disamb_lex_path, \
                                                            incl_prefix='', incl_suffix='' ):
    ''' Creates VabamorfTagger's instance to be used in the evaluation of Vabamorf's binary lexicons.
    '''
    default_vm_tagger = DEFAULT_RESOLVER.taggers.rules['morph_analysis']
    vmtagger_input_args = \
        find_morph_analysis_dependency_layers( default_vm_tagger,old_morph_layer,collection,log,
                                               incl_prefix=incl_prefix, incl_suffix=incl_suffix )
    vmtagger_input_args['output_layer'] = new_morph_layer
    vmtagger_input_args['vm_instance']  = VabamorfInstance( lex_path=new_lex_path, \
                                                            disamb_lex_path=new_disamb_lex_path )
    vm_tagger = VabamorfTagger( **vmtagger_input_args )
    log.info(' Initialized {!r} for evaluation. '.format( vm_tagger) )
    return vm_tagger



def fetch_document_indexes( storage, schema, collection, logger ):
    """ Fetches and returns all document ids of the collection from the PostgreSQL storage.
    """
    from psycopg2.sql import SQL, Identifier
    # Construct the query
    sql_str = 'SELECT id FROM {}.{} ORDER BY id'
    doc_ids = []
    with storage.conn as conn:
        # Named cursors: http://initd.org/psycopg/docs/usage.html#server-side-cursors
        with conn.cursor('read_collection_doc_ids', withhold=True) as read_cursor:
            try:
                read_cursor.execute(SQL(sql_str).format(Identifier(schema),
                                                        Identifier(collection)))
            except Exception as e:
                logger.error(e)
                raise
            finally:
                logger.debug(read_cursor.query.decode())
            for items in read_cursor:
                doc_ids.append ( items[0] )
    return doc_ids



def pick_random_doc_ids( k, storage, schema, collection, logger, sort=True ):
    ''' Picks a random sample of k document ids from the given collection. '''
    from random import sample
    all_doc_ids = fetch_document_indexes( storage, args.schema, args.collection, logger )
    resulting_sample = sample(all_doc_ids, k) if k < len(all_doc_ids) else all_doc_ids
    return sorted(resulting_sample) if sort else resulting_sample



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Evaluates how changing Vabamorf's binary lexicons alters EstNLTK's morphological "+
       "annotations. \n"+
       "Runs given Vabamorf's binary lexicons on a morphologically annotated PostgreSQL "+
       "collection. "+
       "Compares collection's morphological annotations against new annotations produced "+
       "by VabamorfTagger with given binaries. Finds all annotation differences. \n"+
       "Outputs summarizing statistics and detailed annotation differences.")
    # 1) Specification of the evaluation settings #1
    parser.add_argument('vm_bin_dir', type=str, \
                        help="directory containing Vabamorf's binary lexicons (files 'et.dct' and 'et3.dct') "+\
                             "that will be evaluated on the collection.")
    parser.add_argument('collection', type=str, \
                        help='name of the collection on which the evaluation will be performed.')
    parser.add_argument('morph_layer', type=str, \
                        help='name of the morph analysis layer to be evaluated against. '+\
                             'must be a layer of the input collection.')
    # 2) Database access parameters
    parser.add_argument('--pgpass', dest='pgpass', action='store', \
                        default='~/.pgpass', \
                        help='name of the PostgreSQL password file (default: ~/.pgpass). '+\
                             'the format of the file should be:  hostname:port:database:username:password ')
    parser.add_argument('--schema', dest='schema', action='store',\
                        default='public',\
                        help='name of the collection schema (default: public)')
    parser.add_argument('--role', dest='role', action='store',
                        help='role used for accessing the collection. the role must have a read access. (default: None)')
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
    parser.add_argument('--rand_pick', dest='rand_pick', action='store', type=int, \
                        help="integer value specifying the amount of documents to be randomly chosen for "+\
                             "difference evaluation. if specified, then the given amount of documents will be "+\
                             "processed (instead of processing the whole corpus). if the amount exceeds the "+\
                             "corpus size, then the whole corpus is processed. (default: None)" )
    # 4) Logging parameters
    parser.add_argument('--logging', dest='logging', action='store', default='info',\
                        choices=['debug', 'info', 'warning', 'error', 'critical'],\
                        help='logging level (default: info)')
    args = parser.parse_args()

    logger.setLevel( (args.logging).upper() )
    log = logger
    
    storage = PostgresStorage(pgpass_file=args.pgpass,
                              schema=args.schema,
                              role=args.role)
    try:

        # Check input arguments:  vm_bin_dir
        if not os.path.exists(args.vm_bin_dir) or not os.path.isdir(args.vm_bin_dir):
            log.error("(!) Invalid Vabamorf's binaries directory (vm_bin_dir): {!r}".format(args.vm_bin_dir))
            parser.print_usage()
            exit(1)
        
        # Check for existence of binary files
        new_lex_path        = 'et.dct'
        new_disamb_lex_path = 'et3.dct'
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
            
            vm_tagger = create_vm_tagger_based_on_vm_instance( args.morph_layer, collection, log, \
                                                               args.new_morph_layer, new_lex_path, \
                                                               new_disamb_lex_path, \
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
                # TODO: unforunately, this does not work with the current devel_1.6 code, because selecting by keys is broken there
                data_iterator = collection.select( keys=chosen_doc_ids, progressbar='ascii', layers=eval_layers )
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
                # 5) Visualize & output words with different annotations
                formatted, morph_diff_gap_counter = \
                     format_morph_diffs_string( fname_stub, text, alignments, args.morph_layer+'_flat', \
                                                                  args.new_morph_layer+'_flat', \
                                                                  gap_counter=morph_diff_gap_counter,
                                                                  text_cat=text_cat, \
                                                                  focus_attributes=focus_attributes )
                if formatted is not None:
                    fpath = os.path.join(output_dir, f'_{output_file_prefix}__diff_gaps_{output_file_suffix}.txt')
                    write_formatted_diff_str_to_file( fpath, formatted )
                
            summarizer_result_str = morph_diff_summarizer.get_diffs_summary_output( show_doc_count=True )
            log.info( os.linesep+os.linesep+'TOTAL DIFF STATISTICS:'+os.linesep+summarizer_result_str )
            time_diff = datetime.now() - startTime
            log.info('Total processing time: {}'.format(time_diff))
            # Write summarizer's results to output dir
            fpath = os.path.join(output_dir, f'_{output_file_prefix}__diff_stats_{output_file_suffix}.txt')
            with open(fpath, 'w', encoding='utf-8') as out_f:
                out_f.write( 'TOTAL DIFF STATISTICS:'+os.linesep+summarizer_result_str )
                out_f.write( 'Total processing time: {}'.format(time_diff) )
    except:
        raise
    finally:
        storage.close()

       

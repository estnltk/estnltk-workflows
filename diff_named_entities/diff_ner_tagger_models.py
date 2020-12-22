#
#  Runs two NerTagger's models on given PostgreSQL collection. 
#  Finds differences between NE annotations produced by two models. 
#  The collection must have input_layers required by NerTagger.
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
from estnltk.layer_operations import flatten
from estnltk.taggers.standard_taggers.diff_tagger import iterate_diff_conflicts

from conf_utils import pick_random_doc_ids
from conf_utils import create_ner_tagger_from_model
from conf_utils import flip_ner_input_layer_names
from conf_utils import find_ner_dependency_layers
from conf_utils import load_in_doc_ids_from_file

from ner_diff_utils import NerDiffFinder
from ner_diff_utils import NerDiffSummarizer
from ner_diff_utils import write_formatted_diff_str_to_file


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Runs two NerTagger models on given PostgreSQL collection. "+
       "Finds differences between NE annotations produced by two models. "+
       "Outputs summarized statistics about differences, and writes all differences into a "+
       "file. By default, the output will be written into a directory named 'diff_' + "+
       "collection's name. ")
    # 1) Specification of the evaluation settings #1
    parser.add_argument('collection', type=str, \
                        help='name of the collection on which the evaluation will be performed. '+
                             'the collection must have input_layers required by NerTagger.')
    parser.add_argument('first_model', type=str, \
                        help='location of the first NE model to be compared (the "old layer"). '+
                             'must be a directory containing files "model.bin" and "settings.py".')
    parser.add_argument('second_model', type=str, \
                        help='location of the second NE model to be compared against (the "new layer"). '+
                             'must be a directory containing files "model.bin" and "settings.py".')
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
    parser.add_argument('--old_ner_layer', dest='old_ner_layer', action='store', default='old_named_entities',\
                        help="name of the NE layer created by the first_model; assumingly the \"old layer\". "+
                             "(default: 'old_named_entities')")
    parser.add_argument('--new_ner_layer', dest='new_ner_layer', action='store', default='new_named_entities',\
                        help="name of the NE layer created by the second_model; assumingly the \"new layer\". "+
                             "(default: 'new_named_entities')")
    parser.add_argument('--old_ne_attr', dest='old_ne_attr', action='store', default='nertag',\
                        help="name of the attribute containing NE type label in the old "+
                             "layer. (default: 'nertag')")
    parser.add_argument('--new_ne_attr', dest='new_ne_attr', action='store', default='nertag',\
                        help="name of the attribute containing NE type label in the new "+
                             "layer. (default: 'nertag')")
    parser.add_argument('--in_prefix', dest='in_prefix', action='store', default='',\
                        help="prefix for filtering collection layers suitable as NerTagger's input layers."+\
                             " if the collection contains multiple candidates for an input layer (e.g. multiple "+\
                             " 'words' layers), then only layers with the given prefix will be used as input layers. "+\
                             "(default: '')" )
    parser.add_argument('--in_suffix', dest='in_suffix', action='store', default='',\
                        help="suffix for filtering collection layers suitable as NerTagger's input layers."+\
                             " if the collection contains multiple candidates for an input layer (e.g. multiple "+\
                             " 'words' layers), then only layers with the given suffix will be used as input layers. "+\
                             "(default: '')" )
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
    parser.add_argument('-f', '--file_pick', dest='file_pick', action='store', type=str, \
                        help="name of the file containing indexes of the documents that need to be processed "+\
                             "in the difference evaluation. if specified, then only documents listed in the "+\
                             "file will be processed (instead of processing the whole corpus). note: each "+\
                             "document id must be on a separate line in the index file. (default: None)" )
    parser.add_argument('--out_dir_prefix', dest='out_dir_prefix', action='store', default='diff_',\
                        help="a prefix that will be added to the output directory name. the output directory "+\
                             " name will be: this prefix concatenated with the name of the collection. "+\
                             "(default: 'diff_')" )
    args = parser.parse_args()

    logger.setLevel( (args.logging).upper() )
    log = logger
    
    storage = PostgresStorage(pgpass_file=args.pgpass,
                              schema=args.schema,
                              role=args.role)
    try:

        # Check model dirs and layer names
        if args.first_model == args.second_model:
            log.error("(!) Invalid model dictories: first_model cannot be identical to second_model: {!r}".format(args.first_model))
            exit(1)
        if args.old_ner_layer == args.new_ner_layer:
            log.error("(!) Indistinguishable layer names: old_ner_layer cannot be identical to new_ner_layer: {!r}".format(args.old_ner_layer))
            exit(1)

        collection = storage.get_collection( args.collection )
        if not collection.exists():
            log.error(' (!) Collection {!r} does not exist...'.format(args.collection))
            exit(1)
        else:
            docs_in_collection = len( collection )
            log.info(' Collection {!r} exists and has {} documents. '.format( args.collection,
                                                                              docs_in_collection ))
            log.info(' Collection {!r} has layers: {!r} '.format( args.collection, 
                                                                   collection.layers ))
            
            chosen_doc_ids = []
            if args.rand_pick is not None and args.rand_pick > 0:
                # Pick a random sample (instead of the whole corpus)
                chosen_doc_ids = pick_random_doc_ids( args.rand_pick, storage, args.schema, args.collection, logger )
                log.info(' Random sample of {!r} documents chosen for processing.'.format( len(chosen_doc_ids) ))
            elif args.file_pick is not None:
                # Or load target document indexes from the file
                chosen_doc_ids = load_in_doc_ids_from_file( args.file_pick, storage, args.schema, args.collection, logger)
                log.info(' {!r} document indexes loaded from {!r} for processing.'.format( len(chosen_doc_ids), args.file_pick ))
            
            # Create ner_taggers
            first_ner_tagger, first_ner_input_layers_mapping = \
                  create_ner_tagger_from_model( args.old_ner_layer, args.first_model, collection, log, 
                                                incl_prefix=args.in_prefix, incl_suffix=args.in_suffix )
            second_ner_tagger, second_ner_input_layers_mapping = \
                  create_ner_tagger_from_model( args.new_ner_layer, args.second_model, collection, log, 
                                                incl_prefix=args.in_prefix, incl_suffix=args.in_suffix )

            assert first_ner_input_layers_mapping == second_ner_input_layers_mapping

            ner_diff_finder = NerDiffFinder( args.old_ner_layer, args.new_ner_layer, 
                                             old_layer_attr=args.old_ne_attr, 
                                             new_layer_attr=args.new_ne_attr  )
            ner_diff_summarizer = NerDiffSummarizer( args.old_ner_layer, args.new_ner_layer )
            
            startTime = datetime.now()
            
            # Create output directory name
            output_dir = args.out_dir_prefix + args.collection
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            
            # Timestamp for output files
            output_file_prefix = os.path.splitext(sys.argv[0])[0]
            assert os.path.sep not in output_file_prefix
            output_file_suffix = startTime.strftime('%Y-%m-%dT%H%M%S')
            
            eval_layers = list(first_ner_input_layers_mapping.values())
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
                        fname_stub = text.meta[ args.file_name_key ]+f'({key})'
                text_cat = 'corpus'
                if args.text_cat_key is not None:
                    if args.text_cat_key in text.meta.keys() and text.meta[args.text_cat_key] is not None:
                        text_cat = text.meta[ args.text_cat_key ]
                # 1) Add new NE annotations
                # Hack: we need to flip input layer names, because NerTagger 
                #       currently does not allow customizing input layer names
                flip_ner_input_layer_names( text, first_ner_input_layers_mapping )
                first_ner_tagger.tag( text )
                second_ner_tagger.tag( text )
                
                # 2) Find differences between old and new layers
                #    Get differences within their respective contexts (as string)
                #    Get number of grouped differences (diff_gaps)
                diff_layer, formatted_str, diff_gaps = ner_diff_finder.find_difference( text, fname_stub, text_cat )
                
                # 3) Record difference statistics
                ner_diff_summarizer.record_from_diff_layer( 'named_entities', diff_layer, text_cat )
                
                # 4) Output NE-s that have differences in annotations along with their contexts
                if formatted_str is not None and len(formatted_str) > 0:
                    fpath = os.path.join(output_dir, f'_{output_file_prefix}__ann_diffs_{output_file_suffix}.txt')
                    write_formatted_diff_str_to_file( fpath, formatted_str )

            summarizer_result_str = ner_diff_summarizer.get_diffs_summary_output( show_doc_count=True )
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

       

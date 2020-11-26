#
#  Builds a document word index from a corpus in Postgres collection. 
#  The index shows character and word counts (and optionally 
#  sentence counts and some text metadata) for each document in 
#  the corpus. 
#
#  The index could be used as a basis for making a random 
#  selection from the corpus, and later it can be used as 
#  a guide in processing the corpus. 
# 
#   Requirements:   py3.6+,  EstNLTK 1.6.7
#

import re
import os, os.path
import argparse
import sys
import gc

from datetime import datetime

from estnltk import logger
from estnltk.storage.postgres import PostgresStorage

INDEX_FIELD_DELIMITER = '|||'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Builds a document word index from a corpus in Postgres collection. "+
       "The index shows character and word counts (and optionally "+
       "sentence counts and some text metadata) for each document in "+
       "the corpus. ")
    # 1) Specification of the indexable collection
    parser.add_argument('collection', type=str, \
                        help='name of the collection which needs to be indexed.')
    parser.add_argument('words_layer', type=str, \
                        help='name of the words layer to be indexed. must be a '+
                             'layer of the collection.')
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
    # 3) Specification of the indexable collection #2
    parser.add_argument('--sentences_layer', dest='sentences_layer', action='store', type=str, default=None,\
                        help="name of the sentences layer to be indexed. must be a "+
                             'layer of the collection. if not provided, then sentence '+
                             'counts will not be recorded in the index.')
    parser.add_argument('--out_index_file', dest='out_index_file', action='store', type=str, default='koondkorpus_index.txt',\
                        help="name of the output index file. "+\
                             "(default: 'koondkorpus_index.txt')" )
    parser.add_argument('--filename_key', dest='file_name_key', action='store', type=str, default='file',\
                        help="name of the key in text object's metadata which conveys the original file "+\
                             "name. if the key is specified and corresponding keys are available in "+\
                             "metadata (of each text object), then the full file name will be recorded "+\
                             "in the index. (default: 'fname')" )
    parser.add_argument('--textcat_key1', dest='text_cat_key1', action='store', type=str, default='subcorpus',\
                        help="name of the key in text object's metadata which conveys subcorpus "+\
                             "or text category name (the first category). if the key is specified "+\
                             "and corresponding keys are available in metadata (of each text object), "+\
                             "then the category name will be recorded in the index. "+\
                             "(default: 'subcorpus')" )
    parser.add_argument('--textcat_key2', dest='text_cat_key2', action='store', type=str, default='type',\
                        help="name of the key in text object's metadata which conveys subcorpus "+\
                             "or text category name (the second category). if the key is specified "+\
                             "and corresponding keys are available in metadata (of each text object), "+\
                             "then the category name will be recorded in the index. "+\
                             "(default: 'type')" )
    args = parser.parse_args()

    logger.setLevel( (args.logging).upper() )
    log = logger
    
    #================================================
    #   Collect index fields
    #================================================
    
    index_fields = ['doc_id']
    if args.text_cat_key1 is not None:
        index_fields.append( args.text_cat_key1 )
    if args.text_cat_key2 is not None:
        index_fields.append( args.text_cat_key2 )
    if args.file_name_key is not None:
        index_fields.append( args.file_name_key )
    index_fields.append( 'chars' )
    if args.words_layer is not None:
        index_fields.append( args.words_layer )
    if args.sentences_layer is not None:
        index_fields.append( args.sentences_layer )
        
    # Create anew index file
    out_index_file = args.out_index_file
    with open(out_index_file, 'w', encoding='utf-8') as out_f:
        header = index_fields
        out_f.write( INDEX_FIELD_DELIMITER.join(header) )
        out_f.write('\n')
    
    #================================================
    #   Connect with the database                    
    #================================================

    storage = PostgresStorage(pgpass_file=args.pgpass,
                              schema=args.schema,
                              role=args.role)

    try:

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

            focus_layers = []
            if args.words_layer is not None and args.words_layer not in collection.layers:
                log.error(f'(!) Layer {args.words_layer} missing from the collection {args.collection} ...')
                exit(1)
            elif args.words_layer is not None:
                focus_layers.append( args.words_layer )
            if args.sentences_layer is not None and args.sentences_layer not in collection.layers:
                log.error(f'(!) Layer {args.sentences_layer} missing from the collection {args.collection} ...')
                exit(1)
            elif args.sentences_layer is not None:
                focus_layers.append( args.sentences_layer )
            
            #================================================
            #   Process the corpus and create index          
            #================================================
            
            startTime = datetime.now()
            temp_index = []
            data_iterator = collection.select( progressbar='ascii', layers=focus_layers )
            for key, text in data_iterator:
                # *) Initialize empty entry
                doc_entry = { k : '__' for k in index_fields }
                # *) Fetch document metadata
                fname_stub = 'doc' + str(key)
                if args.file_name_key is not None:
                    if args.file_name_key in text.meta.keys() and text.meta[args.file_name_key] is not None:
                        doc_entry[args.file_name_key] = text.meta[ args.file_name_key ]
                if args.text_cat_key1 is not None:
                    if args.text_cat_key1 in text.meta.keys() and text.meta[args.text_cat_key1] is not None:
                        doc_entry[args.text_cat_key1] = text.meta[ args.text_cat_key1 ]
                if args.text_cat_key2 is not None:
                    if args.text_cat_key2 in text.meta.keys() and text.meta[args.text_cat_key2] is not None:
                        doc_entry[args.text_cat_key2] = text.meta[ args.text_cat_key2 ]
                # *) Index key, document length and lengths of the layers
                doc_entry['doc_id'] = key
                doc_entry['chars']  = len(text.text)
                for layer_name in text.layers:
                    if layer_name in doc_entry.keys():
                        doc_entry[layer_name] = len(text[layer_name])
                # *) Check if all keys were filled
                for k in doc_entry.keys():
                    if doc_entry[k] == '__':
                        log.warning(f'(!) Key {k} missing from the doc metadata {text.meta}.')
                doc_entry_str = INDEX_FIELD_DELIMITER.join( [str(doc_entry.get(k)) for k in index_fields] )
                temp_index.append( doc_entry_str )
                # Flush the buffer
                if len(temp_index) > 2500:
                    # Write entries into file
                    with open(out_index_file, 'a', encoding='utf-8') as out_f:
                        for entry_str in temp_index:
                            out_f.write( entry_str )
                            out_f.write('\n')
                    temp_index = []
            # Flush the buffer
            if len(temp_index) > 0:
                # Write entries into file
                with open(out_index_file, 'a', encoding='utf-8') as out_f:
                    for entry_str in temp_index:
                        out_f.write( entry_str )
                        out_f.write('\n')
                temp_index = []
            time_diff = datetime.now() - startTime
            log.info('Index saved into file {!r}'.format(out_index_file))
            log.info('Total processing time: {}'.format(time_diff))
    except:
        raise
    finally:
        storage.close()


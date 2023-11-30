#
#  Builds JSON metadata index from a corpus in Postgres collection. 
#  The index records JSON metadata (text.meta) of each document in 
#  the collection and, optionally, also records text length and 
#  text start snippet of each document. 
#  Results will be recorded in a JSON-L format file. 
# 
#   Requirements:   py3.8+,  EstNLTK 1.7.2+
#

import re
import os, os.path
import argparse
import sys
import json

from datetime import datetime

from estnltk import logger
from estnltk.storage.postgres import PostgresStorage

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Builds JSON metadata index from a corpus in Postgres collection. "+
       "The index collects JSON metadata (text.meta) of each document in "+
       "the collection and, optionally, also collects the text start snippet "+
       "of each document. "+
       "Results will be recorded in a JSON-L format file.")
    # 1) Specification of the indexable collection
    parser.add_argument('collection', type=str, \
                        help='name of the collection which needs to be indexed.')
    parser.add_argument('--snippet_size', type=int, default=0, \
                            help='Size of the document start snippet to be recorded into index.')
    parser.add_argument('--debug_limit', type=int, default=None, \
                            help='Limit the number of documents to be processed (picks only '+\
                                 'given amount of document from the start of the collection).')
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
    parser.add_argument('--out_index_file', dest='out_index_file', action='store', type=str, default='koondkorpus_metadata.jsonl',\
                        help="name of the output index file. "+\
                             "(default: 'koondkorpus_metadata.jsonl')" )

    args = parser.parse_args()

    logger.setLevel( (args.logging).upper() )
    start_snippet_size = args.snippet_size
    debug_processing_limit = args.debug_limit
    log = logger
    
    #================================================
    #   Collect index fields
    #================================================

    # Create anew index file
    out_index_file = args.out_index_file
    with open(out_index_file, 'w', encoding='utf-8') as out_f:
        pass
    
    #================================================
    #   Connect with the database                    
    #================================================

    storage = PostgresStorage(pgpass_file=args.pgpass,
                              schema=args.schema,
                              role=args.role)

    try:
        collection = storage[ args.collection ]
        if not collection.exists():
            log.error(' (!) Collection {!r} does not exist...'.format(args.collection))
            exit(1)
        else:
            docs_in_collection = len( collection )
            log.info(' Collection {!r} exists and has {} documents. '.format( args.collection,
                                                                              docs_in_collection ))
            log.info(' Collection {!r} has layers: {!r} '.format( args.collection, 
                                                                  collection.layers ))
            
            #==========================================================
            #   Process the corpus and create metadata (json) index    
            #==========================================================
            
            startTime = datetime.now()
            temp_index = []
            total_entries = 0
            if debug_processing_limit is None:
                data_iterator = collection.select( progressbar='ascii', layers=[] )
            else:
                data_iterator = collection.select( progressbar='ascii', layers=[] ).head( debug_processing_limit )
            for key, text in data_iterator:
                # *) Initialize entry
                doc_entry = { 'doc_id' : key }
                # *) Add metadata
                for k, v in text.meta.items():
                    doc_entry[k] = v
                # *) Index document (content string) length
                doc_entry['_text_len'] = len(text.text)
                # *) Add text's start snippet
                if start_snippet_size > 0:
                    snippet = text.text[:start_snippet_size]
                    doc_entry['_text_start'] = snippet
                temp_index.append( doc_entry )
                # Flush the buffer
                if len(temp_index) > 2500:
                    # Write entries into file
                    with open(out_index_file, 'a', encoding='utf-8') as out_f:
                        for entry_dict in temp_index:
                            out_f.write( json.dumps( entry_dict, ensure_ascii=False ) )
                            out_f.write('\n')
                            total_entries += 1
                    temp_index = []
            # Flush the buffer
            if len(temp_index) > 0:
                # Write entries into file
                with open(out_index_file, 'a', encoding='utf-8') as out_f:
                    for entry_dict in temp_index:
                        out_f.write( json.dumps( entry_dict, ensure_ascii=False ) )
                        out_f.write('\n')
                        total_entries += 1
                temp_index = []
            time_diff = datetime.now() - startTime
            log.info('Index saved into file {!r}'.format(out_index_file))
            log.info('Total entries collected: {}'.format(total_entries))
            log.info('Total processing time:   {}'.format(time_diff))
    except:
        raise
    finally:
        storage.close()


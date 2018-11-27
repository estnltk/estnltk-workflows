# -*- coding: utf-8 -*-
#
#   Loads doc_id-s of etTenTen 2013 documents either from the file "etTenTen.vert" (or 
#  "ettenten13.processed.prevert"), splits id-s into N subsets (the number of subsets 
#  should be specified via command line argument), and writes into files.
#
#   Use this script to enable parallel processing of etTenTen documents: split docs 
#  into N  subsets  with  this  script,  and  then  evoke  N  instances  of  the  script 
#  "store_ettenten_in_pgcollection.py" to process the files.
#
#   Developed and tested under Python's version:  3.5.5
#

import re
import os, os.path

import argparse
from argparse import RawTextHelpFormatter

from datetime import datetime 
from datetime import timedelta

from estnltk.corpus_processing.parse_ettenten import extract_doc_ids_from_corpus_file


def save_doc_ids( files_list, out_fnm ):
    ''' Saves the list of document ids into the file out_fnm.
        Overwrites existing files.
    '''
    with open(out_fnm, 'w', encoding='utf-8') as f:
       for fnm in files_list:
           f.write(fnm+'\n')


# The main program
if __name__ == '__main__':
    # *** Parse input arguments
    arg_parser = argparse.ArgumentParser(description=\
'''    Loads doc_id-s of etTenTen 2013 documents either from the file "etTenTen.vert" (or 
 "ettenten13.processed.prevert"), splits id-s into N subsets (the number of subsets 
 should be specified via command line argument), and writes into files.

    The output files will have the same prefix as the input file, except periods and hyphens 
 will be replaced with '_' and '.txt' will be added as the file ending.

    Use this script to enable parallel processing of etTenTen documents: split docs 
 into N  subsets  with  this  script,  and  then  evoke  N  instances  of  the  script 
 "store_ettenten_in_pgcollection.py" to process the files.
''',\
    formatter_class=RawTextHelpFormatter )
    arg_parser.add_argument('in_file', default = None, \
                            help='full path to the input ettenten corpus file ("etTenTen.vert" \n'+
                                 'or "ettenten13.processed.prevert").'
    )
    arg_parser.add_argument('--splits', type=int, default = None, \
                            help='number of splits (integer);')
    args = arg_parser.parse_args()
    in_file = args.in_file if os.path.exists(args.in_file) else None
    nr_of_splits = args.splits

    if in_file and nr_of_splits and nr_of_splits > 0:
        startTime = datetime.now() 
        print(' Splitting documents into',nr_of_splits,'groups.')
        print(' This may take a little time ...')
        # *** Create placeholders for groups
        groups = []
        for i in range(nr_of_splits):
            groups.append([])
        # *** Split document id-s into groups
        j = 0
        processed = 0
        for doc_id in extract_doc_ids_from_corpus_file( in_file ):
            groups[j].append( doc_id )
            j += 1
            if j >= nr_of_splits:
                j = 0
            processed += 1
        # Save groups into separate files
        print()
        print(' Saving groups:')
        (f_path, fnm) = os.path.split(in_file)
        fnm = re.sub('[\-\.]', '_', fnm)
        for i in range(nr_of_splits):
            out_fnm = fnm+'__'+str(i+1)+'_of_'+str(nr_of_splits)+'.txt'
            print(' --> '+out_fnm+' ('+str(len(groups[i]))+' items)')
            save_doc_ids( groups[i], out_fnm )
        # Report final statistics about the processing
        print(' Total',processed,'documents listed.')
        time_diff = datetime.now() - startTime
        print(' Total processing time: {}'.format(time_diff))
    else:
        arg_parser.print_help()



# -*- coding: utf-8 -*-
#
#   Loads Koondkorpus XML TEI files (either from zipped archives, or from directories where 
#   the files have been unpacked), splits file names into N subsets (the number of subsets 
#   should be specified via command line argument), and writes into files.
#
#   Use this script to enable parallel processing of Koondkorpus files: split files into N 
#   subsets  with  this  script,  and  then  evoke  N  instances  of  the  script 
#   "store_koondkorpus_in_pgcollection.py" to process the files.
#
#   Developed and tested under Python's version:  3.5.5
#

import re
import os, os.path

import argparse
from argparse import RawTextHelpFormatter

from datetime import datetime 
from datetime import timedelta

from estnltk.corpus_processing.parse_koondkorpus import unpack_zipped_xml_files_iterator


def iter_unpacked_xml_file_names( root_dir ):
    """ Traverses recursively root_dir to find XML TEI documents,
        and yields names of found XML files. Yields tuples:
               (filename, filename_with_full_path)
    
        Parameters
        ----------
        root_dir: str
            The root directory which is recursively traversed to find 
            XML files;
    """
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if len(dirnames) > 0 or len(filenames) == 0 or 'bin' in dirpath:
            continue
        for fnm in filenames:
            full_fnm = os.path.join(dirpath, fnm)
            if fnm.lower().endswith('.xml'):
                yield (fnm, full_fnm)



def iter_packed_xml_file_names( root_dir ):
    """ Finds zipped (.zip and tar.gz) files from the directory root_dir, 
        finds XML TEI documents inside the zipped files, and yields names 
        of found XML files. Yields tuples: 
               (filename, filename_with_full_path_in_zip)
    
        Parameters
        ----------
        root_dir: str
            The root directory which contains zipped (.zip and tar.gz) 
            XML TEI files;
    """
    files = os.listdir( root_dir )
    for in_file in files:
        if in_file.endswith('.zip') or in_file.endswith('.gz'):
           in_path = os.path.join(root_dir, in_file)
           for full_fnm in unpack_zipped_xml_files_iterator(in_path,test_only=True):
               path_head, path_tail = os.path.split(full_fnm)
               if path_tail.lower().endswith('.xml'):
                   yield (path_tail, full_fnm)



def save_file_names( files_list, out_fnm ):
    ''' Saves the list of file names into the file out_fnm.
        Overwrites existing files.
    '''
    with open(out_fnm, 'w', encoding='utf-8') as f:
       for fnm in files_list:
           f.write(fnm+'\n')



# The main program
if __name__ == '__main__':
    # *** Parse input arguments
    arg_parser = argparse.ArgumentParser(description=\
    ''' 
    Loads Koondkorpus XML TEI files (either from zipped archives, or from directories where 
    the files have been unpacked), splits file names into N subsets (the number of subsets 
    should be specified via command line argument), and writes into files.
    
    Use this script to enable parallel processing of Koondkorpus files: split files into N 
    subsets  with  this  script,  and  then  evoke  N  instances  of  the  script 
    "store_koondkorpus_in_pgcollection.py" to process the files.
    ''',\
    formatter_class=RawTextHelpFormatter )
    arg_parser.add_argument('in_dir', default = None, \
                            help='the directory containing input files (packed or unpacked). '
    )
    arg_parser.add_argument('-i', '--input_format', dest='input_format', \
                            help='specifies format of the input files:\n\n'+\
                                 '* unzipped -- in_dir will be traversed recursively for\n'+\
                                 '  XML TEI files of the Koondkorpus. The target files should\n'+\
                                 '  be unpacked, but they must be inside the same directory\n'+\
                                 '  structure as they were in the packages.\n'+\
                                 '\n'+\
                                 ' * zipped -- in_dir will be traversed (non-recursively)\n'+\
                                 "   for .zip and .gz files. Each archive file will be opened,\n"+\
                                 "   and names of the XML TEI files will be extracted.\n"+\
                                 '(default: zipped).',\
                            choices=['zipped', 'unzipped'], \
                            default='zipped' )
    arg_parser.add_argument('--splits', type=int, default = None, \
                            help='number of splits (integer);')
    args = arg_parser.parse_args()
    in_dir = args.in_dir if os.path.isdir(args.in_dir) else None
    nr_of_splits = args.splits

    doc_iterator = None 
    if args.input_format == 'zipped':
       doc_iterator = iter_packed_xml_file_names
    elif args.input_format == 'unzipped':
       doc_iterator = iter_unpacked_xml_file_names
    if not doc_iterator:
       raise Exception('(!) No iterator implemented for the input format',args.input_format)

    if in_dir and nr_of_splits and nr_of_splits > 0:
        startTime = datetime.now() 
        print(' Splitting into',nr_of_splits,'groups.')
        # *** Create placeholders for groups
        groups = []
        for i in range(nr_of_splits):
            groups.append([])
        # *** Split xml file names into groups
        j = 0
        processed = 0
        for (in_file_name, full_path) in doc_iterator( in_dir ):
            groups[j].append( in_file_name )
            j += 1
            if j >= nr_of_splits:
                j = 0
            processed += 1
        # Save groups into separate files
        print()
        print(' Saving groups:')
        dirname = re.sub('[^\w\-_\.]', '_', in_dir)
        for i in range(nr_of_splits):
            out_fnm = dirname+'__'+str(i+1)+'_of_'+str(nr_of_splits)+'.txt'
            print(' --> '+out_fnm+' ('+str(len(groups[i]))+' items)')
            save_file_names( groups[i], out_fnm )
        # Report final statistics about the processing
        print(' Total',processed,'XML files listed.')
        time_diff = datetime.now() - startTime
        print(' Total processing time: {}'.format(time_diff))
    else:
        arg_parser.print_help()



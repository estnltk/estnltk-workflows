#
#   Script for preparing the etTenTen corpus before processing it with EstNLTK 1.6.x
#   
#   Splits the content of "etTenTen.vert" (or "ettenten13.processed.prevert") into
#  separate web pages, converts page contents into EstNLTK's Text objects (which will
#  contain text + metadata, and may optionally contain tokenization layers) and writes 
#  the results into JSON format files.
#
#   Python version:  3.5.4
#

import re
import os
import os.path
import argparse

from datetime import datetime 
from datetime import timedelta

from estnltk.text import Text
from estnltk.converters import text_to_json

from estnltk.corpus_processing.parse_ettenten import parse_ettenten_corpus_file_iterator

import logging

logger = None  # <-- To be initialized later

output_ext = '.json'    # extension of output files



# The main program
if __name__ == "__main__":
    # =======  Parse input arguments
    arg_parser = argparse.ArgumentParser(description=''' 
      Splits the content of "etTenTen.vert" (or "ettenten13.processed.prevert") into
     separate web pages, converts page contents into EstNLTK's Text objects (which will
     contain text + metadata, and may optionally contain tokenization layers) and writes
     the results into JSON format files.
    ''')
    arg_parser.add_argument('in_file', default = None, \
                                       help='the content of etTenTen corpus in a single '+\
                                            'text file (file named "etTenTen.vert" or '+\
                                            '"ettenten13.processed.prevert"). The file '+\
                                            'should (loosely) follow the XML format: the '+\
                                            'content of the whole corpus should be inside '+\
                                            '<corpus>-tags, and each single document (web '+\
                                            'page) should be within <doc>-tags. Textual '+\
                                            'content inside <doc>-tags should be cleaned '+\
                                            'from most of the other HTML-tags, except <p>-tags.',\
                                            )
    arg_parser.add_argument('out_dir', default = None, \
                                       help='the output directory where the results '+\
                                            'of conversion (Text objects in JSON format) will '+\
                                            'be written. Output files will have extension '+\
                                             output_ext,\
                                             )
    arg_parser.add_argument('--add_tokenization', default = False,
                                        help="If set, then EstNLTK's default tokenizers are used "+\
                                             "for populating Texts with layers 'tokens', "+\
                                             "'compound_tokens', 'words' and 'sentences', "+\
                                             "and the layer 'paragraphs' preserves the original "+\
                                             'paragraph annotations from XML while enveloping '+\
                                             "around the layer 'sentences'. If not set, then "+\
                                             "Texts will only have one tokenization layer --"+\
                                             "'original_paragraphs' -- which consists of "+\
                                             "paragraph annotations from the XML. "+\
                                             "(By default, the flag is unset)",\
                                             action='store_true')
    arg_parser.add_argument('--add_attribs', default = True,
                                        help='If set, then the paragraphs layer (either '+\
                                             "'paragraphs' or 'original_paragraphs') will also have "+\
                                             'attributes parsed from the corresponding XML tags. '+\
                                             '(By default, the attributes will not be stored)',\
                                             action='store_true')
    arg_parser.add_argument('--amount', type=int, default = None, \
                                        help='the number of documents that are converted '+\
                                             'into JSON files. This is only used for testing '+\
                                             'purposes, e.g. if one wants to test the '+\
                                             'conversion of a small subset of documents.',\
                                        )
    arg_parser.add_argument('--mimic', default = False, \
                                        help='If set, then writing out JSON files is only '+\
                                             'mimicked, but not actually done. This is used '+\
                                             'for testing purposes, e.g. if one wants to test '+\
                                             'the corpus loading functionality without writing '+\
                                             'anything into files. (Default: not set)',\
                                             action='store_true')
    arg_parser.add_argument('--logging', dest='logging', action='store', default='info',\
                            choices=['debug', 'info', 'warning', 'error', 'critical'],\
                            help='Logging level (default: info)')

    args = arg_parser.parse_args()
    in_file = args.in_file if os.path.isfile(args.in_file) else None
    out_dir = args.out_dir if os.path.isdir(args.out_dir) else None
    store_paragraph_attributes = args.add_attribs
    add_tokenization           = args.add_tokenization
    convert_only_n_docs        = args.amount
    only_mimic_output          = args.mimic

    logger = logging.getLogger('etTenTenConverter')
    logger.setLevel( (args.logging).upper() )
    
    if (in_file and out_dir) or (in_file and only_mimic_output and not out_dir):
        assert output_ext.startswith('.')
        # =======  Initialize
        domains     = {}
        urls        = {}
        document_id = 0
        startTime = datetime.now()
        # =======  Process content doc by doc
        for text in parse_ettenten_corpus_file_iterator( in_file, encoding='utf-8', \
                                                         add_tokenization=add_tokenization, \
                                                         discard_empty_paragraphs=True, \
                                                         store_paragraph_attributes=store_paragraph_attributes ):
            if 'web_domain' not in text.meta:
                for k,v in text.meta.items():
                    logger.error(k,':', v)
                raise Exception(' (!) Web domain name not found from the metadata of text! ' )
            # Construct name of the file (based on web domain name)
            domain_name = text.meta['web_domain']
            domain_name = domain_name.replace('.', '_')
            fnm = domain_name+'__'+str(document_id)+output_ext
            if not only_mimic_output:
                out_file_path = os.path.join(out_dir, fnm)
            else:
                out_file_path = fnm
            logger.debug(' Writing document {0}'.format(out_file_path))
            # Export in json format
            if not only_mimic_output:
                text_to_json(text, file=out_file_path)
            document_id += 1
            if convert_only_n_docs and convert_only_n_docs <= document_id:
                break
        logger.info(' {0} documents converted.'.format(document_id))
        time_diff = datetime.now() - startTime
        logger.info(' Total processing time: {}'.format(time_diff))
    else:
        arg_parser.print_help()

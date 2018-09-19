#
#   Loads Koondkorpus XML TEI files (either from zipped archives, or from directories where
#  the files have been unpacked), creates EstNLTK Text objects based on these files, adds
#  tokenization to Texts (optional), and stores Texts in a PostgreSQL collection.
#   Note: If the given collection already exists, it will be deleted, and a new collection
#  will be created for storing Texts.
# 

import os, sys
import os.path
import argparse
import logging

from collections import OrderedDict

from datetime import datetime
from datetime import timedelta

from nltk.tokenize.simple import LineTokenizer
from estnltk.taggers import SentenceTokenizer
from estnltk.taggers.text_segmentation.whitespace_tokens_tagger \
                     import WhiteSpaceTokensTagger
from estnltk.taggers.text_segmentation.pretokenized_text_compound_tokens_tagger \
                     import PretokenizedTextCompoundTokensTagger

from estnltk.corpus_processing.parse_koondkorpus import get_div_target
from estnltk.corpus_processing.parse_koondkorpus import get_text_subcorpus_name
from estnltk.corpus_processing.parse_koondkorpus import parse_tei_corpus
from estnltk.corpus_processing.parse_koondkorpus import unpack_zipped_xml_files_iterator
from estnltk.corpus_processing.parse_koondkorpus import parse_tei_corpus_file_content

from estnltk.storage.postgres import PostgresStorage

def iter_unpacked_xml(root_dir, encoding='utf-8', create_empty_docs=True):
    """ Traverses recursively root_dir to find XML TEI documents,
        converts found documents to EstNLTK Text objects, and 
        yields created Text objects.
    
        Parameters
        ----------
        root_dir: str
            The root directory which is recursively traversed to find 
            XML files;
        encoding: str
            Encoding of the XML files. (default: 'utf-8')
        create_empty_docs: boolean
            If True, then documents are also created if there is no 
            textual content, but only metadata content.
            Note: an empty document may be a captioned table or a figure, 
            which content has been removed from the XML file. Depending on 
            the goals of the analysis, the caption may still be useful, 
            so, by default, empty documents are preserved;
            (default: True)
    """
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if len(dirnames) > 0 or len(filenames) == 0 or 'bin' in dirpath:
            continue
        for fnm in filenames:
            full_fnm = os.path.join(dirpath, fnm)
            target   = get_div_target(full_fnm)
            docs = parse_tei_corpus(full_fnm, target=[target], encoding=encoding, \
                                    preserve_tokenization=False, \
                                    record_xml_filename=True)
            for doc_id, doc in enumerate(docs):
                if not create_empty_docs and len(doc.text) == 0:
                   # Skip an empty document
                   continue
                yield doc



def iter_packed_xml(root_dir, encoding='utf-8', create_empty_docs=True):
    """ Finds zipped (.zip and tar.gz) files from the directory root_dir, 
        unpacks XML TEI documents from zipped files, converts documents 
        to EstNLTK Text objects, and yields created Text objects.
    
        Parameters
        ----------
        root_dir: str
            The root directory which contains zipped (.zip and tar.gz) 
            XML TEI files;
        encoding: str
            Encoding of the XML files. (default: 'utf-8')
        create_empty_docs: boolean
            If True, then documents are also created if there is no 
            textual content, but only metadata content.
            Note: an empty document may be a captioned table or a figure, 
            which content has been removed from the XML file. Depending on 
            the goals of the analysis, the caption may still be useful, 
            so, by default, empty documents are preserved;
            (default: True)
    """
    files = os.listdir( root_dir )
    for in_file in files:
        if in_file.endswith('.zip') or in_file.endswith('.gz'):
           in_path = os.path.join(root_dir, in_file)
           for (full_fnm, content) in unpack_zipped_xml_files_iterator(in_path,test_only=False):
               div_target = get_div_target(full_fnm)
               docs = parse_tei_corpus_file_content(content, full_fnm, target=[div_target],\
                                                             preserve_tokenization=False, \
                                                             record_xml_filename=True)
               for doc_id, doc in enumerate(docs):
                   if not create_empty_docs and len(doc.text) == 0:
                      # Skip an empty document
                      continue
                   yield doc



def init_preserving_sentence_tokenizer():
    """ Initializes SentenceTokenizer that splits into sentences 
        only by newlines (uses LineTokenizer), and uses no
        additional post-corrections.
        This SentenceTokenizer can be used for restoring the 
        original tokenization in the text from XML file.
    
    Returns
    -------
    SentenceTokenizer
        SentenceTokenizer that splits into sentences only by 
        newlines (uses LineTokenizer), and uses no additional 
        post-corrections;
    """
    return SentenceTokenizer(
           base_sentence_tokenizer=LineTokenizer(),
           fix_paragraph_endings = True,
           fix_compound_tokens = False,
           fix_numeric = False,
           fix_parentheses = False,
           fix_double_quotes = False,
           fix_inner_title_punct = False,
           fix_repeated_ending_punct = False,
           use_emoticons_as_endings = False )



# Special tokenstagger, compoundtokentagger and sentence_tokenizer 
# used by the function process_files
special_tokens_tagger          = None
special_compound_tokens_tagger = None
special_sentence_tokenizer     = None


def process_files(rootdir, doc_iterator, collection, encoding='utf-8', \
                  create_empty_docs=False, logger=None, tokenization=None):
    """ Uses given doc_iterator (iter_packed_xml or iter_unpacked_xml) to
        extract texts from the files in the folder root_dir
    
        Parameters
        ----------
        root_dir: str
            The root directory which contains XML TEI files that 
            doc_iterator can extract;
        doc_iterator: iter_packed_xml or iter_unpacked_xml
            Iterator function that can extract Text objects from 
            (packed or unpacked) files in the root_dir;
        collection:  estnltk.storage.postgres.db.PgCollection
            EstNLTK's PgCollection where extracted Texts should be 
            stored;
        encoding: str
            Encoding of the XML files. (default: 'utf-8')
        create_empty_docs: boolean
            If True, then documents are also created if there is no 
            textual content, but only metadata content.
            (default: False)
    """
    global special_tokens_tagger
    global special_compound_tokens_tagger
    global special_sentence_tokenizer
    assert doc_iterator in [iter_unpacked_xml, iter_packed_xml]
    assert tokenization in [None, 'none', 'preserve', 'estnltk']
    if tokenization:
        if tokenization == 'none':
           tokenization = None
        elif tokenization == 'preserve' and not special_tokens_tagger:
           # Initialize special taggers
           special_tokens_tagger = \
                 WhiteSpaceTokensTagger()
           special_compound_tokens_tagger = \
                 PretokenizedTextCompoundTokensTagger()
           special_sentence_tokenizer = \
                 init_preserving_sentence_tokenizer()
    for doc in doc_iterator(rootdir, encoding=encoding, create_empty_docs=create_empty_docs):
        if tokenization:
           # add tokenization (if required)
           if tokenization == 'preserve':
               # a) preserve original tokenization
               # Tag 'tokens' and 'words' that follow exactly the 
               # tokenization in the XML file
               # (note: 'compound_tokens' will be always empty)
               special_tokens_tagger.tag(doc)
               special_compound_tokens_tagger.tag(doc)
               doc.tag_layer(['words'])
               # Tag 'sentences' and 'paragraphs' that follow 
               # exactly the annotation in XML files
               special_sentence_tokenizer.tag(doc)
               doc.tag_layer(['paragraphs'])
           elif tokenization == 'estnltk':
               # b) use estnltk's tokenization instead
               doc.tag_layer(['tokens', 'compound_tokens', 'words'])
               doc.tag_layer(['sentences', 'paragraphs'])
        if '_xml_file' in doc.meta:
           # record subcorpus name
           subcorpus = get_text_subcorpus_name( None, doc.meta['_xml_file'], doc )
           doc.meta['subcorpus'] = subcorpus
        # Collect metadata
        meta = {}
        for key in ['file', 'subcorpus', 'title', 'type']:
            if key == 'file':
               meta[key] = doc.meta['_xml_file'] if '_xml_file' in doc.meta else ''
            else:
               meta[key] = doc.meta[key] if key in doc.meta else ''
        row_id = collection.insert(doc, meta_data=meta)
        if logger:
            logger.info(' Document #{} inserted.'.format(row_id))
        #print('.', end = '')
        #sys.stdout.flush()
    print()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Loads Koondkorpus XML TEI files (either from zipped archives, or from directories where "+
       "the files have been unpacked), creates EstNLTK Text objects based on these files, adds "+
       "tokenization to Texts (optional), and stores Texts in a PostgreSQL collection. "+
       "Note: If the given collection already exists, it will be deleted, and a new collection "+
       "will be created for storing Texts."
    )
    # 1) Input parameters
    parser.add_argument('rootdir', type=str, \
                        help='the directory containing input corpus files (packed or unpacked)')
    parser.add_argument('-e', '--encoding', type=str, default='utf-8', \
                        help='encoding of the TEI XML files (Default: utf-8).')
    parser.add_argument('-f', '--input_format', dest='input_format', \
                        help='format of the input files (Default: zipped).',\
                        choices=['zipped', 'unzipped'], \
                        default='zipped' )
    # 2) Output parameters: database access
    parser.add_argument('--pgpass', dest='pgpass', action='store', \
                        default='~/.pgpass', \
                        help='name of the PostgreSQL password file (default: ~/.pgpass). '+\
                             'the format of the file should be:  hostname:port:database:username:password ')
    parser.add_argument('--schema', dest='schema', action='store',\
                        default='public',\
                        help='name of the collection schema (default: public)')
    parser.add_argument('--collection', dest='collection', action='store',
                        default='collection',
                        help='name of the collection (default: collection)')
    parser.add_argument('--role', dest='role', action='store',
                        help='collection owner (default: None)')
    # 3) Processing parameters 
    parser.add_argument('-t', '--tokenization', dest='tokenization', \
                        help='specifies if and how texts will be tokenized. none -- no tokenization will '+ \
                             'be applied; preserve -- original tokenization from XML files will be preserved; '+\
                             'estnltk -- original tokenization from XML files will be overwritten by '+\
                             "estnltk's tokenization; "+\
                              "(default: preserve)",\
                        choices=['none', 'preserve', 'estnltk'], \
                        default='preserve' )
    # 4) Logging parameters
    parser.add_argument('--logging', dest='logging', action='store', default='info',\
                        choices=['debug', 'info', 'warning', 'error', 'critical'],\
                        help='logging level (default: info)')
    args = parser.parse_args()

    if not os.path.isdir(args.rootdir):
       print('(!) Argument rootdir should be a directory')
       arg_parser.print_help()
    doc_iterator = None 
    if args.input_format == 'zipped':
       doc_iterator = iter_packed_xml
    elif args.input_format == 'unzipped':
       doc_iterator = iter_unpacked_xml
    if not doc_iterator:
       raise Exception('(!) No iterator implemented for the input format',args.input_format)
    logging.basicConfig( level=(args.logging).upper() )
    log = logging.getLogger(__name__)
    
    storage = PostgresStorage(pgpass_file=args.pgpass,
                              schema=args.schema,
                              role=args.role)
    collection = storage.get_collection(args.collection)
    if collection.exists():
        log.info(' Collection {!r} exists. Overwriting.'.format(args.collection))
        collection.delete()

    if not collection.exists():
         meta_fields = OrderedDict([('file', 'str'),
                                    ('subcorpus', 'str'),
                                    ('title', 'str'),
                                    ('type', 'str')])
         collection = storage.get_collection(args.collection, meta_fields=meta_fields)
         collection.create('collection of estnltk texts with segmentation')
         log.info(' New collection {!r} created.'.format(args.collection))
    
    startTime = datetime.now()
    process_files(args.rootdir, doc_iterator, collection, encoding=args.encoding, \
                  create_empty_docs=False, logger=log, tokenization=args.tokenization)
    storage.close()
    time_diff = datetime.now() - startTime
    log.info('Total processing time: {}'.format(time_diff))
       

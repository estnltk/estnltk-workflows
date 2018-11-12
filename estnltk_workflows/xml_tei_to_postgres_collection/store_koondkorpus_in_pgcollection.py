#
#   Loads Koondkorpus XML TEI files (either from zipped archives, or from directories where
#  the files have been unpacked), creates EstNLTK Text objects based on these files, adds
#  tokenization to Texts (optional), splits Texts into paragraphs or sentences (optional), 
#  and stores Texts in a PostgreSQL collection.
# 

import os, sys
import os.path
import argparse
from argparse import RawTextHelpFormatter

from estnltk import logger

from collections import OrderedDict

from datetime import datetime
from datetime import timedelta

from estnltk.layer_operations import split_by

from estnltk.corpus_processing.parse_koondkorpus import get_div_target
from estnltk.corpus_processing.parse_koondkorpus import get_text_subcorpus_name
from estnltk.corpus_processing.parse_koondkorpus import parse_tei_corpus
from estnltk.corpus_processing.parse_koondkorpus import unpack_zipped_xml_files_iterator
from estnltk.corpus_processing.parse_koondkorpus import parse_tei_corpus_file_content

from estnltk.storage.postgres import PostgresStorage
from psycopg2.sql import SQL, Identifier


def iter_unpacked_xml(root_dir, focus_input_files=None, \
                      encoding='utf-8', create_empty_docs=True,\
                      add_tokenization=False, \
                      preserve_tokenization=False, \
                      sentence_separator='\n', \
                      paragraph_separator='\n\n' ):
    """ Traverses recursively root_dir to find XML TEI documents,
        converts found documents to EstNLTK Text objects, and 
        yields created Text objects.
    
        Parameters
        ----------
        root_dir: str
            The root directory which is recursively traversed to find 
            XML files;
        focus_input_files: set of str
            Set of input XML files that should be exclusively processed 
            from root_dir. If provided, then only files from the set 
            will be processed, and all other files will be skipped.
            If None, then all files in root_dir will be processed.
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
        add_tokenization: boolean
            If True, then tokenization layers 'tokens', 'compound_tokens', 
            'words', 'sentences', 'paragraphs' will be added to all newly created 
            Text instances;
            If preserve_orig_tokenization is set, then original tokenization in 
            the document will be preserved; otherwise, the tokenization will be
            created with EstNLTK's default tokenization tools;
            (Default: False)
        preserve_tokenization: boolean
            If True, then the original segmentation from the XML file (sentences 
            between <s> and </s>, paragraphs between <p> and </p>, and words &
            tokens separated by spaces) is also preserved in the newly created Text 
            instances;
            Note: this only has effect if add_tokenization has been switched on;
            (Default: False)
        sentence_separator: str
            String to be used as a sentence separator during the reconstruction
            of the text. The parameter value should be provided, None is not 
            allowed.
            (Default: '\n')
        paragraph_separator: str
            String to be used as a paragraph separator during the reconstruction
            of the text. The parameter value should be provided, None is not 
            allowed.
            (Default: '\n\n')
    """
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if len(dirnames) > 0 or len(filenames) == 0 or 'bin' in dirpath:
            continue
        for fnm in filenames:
            if focus_input_files != None:
                if fnm not in focus_input_files:
                   # Skip the XML file if it is not listed
                   continue
            full_fnm = os.path.join(dirpath, fnm)
            target   = get_div_target(full_fnm)
            docs = parse_tei_corpus(full_fnm, target=[target], encoding=encoding, \
                                    add_tokenization=add_tokenization, \
                                    preserve_tokenization=preserve_tokenization, \
                                    sentence_separator=sentence_separator,\
                                    paragraph_separator=paragraph_separator,\
                                    record_xml_filename=True)
            for doc_id, doc in enumerate(docs):
                if not create_empty_docs and len(doc.text) == 0:
                   # Skip an empty document
                   continue
                yield doc


log = None

def iter_packed_xml(root_dir, focus_input_files=None, encoding='utf-8', \
                     create_empty_docs=True, \
                     add_tokenization=False, \
                     preserve_tokenization=False, \
                     sentence_separator='\n', \
                     paragraph_separator='\n\n' ):
    """ Finds zipped (.zip and tar.gz) files from the directory root_dir, 
        unpacks XML TEI documents from zipped files, converts documents 
        to EstNLTK Text objects, and yields created Text objects.
    
        Parameters
        ----------
        root_dir: str
            The root directory which contains zipped (.zip and tar.gz) 
            XML TEI files;
        focus_input_files: set of str
            Set of input XML files that should be exclusively processed 
            from root_dir. If provided, then only files from the set 
            will be processed, and all other files will be skipped.
            If None, then all files in root_dir will be processed.
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
        add_tokenization: boolean
            If True, then tokenization layers 'tokens', 'compound_tokens', 
            'words', 'sentences', 'paragraphs' will be added to all newly created 
            Text instances;
            If preserve_orig_tokenization is set, then original tokenization in 
            the document will be preserved; otherwise, the tokenization will be
            created with EstNLTK's default tokenization tools;
            (Default: False)
        preserve_tokenization: boolean
            If True, then the original segmentation from the XML file (sentences 
            between <s> and </s>, paragraphs between <p> and </p>, and words &
            tokens separated by spaces) is also preserved in the newly created Text 
            instances;
            Note: this only has effect if add_tokenization has been switched on;
            (Default: False)
        sentence_separator: str
            String to be used as a sentence separator during the reconstruction
            of the text. The parameter value should be provided, None is not 
            allowed.
            (Default: '\n')
        paragraph_separator: str
            String to be used as a paragraph separator during the reconstruction
            of the text. The parameter value should be provided, None is not 
            allowed.
            (Default: '\n\n')
    """
    #global log
    files = os.listdir( root_dir )
    for in_file in files:
        if in_file.endswith('.zip') or in_file.endswith('.gz'):
           in_path = os.path.join(root_dir, in_file)
           for (full_fnm, content) in unpack_zipped_xml_files_iterator(in_path,test_only=False):
               if focus_input_files != None:
                   path_head, path_tail = os.path.split(full_fnm)
                   if path_tail not in focus_input_files:
                       # Skip the XML file if it is not listed
                       continue
               div_target = get_div_target(full_fnm)
               #log.debug('Loading '+full_fnm+' with '+div_target)
               docs = parse_tei_corpus_file_content(content, full_fnm, target=[div_target],\
                                                             add_tokenization=add_tokenization, \
                                                             preserve_tokenization=preserve_tokenization, \
                                                             sentence_separator=sentence_separator,\
                                                             paragraph_separator=paragraph_separator,\
                                                             record_xml_filename=True)
               for doc_id, doc in enumerate(docs):
                   if not create_empty_docs and len(doc.text) == 0:
                      # Skip an empty document
                      continue
                   yield doc


#
# The following iterator functions borrow from Paul's source at:
#      .../estnltk_workflows/postgres_collections/data_import/create_collection.py
#

# function that keeps the original text without splitting
def to_text(text):
    yield text, None, None

# function that splits the original text into paragraphs
def to_paragraphs(text):
    for para_nr, para in enumerate(split_by(text, layer='paragraphs',
                                            layers_to_keep=['tokens', 'compound_tokens', 'words', 'sentences']), start=1):
        yield para, para_nr, None

# function that splits the original text into sentences
def to_sentences(text):
    sent_nr = 0
    for para_nr, para in enumerate(split_by(text, layer='paragraphs',
                                            layers_to_keep=['tokens', 'compound_tokens', 'words', 'sentences']), start=1):
        for sent in split_by(para, layer='sentences', layers_to_keep=['tokens', 'compound_tokens', 'words']):
            sent_nr += 1
            yield sent, para_nr, sent_nr


def process_files(rootdir, doc_iterator, collection, focus_input_files=None,\
                  encoding='utf-8', create_empty_docs=False, logger=None, \
                  tokenization=None, use_sentence_sep_newlines=False, \
                  splittype='no_splitting', metadata_extent='complete', \
                  buffer_size = 1000, insert_query_size = 5000000, \
                  skippable_documents=None ):
    """ Uses given doc_iterator (iter_packed_xml or iter_unpacked_xml) to
        extract texts from the files in the folder root_dir.
        Optionally, adds tokenization layers to created Text objects.
    
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
        focus_input_files: set of str
            Set of input XML files that should be exclusively
            processed from root_dir. If provided, then only files
            from the set will be processed, and all other files 
            will be skipped.
            If None, then all files returned by doc_iterator will
            be processed.
        encoding: str
            Encoding of the XML files. (default: 'utf-8')
        create_empty_docs: boolean
            If True, then documents are also created if there is no 
            textual content, but only metadata content.
            (default: False)
        logger: logging.Logger
            Logger used for debugging messages;
        tokenization: ['none', 'preserve', 'estnltk']
            specifies if tokenization will be added to Texts, and if 
            so, then how it will be added. 
            * 'none'     -- text   will   be   created  without  any 
                            tokenization layers;
            * 'preserve' -- original tokenization from XML files will 
                            be preserved in layers of the text; 
            * 'estnltk'  -- text's original tokenization will be 
                            overwritten by estnltk's tokenization;
        use_sentence_sep_newlines: boolean
            If set, then during the reconstruction of a text string, 
            sentences from the original XML mark-up will always 
            separated from each other by a newline, regardless the 
            tokenization option used.  As a result, sentence endings 
            can also be noticed in the reconstructed text string.
            Otherwise, a single space character will be used as a 
            sentence separator.
            (default: False)
        splittype: ['no_splitting', 'sentences', 'paragraphs']
            specifies if and how texts should be split before inserting
            into the database:
            * 'no_splitting' -- insert full texts, do no split;
            * 'sentences'    -- split into sentences (a Text object 
                                for each sentence), and insert 
                                sentences into database;
            * 'paragraphs'   -- split into paragraphs (a Text object 
                                for each paragraph), and insert 
                                paragraphs into database;
        metadata_extent: ['minimal', 'complete']
            specifies to which extent created Text object should be 
            populated with metadata. 
            (default: 'complete')
        buffer_size: int (default: 1000)
            buffer_size used during the database insert;
        insert_query_size: int (default: 5000000)
            maximum insert query size used during the database insert;
        skippable_documents: set of str (default: None)
            A set of XML document names corresponding to the documents 
            that have already been processed and inserted into the 
            database. All documents inside this set will skipped.
            An XML document name is a string in the format:
                    XML_file_name + ':' + 
                    subdocument_number + ':' + 
                    paragraph_number + ':' + 
                    sentence_number
            Paragraph_number and sentence_number can be missing, if the
            database does not contain the corresponding fields.
            If skippable_documents is None or empty, all processed files 
            will be inserted into the database.
            Note: skippable_documents is more fine-grained set than 
            focus_input_files, thus overrides the skipping directed by
            the later set.
    """
    
    global special_tokens_tagger
    global special_compound_tokens_tagger
    global special_sentence_tokenizer
    assert doc_iterator in [iter_unpacked_xml, iter_packed_xml]
    assert tokenization in [None, 'none', 'preserve', 'estnltk']
    assert splittype in ['no_splitting', 'sentences', 'paragraphs']
    assert metadata_extent in ['minimal', 'complete']
    add_tokenization      = False
    preserve_tokenization = False
    paragraph_separator   = '\n\n'
    if skippable_documents == None:
        skippable_documents = set()
    if tokenization:
        if tokenization == 'none':
           tokenization = None
        if tokenization == 'preserve':
           add_tokenization      = True
           preserve_tokenization = True
        elif tokenization == 'estnltk':
           add_tokenization      = True
           preserve_tokenization = False
    sentence_separator    = ' '
    if use_sentence_sep_newlines:
        sentence_separator = '\n'
    # Choose how the loaded document will be 
    # split before the insertion
    split = to_text
    if args.splittype == 'no_splitting':
        split = to_text
    elif args.splittype == 'sentences':
       split = to_sentences
    elif args.splittype == 'paragraphs':
       split = to_paragraphs
    last_xml_file = ''
    doc_id = 1
    total_insertions    = 0
    xml_files_processed = 0
    with collection.buffered_insert(buffer_size=buffer_size, query_length_limit=insert_query_size) as buffered_insert:
        for doc in doc_iterator(rootdir, focus_input_files=focus_input_files, encoding=encoding, \
                                create_empty_docs=create_empty_docs, \
                                add_tokenization=add_tokenization, preserve_tokenization=preserve_tokenization,\
                                sentence_separator=sentence_separator, paragraph_separator=paragraph_separator):
            # Get subcorpus name
            subcorpus = ''
            if '_xml_file' in doc.meta:
                subcorpus = get_text_subcorpus_name( None, doc.meta['_xml_file'], doc, expand_names=False )
            # Reset the document counter if we have a new file coming up
            xml_file = doc.meta.get('_xml_file', '')
            if last_xml_file != xml_file:
                doc_nr = 1
            # Split the loaded document into smaller units if required
            for doc_fragment, para_nr, sent_nr in split( doc ):
                meta = {}
                # Gather metadata
                # 1) minimal metadata:
                meta['file'] = xml_file
                doc_fragment.meta['file'] = meta['file']
                # Remove redundant attribute '_xml_file'
                if doc_fragment.meta.get('_xml_file', '') == meta['file']:
                   del doc_fragment.meta['_xml_file']
                doc_fragment.meta['subcorpus'] = subcorpus
                meta['subcorpus'] = subcorpus
                if para_nr is not None:
                   meta['document_nr'] = doc_nr
                   doc_fragment.meta['doc_nr'] = doc_nr
                   meta['paragraph_nr'] = para_nr
                   doc_fragment.meta['para_nr'] = para_nr
                if sent_nr is not None:
                   meta['sentence_nr'] = sent_nr
                   doc_fragment.meta['sent_nr'] = sent_nr
                # 2) complete metadata:
                if metadata_extent == 'complete':
                   for key, value in doc.meta.items():
                       doc_fragment.meta[key] = value
                   # Collect remaining metadata
                   for key in ['title', 'type']:
                       meta[key] = doc_fragment.meta.get(key, '')
                # Create an identifier of the insertable chunk:
                #  XML file + subdocument nr + paragraph nr + sentence nr
                file_chunk_lst = [meta['file']]
                file_chunk_lst.append(':')
                file_chunk_lst.append(str(doc_nr))
                if 'paragraph_nr' in meta:
                   file_chunk_lst.append(':')
                   file_chunk_lst.append(str(meta['paragraph_nr']))
                if 'sentence_nr' in meta:
                   file_chunk_lst.append(':')
                   file_chunk_lst.append(str(meta['sentence_nr']))
                file_chunk_str = ''.join( file_chunk_lst )
                # Finally, insert document (if not skippable)
                if file_chunk_str not in skippable_documents:
                   row_id = buffered_insert(text=doc_fragment, meta_data=meta)
                   total_insertions += 1
                if logger:
                   # Debugging stuff
                   # Listing of annotation layers added to Text
                   with_layers = list(doc_fragment.layers.keys())
                   if with_layers:
                      with_layers = ' with layers '+str(with_layers)
                   else:
                      with_layers = ''
                   if file_chunk_str not in skippable_documents:
                      logger.debug((' {} inserted as Text{}.').format(file_chunk_str, with_layers))
                   else:
                      logger.debug((' {} skipped (already in the database).').format(file_chunk_str))
                   #logger.debug('  Metadata: {}'.format(doc_fragment.meta))
            doc_nr += 1
            if last_xml_file != xml_file:
                xml_files_processed += 1
            last_xml_file = xml_file
            #print('.', end = '')
            #sys.stdout.flush()
    if logger:
        logger.info('Total {} XML files processed.'.format(xml_files_processed))
        logger.info('Total {} estnltk texts inserted into the database.'.format(total_insertions))



def fetch_column_names( storage, schema, collection ):
    """ Finds and returns a list of column names of an existing PostgreSQL
        storage.
    
        Parameters
        ----------
        storage: PostgresStorage
            PostgresStorage to be queried for column names of the collection;
        schema: str
            Name of the schema;
        collection: boolean
            Name of the collection / db table;
            
        Returns
        -------
        list of str
            List of column names in given collection;
    """
    colnames = None
    with storage.conn as conn:
         with conn.cursor() as c:
              c.execute(SQL('SELECT * FROM {}.{} LIMIT 0').format(Identifier(schema),
                                                                  Identifier(collection)))
              colnames = [desc[0] for desc in c.description]
    return colnames



def fetch_skippable_documents( storage, schema, collection, meta_fields, logger ):
    """ Fetches names of existing / skippable documents from the PostgreSQL storage.
        Returns a set of existing document names.
        A document name is represented as a string in the format:
               XML_file_name + ':' + 
               subdocument_number + ':' + 
               paragraph_number + ':' + 
               sentence_number
        Paragraph_number and sentence_number are skipped, if they are not in 
        meta_fields.
        
        Parameters
        ----------
        storage: PostgresStorage
            PostgresStorage to be queried for column names of the collection;
        schema: str
            Name of the schema;
        collection: boolean
            Name of the collection / db table;
        meta_fields: OrderedDict
            Current fields of the collection / database table. 
        logger: logger
            For logging the stuff.
        
        Returns
        -------
        set of str
            Set of document names corresponding to documents already existing in 
            the collection;
    """
    # Filter fields: keep only fields that correspond to the fields of 
    # the current table
    query_fields = ['id', 'file', 'document_nr', 'paragraph_nr', 'sentence_nr']
    query_fields = [f for f in query_fields if f == 'id' or f in meta_fields.keys()]
    prev_fname   = None
    fname_doc_nr = 1
    file_chunks_in_db = set()
    # Construct the query
    sql_str = 'SELECT '+(','.join(query_fields))+' FROM {}.{} ORDER BY '+(','.join(query_fields))
    with storage.conn as conn:
        # Named cursors: http://initd.org/psycopg/docs/usage.html#server-side-cursors
        with conn.cursor('read_fname_chunks', withhold=True) as read_cursor:
            try:
                read_cursor.execute(SQL(sql_str).format(Identifier(schema),
                                                        Identifier(collection)))
            except Exception as e:
                logger.error(e)
                raise
            finally:
                logger.debug(read_cursor.query.decode())
            for items in read_cursor:
                doc_id = items[0]
                fname  = items[1]
                if prev_fname and prev_fname != fname:
                    # Reset document number (in case of a new file)
                    fname_doc_nr = 1
                doc_nr = items[2] if 'document_nr' in query_fields else fname_doc_nr
                paragraph_nr = items[3] if 'paragraph_nr' in query_fields else None
                sentence_nr  = items[4] if 'sentence_nr' in query_fields else None
                # Reconstruct file name chunk
                file_chunk_lst = [fname]
                file_chunk_lst.append(':')
                file_chunk_lst.append(str(doc_nr))
                if paragraph_nr:
                   file_chunk_lst.append(':')
                   file_chunk_lst.append(str(paragraph_nr))
                if sentence_nr:
                   file_chunk_lst.append(':')
                   file_chunk_lst.append(str(sentence_nr))
                file_chunk_str = ''.join( file_chunk_lst )
                # Sanity check: file_chunk_str should be unique
                # if not, then we cannot expect skipping to be 
                # consistent ...
                assert file_chunk_str not in file_chunks_in_db, \
                    ' (!) Document chunk {!r} appears more than once in database.'.format(file_chunk_str)
                file_chunks_in_db.add( file_chunk_str )
                prev_fname = fname
                fname_doc_nr += 1
    return file_chunks_in_db



def load_in_file_names( fnm ):
    ''' Loads names of the input XML files from a text file. 
        In the text file, each name should be on a separate line.
        Returns a set of file names.
    '''
    filenames = set()
    with open(fnm, 'r', encoding='utf-8') as f:
       for line in f:
           line = line.strip()
           if len( line ) > 0:
              filenames.add( line )
    return filenames



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       "Loads Koondkorpus XML TEI files (either from zipped archives, or from directories where \n"+\
       "the files have been unpacked), creates EstNLTK Text objects based on these files, adds \n"+\
       "tokenization to Texts (optional), splits Texts into paragraphs or sentences (optional),\n"+\
       "and stores Texts in a PostgreSQL collection.\n",\
       formatter_class=RawTextHelpFormatter
    )
    # 1) Input parameters
    parser.add_argument('rootdir', type=str, \
                        help='the directory containing input corpus files (packed or unpacked)')
    parser.add_argument('-e', '--encoding', type=str, default='utf-8', \
                        help='encoding of the TEI XML files (Default: utf-8).')
    parser.add_argument('-i', '--input_format', dest='input_format', \
                        help='specifies format of the input files:\n\n'+\
                             '* unzipped -- rootdir will be traversed recursively for\n'+\
                             '  XML TEI files of the Koondkorpus. The target files should\n'+\
                             '  be unpacked, but they must be inside the same directory\n'+\
                             '  structure as they were in the packages.\n'+\
                             '\n'+\
                             ' * zipped -- rootdir will be traversed (non-recursively)\n'+\
                             "   for .zip and .gz files. Each archive file will be opened,\n"+\
                             "   and XML TEI files will be extracted.\n"+\
                             '(default: zipped).',\
                        choices=['zipped', 'unzipped'], \
                        default='zipped' )
    # 2) Output parameters: database access
    parser.add_argument('--pgpass', dest='pgpass', action='store', \
                        default='~/.pgpass', \
                        help='name of the PostgreSQL password file (default: ~/.pgpass).\n'+\
                             'The format of the file should be: \n'+\
                             '       hostname:port:database:username:password ')
    parser.add_argument('--schema', dest='schema', action='store',\
                        default='public',\
                        help='name of the collection schema (default: public)')
    parser.add_argument('--collection', dest='collection', action='store',
                        default='collection',
                        help='name of the collection (default: collection)')
    parser.add_argument('--role', dest='role', action='store',
                        help='collection owner (default: None)')
    parser.add_argument('--mode', dest='mode', action='store', choices=['overwrite', 'append'],
                        help='required if the collection already exists')
    parser.add_argument('-b', '--buffer_size', dest='buffer_size', type=int, default=1000,
                        help='buffer size in buffered database insert (default: 1000)')
    parser.add_argument('-q', '--insert_query_size', dest='insert_query_size', type=int, default=5000000,
                        help='maximum insert query size in buffered database insert (default: 5000000)')
    parser.add_argument('-s', '--skip_existing', dest='skip_existing', \
                        default=False, \
                        action='store_true', \
                        help="If set, then all the newly created documents are checked for their\n"+
                             "existence in the database, and any document already in the database\n"+\
                             "will be skipped. Note that the checking is based on file / document\n"+\
                             "names, not by their content. So, this only works if all the XML files\n"+\
                             "in the corpus have unique file names.\n"+
                             "(default: False)",\
                        )
    # 3) Processing parameters 
    parser.add_argument('--in_files', dest='in_files', default = None, \
                        help='specifies a text file containing names of the input XML files\n'+\
                             '(files from rootdir) that should be processed. All other files\n'+\
                             'in rootdir will be skipped.\n\n'+\
                             'File names in the text file should be separated by newlines.\n\n'+\
                             'Use this argument to specify a subset of XML files to be processed\n'+\
                             'while parallelizing the process. \n'+\
                             'You can use the script "split_corpus_files_into_subsets.py" to\n'+\
                             'split the input corpus (either packed or unpacked) into subsets\n'+\
                             'of XML files.')
    parser.add_argument('-t', '--tokenization', dest='tokenization', \
                        help='specifies if and how texts will be reconstructed and tokenized: \n\n'+ \
                             '* none -- the text string will be reconstructed by joining words \n'+\
                             '  and sentences from the original XML mark-up by spaces, and paragraphs\n'+\
                             '  by double newlines. Tokenization layers will not be created.\n\n'+\
                             
                             '* preserve -- the text string will be reconstructed by joining \n'+\
                             '  words from the original XML mark-up by spaces, sentences by \n'+\
                             '  spaces, and paragraphs by double newlines. Tokenization layers \n'+\
                             '  will be created, and they\'ll preserve the original tokenization \n'+\
                             '  of XML files.\n'+\
                             "    Note #1: tokenization layers 'tokens', 'compound_tokens', \n"+\
                             "    'words', 'sentences', 'paragraphs' will be created;\n"+\
                             "    Note #2: the layer 'compound_tokens' will always remain empty \n"+\
                             '    because koondkorpus files do no contain information about token \n'+\
                             '    compounding;\n'+\
                             "    Note #3: the layer 'tokens' will be equal to the layer 'words';"+\
                             '  \n\n'+\
                             '* estnltk -- the text string will be reconstructed by joining words \n'+\
                             '  and sentences from the original XML mark-up by spaces, and \n'+\
                             "  paragraphs by double newlines. Tokenization layers will be created \n"+\
                             "  with EstNLTK's default tokenizers, overwriting the original \n"+\
                             '  tokenization mark-up from XML files.\n'
                             "    Note #1: tokenization layers 'tokens', 'compound_tokens', \n"+\
                             "    'words', 'sentences', 'paragraphs' will be created;\n"+\
                             "(default: none)",\
                        choices=['none', 'preserve', 'estnltk'], \
                        default='none' )
    parser.add_argument('--splittype', dest='splittype', action='store',
                        default='no_splitting', choices=['no_splitting', 'sentences', 'paragraphs'],
                        help='specifies if and how the source texts should be split before\n'+
                             'inserting into the database. Options:\n'+
                             '\n'+
                             '* no_splitting -- each source text will be inserted into the database\n'+\
                             '  as whole, without any splitting applied;\n'+\
                             '\n'+
                             '* paragraphs -- source texts will be split into paragraphs (a Text object\n'+\
                             '  will be created for each paragraph), and then inserted into the\n'+\
                             '  database;\n'
                             '\n'+
                             '* sentences -- source texts will be split into sentences (a Text object\n'+\
                             '  will be created for each sentence), and then inserted into the\n'+\
                             '  database;\n'+\
                             '(default: no_splitting)\n\n'
                             '(!) Note: you can only use --splittype if tokenization is turned on!'
                        )
    parser.add_argument('-n', '--use_sentence_sep_newlines', dest='use_sentence_sep_newlines', \
                        default=False, \
                        action='store_true', \
                        help="If set, then during the reconstruction of a text string, sentences\n"+
                             "from the original XML mark-up will always be separated from each other\n"+
                             "by a newline, regardless the tokenization option used.  As  a  result,\n"+\
                             "sentence endings can also  be  noticed  in  the  reconstructed  text\n"+\
                             "string.\n"+\
                             "Otherwise, a single space character will be used as a sentence separator.\n"+\
                             "(default: False)",\
                        )
    parser.add_argument('-m', '--metadata_extent', dest='metadata_extent', \
                        help='specifies to which extent created Text objects should be \n'+\
                             'populated with metadata. Options:\n\n'
                             ' * minimal -- minimal amount of metadata. Fields: \n'+\
                             "      1. 'subcorpus'    -- short name of the subcorpus; \n"+\
                             "      2. 'file'         -- the XML file name; \n"+\
                             "      3. 'document_nr'  -- number of document in the file; \n"+\
                             "          (if text was split into paragraphs or sentences);\n"+\
                             "      4. 'paragraph_nr' -- paragraph's number in the document\n"+\
                             "          (if text was split into paragraphs or sentences);\n"+\
                             "      5. 'sentence_nr'  -- sentence's number in the document\n"+\
                             "          (if text was split into sentences);\n"+\
                             '\n'+\
                             ' * complete -- all metadata included. Fields: \n'+\
                             "      1. 'subcorpus'    -- short name of the subcorpus; \n"+\
                             "      2. 'file'         -- the XML file name; \n"+\
                             "      3. 'document_nr'  -- number of document in the file; \n"+\
                             "          (if text was split into paragraphs or sentences);\n"+\
                             "      4. 'paragraph_nr' -- paragraph's number in the document\n"+\
                             "          (if text was split into paragraphs or sentences);\n"+\
                             "      5. 'sentence_nr'  -- sentence's number in the document\n"+\
                             "          (if text was split into sentences);\n"+\
                             "      6. 'title'        -- title of the document (if available); \n"+\
                             "      7. 'type'         -- type of the document (if available); \n"+\
                             '(default: complete)',\
                        choices=['minimal', 'complete'], \
                        default='complete' )
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
    if args.splittype != 'no_splitting':
       if args.tokenization == 'none':
          raise Exception('(!) splittype '+str(args.splittype)+' cannot be used without tokenization!')
    if args.buffer_size and args.buffer_size < 0:
       parser.error("Minimum buffer_size is 0")
    if args.insert_query_size and args.insert_query_size < 50:
       parser.error("Minimum insert_query_size is 50")
       
    
    logger.setLevel( (args.logging).upper() )
    log = logger

    # List of input XML files (if selective processing is used)
    focus_input_files = None
    if args.in_files:
       if not os.path.isfile(args.in_files):
          raise Exception('(!) Unable to load list of input file names from file: '+str(args.in_files)+'!')
       else:
          focus_input_files = load_in_file_names( args.in_files )
          log.info('Using XML files listed in {!r} and processing only {} files from {!r}.'.format(args.in_files, len(focus_input_files), args.rootdir) )
    
    # Collect required database meta fields
    fields = [ ('subcorpus', 'str') ]
    fields.append( ('file', 'str') )
    if args.splittype == 'sentences':
         fields.append( ('document_nr', 'bigint') )
         fields.append( ('paragraph_nr', 'int') )
         fields.append( ('sentence_nr', 'bigint') )
    elif args.splittype == 'paragraphs':
         fields.append( ('document_nr', 'bigint') )
         fields.append( ('paragraph_nr', 'int') )
    if args.metadata_extent == 'complete':
         fields.append( ('title', 'str') )
         fields.append( ('type', 'str') )
    meta_fields = OrderedDict( fields )
    
    # Connect with the storage
    storage = PostgresStorage(pgpass_file=args.pgpass,
                              schema=args.schema,
                              role=args.role)
    collection = storage.get_collection(args.collection, meta_fields=meta_fields)
    if collection.exists():
        if args.mode is None:
             log.error(' (!) Collection {!r} already exists, use --mode {{overwrite,append}}.'.format(args.collection))
             exit(1)
        if args.mode == 'overwrite':
             log.info(' Collection {!r} exists. Overwriting.'.format(args.collection))
             collection.delete()
        elif args.mode == 'append':
             # A small sanity check before appending: existing meta fields of the table 
             # should match with newly specified meta fields
             # Note: even if the check will be passed, this still does not assure 100% 
             # that we use exactly the same configuration, e.g. 
             #   two executions of the script may use different approaches for tokenization;
             existing_columns = fetch_column_names( storage, args.schema, args.collection )
             new_columns = ['id', 'data'] + [ name for (name, type) in meta_fields.items() ]
             if existing_columns != new_columns:
                  msg = ' (!) Existing collection {!r} has columns {}, but the new insertions are for columns {}. '
                  msg += 'Please re-check command line arguments to provide right configuration for insertions.'
                  log.error( msg.format(args.collection, existing_columns, new_columns))
                  exit(1)
             log.info('Collection {!r} exists. Appending.'.format(args.collection))

    if not collection.exists():
         collection = storage.get_collection(args.collection, meta_fields=meta_fields)
         tokenization_desc = ''
         if args.tokenization == 'preserve':
             tokenization_desc = ' with original segmentation'
         elif args.tokenization == 'estnltk':
             tokenization_desc = ' with segmentation'
         collection.create('collection of estnltk texts'+tokenization_desc)
         log.info(' New collection {!r} created.'.format(args.collection))
    
    docs_already_in_db = None
    if args.skip_existing == True and args.mode == 'append':
         # If skipping is required, load documents that are already in DB
         docs_already_in_db = \
             fetch_skippable_documents(storage, args.schema, args.collection, meta_fields, log)
         log.info(('Collection {!r} contains {} existing documents. '+\
                   'Existing documents will be skipped.').format(args.collection, len(docs_already_in_db)) )
    
    if args.splittype == 'no_splitting':
         log.info(' Source texts will not be splitted.')
    elif args.splittype == 'sentences':
         log.info(' Source texts will be splitted by sentences.')
    elif args.splittype == 'paragraphs':
         log.info(' Source texts will be splitted by paragraphs.')

    startTime = datetime.now()
    process_files(args.rootdir, doc_iterator, collection, encoding=args.encoding, \
                  create_empty_docs=False, logger=log, tokenization=args.tokenization,\
                  use_sentence_sep_newlines=args.use_sentence_sep_newlines, \
                  splittype=args.splittype, metadata_extent=args.metadata_extent, \
                  focus_input_files=focus_input_files, buffer_size=args.buffer_size, \
                  insert_query_size=args.insert_query_size, \
                  skippable_documents=docs_already_in_db)
    storage.close()
    time_diff = datetime.now() - startTime
    log.info('Total processing time: {}'.format(time_diff))
       

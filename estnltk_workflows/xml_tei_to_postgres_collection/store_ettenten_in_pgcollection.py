#
#   Loads etTenTen 2013 corpus from a file ("etTenTen.vert" or "ettenten13.processed.prevert"),
#  creates EstNLTK Text objects based on etTenTen's documents, adds tokenization to Texts (optional), 
#  and stores Texts in a PostgreSQL collection.
# 

import re
import os, sys
import os.path
import argparse

from datetime import datetime

from argparse import RawTextHelpFormatter
from collections import OrderedDict

from psycopg2.sql import SQL, Identifier

from estnltk import logger
from estnltk.corpus_processing.parse_ettenten import parse_ettenten_corpus_file_iterator
from estnltk.storage.postgres import PostgresStorage



def process_files(in_file, collection, focus_doc_ids=None,\
                  encoding='utf-8', discard_empty_paragraphs=True, logger=None, \
                  tokenization=None, insert_query_size = 5000000, \
                  skippable_documents=None, doc_id_to_texttype=None ):
    """ Reads etTenTen 2013 corpus from in_file, extracts documents and 
        reconstructs corresponding Text objects, and stores the results 
        in given database collection.
        Optionally, adds tokenization layers to created Text objects.
    
        Parameters
        ----------
        in_file: str
           Full name of etTenTen corpus file (name with path);
        collection:  estnltk.storage.postgres.collection.PgCollection
            EstNLTK's PgCollection where extracted Texts should be 
            stored;
        focus_doc_ids: set of str
            Set of document id-s corresponding to the documents which 
            need to be extracted from the in_file.
            If provided, then only documents with given id-s will be 
            processed, and all other documents will be skipped.
            If None or empty, then all documents in the file will be 
            processed;
        encoding: str
            Encoding of in_file. Defaults to 'utf-8';
        discard_empty_paragraphs: boolean
            If set, then empty paragraphs will be discarded.
            (default: True)
        logger: logging.Logger
            Logger used for debugging etc messages;
        tokenization: ['none', 'preserve', 'estnltk']
            specifies if tokenization will be added to Texts, and if 
            so, then how it will be added. 
            * 'none'     -- text   will   be   created  without  any 
                            tokenization layers;
            * 'preserve' -- original tokenization from XML files will 
                            be preserved in layers of the text. Note
                            that etTenTen only has original tokenization 
                            for paragraphs, and thus Texts will only have
                            original_paragraphs layer, nothing more.
            * 'estnltk'  -- text's original tokenization will be 
                            overwritten by estnltk's tokenization;
        insert_query_size: int (default: 5000000)
            maximum insert query size used during the database insert;
        skippable_documents: set of str (default: None)
            A set of web document ids corresponding to the documents 
            that have already been processed and inserted into the 
            database. All documents inside this set will skipped.
            A web document is a string in the format:
               original_doc_id + ':' + 
               subdocument_number + ':' + 
               paragraph_number + ':' + 
               sentence_number
            Subdocument_number, paragraph_number and sentence_number are 
            skipped, if the database does not contain the corresponding 
            fields.
            If skippable_documents is None or empty, all processed files 
            will be inserted into the database.
            Note: skippable_documents is more fine-grained set than 
            focus_doc_ids, thus overrides the skipping directed by
            the later set.
        doc_id_to_texttype: dict (default: None)
            A mapping from document ids (strings) to their texttypes.
            Should cover all documents listed in focus_doc_ids, or
            if focus_doc_ids==None, all documents in in_file;
    """
    assert tokenization in [None, 'none', 'preserve', 'estnltk']
    add_tokenization      = False
    preserve_tokenization = False
    if skippable_documents == None:
        skippable_documents = set()
    if tokenization:
        if tokenization == 'none':
           add_tokenization      = False
           preserve_tokenization = False
        if tokenization == 'preserve':
           add_tokenization      = False
           preserve_tokenization = True
        elif tokenization == 'estnltk':
           add_tokenization      = True
           preserve_tokenization = False

    doc_nr = 1
    last_original_doc_id  = None
    total_insertions = 0
    docs_processed   = 0
    with collection.insert(query_length_limit=insert_query_size) as buffered_insert:
        for web_doc in parse_ettenten_corpus_file_iterator( in_file, encoding=encoding, \
                                              focus_doc_ids=focus_doc_ids, \
                                              discard_empty_paragraphs=discard_empty_paragraphs, \
                                              add_tokenization=add_tokenization, \
                                              store_paragraph_attributes=True, \
                                              paragraph_separator='\n\n' ):
            # Rename id to original_doc_id (to avoid confusion with DB id-s)
            original_doc_id = web_doc.meta.get('id')
            web_doc.meta['original_doc_id'] = original_doc_id
            del web_doc.meta['id']
            
            # Reset subdocument counter (if required)
            if last_original_doc_id != original_doc_id:
                doc_nr = 1
            
            # Delete original_paragraphs layer (if tokenization == None)
            if not add_tokenization and not preserve_tokenization:
                delattr(web_doc, 'original_paragraphs') # Remove layer from the text

            # Add texttype (if mapping is available)
            if doc_id_to_texttype and original_doc_id in doc_id_to_texttype:
                web_doc.meta['texttype'] = doc_id_to_texttype[original_doc_id]
            
            # Gather metadata
            meta = {}
            for key, value in web_doc.meta.items():
                meta[key] = value

            # Create an identifier of the insertable chunk:
            #  original_doc_id + ':' + subdocument_number (+ ':' + paragraph_number + ':' + sentence_number)
            file_chunk_lst = [web_doc.meta['original_doc_id']]
            file_chunk_lst.append(':')
            file_chunk_lst.append(str(doc_nr))
            file_chunk_str = ''.join( file_chunk_lst )

            # Finally, insert document (if not skippable)
            if file_chunk_str not in skippable_documents:
               row_id = buffered_insert(text=web_doc, meta_data=meta)
               total_insertions += 1
            if logger:
               # Debugging stuff
               # Listing of annotation layers added to Text
               with_layers = list(web_doc.layers.keys())
               if with_layers:
                  with_layers = ' with layers '+str(with_layers)
               else:
                  with_layers = ''
               if file_chunk_str not in skippable_documents:
                  logger.debug((' {}:{} inserted as Text{}.').format(meta['web_domain'], file_chunk_str, with_layers))
               else:
                  logger.debug((' {}:{} skipped (already in the database).').format(meta['web_domain'], file_chunk_str))
            doc_nr += 1
            last_original_doc_id = original_doc_id
            docs_processed += 1
            #print('.', end = '')
            #sys.stdout.flush()
    if logger:
        logger.info('Total {} input documents processed.'.format(docs_processed))
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
               original_doc_id + ':' + 
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
    query_fields = ['original_doc_id', 'id', 'paragraph_nr', 'sentence_nr']
    query_fields = [f for f in query_fields if f == 'id' or f in meta_fields.keys()]
    prev_original_doc_id = None
    subdocument_nr  = 1
    file_chunks_in_db = set()
    # Construct the query
    sql_str = 'SELECT '+(','.join(query_fields))+' FROM {}.{} ORDER BY '+(','.join(query_fields))
    with storage.conn as conn:
        # Named cursors: http://initd.org/psycopg/docs/usage.html#server-side-cursors
        with conn.cursor('read_doc_id_chunks', withhold=True) as read_cursor:
            try:
                read_cursor.execute(SQL(sql_str).format(Identifier(schema),
                                                        Identifier(collection)))
            except Exception as e:
                logger.error(e)
                raise
            finally:
                logger.debug(read_cursor.query.decode())
            for items in read_cursor:
                original_doc_id = str(items[0])
                doc_id     = items[1]
                if prev_original_doc_id and prev_original_doc_id != original_doc_id:
                    # Reset web document id (in case of a new document)
                    subdocument_nr = 1
                paragraph_nr = items[3] if 'paragraph_nr' in query_fields else None
                sentence_nr  = items[4] if 'sentence_nr' in query_fields else None
                # Reconstruct file name chunk
                file_chunk_lst = [str(original_doc_id)]
                file_chunk_lst.append(':')
                file_chunk_lst.append(str(subdocument_nr))
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
                prev_original_doc_id = str(original_doc_id)
                subdocument_nr += 1
    return file_chunks_in_db



def load_in_doc_ids( fnm ):
    '''Loads insertable document ids from a text file. In the 
       text file, each name should be on a separate line.
       Returns a set of file names.
    '''
    ids = set()
    with open(fnm, 'r', encoding='utf-8') as f:
       for line in f:
           line = line.strip()
           if len( line ) > 0:
              ids.add( line )
    return ids



def load_doc_texttypes( doc_texttypes_file, focus_doc_ids ):
    '''Loads document texttypes from a text file that lists
       doc tags and corresponding attributes. For example,
       a single doc tag should be in the format:
        
          <doc id="5" ... url="http://blog.vm.ee/" ... texttype="blog">
        
       In the text file, each document tag should be on a 
       separate line.
       Returns a mapping from doc id-s to corresponding 
       texttype-s.
    '''
    mapping = dict()
    doc_id_finder = re.compile('\sid="([0-9]+)"\s')
    texttype_finder = re.compile('\stexttype="([^"]+)"')
    with open(doc_texttypes_file, 'r', encoding='utf-8') as f:
       for line in f:
           line = line.strip()
           if len(line) > 0:
               docid = None
               texttype = None
               m1 = doc_id_finder.search(line)
               m2 = texttype_finder.search(line)
               if not m1:
                   raise Exception('(!) Unexpected doc tag format: missing id in:'+str(line))
               else:
                   docid = m1.group(1)
               if focus_doc_ids and docid not in focus_doc_ids:
                   # Skip docid not in focus_doc_ids
                   continue
               if not m2:
                   raise Exception('(!) Unexpected doc tag format: missing texttype in:'+str(line))
               else:
                   texttype = m2.group(1)
               mapping[docid] = texttype
    if focus_doc_ids:
        assert len(focus_doc_ids) == len(mapping.keys()), \
            '(!) Number of focused doc ids does not match the '+\
            'number of loaded texttypes: {} vs {}'.format( len(focus_doc_ids), \
                                                           len(mapping.keys()) )
    return mapping



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
       '  Loads etTenTen 2013 corpus from a file ("etTenTen.vert" or "ettenten13.processed.prevert"),\n'+\
       "creates an EstNLTK Text object for each document in the corpus, adds tokenization to Texts \n"+\
       "(optional), and stores Texts in a PostgreSQL collection.",\
       formatter_class=RawTextHelpFormatter
    )
    # 1) Input parameters
    parser.add_argument('in_file', default = None, \
                        help='full path to the (unpacked) input etTenTen 2013 corpus file \n'+ \
                             '("etTenTen.vert" or "ettenten13.processed.prevert").')
    parser.add_argument('-e', '--encoding', type=str, default='utf-8', \
                        help='encoding of the input file (Default: utf-8).')
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
    parser.add_argument('-q', '--insert_query_size', dest='insert_query_size', type=int, default=5000000,
                        help='Maximum number of bytes/symbols allowed in database insert.\n'+
                             'The insertion buffer is flushed every time this maximum gets exceeded.\n'+
                             '(default: 5000000)')
    parser.add_argument('-s', '--skip_existing', dest='skip_existing', \
                        default=False, \
                        action='store_true', \
                        help="If set, then all the newly created documents are checked for their\n"+
                             "existence in the database, and any document already in the database\n"+\
                             "will be skipped. Note that the checking is based on document ids,\n"+\
                             "not by their content.\n"+\
                             "(default: False)",\
                        )
    # 3) Processing parameters 
    parser.add_argument('-i', '--in_doc_ids', dest='in_doc_ids', default = None, \
                        help='specifies a text file containing ids of documents (from the corpus)\n'+\
                             'that should be processed. All other documents in   in_file  will be\n'+\
                             'skipped.\n\n'+\
                             'Documents ids should be integers and separated from each other by \n'+\
                             'newlines. Use this argument to specify a subset of documents to be \n'+\
                             'processed while parallelizing the process. \n'+\
                             'You can use the script "split_ettenten_files_into_subsets.py" to\n'+\
                             'split the input corpus into subsets of document ids.\n' )
    parser.add_argument('--texttypes', dest='doc_texttypes', default = None, \
                        help='specifies a text file that lists XML doc tags with texttype attributes.\n'+\
                             'Each doc tag should also specify document id, which will be used to \n'+\
                             'identify the document. An example of tag:\n'+\
                             '  <doc id="5" ... url="http://blog.vm.ee/" ... texttype="blog">\n'+\
                             'There should be a tag available for each document in  in_file.\n'+\
                             'Each tag should be on a separate line in doc_texttypes file.' )
    parser.add_argument('-t', '--tokenization', dest='tokenization', \
                        help='specifies if and how texts will be reconstructed and tokenized: \n\n'+ \
                             '* none -- the text string will be reconstructed by joining paragraphs\n'+\
                             '  from the original mark-up by double newlines. No tokenization layers \n'+\
                             '  will be created.\n\n'+\
                             
                             '* preserve -- the text string will be reconstructed by joining \n'+\
                             '  paragraphs from the original mark-up by double newlines. A layer\n'+\
                             '  containing original paragraph annotations will also be added to the\n'+\
                             '  Text object, but otherwise there will be no tokenization layers.\n'+\
                             "    Note #1: tokenization layer 'original_paragraphs' will be created;\n\n"+\
                             
                             '* estnltk -- the text string will be reconstructed by joining \n'+\
                             '  paragraphs from the original mark-up by double newlines. Other \n'+\
                             "  tokenization layers will be created with EstNLTK's default tokenizers\n"+\
                             "  and EstNLTK's tokenization annotations will be fit inside the original \n"+\
                             '  paragraphs.\n'
                             "    Note #1: tokenization layers 'tokens', 'compound_tokens', \n"+\
                             "    'words', 'sentences', 'paragraphs' will be created;\n"+\
                             "(default: none)",\
                        choices=['none', 'preserve', 'estnltk'], \
                        default='none' )
    # 4) Logging parameters
    parser.add_argument('--logging', dest='logging', action='store', default='info',\
                        choices=['debug', 'info', 'warning', 'error', 'critical'],\
                        help='logging level (default: info)')
    args = parser.parse_args()

    if not os.path.isfile(args.in_file):
       parser.error('(!) Argument in_file should be an existing file')
    if args.insert_query_size and args.insert_query_size < 50:
       parser.error("Minimum insert_query_size is 50")
    
    logger.setLevel( (args.logging).upper() )
    log = logger

    # Load the list of insertable document ids (if selective processing is used)
    focus_doc_ids = None
    if args.in_doc_ids:
       if not os.path.isfile(args.in_doc_ids):
          raise Exception('(!) Unable to load list of document ids from file: '+str(args.in_doc_ids)+'!')
       else:
          log.debug('Loading insertable document ids from file {!r}.'.format( args.in_doc_ids) )
          focus_doc_ids = load_in_doc_ids( args.in_doc_ids )
          log.info('Using document ids listed in {!r} and processing only {} documents from {!r}.'.format( args.in_doc_ids, len(focus_doc_ids), args.in_file ) )

    # Load a mapping from doc ids to document types (if available)
    doc_id_to_texttype = None
    if args.doc_texttypes:
       if not os.path.isfile(args.doc_texttypes):
          raise Exception('(!) Unable to load document texttypes from file: '+str(args.doc_texttypes)+'!')
       else:
          log.debug('Loading document texttypes from file {!r}.'.format( args.doc_texttypes) )
          doc_id_to_texttype = load_doc_texttypes( args.doc_texttypes, focus_doc_ids )
          log.info('Using {} document texttypes from file {!r}.'.format( len(doc_id_to_texttype.keys()), args.doc_texttypes ) )

    # Collect required database meta fields
    # An example of doc tag (metadata in attribs)
    #   <doc id="5" length=" 10k-100k" crawl_date="2013-01-10" url="http://blog.vm.ee/" web_domain="blog.vm.ee" langdiff="0.40" texttype="blog">
    fields = [ ('original_doc_id', 'bigint') ]
    fields.append( ('url', 'str') )
    fields.append( ('web_domain', 'str') )
    fields.append( ('crawl_date', 'str') )
    fields.append( ('langdiff', 'float') )
    if doc_id_to_texttype is not None:
         fields.append( ('texttype', 'str') )
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
    
    startTime = datetime.now()
    process_files( args.in_file, collection, focus_doc_ids=focus_doc_ids,\
                   encoding=args.encoding, discard_empty_paragraphs=True, logger=log, \
                   tokenization=args.tokenization, insert_query_size=args.insert_query_size, \
                   skippable_documents=docs_already_in_db, doc_id_to_texttype=doc_id_to_texttype )
    storage.close()
    time_diff = datetime.now() - startTime
    log.info('Total processing time: {}'.format(time_diff))

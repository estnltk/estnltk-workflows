#
#  Rewrites texts & metadata from one EstNLTK's 
#  pgcollection to another.
#
#  Picks a small random subset of documents for 
#  rewriting;
#
#  Assumes meta fields corresponding to those 
#  of koondkorpus;
#

import os, sys, re
import os.path
import logging
from random import randint, seed

from datetime import datetime

from collections import OrderedDict

from psycopg2.sql import SQL, Identifier

from estnltk import Text
from estnltk.storage.postgres import PostgresStorage

from pgpass_utils import read_info_from_pgpass_file

PGPASS_FILE = 'pgpass.txt'

SRC_COLLECTION = 'koondkorpus'
SRC_SCHEMA     = ''
SRC_ROLE       = ''

TARGET_COLLECTION = 'koondkorpus_subset_of_5000_v2'
TARGET_COLLECTION_DESC = 'Collection of 5000 randomly picked Koondkorpus texts (v2)'
TARGET_SCHEMA     = ''
TARGET_ROLE       = ''


def fetch_column_names( storage, schema, collection ):
    """ Finds and returns a list of column names of an existing PostgreSQL
        storage."""
    colnames = None
    with storage.conn as conn:
         with conn.cursor() as c:
              c.execute(SQL('SELECT * FROM {}.{} LIMIT 0').format(Identifier(schema),
                                                                  Identifier(collection)))
              colnames = [desc[0] for desc in c.description]
    return colnames


def fetch_number_of_rows( storage, schema, collection ):
    """ Finds and returns the number of rows in the PostgreSQL
        collection."""
    number_of_rows = None
    with storage.conn as conn:
         with conn.cursor() as c:
              c.execute(SQL('SELECT COUNT(*) FROM {}.{};').format(Identifier(schema),
                                                                  Identifier(collection)))
              number_of_rows = c.fetchone()[0]
    return number_of_rows

logging_level = 'info'
logging.basicConfig( level=(logging_level).upper() )
log = logging.getLogger(__name__)

# ===========================================
#   Create access
# ===========================================

# Load the access info
access_info = read_info_from_pgpass_file( PGPASS_FILE )
storage1 = PostgresStorage(dbname   = access_info['dbname'],
                           user     = access_info['user'], 
                           password = access_info['passwd'], 
                           host     = access_info['host'], 
                           port     = access_info['port'],
                           schema   = SRC_SCHEMA, 
                           role     = SRC_ROLE)

# ===========================================
#   Prepare input & output corpus
# ===========================================
in_collection = storage1.get_collection(SRC_COLLECTION)
assert in_collection.exists()
log.info(' Collection {!r} exists. '.format(SRC_COLLECTION))

print('Other existing collections:')
for collection in storage1.collections:
    print('  ',collection)

# Create meta fields for the target collection
knowns_fields = [ ('subcorpus', 'str'), \
                  ('file', 'str'), \
                  ('document_nr', 'bigint'), \
                  ('paragraph_nr', 'int'), \
                  ('sentence_nr', 'bigint'), \
                  ('document_nr', 'bigint'), \
                  ('paragraph_nr', 'int'), \
                  ('title', 'str'), \
                  ('type', 'str') ]
knowns_fields = OrderedDict( knowns_fields )
in_collection_cols = fetch_column_names( storage1, SRC_SCHEMA, SRC_COLLECTION )
new_meta_fields = OrderedDict()
for col in in_collection_cols:
    if col.lower() in ['id','data']:
        continue
    if col.lower() in knowns_fields:
        new_meta_fields[col.lower()] = knowns_fields[col.lower()]
    else:
        raise Exception('(!) Unexpceted field {!r} in {!r}'.format(col.lower(), SRC_COLLECTION))
print(' Collection {!r} has columns {!r}'.format(SRC_COLLECTION, in_collection_cols))

# Metadata keys mapping: from text.meta to database.field
meta_mapping = { 'file':'file', \
                 'subcorpus':'subcorpus',\
                 'document_nr':'doc_nr',\
                 'paragraph_nr':'para_nr',\
                 'sentence_nr':'sent_nr',\
                 'title':'title',\
                 'type':'type' }

# Pick a random subset for rewriting
seed(5)
random_set_size = 5000
max_size = len(in_collection)
picked_ids = []
failed_attempts = 0
while len(picked_ids) < random_set_size:
    i = randint(1, max_size - 1)
    if i not in picked_ids:
        picked_ids.append( i )
    else:
        failed_attempts += 1
        if failed_attempts >= 20:
            print('(!) 20 unsuccessful random picks in a row: terminating ...')
            break
picked_ids = sorted(picked_ids)

storage2 = PostgresStorage(dbname   = access_info['dbname'],
                           user     = access_info['user'], 
                           password = access_info['passwd'], 
                           host     = access_info['host'], 
                           port     = access_info['port'],
                           schema   = TARGET_SCHEMA, 
                           role     = TARGET_ROLE)

try:
    out_collection = storage2.get_collection(TARGET_COLLECTION, meta_fields=new_meta_fields)
    if out_collection.exists():
        log.info(' (!) Collection {!r} already exists. Terminating process.'.format(TARGET_COLLECTION))
        #log.info(' (!) Collection {!r} already exists. Deleting it now.'.format(TARGET_COLLECTION))
        #out_collection.delete()
    else:
        # process with rewriting the collection
        log.info(' (!) Creating brand new collection {!r}.'.format(TARGET_COLLECTION))
        out_collection.create(TARGET_COLLECTION_DESC)
        docs = 0
        insert_query_size = 5000000
        with out_collection.insert(query_length_limit=insert_query_size) as buffered_insert:
            #for key, text in in_collection.select( keys=[1,500,1000,5000,10000,50000,75000,100000,350000,700000], progressbar='ascii', layers=[] ):
            for key, text in in_collection.select( keys=picked_ids, progressbar='ascii', layers=[] ):
                # collect metadata
                meta = {}
                for column in in_collection_cols:
                    if column.lower() in ['id', 'data']:
                        continue
                    assert column in meta_mapping
                    value = text.meta.get(meta_mapping[column], None)
                    meta[column] = value
                # create new text object (with new structure)
                new_text = Text( text.text )
                new_text.meta = text.meta
                assert new_text.text == text.text
                row_id = buffered_insert(text=new_text, meta_data=meta)
                docs += 1
        print('Total documents rewritten:', docs)
finally:
    storage1.close()
    storage2.close()

#
#  Splits texts of Koondkorpus' PostgreSQL collection into sentences. 
#  Adds 'morph_extended' layer to each sentence and saves each sentence 
#  (along with appropriate collection metadata) as a separate Text object 
#  into a new collection. 
#
#  This script supports data parallelization: you can launch multiple instances 
#  of the script and give each instance a (non-overlapping) sub set of data for 
#  processing. Use command line parameters `module,remainder` to process only 
#  texts for which holds `text_id % module == remainder`. 
#  For instance: 
#
#      python 00_split_into_sentences.py  confs\..split_into_sentences.ini   2,0
#      ... processes texts 0, 2, 4, 6, 8, ...
#  
#  This code is based on Kaire's script "1_collection_splitting.py". 
#  Original source:
#     https://github.com/estnltk/syntax_experiments/blob/59190d83e79b780890150ab7d515c2d72dc8a9e6/collection_splitting/1_collection_splitting.py
#

import re
import argparse
import os, os.path
import configparser

from estnltk import Text, logger
from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import create_schema
from estnltk.storage.postgres import BlockQuery

from estnltk_core.layer_operations import split_by_sentences

# Parse input parameters
parser = argparse.ArgumentParser(description=
    "Splits texts of Koondkorpus' PostgreSQL collection into sentences and "+
    "saves to a new collection. "+
    "In detail: splits documents into sentences, adds layers up to the "+
    "'morph_extended' layer to each sentence, and saves each sentence "+
    '(along with appropriate collection metadata) as a separate Text object '+
    'into a new collection. '+
    'This script supports data parallelization: you can launch multiple instances '+
    'of the script and give each instance a (non-overlapping) sub set of the source '+
    'collection for processing. Use command line parameters `module,remainder` '+
    r'to process only texts for which holds `text_id % module == remainder`. ')
parser.add_argument("file", help="Configuration INI file name. Specifies parameters of the source and target collection.", 
                    type=str)
parser.add_argument("module_and_reminder", 
                    nargs='?', type=str, default='1,0', 
                    help="Comma separated integer values of module and remainder, e.g. `1,0`, `2,1`, `4,2`. "+
                         "These values are used to control data parallelization: only texts with "+
                         "`text_id %% module == remainder` will be selected for processing. "+
                         "(default: '1,0')")
parser.add_argument('--text_id', dest='text_id_name', action='store', type=str, default='text_no',\
                    help="name of the `text_id` metadata field in the target collection. "+
                         "(default: 'text_no')" )
args = parser.parse_args()
module_and_reminder = args.module_and_reminder
m = re.match('(\d+)[,:;](\d+)', module_and_reminder)
if m:
    module = int(m.group(1))
    assert module > 0
    remainder = int(m.group(2))
    assert remainder < module
else:
    module = 1
    remainder = 0
text_id_name = args.text_id_name

# Read configuration
conf_file = args.file
config_file_full = os.path.abspath(os.path.expanduser(os.path.expandvars(str(conf_file))))
config = configparser.ConfigParser()
if len(config.read(config_file_full)) != 1:
    raise ValueError("File {file} is not accessible or is not in valid INI format".format(file=config_file_full))
# Check the configuration
for option in ["host", "port", "database_name", "username", "password", "work_schema", "role", "collection"]:
    if option not in list(config["source_database"]) or option not in list(config["target_database"]):
        msg = "Error in file {}. Missing field \"{}\".\n".format(conf_file, option)
        print(msg)
        raise SystemExit
    if option == "collection" and "collection_description" not in list(config["target_database"]):
        msg = "Error in file {}. Missing field \"{}\".\n".format(conf_file, "collection_description")
        print(msg)
        raise SystemExit
    if config["source_database"][option] == "" or config["target_database"][option] == "":
        msg = "Error in file {}. Empty value for \"{}\".\n".format(conf_file, option)
        print(msg)
        raise SystemExit

# Source: postgres storage where the input texts are
source_storage = PostgresStorage(host=config["source_database"]["host"],
                          port=config["source_database"]["port"],
                          dbname=config["source_database"]["database_name"],
                          user=config["source_database"]["username"],
                          password=config["source_database"]["password"],
                          schema=config["source_database"]["work_schema"], 
                          role=config["source_database"]["role"],
                          temporary=False)

source_collection = source_storage[config["source_database"]["collection"]]

# Target: database where the split sentences will be saved
target_storage = PostgresStorage(host=config["target_database"]["host"],
                          port=config["target_database"]["port"],
                          dbname=config["target_database"]["database_name"],
                          user=config["target_database"]["username"],
                          password=config["target_database"]["password"],
                          schema=config["target_database"]["work_schema"], 
                          role=config["target_database"]["role"],
                          temporary=False)

# Create new / target collection if needed
if config["target_database"]["collection"] not in target_storage.collections:
    meta_fields={text_id_name: 'int', 
                 'sent_start': 'int', 
                 'sent_end': 'int', 
                 'subcorpus':'str', 
                 'file': 'str', 
                 'title': 'str', 
                 'type': 'str'}
    target_storage.add_collection(config["target_database"]["collection"], 
                                  description=config["target_database"]["collection_description"], 
                                  meta=meta_fields)

collection = target_storage[config["target_database"]["collection"]]

# Split texts into sentences, re-annotate and save as sentence Text objects
try:
    for text_id, text_obj in source_collection.select( BlockQuery(module, remainder) ):
        analysed = Text(text_obj.text).tag_layer(['sentences', 'morph_extended'])
        sentence_starts = [span.start for span in analysed.sentences]
        sentence_ends = [span.end for span in analysed.sentences]
        sentences = split_by_sentences(analysed, layers_to_keep=list(analysed.layers))
        with collection.insert() as collection_insert:
            sent_counter = 0
            for sent in sentences:
                sent.meta[text_id_name] = text_id
                sent.meta['sent_start'] = sentence_starts[sent_counter]
                sent.meta['sent_end'] = sentence_ends[sent_counter]
                sent.meta['subcorpus'] = text_obj.meta["subcorpus"]
                sent.meta['file'] = text_obj.meta["file"]
                sent.meta['title'] = text_obj.meta["title"]
                sent.meta['type'] = text_obj.meta["type"]
                collection_insert(sent, meta_data=sent.meta)
                sent_counter += 1
except Exception as e: 
    print(f"Problem during splitting and saving sentences with text id {text_id}: ", str(e).strip())
    raise
finally:
    target_storage.close()
    source_storage.close()

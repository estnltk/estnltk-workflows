# Workflows for processing Estonian National Corpus 2023

This folder contains workflows for processing the vert format Estonian National Corpus 2023 (_Eesti keele koondkorpus_) with EstNLTK. 

---

## Preprocessing and syntactic annotation workflow

🚧🚧🚧 TO BE DOCUMENTED LATER (work in progress) 🚧🚧🚧

--- 

## Database insertion workflow

### Requirements

* [**Psycopg 2**](https://www.psycopg.org) (installation: `pip install psycopg2-binary`)
* estnltk ( v1.7.3+ )

### Setup and configuration

For each insertable collection, there should be: 1) a collection directory (e.g. `literature_old`, `literature_contemporary`), and 2) collection's configuration file (e.g. `literature_old.ini`, `literature_contemporary.ini`). 
Collection directories are large, and will be distributed separately; configuration files can be found from the directory [confs/](confs/).

#### Configuring database access

The first step is to configure database access parameters. 
This can be done in file [confs/database_conf.ini](confs/database_conf.ini), and the configuration will be commonly used for all collections. 
Find more information about the access parameters from [this tutorial](https://github.com/estnltk/estnltk/blob/main/tutorials/storage/storing_text_objects_in_postgres.ipynb).

#### Creating database tables

Before document insertion, all necessary tables need to be created. 
This can be done with script `d_create_collection_tables.py`. 
Pass name of the collection's configuration as an argument of the script. 

Example:

`python  d_create_collection_tables.py  confs/literature_old.ini` creates collection tables for `literature_old`. Normally (i.e. if the configuration has not been changed), the following tables will be created:

  * `__collections` - a meta table listing all collections and their versions. This is created only once per schema.
  * `literature_old` - base table of the collection, for storing all documents (EstNLTK `Text` objects in JSON), but without linguistic annotations;
  * `literature_old__structure` - table describing structure of the collection: which annotation layers each document has and what are specific properties of layers;  
  * `literature_old__meta` - table for storing metadata of collection's documents (e.g. `src`, `original_author`, `original_title`,  `title`, `publisher`);
  * `literature_old__words__layer` - table for storing word annotations of all documents;
  * `literature_old__sentences__layer` - table for storing sentence annotations of all documents;
  * `literature_old__sentences__hash` - table for storing sentence hash fingerprints of all documents; hash fingerprints are collected and stored for the purposes of later collection updating: for detecting which sentence tokenizations have been changed and which remain same;
  * `literature_old__morphosyntax__layer` - table for storing morphological and syntactic annotations of all documents;
 
_Erasing mode:_ pass flag `-r` to the script to remove the existing collection and start from the scratch. Be aware that this deletes all the existing tables along with their content.


### Document insertion

Use script `e_import_json_files_to_collection.py` for importing contents of the collection directory to the Postgres database. 
Pass name of the collection's configuration as an argument of the script.  

Example:

`python  e_import_json_files_to_collection.py  confs/literature_old.ini` reads document JSON files from the collection directory `literature_old`, and stores in the collection's tables.

Note: its advisable to use the collection via [EstNLTK's database interface](https://github.com/estnltk/estnltk/blob/main/tutorials/storage/storing_text_objects_in_postgres.ipynb) **only after the document insertion has been completed**. During the insertion, the collection may be in an inconsistent state: some of the documents/annotations might be incomplete, and queries might give errors.
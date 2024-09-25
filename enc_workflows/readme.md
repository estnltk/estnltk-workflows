# Workflows for processing Estonian National Corpus 2023

This folder contains workflows for processing the vert format Estonian National Corpus 2023 (_Eesti keele koondkorpus_) with EstNLTK. 

---

## Preprocessing and syntactic annotation workflow

🚧🚧🚧 TO BE DOCUMENTED LATER (work in progress) 🚧🚧🚧

--- 

## Database insertion workflow

### Requirements

* [**Psycopg 2**](https://www.psycopg.org) (installation: `pip install psycopg2-binary`)
* [estnltk](https://github.com/estnltk/estnltk) ( v1.7.3+ )

### Setup and configuration

In order to insert a collection, you need: 1) a collection directory (e.g. `literature_old`, `literature_contemporary`) containing document JSON files (and metadata descriptions), and 2) collection's configuration file (e.g. `literature_old.ini`, `literature_contemporary.ini`).

Collection directories are large, and will be distributed separately; configuration files can be found from the directory [confs/](confs/).

Collection directories have a structure, in which the first level subdirectories correspond to processed .vert files, the second level subdirectories correspond to document groups (each group contains at maximum 30000 documents) and the third level subdirectories contain actual document JSON files (`doc.json`). 

An example: the structure of collection directory `literature_old`:

	literature_old
	├── meta_fields.txt
	└── nc23_Literature_Old
	    ├── 0
	    │   ├── 0
	    │   │   └── doc.json
	    │   ├── 1
	    │   │   └── doc.json
	    │   ├── 2
	    │   │   └── doc.json
	    │   ├── 3
	    │   │   └── doc.json

		 ...

	    │   ├── 244
	    │   │   └── doc.json
	    │   └── 245
	    │       └── doc.json
	    └── meta_fields.txt 


#### Configuring database access

The first step is to configure database access parameters. 
This can be done in file [confs/database_conf.ini](confs/database_conf.ini). The configuration will be commonly used for all collections. 
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
  * `literature_old__meta` - table for storing metadata of collection's documents (e.g. `initial_id*`, `initial_src*`, `original_author`, `original_title`,  `title`, `publisher`);
  * `literature_old__words__layer` - table for storing word annotations of all documents;
  * `literature_old__sentences__layer` - table for storing sentence annotations of all documents;
  * `literature_old__sentences__hash` - table for storing sentence hash fingerprints of all documents; hash fingerprints are collected and stored for the purposes of later collection updating: for detecting which sentence tokenizations have been changed and which remain same;
  * `literature_old__morphosyntax__layer` - table for storing morphological and syntactic annotations of all documents;
 
_Erasing mode:_ pass flag `-r` to the script to remove the existing collection and start from the scratch. Be aware that this deletes all the existing tables along with their content.

_\* Remarks about metadata_:

* Document id-s. Each document of a collection will get unique key `text_id` starting from `0`. This key is also used to link document's content distributed over different tables (e.g. content in the metadata table, and in layer tables). Not to be confused with `initial_id*` in the metadata table which stores the `id` value initially extracted from the `doc` tag in the original vert file, and `_vert_doc_id` which corresponds to the actual index of the document in the original .vert file (starting from `0`). 

* Source corpus. Base table of the collection (e.g. `literature_old`) will have metadata column `src` which stores normalized source corpus name (e.g. `Literature Old` or `Literature Contemporary`). The metadata table will also have column `initial_src*` which stores the precise source corpus name extracted from the vert file (e.g. `Literature Old 1864–1945` or `Literature Contemporary 2000–2023`). 

### Document insertion

Use script `e_import_json_files_to_collection.py` for importing contents of the collection directory to the Postgres database. 
Pass name of the collection's configuration as an argument of the script.  

Example:

`python  e_import_json_files_to_collection.py  confs/literature_old.ini` reads document JSON files from the collection directory `literature_old`, and stores in the collection's tables.

Note: its advisable to use the collection via [EstNLTK's database interface](https://github.com/estnltk/estnltk/blob/main/tutorials/storage/storing_text_objects_in_postgres.ipynb) **only after the document insertion has been completed**. During the insertion, the collection may be in an inconsistent state: some of the documents/annotations might be incomplete, and queries might give errors.

#### Data parallelization

The insertion script also supports data parallelization: you can launch multiple instances of the script and give each instance a (non-overlapping) sub set of data for insertion. 
For this, use command line parameters `DIVISOR,REMAINDER` (both integers) to insert only texts for which holds `text_id % DIVISOR == REMAINDER`. 

Example 1: Launch two separate jobs for inserting `balanced_and_reference_corpus` data:

	$ python  e_import_json_files_to_collection.py  confs/balanced_and_reference_corpus.ini  2,0

(this inserts only texts with id-s: 0, 2, 4, 6, 8, ... )

	$ python  e_import_json_files_to_collection.py  confs/balanced_and_reference_corpus.ini  2,1

(this inserts only texts with id-s: 1, 3, 5, 7, 9, ... )

Example 2: Launch three separate jobs for inserting `balanced_and_reference_corpus` data:

	$ python  e_import_json_files_to_collection.py  confs/balanced_and_reference_corpus.ini  3,0

(this inserts only texts with id-s: 0, 3, 6, 9, 12, ... )

	$ python  e_import_json_files_to_collection.py  confs/balanced_and_reference_corpus.ini  3,1

(this inserts only texts with id-s: 1, 4, 7, 10, 13, ... )

	$ python  e_import_json_files_to_collection.py  confs/balanced_and_reference_corpus.ini  3,2

(this inserts only texts with id-s: 2, 5, 8, 11, 14, ... )

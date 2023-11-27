# Workflow for importing etTenTen 2013 corpus into a PostgreSQL database

---

⚠️ This is a legacy workflow, no longer supported.

---

This folder contains command line workflow for loading the _etTenTen corpus_ with EstNLTK and saving into a PostgreSQL database. 

_Note_: for using the workflow, you need [**Psycopg 2**](http://initd.org/psycopg) package that allows to communicate with PostgreSQL database. Installation:

	pip install psycopg2-binary

For importing the etTenTen corpus (aka _Eesti veeb 2013_, _Veebikorpus13_, _Estonian Web 2013_, or _etWaC 2013_) with EstNLTK, please proceed in the following steps:

**1.** Download and unpack the corpus, e.g. from here: [https://metashare.ut.ee/repository/browse/ettenten-korpus-toortekst/b564ca760de111e6a6e4005056b4002419cacec839ad4b7a93c3f7c45a97c55f](https://metashare.ut.ee/repository/browse/ettenten-korpus-toortekst/b564ca760de111e6a6e4005056b4002419cacec839ad4b7a93c3f7c45a97c55f) (or [https://doi.org/10.15155/1-00-0000-0000-0000-0011fl](https://doi.org/10.15155/1-00-0000-0000-0000-0011fl) )


You should get a large file with the extension _vert_ or _prevert_ (e.g. `ettenten13.processed.prevert`). 

**2.** (_Optional_) Use the script  **`split_ettenten_files_into_subsets.py`** for splitting the large file into N smaller subsets of documents. This will enable parallel processing of the subsets in the step **3**.

**3.** Proceed with the script **`store_ettenten_in_pgcollection.py`**. This script loads etTenTen 2013 corpus from a file ("etTenTen.vert" or "ettenten13.processed.prevert"), creates EstNLTK Text objects based on etTenTen's documents, adds tokenization to Texts (optional), and stores Texts in a PostgreSQL collection. Optionally, you may want to evoke N instances of `store_ettenten_in_pgcollection.py` for faster processing.

For detailed help about the command, run: `python store_ettenten_in_pgcollection.py -h`

## Helpful utilities

There are also some additional scripts that may be helpful for managing large PostgreSQL collection.

 * **`build_pgcollection_index.py`** -- Builds word count index from a corpus in given Postgres collection. The index shows character and word counts (and optionally sentence counts and some text metadata) for each document in the corpus. For detailed usage information, run: `python build_pgcollection_index.py -h`

 * **`select_randomly_from_index.py`** -- Selects a random subset of documents from a word count index, preserving the proportional distribution of documents with respect to a target category. For more info, run: `python select_randomly_from_index.py -h`
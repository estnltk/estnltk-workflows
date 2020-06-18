## Scripts for rewriting Koondkorpus collection from v0 to v2

This folder contains scripts for rewriting Koondkorpus collection from the version v0 (the old structure, used in 2018) to the version v2 (the new structure, introduced in 2019).

_Requirements_: EstNLTK v1.6.6+

_Before running a script:_ 1) fill in `pgpass.txt` with general database access information; 2) check the source of the script -- if it contains variables `SOURCE_COLLECTION`, `SOURCE_SCHEMA`, `TARGET_COLLECTION` etc. at the beginning, fill in these variables with proper values.

### Scripts

* 1) `rewrite_pgcollection.py` -- rewrites all koondkorpus' texts & metadata from one EstNLTK's pgcollection to another. Note: no layers will be rewritten at this point;
	*  `rewrite_pgcollection_random_subset.py` -- same as previous, except rewrites only a subset of 5000 randomly chosen documents (for testing);
* 2) `create_index_and_detect_duplicates.py` -- creates an index of  koondkorpus collection's documents, and detects duplicates along the way. Note: the index is required for mapping documents between two collections (for carrying over layers), because the order of documents in two collections is not the same;
    * Note: you can run this script twice: first for the collection `'koondkorpus'` and then for `'koondkorpus_v2'`. Then you can use the script `chk_koondkorpus_hash_indexes.py` to validate that the full mapping can be successfully created between two document collections; 
* 3) `carry_over_original_words.py` -- carries over original 'words' layer from koondkorpus v0 to v2;
* 4) `carry_over_original_sentences.py` -- carries over original 'sentences' layer from koondkorpus v0 to v2;
* 5) `carry_over_original_morph_analysis.py` -- carries over `'ot_morph_analysis'` layer from koondkorpus v0 to v2;  
 
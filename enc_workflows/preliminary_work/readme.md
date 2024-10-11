## Preliminary processing and analysis

This folder contains scripts for preliminary indexing of ENC .vert files and preliminary analysis of the corpus based on information in indexes. Scripts require EstNLTK's version v1.7.3+ for  corpus reading.

### Scripts

#### Creating indexes

* `00a_create_vert_doc_id_index.py` --- Creates an index file listing all documents id-s in  *.vert files of the root dir. This index can be later used for making random document selections from the whole corpus. Outputs the index into file `'vert_document_index.csv'`. 
 `python  d_create_collection_tables.py  confs/literature_old.ini` 

* `00b_create_vert_meta_and_counts_index.py` -- Creates two indexes from *.vert files in the root directory: 1) Count index `'vert_counts.csv'` recording document, sentence, word counts in each *.vert file; 2) Meta index files recording document metadata (including words and sentences counts) of each document in a vert file.  Meta index is saved in json format, into file with name pattern `f'meta_indx_{corpus_name}.jl'`, e.g. meta index of `'nc19_Web_2013.vert'` will be saved into `'meta_indx_nc19_Web_2013.jl'`. Note, this is a long process, expected processing time ~6 days.

#### Analysing corpus based on indexes
 
* `01a_find_metadata_stats.py` -- Finds document metadata fields and metadata value examples based on `meta_indx_*.jl` files in the root directory. Prints results to the screen. For more details about the usage, please see header of the script.

* `01b_find_largest_docs_from_meta_index.py` -- Finds largest documents based on `meta_indx_*.jl` files in the root directory. If [_koondkorpus word index file_](https://github.com/estnltk/estnltk-workflows/blob/master/koondkorpus_workflows/import_to_postgres/build_pgcollection_index.py) is available, then also estimates numbers of characters in text (and text size in bytes) based on the number of word counts in meta index files. Prints results to the screen. 

#### Picking random subsets of documents

* `02a_pick_randomly_from_doc_id_index.py` -- Picks randomly given number of documents (doc_id-s) from `'vert_document_index.csv'` file in the root directory. Saves results into `f'random_pick_x{pick_number}_from_vert.csv'`.

* `02b_pick_randomly_from_meta_index.py` -- Picks randomly given number of documents (doc_id-s) from `meta_indx_*.jl` files in the root directory. Saves results into `f'random_pick_x{pick_number}_from_vert.csv'.`

* `02c_extract_randomly_picked_docs.py` -- Extracts EstNLTK's `Text` objects of selected documents (given by doc_id-s) from *.vert files and saves into path: `'{output_dir}/{vert_file_name_without_ext}_doc_{doc_id}.json'`. The input file must be a random selection index CSV file (created via scripts `02a` or `02b`). The `{output_dir}` is constructed by removing '.csv' from the input file and creating a directory with corresponding name.

#### Testing syntactic parsing

* `03a_compare_stanza_parsing_approaches.py` -- Compares 2 stanza parsing approaches, which differ in the input: 1) parsing texts based on the original `morph_extended` layer extracted from the .vert files; 2) parsing texts based on `morph_extended` layer recreated from the scratch via Vabamorf tool. Collects statistics about differences and outputs after processing. Processes input directory containing EstNLTK's `Text` objects in JSON format (created via script `02c`). For the purpose of more detailed comparison, writes `Text` object JSON files with different parsing layers into folder `f'{input_json_dir}_output'`. 

* `03b_pick_random_diffs_from_different_stanza_approaches.py` -- Picks randomly 100 sentences that have syntactic analysis 
differences from the directory containing EstNLTK JSON files (created via script `03a`). Saves results into file `'random_pick_100_differences.txt'`.
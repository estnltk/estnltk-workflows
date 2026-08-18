# Workflows for processing Estonian National Corpus 2023

This folder contains workflows for processing Estonian National Corpus 2023 (_Eesti keele koondkorpus_) with EstNLTK. 
The scripts here are for adding new layers. Continuation of the scirpts in [enc_workflows](https://github.com/estnltk/estnltk-workflows/tree/master/enc_workflows).

---

#### Adding layers to texts in database

This script adds a morph_analysis layer using VabamorfWithBertTagger.

`python  g_add_morph_with_bert_layer.py  confs/example_conf_morph_with_bert.ini ` adds new layer tables to an existing collection defined in the ini file. 


#### Data parallelization

The tagging script also supports data parallelization: you can launch multiple instances of the script and give each instance a (non-overlapping) sub set of data for tagging. 
For this, use command line parameters `DIVISOR,REMAINDER` (both integers) to tag only texts for which holds `text_id % DIVISOR == REMAINDER`. 

Example 1: Launch two separate jobs for tagging `balanced_and_reference_corpus` data:

	$ python  g_add_morph_with_bert_layer.py  confs/example_conf_morph_with_bert.ini  2,0

(this tags only texts with id-s: 0, 2, 4, 6, 8, ... )

	$ python  g_add_morph_with_bert_layer.py  confs/example_conf_morph_with_bert.ini  2,1

(this tags only texts with id-s: 1, 3, 5, 7, 9, ... )

Example 2: Launch three separate jobs for tagging `balanced_and_reference_corpus` data:

	$ python  g_add_morph_with_bert_layer.py  confs/example_conf_morph_with_bert.ini  3,0

(this tags only texts with id-s: 0, 3, 6, 9, 12, ... )

	$ python  g_add_morph_with_bert_layer.py  confs/example_conf_morph_with_bert.ini  3,1

(this tags only texts with id-s: 1, 4, 7, 10, 13, ... )

	$ python  g_add_morph_with_bert_layer.py  confs/example_conf_morph_with_bert.ini  3,2

(this tags only texts with id-s: 2, 5, 8, 11, 14, ... )

If the process was interrupted, tagging can be continued for the block using `append`.

	$ python  g_add_morph_with_bert_layer.py  confs/example_conf_morph_with_bert.ini  2,0 append



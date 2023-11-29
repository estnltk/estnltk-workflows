# Workflows for annotating layers in Koondkorpus' PostgreSQL collections

This folder contains command line workflows for adding EstNLTK's annotation layers to Koondkorpus' collections in PostgreSQL DB.

### Requirements

* [**Psycopg 2**](https://www.psycopg.org) (installation: `pip install psycopg2-binary`)
* estnltk (v1.7.2+)
* estnltk_neural (v1.7.2+)

### Configurations

For tagging a layer, you need the following configurations:

* `pgpass.txt` file -- contains a line with database connection information: `host:port:dbname:user:password`;
* `conf_*.ini` file -- contains information about `schema`, `role` and target `collection`, and tagger specific information (such as names of the input and output layers, and paths to required resources). See the directory [confs](confs/) for example configurations;

### Scripts

* `00_split_into_sentences.py` -- splits texts of a Koondkorpus' PostgreSQL collection into sentences and saves into a new collection. Automatically adds segmentation and morphological analysi layers up to `"morph_extended"` to each sentence.    This script assumes data parallelization: you can launch multiple instances of the script and give each instance a (non-overlapping) sub set of the source collection for processing. Use command line parameters `module remainder` to process only texts for which holds `text_id % module == remainder`. Example usage:

	`python  00_split_into_sentences.py  confs\conf_koondkorpus_00_split_into_sentences.ini  1  0`

* `01_add_base_layer.py` -- depending on the input configuration, adds either a segmentation layer (`"tokens"`, `"compound_tokens"`,  `"words"`, `"sentences"`, `"paragraphs"`) or a morphological analysis layer (`"morph_analysis"` or `"morph_extended"`). Example usages:

	`python  01_add_base_layer.py  pgpass.txt  confs/conf_koondkorpus_01_add_tokens.ini`

	`python  01_add_base_layer.py  pgpass.txt  confs/conf_koondkorpus_04_add_sentences.ini`

	`python  01_add_base_layer.py  pgpass.txt  confs/conf_koondkorpus_05_add_morph_analysis.ini`

* `02_add_clauses_layer.py` -- adds clauses layer;

* `02_add_named_entities_layer.py` -- adds named entities layer with EstNLTK's basic NerTagger;

* `02_add_pre_timexes_layer.py` -- adds pre\_timexes layer with EstNLTK's TimexPhrasesTagger;

* `02_add_composite_stanza_syntax_layer.py` -- adds stanza syntax layer via a composite tagger. Essentially, this tagger creates two layers: the morph\_extended layer, which is an input layer required by the syntax tagger, and the stanza's syntax layer (which  will be the final output layer). The morph\_extended layer will be disposed after it is no longer needed;

* `02_add_stanza_syntax_layer.py` -- adds stanza syntax layer. Note that depending on the model used, this tagger requires that input morph\_extended or morph\_analysis layers already exist in the database. If you do not want to save morph\_extended layer to the database, use the script `02_add_composite_stanza_syntax_layer.py` instead.

* `02_add_timexes_layer.py` -- adds timexes layer with EstNLTK's TimexTagger. Note that this script uses Koondkorpus-specific logic for finding document creation times required for normalization of relative dates (for details, see the module `document_creation_times.py`).

Note: all scripts create detached layers.

### Data parallelization

Each of the annotation scripts also supports data parallelization: you can launch multiple instances of the script and give each instance a (non-overlapping) sub set of data for annotation. For this, use command line parameters `MODULE,REMAINDER` to annotate only texts for which holds `text_id % MODULE == REMAINDER`. 

Example 1: Launch two separate jobs for `tokens` annotation:

`$ python  01_add_base_layer.py  pgpass.txt  confs/conf_koondkorpus_01_add_tokens.ini  2,0`

`$ python  01_add_base_layer.py  pgpass.txt  confs/conf_koondkorpus_01_add_tokens.ini  2,1`

Example 2: Launch three separate jobs for `morph_analysis` annotation:

`$ python  01_add_base_layer.py  pgpass.txt  confs/conf_koondkorpus_05_add_morph_analysis.ini  3,0`

`$ python  01_add_base_layer.py  pgpass.txt  confs/conf_koondkorpus_05_add_morph_analysis.ini  3,1`

`$ python  01_add_base_layer.py  pgpass.txt  confs/conf_koondkorpus_05_add_morph_analysis.ini  3,2`





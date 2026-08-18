# Estonian National Corpus 2023 processing documentation

The workflow for processing the corpus can be found in [enc_workflows](https://github.com/estnltk/estnltk-workflows/tree/master/enc_workflows) and [adding morph_with_bert layer](https://github.com/estnltk/estnltk-workflows/tree/master/enc_workflows/add_layers_to_postgres).

The balanced_and_reference_corpus data was processed in July 2026.


## Data 

balanced_and_reference_corpus has 738252 json files separated into multiple subfolders. Each json file contains one text that could have more than one sentence. 

The zip file is around 11GB and extracting the files on disk requires another 85GB of free space.

The corpus was processed with the conf files provided in the workflow and had the following layers:

- clauses_v2025_12
- estbert_ner_v2025_12
- morphosyntax_v2024_06
- ner_v2025_12
- timexes_v2025_12
- sentences
- words	


## Workflow 

It is recommended to create a new conda environment for this workflow.

The workflow requires estnltk version 1.7.4 and additional changes from devel_1.7 branch:

- BertMorphTagger update https://github.com/estnltk/estnltk/commit/82d206ec15a24b8a4558477b277dd427d1b48d80 to use GPU.
- Update postgres `estnltk/storage/postgres` from devel branch (copy postgres folder to the right place in your env) and change "1.7.5" to "1.7.4" in [f_create_collection_indexes.py](https://github.com/estnltk/estnltk-workflows/blob/master/enc_workflows/f_create_collection_indexes.py#L38).
- Add "from estnltk.helpers.logger import get_logger_with_tqdm_handler"  to `estnltk/_init_.py`.
- Update `estnltk/helpers/logger.py` from devel branch.


### Inserting data

The preparation and inserting the data is done with the [enc_workflows](https://github.com/estnltk/estnltk-workflows/tree/master/enc_workflows) scripts (d_, e_, f_). [balanced_and_reference_corpus.ini](https://github.com/estnltk/estnltk-workflows/blob/master/enc_workflows/confs/balanced_and_reference_corpus.ini) configuration file was used. 

Since the processing of the corpus was done on HPC Rocket, each py script had it's own bash script. 

`d_create_collection_tables.py` required 1GB of memory and CPU. The script finished the job in 1 minute.

`e_import_json_files_to_collection.py` was run in 10 parallel blocks. It required 3GB of memory and CPU. The scripts finished in less than 3 hours.

The bash script content:

```
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: sbatch $0 <divisor> <remainder>"
    exit 1
fi

srun path/to/env/python e_import_json_files_to_collection.py balanced_and_reference_corpus.ini "$1","$2"
```

Commands to run the bash scripts:

$ sbatch e_import_json_files_to_collection.sh 10 0  
$ sbatch e_import_json_files_to_collection.sh 10 1  
...  
$ sbatch e_import_json_files_to_collection.sh 10 8  
$ sbatch e_import_json_files_to_collection.sh 10 9  


`f_create_collection_indexes.py` required 1GB of memory and CPU. The script finished in 20 minutes.


### Adding morph_with_bert layer

The [g_add_morph_with_bert_layer.py](https://github.com/estnltk/estnltk-workflows/blob/master/enc_workflows/add_layers_to_postgres/g_add_morph_with_bert_layer.py) script was added to the workflow. It uses [conf_morph_with_bert.ini](https://github.com/estnltk/estnltk-workflows/blob/master/enc_workflows/add_layers_to_postgres/confs/example_conf_morph_with_bert.ini).

It will add a new layer `morph_with_bert` with VabamorfWithBertTagger.

Running the script required GPU, 4GB of memory (each block). Similarly to `e_import_json_files_to_collection` it was run in 10 parallel blocks. The script was done after 7.5 hours.



## Problems

### Access rights to the data after inserting 

Since the "role" in the conf file was the same as the username, then the reading and writing rights belonged to only that user. The correct role for processing the corpus is "enc_2023_create".

To fix the problem, the access rights had to be given to 'enc_2023_read' and 'enc_2023_create'.

The changing of access rights required logging into the server. On Ubuntu, it could be done from normal terminal using the command:

$ psql -h ServerHost -p 5432 - U YourUsername -d DatabaseName

After entering the user password, a prompt should be available:

```
DatabaseName=>
```

From there the following commands were given:

GRANT SELECT ON enc_2023."balanced_and_reference_corpus" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__clauses_v2025_12__layer" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__estbert_ner_v2025_12__layer" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__metadata" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__morph_with_bert_v2026_07__layer" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__morphosyntax_v2024_06__layer" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__ner_v2025_12__layer" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__sentences__hash" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__sentences__layer" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__structure" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__timexes_v2025_12__layer" TO enc_2023_read;  
GRANT SELECT ON enc_2023."balanced_and_reference_corpus__words__layer" TO enc_2023_read;  

GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__clauses_v2025_12__layer" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__estbert_ner_v2025_12__layer" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__metadata" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__morph_with_bert_v2026_07__layer" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__morphosyntax_v2024_06__layer" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__ner_v2025_12__layer" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__sentences__hash" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__sentences__layer" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__structure" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__timexes_v2025_12__layer" TO enc_2023_create;  
GRANT SELECT, INSERT, UPDATE, TRIGGER, REFERENCES, DELETE, TRUNCATE ON enc_2023."balanced_and_reference_corpus__words__layer" TO enc_2023_create;  

To log out:

$ DatabaseName=> \q


## Code related problems

The g_add_morph_analysis_layer_gpu script gave errors on some occasions:

1. The input text segment was too long. BertMorphTagger function `_split_sentence_into_smaller_chunks` segments the text if it's too long and originally the max_size was set to 1000. 
This gave an error "Input length exceeds the model's max_seq_length of 512 tokens" and max_size was changed to 900.

2. Some texts contained an encoding problem and a "�" symbol was in the text. The tokenizer ignored that symbols and when tokens were matched back to base spans then it created errors since the special symbols are present in the words layer.

BertMorphTagger was modified to collect all the special symbols and their spans and add them to the new layer. The [patch](https://github.com/estnltk/estnltk/blob/devel_1.7/estnltk_neural/estnltk_neural/taggers/neural_morph/bert_based/bert_morph_tagger.py#L291-L315) used in the tagger is also available in [devel_1.7](https://github.com/estnltk/estnltk/blob/devel_1.7/estnltk_neural/estnltk_neural/taggers/neural_morph/bert_based/bert_encoding_error_patch.py) .



Testing the scripts and fixing errors required collection and layer deletion. 

The collection was accessed through estnltk's PostgresStorage. The 'delete_collection' command did delete the layers but not the hash and metadata tables. So running the scripts again would result in errors.

To remove the collection and all its tables, all the following commands should be given:

```
storage.delete_collection('balanced_and_reference_corpus')
c = storage.conn.cursor()
c.execute('DROP TABLE IF EXISTS enc_2023."balanced_and_reference_corpus__sentences__hash" CASCADE;')
source_storage.conn.commit()
c.execute('DROP TABLE IF EXISTS enc_2023."balanced_and_reference_corpus__metadata" CASCADE;')
source_storage.conn.commit()
```





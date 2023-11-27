# Workflows for converting etTenTen 2013 to JSON files and annotating with EstNLTK

---

⚠️ Thes is a legacy workflow, no longer supported.

---

This folder contains command line workflows for processing the etTenTen 2013 corpus with EstNLTK and saving into JSON format files.
You can use these scripts if you need a basic pipeline for processing large corpora and adding linguistic analyses. 
If you need a pipeline tailored for your purposes, you can also follow the example of these scripts while implementing your own pipeline.

## Workflow for processing the etTenTen corpus (2013)

For processing the etTenTen corpus (aka _Eesti veeb 2013_, _Veebikorpus13_, _Estonian Web 2013_, or _etWaC 2013_) with EstNLTK 1.6.x, please proceed in the following steps:

**1.** Download and unpack the corpus, e.g. from here: [https://metashare.ut.ee/repository/browse/ettenten-korpus-toortekst/b564ca760de111e6a6e4005056b4002419cacec839ad4b7a93c3f7c45a97c55f](https://metashare.ut.ee/repository/browse/ettenten-korpus-toortekst/b564ca760de111e6a6e4005056b4002419cacec839ad4b7a93c3f7c45a97c55f) (or [https://doi.org/10.15155/1-00-0000-0000-0000-0011fl](https://doi.org/10.15155/1-00-0000-0000-0000-0011fl) )

You should get a large file with the extension _vert_ or _prevert_ (e.g. `ettenten13.processed.prevert`).
 
**2.** Use the script **`convert_ettenten_to_json.py`** for splitting the large file into JSON format files, one file per each document of the corpus. The script needs two input arguments: name of the corpus file, and the output directory where the script can store the JSON format files. It is advisable to create a new directory for the output. Be aware that all the converted files will be put into the output directory; so, there will be a lot of files (approx. 686,000 files).

**3.** (_Optional_) Use **`split_large_corpus_files_into_subsets.py`** for splitting the large set of files from the previous step into N smaller subsets. This will enable parallel processing of the subsets.

**4.** Use the script **`process_and_save_results.py`** to analyze the JSON format files with EstNLTK 1.6.x. The script will add linguistic annotations up to the level of _morphology_. Before using the script, you'll also need to create a new folder where the script can store the results of analysis. 

   Optionally, you may want to evoke N instances of **`process_and_save_results.py`** for faster processing. You can get more information about the processing options with:

        python  process_and_save_results.py  -h

   The script will write out the results of processing as JSON format files.

## Helpful utilities

There are also some additional scripts that may be helpful for managing large corpora.

 * **`select_randomly_from_large_corpus.py`** -- selects randomly a subset of files from the source directory (a directory which contains a large amount of files), and copies into the target directory. You can use this script to get a random sample of etTenTen, e.g. for tool development, or for experiments;

 *  **`remove_large_amount_of_files.py`** -- deletes all the files from the target directory (a directory which contains a large amount of files). You can use this script to perform a cleanup after corpus processing;


## Processing results

 * [Estonian Web 2013 analysed with EstNLTK ver.1.6_beta](https://metashare.ut.ee/repository/browse/veebikorpus13-korpus-analuusitud-estnltk-v16b-abil/bfd3d46a38dd11e8a6e4005056b4002403878274d5ac4a488f0b5aea5a1d8015/) (last checked: 2017-12-22);
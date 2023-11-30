# Workflow for processing Estonian Reference Corpus with EstNLTK and saving into JSON files

This folder contains a command line workflow for loading and processing the Estonian Reference Corpus with EstNLTK and saving into JSON format files.
You can use these scripts if you need a basic pipeline for processing the corpus and adding linguistic analyses. 
If you need a pipeline tailored for your purposes, you can also follow the example of these scripts while implementing your own pipeline.

## Processing Estonian Reference Corpus (1990-2008)

For processing the whole Estonian Reference Corpus (_Eesti keele koondkorpus_) with EstNLTK, proceed in the following steps:

**1.** First, download all the (zipped) XML files from the page [http://www.cl.ut.ee/korpused/segakorpus/](http://www.cl.ut.ee/korpused/segakorpus/). (The web page contains links to different subcorpora, follow the links and download zipped XML files.) Put them into a separate folder, e.g. folder named `koond`.
 
   _Note_: In Bash shell, you can use the script `download_koondkorpus_zip_files.sh` for downloading the files.

   After downloading, you should have the following files ( checked with UNIX command: `ls -1 koond` ):
     
        Agraarteadus.zip
        Arvutitehnika.zip
        Doktoritood.zip
        EestiArst.zip
        Ekspress.zip
        foorum_uudisgrupp_kommentaar.zip
        Horisont.zip
        Ilukirjandus.zip
        jututoad_xml.zip
        Kroonika.zip
        LaaneElu.zip
        Luup.zip
        Maaleht.zip
        Paevaleht.zip
        Postimees.zip
        Riigikogu.zip
        Seadused.zip
        SLOleht.tar.gz
        Teadusartiklid.zip
        Valgamaalane.zip

  (20 files at total)

**2.** Unpack the files. In UNIX, you can use commands:

        cd koond/
        unzip "*.zip"
        tar xvzf SLOleht.tar.gz

**3.** Next, XML files need to be converted into the _json_ format. First, create a new folder where the results of the conversion fill be stored. Then, use the script **`convert_koondkorpus_to_json.py`** to do the conversion. The script needs a starting directory and an output directory as arguments. For the starting directory, you can pass the name of the directory into which you unpacked the zip files in the previous step. The script will recursively traverse the directory structure, and find all the XML files suitable for converting.

   Be aware that all the converted files will be put into the output directory. So, after the conversion, there will be a lot of files in the output folder (approx. 705,000 files).
   
   You can check other possible arguments of the script with the flag `-h`:

        python  convert_koondkorpus_to_json.py  -h

     
**4.** (_Optional_) Use the script  **`split_large_corpus_files_into_subsets.py`** for splitting the large set of files from the previous step into N smaller subsets. This will enable parallel processing of the subsets.

**5.** Use the script **`process_and_save_results.py`** to analyze the JSON format files with EstNLTK. The script will add linguistic annotations up to the level of _morphology_. Before using the script, you'll also need to create a new folder where the script can store the results of analysis. 

   Optionally, you may want to evoke N instances of 
**`process_and_save_results.py`** for faster processing. You can get more information about the processing options with:
   
        python  process_and_save_results.py  -h


   The script will write out the results of processing as JSON format files.

## Helpful utilities

There are also some additional scripts that may be helpful for managing large corpora.

 * **`select_randomly_from_large_corpus.py`** -- selects randomly a subset of files from the source directory (a directory which contains a large amount of files), and copies into the target directory. You can use this script to get a random sample of koondkorpus, e.g. for tool development, or for experiments;

 *  **`remove_large_amount_of_files.py`** -- deletes all the files from the target directory (a directory which contains a large amount of files). You can use this script to perform a cleanup after corpus processing;


## Processing results

 * [Estonian Reference Corpus analysed with EstNLTK ver.1.6_beta](https://metashare.ut.ee/repository/browse/eesti-keele-koondkorpus-analuusitud-estnltk-v16b-abil/57b7a8e838e211e8a6e4005056b4002423f99f9dc9d44a0ea16db2b48c1d7057/) (last checked: 2017-12-28);

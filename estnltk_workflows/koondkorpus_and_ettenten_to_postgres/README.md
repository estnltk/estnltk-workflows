# Workflows: importing large corpora into a PostgreSQL database

This folder contains command line workflows for importing large corpora -- the _Estonian Reference Corpus_ and the _etTenTen corpus_ -- with EstNLTK and saving into a PostgreSQL database. 
You can use these scripts if you need a basic pipeline for importing these corpora into a database.

_Note_: for using these workflows, you need [**Psycopg 2**](http://initd.org/psycopg) package that allows to communicate with PostgreSQL database. Installation:

	conda install psycopg2

## Workflow 1: importing Estonian Reference Corpus (1990-2008) 

In order to import the Estonian Reference Corpus (_Eesti keele koondkorpus_) into a PostgreSQL database with EstNLTK, proceed in the following steps:

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

**2.** (_Optional_) You can unpack all the files before processing (this can be useful for debugging). To unpack the files in UNIX, you can use commands:

        cd koond/
        unzip "*.zip"
        tar xvzf SLOleht.tar.gz

  Note that you can also just proceed with the packed files, because processing scripts can deal with both packed and unpacked XML files.

**3.** (_Optional_) Use the script  **`split_koondkorpus_files_into_subsets.py`** for splitting the set of XML files from the first step into N smaller subsets. This will enable parallel processing of the subsets in the step **4**.

Note that the script works with both zipped and unzipped files. For detailed help, run: `python split_koondkorpus_files_into_subsets.py -h`

**4.** Proceed with the script **`store_koondkorpus_in_pgcollection.py`**. This script loads _Koondkorpus_ XML TEI files (either from zipped archives, or from directories where the files have been unpacked), creates EstNLTK Text objects based on these files, adds tokenization to Texts (optional), splits Texts into paragraphs or sentences (optional), and stores Texts in a PostgreSQL collection. Optionally, you may want to evoke N instances of 
`store_koondkorpus_in_pgcollection.py` for faster processing.

For detailed help about the command, run: `python store_koondkorpus_in_pgcollection.py -h`


## Workflow 2: importing the etTenTen corpus (2013)

For importing the etTenTen corpus (aka _Eesti veeb 2013_, _Veebikorpus13_, _Estonian Web 2013_, or _etWaC 2013_) with EstNLTK 1.6.x, please proceed in the following steps:

**1.** Download and unpack the corpus, e.g. from here: [https://metashare.ut.ee/repository/browse/ettenten-korpus-toortekst/b564ca760de111e6a6e4005056b4002419cacec839ad4b7a93c3f7c45a97c55f](https://metashare.ut.ee/repository/browse/ettenten-korpus-toortekst/b564ca760de111e6a6e4005056b4002419cacec839ad4b7a93c3f7c45a97c55f)

You should get a large file with the extension _vert_ or _prevert_ (e.g. `ettenten13.processed.prevert`). 

**2.** (_Optional_) Use the script  **`split_ettenten_files_into_subsets.py`** for splitting the large file into N smaller subsets of documents. This will enable parallel processing of the subsets in the step **3**.

**3.** Proceed with the script **`store_ettenten_in_pgcollection.py`**. This script loads etTenTen 2013 corpus from a file ("etTenTen.vert" or "ettenten13.processed.prevert"), creates EstNLTK Text objects based on etTenTen's documents, adds tokenization to Texts (optional), and stores Texts in a PostgreSQL collection. Optionally, you may want to evoke N instances of `store_ettenten_in_pgcollection.py` for faster processing.

For detailed help about the command, run: `python store_ettenten_in_pgcollection.py -h`

## Helpful utilities

There are also some additional scripts that may be helpful for managing large PostgreSQL collections like etTenTen and koondkorpus.

 * **`build_pgcollection_index.py`** -- Builds word count index from a corpus in given Postgres collection. The index shows character and word counts (and optionally sentence counts and some text metadata) for each document in the corpus. For detailed usage information, run: `python build_pgcollection_index.py -h`
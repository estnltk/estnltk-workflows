# Workflow: importing Estonian Reference Corpus (1990-2008) to a PostgreSQL database

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


# Workflow: importing texts of the Estonian Reference Corpus and storing in PostgreSQL database

 * `store_koondkorpus_in_pgcollection.py` -  Loads [Koondkorpus](http://www.cl.ut.ee/korpused/segakorpus/) XML TEI files (either from zipped archives, or from directories where the files have been unpacked), creates EstNLTK Text objects based on these files, adds   tokenization to Texts (optional), and stores Texts in a PostgreSQL collection. For detailed help about the command, run: `python store_koondkorpus_in_pgcollection.py -h`


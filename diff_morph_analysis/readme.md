# Evaluating morphological annotations against previous versions of the annotations

Scripts for evaluating morphological analysis annotations against previous versions of the annotations available in a PostgreSQL collection.

Requirements: Python 3.6+, [Psycopg 2](https://www.psycopg.org), EstNLTK v1.6.7+

* `diff_vm_bin.py` -- Runs VabamorfTagger with given Vabamorf's binary lexicons on a PostgreSQL collection, and finds differences in morphological annotations. Outputs summarized statistics about differences, writes all differences into a file. For detailed usage information, run: `python diff_vm_bin.py -h`
 

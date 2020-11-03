# Evaluating morphological annotations against previous versions of  annotations

Scripts for evaluating morphological analysis annotations against previous versions of annotations in a PostgreSQL collection.

Requirements: Python 3.6+, [Psycopg 2](https://www.psycopg.org), EstNLTK v1.6.7+

* `diff_vm_bin.py` -- Runs VabamorfTagger with given Vabamorf's binary lexicons on a PostgreSQL collection, and finds differences in morphological annotations. Outputs summarized statistics about differences, and writes all differences into a file. For detailed usage information, run: `python diff_vm_bin.py -h`

* `pick_randomly_from_diffs.py` -- Selects a random subset of differences from an output file produced by a difference finding script. For detailed usage information, run: `python pick_randomly_from_diffs.py -h`
 

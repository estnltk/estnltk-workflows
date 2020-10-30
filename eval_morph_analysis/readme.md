# Evaluating morphological annotations against previous versions

This folder contains scripts for evaluating morphological analysis annotations against previous versions of the annotations.

Requirements: Python 3.6+, [Psycopg 2](https://www.psycopg.org), EstNLTK v1.6.7+

* `eval_vm_bin.py` -- Evaluates how changing Vabamorf's binary lexicons alters EstNLTK's morphological annotations. Runs given Vabamorf's binary lexicons on a morphologically annotated PostgreSQL collection. Compares collection's morphological annotations against new annotations produced by VabamorfTagger with given binaries and finds all annotation differences. For detailed usage information, run: `python eval_vm_bin.py -h`
 

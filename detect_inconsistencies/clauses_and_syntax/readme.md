## Detecting clauses and syntax annotation inconsistencies

Scripts for running clauses and syntax annotation inconsistencies detector (module    `estnltk.consistency.clauses_and_syntax_consistency`) on a PostgreSQL collection, for getting statistics of clause annotations, and for taking out subsets of proposed annotation corrections (for manual evaluation).

Requirements: Python 3.8+, [Psycopg 2](https://www.psycopg.org), EstNLTK v1.7.2+

* `clauses_statistics.py` -- Finds statistics of clauses based on a rough classification of clauses. Clause classes capture information about clause type, verb containment, whether the clause parenthesised or quoted, and the starting word of the clause. Configuration of the processing (db connection, target collection, layer names etc) must be specified in the INI file, see [`conf_clauses_stats_koondkorpus_random_5M_words.ini`](conf_clauses_stats_koondkorpus_random_5M_words.ini) for an example. 
Running: `python clauses_statistics.py <config_INI>`

* `detect_clause_errors.py` -- Runs `detect_clause_errors`  (from    `estnltk.consistency.clauses_and_syntax_consistency`) on a large corpus. Basically: detects potential errors in clauses layer using information from the syntax layer. Outputs detected errors and error statistics. Optionally, saves erroneous sentences as Estnltk json objects (for later testing and development). Configuration of the processing (db connection, target collection, layer names etc) must be specified in the INI file, see [`conf_koondkorpus_random_5M_words.ini`](conf_koondkorpus_random_5M_words.ini) for an example. 
Running: `python detect_clause_errors.py <config_INI>`

* `pick_randomly_from_errs.py` -- Picks a random subset of errors from an errors log or from erroneous sentences jsonl file (for manual evaluation). For detailed usage information, run: `python pick_randomly_from_errs.py -h`
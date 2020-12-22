# Comparing different versions of named entity annotations

Scripts for running named entity (NE) recognizer on a PostgreSQL collection that has named entity annotations, and comparing old and new named entity annotations.

Requirements: Python 3.6+, [Psycopg 2](https://www.psycopg.org), EstNLTK v1.6.7+

* `diff_ner_tagger.py` -- Runs NerTagger on given PostgreSQL collection that has named entity annotations. Finds differences between old and new NE annotations. Outputs summarized statistics about differences, and writes all differences into a file. For detailed usage information, run: `python diff_ner_tagger.py -h`

* `diff_ner_tagger_models.py` -- Runs two NerTagger's models given PostgreSQL collection, and finds differences between NE annotations produced by two models. Outputs summarized statistics about differences, and writes all differences into a file. _Note:_ The collection must have input layers required by NerTagger. For detailed usage information, run: `python diff_ner_tagger_models.py -h`

* `pick_randomly_from_diffs.py` -- Selects a random subset of differences from an output file produced by a difference finding script. For detailed usage information, run: `python pick_randomly_from_diffs.py -h`

* [`ner_diff_finder.ipynb`](ner_diff_finder.ipynb) -- tutorial exemplifying usage of `NerDiffFinder` (component which helps to find and group differences between two NE layers).
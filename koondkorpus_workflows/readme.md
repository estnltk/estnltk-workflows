# Workflows for processing Estonian Reference Corpus

This folder contains workflows for processing the [Estonian Reference Corpus](http://www.cl.ut.ee/korpused/segakorpus/) (_Eesti keele koondkorpus_) with EstNLTK. 

## Corpus import

* [create_json](create_json) -- workflow for importing the corpus from XML files, processing with EstNLTK and saving into JSON format files;

* [create_postgres](create_postgres) -- workflow for importing the corpus from XML files, processing with EstNLTK and saving into a [PostgreSQL](https://www.postgresql.org) database; 

## Annotation

* [postgres_add_layers](postgres_add_layers) -- workflows for adding EstNLTK's annotation layers to koondkorpus collections in a PostgreSQL DB. Also includes workflows for: a) splitting koondkorpus documents into sentences and saving into a separate collection, b) automatically detecting creation times of koondkorpus documents;
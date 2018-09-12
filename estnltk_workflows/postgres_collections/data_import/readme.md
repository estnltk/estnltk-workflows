# Create collection

Assume that there is a table `public.source_texts` with `text` and `id` columns.

Create a collection `collection` of `estnltk` texts with `words`, `sentences`, `paragraphs` and 
`morph_analysis` layers attached. 
```
estnltk-workflows$ estnltk_workflows/postgres_collections/data_import/create_collection
```
In the database the table `grammarextractor.collection` is created.
While creating the collection, source texts can be splitted into sentences or paragraphs.
Relation between collection objects and source text sentences and paragraphs is stored in the 
`grammarextractor.collection` meta columns.

Many of the default parameters can be changed with the command line options or using the configuration file.
For details, display help.
```
estnltk-workflows$ estnltk_workflows/postgres_collections/data_import/create_collection -h
```

#### Table `grammarextractor.collection`

  Column       |  Type   |
---------------|---------|
 id            | bigint  |  
 data          | jsonb   |  
 source_id     | bigint  | 
 paragraph_nr  | integer | 
 sentence_nr   | integer | 


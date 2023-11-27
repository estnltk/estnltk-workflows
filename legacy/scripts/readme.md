# Create collection

---

⚠️ This is legacy code, no longer supported.

---

Assume that there is a table that contains at least a unique id column and either a column of plain texts or a column
of `Text` objects in EstNltk 1.6 json format.

Display help
```
estnltk-workflows$ scripts/create_collection -h
```
and modify `scripts/create_collection.conf` file accordingly or use command line arguments.

Create a collection `estnltk` texts with `words`, `sentences`, `paragraphs` and 
`morph_analysis` layers attached. 
```
estnltk-workflows$ scripts/create_collection @scripts/create_collection.conf
```

In the database two tables for collection and collection_structure are created.
While creating the collection, source texts can be splitted into sentences or paragraphs.
Relation between collection objects and source text sentences and paragraphs is stored in the 
collection table meta data columns.

#### Collection table

  Column       |  Type   |
---------------|---------|
 id            | bigint  |  
 data          | jsonb   |  
 source_id     | bigint  | 
 paragraph_nr  | integer | 
 sentence_nr   | integer | 

# =========================================================
# =========================================================
#  Utilities for laying out:
#   1) processing configuration ( e.g. which 
#      subset of documents will be processed )
#   2) tools/taggers that will be evaluated;
# =========================================================
# =========================================================

import os, os.path, re
from collections import defaultdict
from random import sample

from psycopg2.sql import SQL, Identifier

from estnltk.text import Text

from estnltk.taggers import NerTagger


# =================================================
# =================================================
#    Choosing a random subset for processing
# =================================================
# =================================================

def fetch_document_indexes( storage, schema, collection, logger ):
    """ Fetches and returns all document ids of the collection from the PostgreSQL storage.
    """
    # Construct the query
    sql_str = 'SELECT id FROM {}.{} ORDER BY id'
    doc_ids = []
    with storage.conn as conn:
        # Named cursors: http://initd.org/psycopg/docs/usage.html#server-side-cursors
        with conn.cursor('read_collection_doc_ids', withhold=True) as read_cursor:
            try:
                read_cursor.execute(SQL(sql_str).format(Identifier(schema),
                                                        Identifier(collection)))
            except Exception as e:
                logger.error(e)
                raise
            finally:
                logger.debug( read_cursor.query.decode() )
            for items in read_cursor:
                doc_ids.append ( items[0] )
    return doc_ids



def pick_random_doc_ids( k, storage, schema, collection, logger, sort=True ):
    ''' Picks a random sample of k document ids from the given collection. '''
    all_doc_ids = fetch_document_indexes( storage, schema, collection, logger )
    resulting_sample = sample(all_doc_ids, k) if k < len(all_doc_ids) else all_doc_ids
    return sorted(resulting_sample) if sort else resulting_sample



# =================================================
# =================================================
#    Finding & fixing dependency layers
# =================================================
# =================================================

def find_ner_dependency_layers( ner_input_layers, ner_layer, collection, log, 
                                           incl_prefix='', incl_suffix='' ):
    ''' Finds a mapping from ner_input_layers to layers available in the 
        collection. 
        Mapping relies on an assumption that NER input layer names are substrings
        of the corresponding layer names in the collection.
        If incl_prefix and incl_suffix have been specified (that is: are non-empty
        strings), then they are used to filter collection layers. Only those 
        collection layer names that satisfy the constraint startswith( incl_prefix ) 
        and endswith( incl_suffix ) will be used for the mapping.
    '''
    # 1) Match  ner_input_layers  to  collection's layers
    if ner_input_layers is None:
        ner_input_layers = ['morph_analysis', 'words', 'sentences']
    input_layer_matches = defaultdict(list)
    for  input_layer  in  ner_input_layers:
        for collection_layer in collection.layers:
            if not collection_layer.startswith(incl_prefix):
                # If the layer name does not have required prefix, skip it
                continue
            if not collection_layer.endswith(incl_suffix):
                # If the layer name does not have required suffix, skip it
                continue
            if input_layer in collection_layer:
                input_layer_matches[input_layer].append( collection_layer )
                if len( input_layer_matches[input_layer] ) > 1:
                    log.error(("(!) NER input layer {!r} has more than 1 "+\
                               "possible matches in the collection {!r}: {!r}").format(input_layer,
                                                                                       collection.name,
                                                                                       input_layer_matches[input_layer]))
                    log.error(("Please use arguments in_prefix and/or in_suffix to specify, "+
                               "which layers are relevant dependencies of the {!r} layer.").format(ner_layer))
                    exit(1)
        if len( input_layer_matches[input_layer] ) == 0:
            log.error(("(!) NER input layer {!r} could not be found from "+\
                       "layers of the collection {!r}. Collection's layers are: {!r}").format(input_layer,
                                                                                       collection.name,
                                                                                       collection.layers))
            exit(1)
    # 2) Convert value types from list to string
    for input_arg in input_layer_matches.keys():
        val = input_layer_matches[input_arg]
        assert isinstance(val, list) and len(val) == 1
        input_layer_matches[input_arg] = val[0]
    return input_layer_matches



def flip_ner_input_layer_names( text_obj, ner_input_layers_mapping ):
    ''' Flips NER input layer names in the text_obj. 
        Switches from the layer names listed in ner_input_layers_mapping.keys() 
        to layer names listed in ner_input_layers_mapping.values();
    '''
    reverse_map = { v:k for (k, v) in ner_input_layers_mapping.items() }
    new_layers = []
    # Remove layers in the order of dependencies
    for in_layer in ['morph_analysis', 'sentences', 'words']:
        if in_layer in ner_input_layers_mapping:
            collection_layer_name = ner_input_layers_mapping[in_layer]
            if collection_layer_name == in_layer:
                # No need to rename: move along!
                continue
            # Remove collection layer from text object
            layer_pointer    = text_obj[collection_layer_name]
            collection_layer = text_obj.pop_layer( collection_layer_name )
            if len(layer_pointer) == 0 and collection_layer is None:
                # Hack for getting around of pop_layer()'s bug
                # (returns None on empty layer)
                collection_layer = layer_pointer
            # Rename layer
            collection_layer.name = in_layer
            # Rename layer's dependencies
            if collection_layer.parent != None and \
               collection_layer.parent in reverse_map:
                collection_layer.parent = reverse_map[collection_layer.parent]
            if collection_layer.enveloping != None and \
               collection_layer.enveloping in reverse_map:
                collection_layer.enveloping = reverse_map[collection_layer.enveloping]
            new_layers.append( collection_layer )
    if new_layers:
        # Add layers in the reversed order of dependencies
        for in_layer in ['words', 'sentences', 'morph_analysis']:
            for layer in new_layers:
                if layer.name == in_layer:
                    text_obj.add_layer(layer)



# =================================================
# =================================================
#    Creating named entity taggers
# =================================================
# =================================================

def create_ner_tagger( old_ner_layer, collection, log, new_ner_layer,
                                      incl_prefix='', incl_suffix='' ):
    ''' Creates NerTagger for analysing given collection. 
        Collects input layers of the tagger based on the layers 
        available in the collection. '''
    # NerTagger's input layers
    default_ner_input_layers = ['morph_analysis', 'words', 'sentences']
    # Mapping from NerTagger's input layers to corresponding layers in the collection
    input_layers_mapping = \
        find_ner_dependency_layers( default_ner_input_layers, old_ner_layer, collection, log,
                                                              incl_prefix=incl_prefix, incl_suffix=incl_suffix )
    #
    # TODO: NerTagger's current interface does not allow to properly use customized 
    #       layer names, as names of the default layers are hard-coded. Therefore,
    #       we cannot specify collection's input layers upon initialization of NerTagger,
    #       but we return then so that they can be used for quiering the collection;
    #
    ner_tagger = NerTagger( output_layer=new_ner_layer )
    log.info(' Initialized {!r} for evaluation. '.format( ner_tagger ) )
    return ner_tagger, input_layers_mapping



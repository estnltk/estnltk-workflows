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

from estnltk.resolve_layer_dag import DEFAULT_RESOLVER
from estnltk.vabamorf.morf import Vabamorf as VabamorfInstance
from estnltk.taggers import VabamorfTagger


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
#    Finding dependency layers
# =================================================
# =================================================

def find_morph_analysis_dependency_layers( vabamorftagger, morph_layer, collection, log, 
                                           incl_prefix='', incl_suffix='' ):
    ''' Finds a mapping from VabamorfTagger's input layer names to layers 
        available in the collection. 
        Mapping relies on an assumption that input layer names are substrings
        of the corresponding layer names in the collection.
        If incl_prefix and incl_suffix have been specified (that is: are non-empty
        strings), then they are used to filter collection layers. Only those 
        collection layer names that satisfy the constraint startswith( incl_prefix ) 
        and endswith( incl_suffix ) will be used for the mapping.
    '''
    # 1) Match VabamorfTagger's input argument names to its input_layers
    input_arg_matches = defaultdict(str)
    input_arg_matches['input_words_layer'] = ''
    input_arg_matches['input_sentences_layer'] = ''
    input_arg_matches['input_compound_tokens_layer'] = ''
    for input_layer in vabamorftagger.input_layers:
        for input_arg in input_arg_matches.keys():
            if input_layer in input_arg:
                input_arg_matches[input_arg] = input_layer
    # 2) Match VabamorfTagger's input_layers to collection's layers
    input_layer_matches = defaultdict(list)
    for input_layer in vabamorftagger.input_layers:
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
                    log.error(("(!) VabamorfTagger's input layer {!r} has more than 1 "+\
                               "possible matches in the collection {!r}: {!r}").format(input_layer,
                                                                                       collection.name,
                                                                                       input_layer_matches[input_layer]))
                    log.error(("Please use arguments in_prefix and/or in_suffix to specify, "+
                               "which layers are relevant dependencies of the {!r} layer.").format(morph_layer))
                    exit(1)
        if len( input_layer_matches[input_layer] ) == 0:
            log.error(("(!) VabamorfTagger's input layer {!r} could not be found from "+\
                       "layers of the collection {!r}. Collection's layers are: {!r}").format(input_layer,
                                                                                       collection.name,
                                                                                       collection.layers))
            exit(1)
    # 3) Match input_arg -> input_layers -> collection_layer_name
    input_arg_to_collection_layer = defaultdict(str)
    for input_arg in input_arg_matches.keys():
        input_layer = input_arg_matches[input_arg]
        input_arg_to_collection_layer[input_arg] = input_layer_matches[input_layer]
    # 4) Convert value types from list to string
    for input_arg in input_arg_to_collection_layer.keys():
        val = input_arg_to_collection_layer[input_arg]
        assert isinstance(val, list) and len(val) == 1
        input_arg_to_collection_layer[input_arg] = val[0]
    return input_arg_to_collection_layer


# =================================================
# =================================================
#    Dividing large file into chunks
# =================================================
# =================================================

def find_division_into_chunks( layer, chunk_size=1000000 ):
    ''' Finds a division of text into even sized chunks, 
        so that the chunks follow start/end positions
        of the given layer (a guiding layer).
        A 'sentences' layer serves best as the guiding 
        layer.
    '''
    division = []
    i = 0
    cur_size = 0
    start_i  = 0
    while ( i < len(layer) ):
        cur_size += len(layer[i].enclosing_text)
        if cur_size >= chunk_size:
            start_pos = layer[start_i].start
            end_pos   = layer[i].end
            division.append( (start_pos, end_pos) )
            start_i = i + 1
            cur_size = 0
        i += 1
    if cur_size > 0:
        assert i == len(layer)
        start_pos = layer[start_i].start
        end_pos   = layer[i-1].end
        division.append( (start_pos, end_pos) )
    return division


# =================================================
# =================================================
#    Creating morph_analysis taggers
# =================================================
# =================================================

def create_vm_tagger_based_on_vm_instance( old_morph_layer, collection, log, new_morph_layer, \
                                                            new_lex_path, new_disamb_lex_path, \
                                                            incl_prefix='', incl_suffix='' ):
    ''' Creates VabamorfTagger based on Vabamorf's binary lexicons.
        Fixes input layers of the tagger based on the layers available 
        in the given collection. '''
    default_vm_tagger = DEFAULT_RESOLVER.taggers.rules['morph_analysis']
    vmtagger_input_args = \
        find_morph_analysis_dependency_layers( default_vm_tagger,old_morph_layer,collection,log,
                                               incl_prefix=incl_prefix, incl_suffix=incl_suffix )
    vmtagger_input_args['output_layer'] = new_morph_layer
    vmtagger_input_args['vm_instance']  = VabamorfInstance( lex_path=new_lex_path, \
                                                            disamb_lex_path=new_disamb_lex_path )
    vm_tagger = VabamorfTagger( **vmtagger_input_args )
    log.info(' Initialized {!r} for evaluation. '.format( vm_tagger) )
    return vm_tagger


def create_vm_tagger( old_morph_layer, collection, log, new_morph_layer, \
                                       incl_prefix='', incl_suffix='', \
                                       **kwargs ):
    ''' Creates VabamorfTagger based on given analysis settings (**kwargs). 
        Supported parameters: 'guess', 'propername', 'disambiguate', 'compound', 
        'phonetic', 'slang_lex'. If kwargs is not specified, then creates 
        VabamorfTagger with default settings. 
        Fixes input layers of the tagger based on the layers available 
        in the given collection. '''
    default_vm_tagger = DEFAULT_RESOLVER.taggers.rules['morph_analysis']
    vmtagger_input_args = \
        find_morph_analysis_dependency_layers( default_vm_tagger,old_morph_layer,collection,log,
                                               incl_prefix=incl_prefix, incl_suffix=incl_suffix )
    vmtagger_input_args['output_layer'] = new_morph_layer
    for kwarg in kwargs.keys():
        if kwarg in ['guess', 'propername', 'disambiguate', 'compound', 'phonetic', 'slang_lex']:
            vmtagger_input_args[kwarg] = kwargs.get(kwarg)
    vm_tagger = VabamorfTagger( **vmtagger_input_args )
    log.info(' Initialized {!r} for evaluation. '.format( vm_tagger) )
    return vm_tagger


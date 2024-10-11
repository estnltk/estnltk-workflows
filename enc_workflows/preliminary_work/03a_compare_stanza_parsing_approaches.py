#
#  Applies 2 stanza parsing approaches on input_json_dir: 
#  *) parse texts based on original morph_extended input; 
#  *) parse texts based on morph_extended recreated from the scratch; 
#
#  Collects statistics about differences and outputs after 
#  processing. 
#
#  Writes Text object JSON files with different parsing layers 
#  into folder f'{input_json_dir}_output'.
#

import json
import re, sys
import os, os.path

from datetime import datetime

from tqdm import tqdm

from collections import defaultdict

from estnltk import Text, Annotation
from estnltk.converters import json_to_text
from estnltk.converters import text_to_json

from estnltk_core.layer_operations import flatten

skip_prefix_list = ['nc23_Academic_', 
                    'nc23_Literature_Contemporary_', 
                    'nc23_Literature_Old']
skip_prefix_list = []
force_prefix_list = ['nc23_Academic_', 
                     'nc23_Literature_Contemporary_', 
                     'nc23_Literature_Old']
force_prefix_list = []

from estnltk.taggers import VabamorfTagger
from estnltk.taggers import MorphExtendedTagger

from estnltk_neural.taggers import StanzaSyntaxTagger


original_morph_based_parser = StanzaSyntaxTagger( output_layer='original_morph_based_syntax', \
                                                  input_type='morph_extended', \
                                                  words_layer="original_words",  \
                                                  sentences_layer="original_sentences", \
                                                  input_morph_layer='original_morph_analysis', \
                                                  random_pick_seed=1 )

estnltk_morph_tagger = VabamorfTagger(output_layer='estnltk_morph_analysis',
                           input_words_layer="original_words",
                           input_sentences_layer="original_sentences",
                           input_compound_tokens_layer='original_compound_tokens',
                           slang_lex=True)
estnltk_morph_extended_tagger = MorphExtendedTagger( output_layer='estnltk_morph_extended',
                                                     input_morph_analysis_layer='estnltk_morph_analysis' )
estnltk_morph_based_parser = StanzaSyntaxTagger( output_layer='estnltk_morph_based_syntax', \
                                                 input_type='morph_extended', \
                                                 words_layer="original_words", \
                                                 sentences_layer="original_sentences", \
                                                 input_morph_layer='estnltk_morph_extended', \
                                                 random_pick_seed=1 )

def convert_original_morph_to_stanza_input_morph(morph_layer):
    '''Converts (extended) morphological analysis layer imported from the 
       ENC corpus to StanzaSyntaxTagger's input format. 
       Basically, overwrites 'form' values with 'extended_form' values 
       in the layer. The input layer will be modified, and the method 
       returns nothing.
    '''
    assert 'extended_form' in morph_layer.attributes
    for morph_span in morph_layer:
        assert len(morph_span.annotations) == 1
        annotations = morph_span.annotations[0]
        annotations_dict = {a:annotations[a] for a in morph_layer.attributes}
        annotations_dict['form'] = annotations_dict['extended_form']
        morph_span.clear_annotations()
        assert len(morph_span.annotations) == 0
        morph_span.add_annotation( Annotation(morph_span, annotations_dict) )
        assert len(morph_span.annotations) == 1

def count_and_percent(items, items_total):
    assert items_total > 0
    return f'{items} / {items_total} ({(items/items_total)*100.0:.2f}%)'

if len(sys.argv) > 1:
    json_corpus_dir = sys.argv[1]
    assert os.path.isdir(json_corpus_dir), f'(!) Missing input directory {json_corpus_dir!r}'
    output_folder = f'{json_corpus_dir}_output'
    os.makedirs(output_folder, exist_ok=True)
    start = datetime.now()
    total_spans = 0
    total_lemma_diff = 0
    total_upos_diff = 0
    total_form_diff = 0
    total_deprel_diff = 0
    total_head_diff = 0
    total_deprel_head_diff = 0
    for fname in tqdm(os.listdir(json_corpus_dir), ascii=True):
        skip = False
        has_prefix = False
        for force_prefix in force_prefix_list:
            if fname.startswith(force_prefix):
                has_prefix = True
        if force_prefix_list and not has_prefix:
            print(f'Skipping {fname} ({force_prefix}) ...')
            skip = True
        for skip_prefix in skip_prefix_list:
            if fname.startswith(skip_prefix):
                print(f'Skipping {fname} ...')
                skip = True
        if skip:
            continue
        if fname.endswith('.json'):
            text_obj = json_to_text(file=os.path.join(json_corpus_dir, fname))
            assert "original_morph_analysis" in text_obj.layers
            assert "original_words" in text_obj.layers
            assert "original_sentences" in text_obj.layers
            # A) Tag syntax based on original morph_extended from vert files
            if ('form' in text_obj["original_morph_analysis"].attributes) and \
               ('extended_form' in text_obj["original_morph_analysis"].attributes):
                convert_original_morph_to_stanza_input_morph( text_obj["original_morph_analysis"] )
            original_morph_based_parser.tag(text_obj)
            # B) Tag syntax based on estnltk's morph_extended (retagged from the scratch)
            estnltk_morph_tagger.tag(text_obj)
            estnltk_morph_extended_tagger.tag(text_obj)
            estnltk_morph_based_parser.tag(text_obj)
            # Make syntax layers flat
            flat_syntax_1 = flatten(text_obj['original_morph_based_syntax'], 
                                    'original_morph_based_syntax_flat')
            text_obj.add_layer( flat_syntax_1 )
            flat_syntax_2 = flatten(text_obj['estnltk_morph_based_syntax'], 
                                    'estnltk_morph_based_syntax_flat')
            text_obj.add_layer( flat_syntax_2 )
            # Remove all redundant layers
            for layer in list(text_obj.layers):
                if layer in ['original_morph_based_syntax_flat', \
                             'estnltk_morph_based_syntax_flat']:
                    continue
                if layer in text_obj.layers:
                    text_obj.pop_layer( layer )
            assert set(text_obj.layers) == {'original_morph_based_syntax_flat', \
                                            'estnltk_morph_based_syntax_flat'}
            # Save results for further studies
            text_to_json(text_obj, file=os.path.join(output_folder, fname))
            # Find differences
            for orig_span, estnltk_span in zip( text_obj['original_morph_based_syntax_flat'], \
                                                text_obj['estnltk_morph_based_syntax_flat'] ):
                assert orig_span.base_span == estnltk_span.base_span
                orig_ann = orig_span.annotations[0]
                estnltk_ann = estnltk_span.annotations[0]
                if orig_ann['lemma'] != estnltk_ann['lemma']:
                    total_lemma_diff += 1
                if orig_ann['upostag'] != estnltk_ann['upostag']:
                    total_upos_diff += 1
                if orig_ann['feats'] != estnltk_ann['feats']:
                    total_form_diff += 1
                if orig_ann['head'] != estnltk_ann['head']:
                    total_head_diff += 1
                if orig_ann['deprel'] != estnltk_ann['deprel']:
                    total_deprel_diff += 1
                if orig_ann['head'] != estnltk_ann['head'] or \
                   orig_ann['deprel'] != estnltk_ann['deprel']:
                    total_deprel_head_diff += 1
                total_spans += 1
    print()
    print(f'Total processing time: {datetime.now() - start}')
    print()
    print('Differences: ')
    print()
    print(f'  Lemma differences:    {count_and_percent(total_lemma_diff, total_spans)}')
    print(f'  UPOS differences:     {count_and_percent(total_upos_diff, total_spans)}')
    print(f'  Feats differences:    {count_and_percent(total_form_diff, total_spans)}')
    print()
    print(f'  Deprel differences:   {count_and_percent(total_deprel_diff, total_spans)}')
    print(f'  Head differences:     {count_and_percent(total_head_diff, total_spans)}')
    print(f'  Deprel or head diff:  {count_and_percent(total_deprel_head_diff, total_spans)}')
    print()
else:
    print('Directory with estnltk json files is required as an input argument.')

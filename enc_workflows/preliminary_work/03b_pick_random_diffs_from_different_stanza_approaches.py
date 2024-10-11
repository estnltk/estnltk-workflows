#
#  Picks randomly 100 sentences that have syntactic analysis 
#  differences from the directory containing estnltk json files.
#
#  All estnltk json documents must have syntactic annotation layers 
#  "original_morph_based_syntax_flat" and "estnltk_morph_based_syntax_flat" 
#  which will be compared. 
#
#  Saves results into file f'random_pick_100_differences.txt'.
#

import json
import re, sys
import os, os.path

from datetime import datetime

from tqdm import tqdm

from random import randint, seed

from collections import defaultdict

from estnltk import Text
from estnltk.converters import json_to_text
from estnltk.converters import text_to_json


random_seed_value = 1

# The number of random differences to be picked
random_pick_goal = 100


def count_and_percent(items, items_total):
    assert items_total > 0
    return f'{items} / {items_total} ({(items/items_total)*100.0:.2f}%)'


def split(a, n):
    """
    Splits list `a` into `n` roughly equal-sized subsets.
    If `a` is not exactly divisible by `n`, then finds the
    reminder `r` of the division and enlarges sizes of first 
    `r` subsets by 1. 
    Returns a generator of the split. 
    
    Examples:
    
    >>> sp1 = split([1,1,2,2,3,3], 3)
    >>> list(sp1)
    [[1, 1], [2, 2], [3, 3]]
    >>> sp2 = split([1,2,2,3,3,3,4,4,4,4,5,5,5,5,5], 6)
    >>> list(sp2)
    [[1, 2, 2], [3, 3, 3], [4, 4, 4], [4, 5], [5, 5], [5, 5]]
    >> sp3 = split([[1], [2,2], [3,3,3], [4,4,4,4]], 3)
    >> list(sp3)
    [[[1], [2, 2]], [[3, 3, 3]], [[4, 4, 4, 4]]]
    
    Original source code from:
    https://github.com/estnltk/syntax_experiments/blob/ablation_experiments/02_split_data.py
    """
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


if len(sys.argv) > 1:
    json_corpus_dir = sys.argv[1]
    assert os.path.isdir(json_corpus_dir), f'(!) Missing input directory {json_corpus_dir!r}'
    #output_folder = f'{json_corpus_dir}_output'
    #os.makedirs(output_folder, exist_ok=True)
    start = datetime.now()
    total_spans = 0
    total_lemma_diff = 0
    total_upos_diff = 0
    total_form_diff = 0
    total_deprel_diff = 0
    total_head_diff = 0
    total_deprel_head_diff = 0
    total_sentences = 0
    sentences_with_dep_diffs = []
    total_sents_by_src = defaultdict(int)
    for fname in tqdm(os.listdir(json_corpus_dir), ascii=True):
        if fname.endswith('.json'):
            text_obj = json_to_text(file=os.path.join(json_corpus_dir, fname))
            assert 'original_morph_based_syntax_flat' in text_obj.layers
            assert 'estnltk_morph_based_syntax_flat' in text_obj.layers
            syntax_from_original = text_obj['original_morph_based_syntax_flat']
            syntax_from_estnltk  = text_obj['estnltk_morph_based_syntax_flat']
            assert len(syntax_from_original) == len(syntax_from_estnltk)
            assert 'src' in text_obj.meta
            text_src = text_obj.meta['src']
            # Find differences
            sentence = []
            sentence_id = 0
            has_dep_diff = False
            for orig_span, estnltk_span in zip( syntax_from_original, \
                                                syntax_from_estnltk ):
                assert orig_span.base_span == estnltk_span.base_span
                orig_ann = orig_span.annotations[0]
                estnltk_ann = estnltk_span.annotations[0]
                assert estnltk_ann['id'] == orig_ann['id']
                span_id = estnltk_ann['id']
                if str(span_id) == '1' and len(sentence) > 0:
                    sentence_id += 1
                    total_sents_by_src[text_src] += 1
                    if has_dep_diff:
                        # Record dependency difference
                        sent_ref = {
                            'sent_id': sentence_id,
                            'src': text_src,
                            'file': fname, 
                            'meta': text_obj.meta, 
                            'content': sentence 
                        }
                        sentences_with_dep_diffs.append( sent_ref )
                    # Restart sentence
                    total_sentences += 1
                    sentence = []
                    has_dep_diff = False
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
                    has_dep_diff = True
                    total_deprel_head_diff += 1
                sentence.append( (orig_span, estnltk_span) )
                total_spans += 1
            if len( sentence ) > 0:
                sentence_id += 1
                total_sents_by_src[text_src] += 1
                if has_dep_diff:
                    # Record dependency difference
                    # 
                    sent_ref = {
                        'sent_id': sentence_id,
                        'src': text_src,
                        'file': fname, 
                        'meta': text_obj.meta, 
                        'content': sentence 
                    }
                    sentences_with_dep_diffs.append( sent_ref )
                # Restart sentence
                total_sentences += 1
                sentence = []
                has_dep_diff = False
        #if len(sentences_with_dep_diffs) > 500:
        #    break
    print()
    print(f'Total processing time: {datetime.now() - start}')
    print()
    print('Total differences: ')
    print()
    print(f'  Lemma differences:    {count_and_percent(total_lemma_diff, total_spans)}')
    print(f'  UPOS differences:     {count_and_percent(total_upos_diff, total_spans)}')
    print(f'  Feats differences:    {count_and_percent(total_form_diff, total_spans)}')
    print()
    print(f'  Deprel differences:   {count_and_percent(total_deprel_diff, total_spans)}')
    print(f'  Head differences:     {count_and_percent(total_head_diff, total_spans)}')
    print(f'  Deprel or head diff:  {count_and_percent(total_deprel_head_diff, total_spans)}')
    print()
    print(f'  Sentences with head or deprel diff:  {count_and_percent(len(sentences_with_dep_diffs), total_sentences)}')
    print()

    # Group sentences by sources
    sents_by_sources = {}
    for sent_ref in sentences_with_dep_diffs:
        if sent_ref['src'] not in sents_by_sources.keys():
            sents_by_sources[sent_ref['src']] = []
        sents_by_sources[sent_ref['src']].append(sent_ref)
    print('Sentences with head or deprel differences by src: ')
    print()
    sorted_sources = sorted(list(sents_by_sources.keys()))
    for src in sorted_sources:
        print(f'  {count_and_percent(len(sents_by_sources[src]), total_sents_by_src[src])} -- {src}')
    print()
    
    print('Overall random pick goal:')
    print(random_pick_goal)
    print()
    
    # Distribute picks among sources
    random_pick_indexes = [i for i in range(random_pick_goal)]
    split_subsets = list(split(random_pick_indexes, len(sorted_sources)))
    random_pick_goals = {}
    locked = set()
    overflow = 0
    for subset, source in zip(split_subsets, sorted_sources):
        available = len(sents_by_sources[source])
        random_pick_goals[source] = len(subset)
        if available < random_pick_goals[source]:
            random_pick_goals[source] = available
            overflow += (len(subset) - available)
            locked.add(source)
    # If some sources are under-distributed, make corrections and 
    # distribute it picks among other sources
    while overflow > 0 and len(locked) < len(sorted_sources):
        for source in sorted_sources:
            if source not in locked and overflow > 0:
                available = len( sents_by_sources[source] )
                if available > random_pick_goals[source]:
                    random_pick_goals[source] += 1
                    overflow -= 1
                else:
                    locked.add(source)
    print('Random pick goals:')
    print(random_pick_goals)

    print('Making random picks...')
    seed( random_seed_value )
    random_picks = []
    for source in sorted_sources:
        target_amount = random_pick_goals[source]
        available = sents_by_sources[source]
        assert target_amount <= len(available)
        if int(target_amount) == 0:
            # Skip altogether
            continue
        elif int(target_amount) == len(available):
            # Add all without random selection
            for sent_ref in available:
                random_picks.append( sent_ref )
            continue
        failed_attempts = 0
        current_selection_ids = set()
        while len(current_selection_ids) < target_amount:
            i = randint(0, len(available) - 1)
            if i not in current_selection_ids:
                current_selection_ids.add( i )
                failed_attempts = 0
            else:
                failed_attempts += 1
                if failed_attempts >= 20:
                    print('(!) 20 unsuccessful random picks in a row: terminating ...')
                    break
        for pid in sorted(list(current_selection_ids)):
            random_picks.append( available[pid] )
    assert len(random_picks) == random_pick_goal, \
        f'Picked: {len(random_picks)} != goal: {random_pick_goal}'
    fname = f'random_pick_{random_pick_goal}_differences.txt'
    with open(fname, mode='w', encoding='utf-8') as out_f:
        for sent in random_picks:
            meta = sent['meta']
            out_f.write( f"{meta['src']} :: document_id: {meta['id']} :: sentence_nr: {sent['sent_id']}\n" )
            for (orig_span, estnltk_span) in sent['content']:
                orig_ann = orig_span.annotations[0]
                estnltk_ann = estnltk_span.annotations[0]
                span_id   = estnltk_ann['id']
                word_str  = estnltk_span.text
                orig_head = orig_ann['head']
                new_head  = estnltk_ann['head']
                orig_deprel = orig_ann['deprel']
                new_deprel  = estnltk_ann['deprel']
                if orig_head == new_head and orig_deprel == new_deprel:
                    # No difference
                    out_f.write( f"{span_id}\t{word_str}\t{orig_deprel}\t{orig_head}\n" )
                else:
                    out_f.write( f"* {span_id}\t{word_str}\t" )
                    if orig_deprel != new_deprel:
                        out_f.write( f"{orig_deprel} => {new_deprel}\t" )
                    else:
                        out_f.write( f"{orig_deprel}\t" )
                    if orig_head != new_head:
                        out_f.write( f"{orig_head} => {new_head}" )
                    else:
                        out_f.write( f"{orig_head}" )
                    out_f.write( "\n" )
                if orig_ann['head'] != estnltk_ann['head']:
                    total_head_diff += 1
                if orig_ann['deprel'] != estnltk_ann['deprel']:
                    total_deprel_diff += 1
            out_f.write( "\n" )
            out_f.write( "\n" )
    print('Done.')
else:
    print('Directory with estnltk json files is required as an input argument.')

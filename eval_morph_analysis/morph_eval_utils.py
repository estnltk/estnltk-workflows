# =========================================================
# =========================================================
#  Utilities for finding & recording morph_analysis
#  differences
# =========================================================
# =========================================================
#
#  Based on: 
#   https://github.com/estnltk/eval_experiments_lrec_2020
#

import os, os.path, re
from collections import defaultdict

from estnltk.text import Text
from estnltk.layer.layer import Layer
from estnltk.layer_operations import flatten
from estnltk.layer.annotation import Annotation
from estnltk.taggers import DiffTagger
from estnltk.taggers.standard_taggers.diff_tagger import iterate_diff_conflicts
from estnltk.taggers.standard_taggers.diff_tagger import iterate_modified

# =================================================
# =================================================
#    Creating flat layers
# =================================================
# =================================================

def create_flat_v1_6_morph_analysis_layer( text_obj, morph_layer, output_layer, add_layer=True ):
    '''Creates copy of estnltk v1.6 morph_analysis layer that is a flat layer containing only segmentation. '''
    assert isinstance(text_obj, Text)
    assert morph_layer in text_obj.layers, '(!) Layer {!r} missing from: {!r}'.format(morph_layer, text_obj.layers)
    flat_morph = flatten(text_obj[ morph_layer ], output_layer )
    if add_layer:
        text_obj.add_layer( flat_morph )
    return flat_morph


# =================================================
# =================================================
#    Get morph analysis diff
#    ( Vabamorf's annotations )
# =================================================
# =================================================

def get_estnltk_morph_analysis_diff_annotations( text_obj, layer_a, layer_b, diff_layer ):
    ''' Collects differing sets of annotations from EstNLTK's morph_analysis diff_layer. '''
    STATUS_ATTR = '__status'
    assert isinstance(text_obj, Text)
    assert layer_a in text_obj.layers, '(!) Layer {!r} missing from: {!r}'.format(layer_a, text_obj.layers)
    assert layer_b in text_obj.layers, '(!) Layer {!r} missing from: {!r}'.format(layer_b, text_obj.layers)
    assert diff_layer in text_obj.layers, '(!) Layer {!r} missing from: {!r}'.format(diff_layer, text_obj.layers)
    layer_a_spans = text_obj[layer_a]
    layer_b_spans = text_obj[layer_b]
    common_attribs = set(text_obj[layer_a].attributes).intersection( set(text_obj[layer_b].attributes) )
    assert len(common_attribs) > 0, '(!) Layers {!r} and {!r} have no common attributes!'.format(layer_a, layer_b)
    assert STATUS_ATTR not in common_attribs, "(!) Unexpected attribute {!r} in {!r}.".format(STATUS_ATTR, common_attribs)
    assert layer_a not in ['start', 'end']
    assert layer_b not in ['start', 'end']
    collected_diffs = []
    missing_annotations = 0
    extra_annotations   = 0
    a_id = 0
    b_id = 0
    for diff_span in iterate_modified( text_obj[diff_layer], 'span_status' ):
        ds_start = diff_span.start
        ds_end =   diff_span.end
        # Find corresponding span in both layer
        a_span = None
        b_span = None
        while a_id < len(layer_a_spans):
            cur_a_span = layer_a_spans[a_id]
            if cur_a_span.start == ds_start and cur_a_span.end == ds_end:
                a_span = cur_a_span
                break
            a_id += 1
        while b_id < len(layer_b_spans):
            cur_b_span = layer_b_spans[b_id]
            if cur_b_span.start == ds_start and cur_b_span.end == ds_end:
                b_span = cur_b_span
                break
            b_id += 1
        if a_span == None:
            raise Exception('(!) {!r} not found from layer {!r}'.format(diff_span, layer_a))
        if b_span == None:
            raise Exception('(!) {!r} not found from layer {!r}'.format(diff_span, layer_b))
        a_annotations = []
        for a_anno in a_span.annotations:
            a_dict = a_anno.__dict__.copy()
            a_dict = {a:a_dict[a] for a in a_dict.keys() if a in common_attribs}
            a_dict[STATUS_ATTR] = None
            a_annotations.append( a_dict )
        b_annotations = []
        for b_anno in b_span.annotations:
            b_dict = b_anno.__dict__.copy()
            b_dict = {b:b_dict[b] for b in b_dict.keys() if b in common_attribs}
            b_dict[STATUS_ATTR] = None
            b_annotations.append( b_dict )
        for a_anno in a_annotations:
            match_found = False
            for b_anno in b_annotations:
                if a_anno == b_anno:
                    a_anno[STATUS_ATTR] = 'COMMON'
                    b_anno[STATUS_ATTR] = 'COMMON'
                    match_found = True
                    break
            if not match_found:
                missing_annotations += 1
                a_anno[STATUS_ATTR] = 'MISSING'
        for b_anno in b_annotations:
            if b_anno not in a_annotations:
                extra_annotations += 1
                b_anno[STATUS_ATTR] = 'EXTRA'
        collected_diffs.append( {'text':diff_span.text, layer_a: a_annotations, layer_b: b_annotations, 'start':diff_span.start, 'end':diff_span.end} )
    # Sanity check: missing vs extra annotations:
    # Note: text_obj[diff_layer].meta contains more *_annotations items, because it also 
    #       counts annotations in missing spans and extra spans; Unfortunately, merely
    #       subtracting:
    #                       missing_annotations - missing_spans
    #                       extra_annotations - extra_spans
    #       does not work either, because one missing or extra span may contain more 
    #       than one annotation. So, we have to re-count extra and missing annotations ...
    normalized_extra_annotations   = 0
    normalized_missing_annotations = 0
    for span in text_obj[diff_layer]:
        for status in span.span_status:
            if status == 'missing':
                normalized_missing_annotations += 1
            elif status == 'extra':
                normalized_extra_annotations += 1
    assert missing_annotations == text_obj[diff_layer].meta['missing_annotations'] - normalized_missing_annotations
    assert extra_annotations == text_obj[diff_layer].meta['extra_annotations'] - normalized_extra_annotations
    return collected_diffs


def get_estnltk_morph_analysis_annotation_alignments( collected_diffs, layer_names, focus_attributes=['root','partofspeech', 'form'], remove_status=True ):
    ''' Calculates annotation alignments between annotations in collected_diffs. '''
    assert isinstance(layer_names, list) and len(layer_names) == 2
    STATUS_ATTR = '__status'
    MATCHING_ATTR    = '__matching'
    MISMATCHING_ATTR = '__mismatching'
    alignments  = []
    annotations_by_layer = defaultdict(int)
    if len(collected_diffs) > 0:
        first_diff = collected_diffs[0]
        all_attributes = []
        for key in first_diff.keys():
            if key not in ['text', 'start', 'end']:
                all_attributes = [k for k in first_diff[key][0].keys() if k != STATUS_ATTR]
                assert key in layer_names
        assert len( all_attributes ) > 0
        assert len([a for a in focus_attributes if a in all_attributes]) == len(focus_attributes)
        for word_diff in collected_diffs:
            alignment = word_diff.copy()
            a_anns = word_diff[layer_names[0]]
            b_anns = word_diff[layer_names[1]]
            annotations_by_layer[layer_names[0]] += len(a_anns)
            annotations_by_layer[layer_names[1]] += len(b_anns)
            alignment['alignments'] = []
            del alignment[layer_names[0]]
            del alignment[layer_names[1]]
            a_used = set()
            b_used = set()
            for a_id, a in enumerate(a_anns):
                # Find fully matching annotation
                for b_id, b in enumerate(b_anns):
                    if a == b:
                        al = {STATUS_ATTR:'COMMON', layer_names[0]:a, layer_names[1]:b }
                        al[MISMATCHING_ATTR] = []
                        al[MATCHING_ATTR] = all_attributes.copy()
                        alignment['alignments'].append( al )
                        a_used.add(a_id)
                        b_used.add(b_id)
                        break
                if a_id in a_used:
                    continue
                # Find partially matching annotation
                closest_b = None
                closest_b_id = None
                closest_common   = []
                closest_uncommon = []
                for b_id, b in enumerate(b_anns):
                    if a_id in a_used or b_id in b_used:
                        continue
                    if b[STATUS_ATTR] == 'COMMON':
                        # Skip b that has been previously found as being common
                        continue
                    if a != b:
                        #count common attribs
                        matching_attribs = []
                        mismatching = []
                        for attr in all_attributes:
                            if a[attr] == b[attr]:
                                matching_attribs.append(attr)
                            else:
                                mismatching.append(attr)
                        if len(matching_attribs) > len(closest_common):
                            focus_1 = []
                            focus_2 = []
                            if closest_b != None:
                                focus_1 = [a for a in focus_attributes if a in matching_attribs]
                                focus_2 = [a for a in focus_attributes if a in closest_common]
                            # in case of a tie, prefer matches with more focus attributes
                            if len(focus_1) == len(focus_2) or len(focus_1) > len(focus_2):
                                closest_common   = matching_attribs
                                closest_uncommon = mismatching
                                closest_b_id = b_id
                                closest_b = b
                if closest_b != None:
                    al = {STATUS_ATTR:'MODIFIED', layer_names[0]:a, layer_names[1]:closest_b }
                    al[MISMATCHING_ATTR] = closest_uncommon
                    al[MATCHING_ATTR] = closest_common
                    alignment['alignments'].append( al )
                    a_used.add(a_id)
                    b_used.add(closest_b_id)
                else:
                    al = {STATUS_ATTR:'MISSING', layer_names[0]:a, layer_names[1]:{} }
                    al[MISMATCHING_ATTR] = all_attributes.copy()
                    al[MATCHING_ATTR] = []
                    alignment['alignments'].append( al )
                    a_used.add(a_id)
            for b_id, b in enumerate(b_anns):
                if b_id not in b_used:
                    al = {STATUS_ATTR:'EXTRA', layer_names[0]:{}, layer_names[1]:b }
                    al[MISMATCHING_ATTR] = all_attributes.copy()
                    al[MATCHING_ATTR] = []
                    alignment['alignments'].append( al )
            alignments.append( alignment )
    # Sanity check: check that we haven't lost any annotations during the careful alignment
    annotations_by_layer_2 = defaultdict(int)
    for word_diff in alignments:
        for al in word_diff['alignments']:
            for layer in layer_names:
                if len(al[layer].keys()) > 0:
                    annotations_by_layer_2[layer] += 1
    for layer in layer_names:
        if annotations_by_layer[layer] != annotations_by_layer_2[layer]:
           # Output information about the context of the failure
            from pprint import pprint
            print('='*50)
            print(layer,'  ',annotations_by_layer[layer], '  ', annotations_by_layer_2[layer])
            print('='*50)
            pprint(collected_diffs)
            print('='*50)
            pprint(alignments)
            print('='*50)
        assert annotations_by_layer[layer] == annotations_by_layer_2[layer], '(!) Failure in annotation conversion.'
    # Remove STATUS_ATTR's from annotations dict's (if required)
    if remove_status:
        for word_diff in alignments:
            for al in word_diff['alignments']:
                for layer in layer_names:
                    if STATUS_ATTR in al[layer].keys():
                        del al[layer][STATUS_ATTR]
    return alignments


def get_concise_morph_diff_alignment_str( alignments, layer_a, layer_b, focus_attributes=['root','partofspeech','form'], return_list=False ):
    ''' Formats differences of morph analysis annotations as a string (or a list of strings).'''
    STATUS_ATTR = '__status'
    MATCHING_ATTR    = '__matching'
    MISMATCHING_ATTR = '__mismatching'
    out_str = []
    max_len = max(len(layer_a), len(layer_b))
    max_label_len = max( [len(a) for a in ['MODIFIED', 'MISSING', 'EXTRA', 'COMMON']])
    for alignment in alignments:
        assert STATUS_ATTR      in alignment.keys()
        assert MATCHING_ATTR    in alignment.keys()
        assert MISMATCHING_ATTR in alignment.keys()
        assert layer_a in alignment.keys()
        assert layer_b in alignment.keys()
        if alignment[STATUS_ATTR] == 'MODIFIED':
            focus_is_matching = len([fa for fa in focus_attributes if fa in alignment[MATCHING_ATTR]]) == len(focus_attributes)
            if not focus_is_matching:
                a = [alignment[layer_a][fa] for fa in focus_attributes]
                b = [alignment[layer_b][fa] for fa in focus_attributes]
                out_str.append( (' --- {:'+str(max_label_len)+'} {} ').format(alignment[STATUS_ATTR], '-'*50) )
                out_str.append((' {:'+str(max_len)+'}   ').format(layer_a) + ' '+str(a))
                out_str.append((' {:'+str(max_len)+'}   ').format(layer_b) + ' '+str(b))
            else:
                a = [alignment[layer_a][fa] for fa in focus_attributes+alignment[MISMATCHING_ATTR]]
                b = [alignment[layer_b][fa] for fa in focus_attributes+alignment[MISMATCHING_ATTR]]
                out_str.append( (' --- {:'+str(max_label_len)+'} {} ').format(alignment[STATUS_ATTR], '-'*50) )
                out_str.append((' {:'+str(max_len)+'}   ').format(layer_a) + ' '+str(a))
                out_str.append((' {:'+str(max_len)+'}   ').format(layer_b) + ' '+str(b))
        elif alignment[STATUS_ATTR] == 'COMMON':
            a = [alignment[layer_a][fa] for fa in focus_attributes]
            b = [alignment[layer_b][fa] for fa in focus_attributes]
            out_str.append( (' --- {:'+str(max_label_len)+'} {} ').format(alignment[STATUS_ATTR], '-'*50) )
            out_str.append((' {:'+str(max_len)+'}   ').format(layer_a) + ' '+str(a))
            out_str.append((' {:'+str(max_len)+'}   ').format(layer_b) + ' '+str(b))
        elif alignment[STATUS_ATTR] in ['EXTRA', 'MISSING']:
            a = [alignment[layer_a][fa] for fa in focus_attributes] if len(alignment[layer_a].keys()) > 0 else []
            b = [alignment[layer_b][fa] for fa in focus_attributes] if len(alignment[layer_b].keys()) > 0 else []
            out_str.append( (' --- {:'+str(max_label_len)+'} {} ').format(alignment[STATUS_ATTR], '-'*50) )
            if a:
                out_str.append((' {:'+str(max_len)+'}   ').format(layer_a) + ' '+str(a))
            if b:
                out_str.append((' {:'+str(max_len)+'}   ').format(layer_b) + ' '+str(b))
        else:
            raise Exception( '(!) unexpected __status: {!r}'.format(alignment[STATUS_ATTR]) )
    return '\n'.join( out_str ) if not return_list else out_str


def _text_snippet( text_obj, start, end ):
    '''Takes a snippet out of the text, assuring that text boundaries are not exceeded.'''
    start = 0 if start < 0 else start
    start = len(text_obj.text) if start > len(text_obj.text) else start
    end   = len(text_obj.text) if end > len(text_obj.text)   else end
    end   = 0 if end < 0 else end
    snippet = text_obj.text[start:end]
    snippet = snippet.replace('\n', '\\n')
    return snippet


def format_morph_diffs_string( fname_stub, text_obj, diff_word_alignments, layer_a, layer_b, gap_counter=0, text_cat='',
                                                                           focus_attributes=['root','partofspeech','form'] ):
    '''Formats aligned differences as human-readable text snippets.'''
    assert layer_a in text_obj.layers, '(!) Layer {!r} missing from: {!r}'.format(layer_a, text_obj.layers)
    assert layer_b in text_obj.layers, '(!) Layer {!r} missing from: {!r}'.format(layer_b, text_obj.layers)
    N = 60
    output_lines = []
    for word_alignments in diff_word_alignments:
        w_start = word_alignments['start']
        w_end   = word_alignments['end']
        before = '...'+_text_snippet( text_obj, w_start - N, w_start )
        after  = _text_snippet( text_obj, w_end, w_end + N )+'...'
        output_lines.append('='*85)
        output_lines.append('')
        output_lines.append('  '+text_cat+'::'+fname_stub+'::'+str(gap_counter))
        output_lines.append('')
        output_lines.append( before+' {'+word_alignments['text']+'} '+after  )
        sub_strs = get_concise_morph_diff_alignment_str(word_alignments['alignments'], layer_a, layer_b, \
                                                        return_list=True, focus_attributes=focus_attributes )
        output_lines.extend( sub_strs )
        output_lines.append('')
        gap_counter += 1
    if len(output_lines)>0:
        output_lines.append('')
    return '\n'.join(output_lines) if len(output_lines)>0 else None, gap_counter


def write_formatted_diff_str_to_file( out_fname, output_lines ):
    '''Writes/appends formatted differences to the given file.'''
    if not os.path.exists(out_fname):
        with open(out_fname, 'w', encoding='utf-8') as f:
            pass
    with open(out_fname, 'a', encoding='utf-8') as f:
        ## write content
        f.write(output_lines)
        if not output_lines.endswith('\n'):
            f.write('\n')


class MorphDiffSummarizer:
    '''Aggregates and summarizes morph_analysis annotations difference statistics based on information from diff layers.'''

    def __init__(self, first_model, second_model):
        self.diffs_counter = {}
        self.first_model    = first_model
        self.second_model   = second_model
    
    def record_from_diff_layer( self, layer_name, layer, text_category ):
        assert isinstance(text_category, str)
        assert len(text_category) > 0
        if layer_name not in self.diffs_counter:
            self.diffs_counter[layer_name] = {}
        if 'total' not in self.diffs_counter[layer_name]:
            self.diffs_counter[layer_name]['total'] = defaultdict(int)
        for key in layer.meta:
            self.diffs_counter[layer_name]['total'][key] += layer.meta[key]
        self.diffs_counter[layer_name]['total']['_docs'] += 1
        if text_category not in self.diffs_counter[layer_name]:
            self.diffs_counter[layer_name][text_category] = defaultdict(int)
        for key in layer.meta:
            self.diffs_counter[layer_name][text_category][key] += layer.meta[key]
        self.diffs_counter[layer_name][text_category]['_docs'] += 1

    def get_diffs_summary_output( self, show_doc_count=True ):
        output = []
        for layer in sorted( self.diffs_counter.keys() ):
            output.append( layer )
            output.append( '\n' )
            diff_categories = [k for k in sorted(self.diffs_counter[layer].keys()) if k != 'total']
            single_category = len(diff_categories) == 1
            assert 'total' in self.diffs_counter[layer]
            diff_categories.append('total')
            longest_cat_name = max( [len(k) for k in diff_categories] )
            for category in diff_categories:
                src = self.diffs_counter[layer][category]
                if category == 'total' and single_category:
                    # No need to display TOTAL, if there was only one category
                    continue
                if category == 'total':
                    category = 'TOTAL'
                output.append( (' {:'+str(longest_cat_name+1)+'}').format(category) )
                if show_doc_count:
                    output.append('|')
                    output.append(' #docs: {} '.format(src['_docs']) )
                # unchanged_spans + modified_spans + missing_spans = length_of_old_layer
                # unchanged_spans + modified_spans + extra_spans = length_of_new_layer
                # unchanged_annotations + missing_annotations = number_of_annotations_in_old_layer
                # unchanged_annotations + extra_annotations   = number_of_annotations_in_new_layer
                
                first_layer_len  = src['unchanged_annotations'] + src['missing_annotations']
                second_layer_len = src['unchanged_annotations'] + src['extra_annotations']
                total_spans = first_layer_len + second_layer_len
                output.append('|')
                common_spans = src['unchanged_spans'] + src['modified_spans']
                ratio = src['modified_spans'] / common_spans
                output.append(' modified spans: {} / {} ({:.4f}) '.format(src['modified_spans'], common_spans, ratio ))
                output.append('|')
                # Ratio: https://docs.python.org/3.6/library/difflib.html#difflib.SequenceMatcher.ratio
                ratio = (src['unchanged_annotations']*2.0) / total_spans
                output.append(' annotations ratio: {} / {} ({:.4f}) '.format(src['unchanged_annotations']*2, total_spans, ratio ))
                missing_percent = (src['missing_annotations']/total_spans)*100.0
                output.append('|')
                output.append(' only in {}: {} ({:.4f}%) '.format(self.first_model, src['missing_annotations'], missing_percent ))
                extra_percent = (src['extra_annotations']/total_spans)*100.0
                output.append('|')
                output.append(' only in {}: {} ({:.4f}%) '.format(self.second_model, src['extra_annotations'], extra_percent ))
                output.append('\n')
            output.append('\n')
        return ''.join(output)


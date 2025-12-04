# ===========================================================================
# ===========================================================================
#  Utilities for finding & grouping differences of span layers
#
#  The original source comes from:
#  https://github.com/soras/vk_ner_lrec_2022/blob/main/error_inspection/ner_diff_utils.py
#  https://github.com/estnltk/estnltk-workflows/blob/master/diff_named_entities
#  https://github.com/estnltk/estnltk-workflows/blob/master/diff_named_entities/ner_diff_utils.py
#
#  Requires estnltk v1.7.5+
# ===========================================================================
# ===========================================================================

import os, os.path, re
from collections import defaultdict

from estnltk_core import Span
from estnltk_core import Annotation
from estnltk_core.layer_operations import flatten

from estnltk.text import Text
from estnltk.taggers import DiffTagger

from estnltk.taggers.system.diff_tagger import iterate_diff_conflicts
from estnltk.taggers.system.diff_tagger import iterate_modified
from estnltk.taggers.system.diff_tagger import iterate_missing
from estnltk.taggers.system.diff_tagger import iterate_extra

# =======================================
#  Grouping and displaying differences   
# =======================================

def _can_be_added_to_group( span, group ):
    ''' A helper method for group_diff_spans(). 
        A new span can be added to the group iff:
        *) the group is empty, or
        *) the span is already in the group, or
        *) the span continues one span of the group;
    '''
    continues_group = False
    is_in_group = False
    (start, end) = span
    for (g_start, g_end) in group:
        if (g_start, g_end) == (start, end):
            is_in_group = True
        if g_end == start:
            continues_group = True
    return len(group)==0 or (len(group)>0 and (continues_group or is_in_group))


def group_continuous_differences( diff_layer, in_layer_a, in_layer_b ):
    '''Makes groups from continuous difference conflicts, and orders all 
       differences by their start & end locations.
       
       Returns grouped and ordered list of differences. Each difference is 
       a dictionary, which specifies: the location of difference, spans 
       from layer a in the location and spans from layer b on the location.
       
       Why? In order to visualize differences compactly, we need to first 
       aggregate them.
    '''
    grouped_conflicts = []
    #
    # 1) First, collect conflicting / partly overlapping spans
    #
    last_a_group_locs = []
    last_b_group_locs = []
    last_a_group_spans = []
    last_b_group_spans = []
    # Record conflicting spans 
    # (so that they can be subtracted from missing/extra spans)
    recorded_conflicts_a = defaultdict(int)
    recorded_conflicts_b = defaultdict(int)
    for (a, b) in iterate_diff_conflicts( diff_layer, 'span_status' ):
         a_loc = (a.start, a.end)
         b_loc = (b.start, b.end)
         a_name = a.input_layer_name[0]
         b_name = b.input_layer_name[0]
         assert a_name != '__loc', f'(!) Invalid layer name: {a_name}. Please use different name.'
         assert b_name != '__loc', f'(!) Invalid layer name: {b_name}. Please use different name.'
         # verify if one of the spans can be used to extend 
         # the last group
         a_ok = _can_be_added_to_group( a_loc, last_a_group_locs )
         b_ok = _can_be_added_to_group( b_loc, last_b_group_locs )
         if a_ok or b_ok:
            if a_loc not in last_a_group_locs:  # avoid duplicates
                last_a_group_locs.append(a_loc)
                last_a_group_spans.append( a )
            if b_loc not in last_b_group_locs:  # avoid duplicates
                last_b_group_locs.append(b_loc)
                last_b_group_spans.append( b )
         else:
            # Find whole conflicting span
            conflict_start = min(last_a_group_locs[0][0],last_b_group_locs[0][0])
            conflict_end   = max(last_a_group_locs[-1][-1],last_b_group_locs[-1][-1])
            conflict_loc   = (conflict_start, conflict_end)
            grouped_conflicts.append( {'__loc':conflict_loc,
                                      a_name:last_a_group_spans, 
                                      b_name:last_b_group_spans} )
            # Restart grouping
            last_a_group_locs = [ a_loc ]
            last_b_group_locs = [ b_loc ]
            last_a_group_spans = [ a ]
            last_b_group_spans = [ b ]
         # Record conflict location (to avoid double counting)
         recorded_conflicts_a[a_loc] = 1
         recorded_conflicts_b[b_loc] = 1
    assert len(last_a_group_locs) == 0 or (len(last_a_group_locs) > 0 and len(last_b_group_locs) > 0)
    assert len(last_b_group_locs) == 0 or (len(last_a_group_locs) > 0 and len(last_b_group_locs) > 0)
    if len(last_a_group_locs) > 0 and len(last_b_group_locs) > 0:
        # Find whole conflicting span
        conflict_start = min(last_a_group_locs[0][0],last_b_group_locs[0][0])
        conflict_end   = max(last_a_group_locs[-1][-1],last_b_group_locs[-1][-1])
        conflict_loc   = (conflict_start, conflict_end)
        grouped_conflicts.append( {'__loc':conflict_loc,
                                  a_name:last_a_group_spans, 
                                  b_name:last_b_group_spans} )
    #
    # 2) Second, collect modified spans (equal spans with differing annotations)
    #
    for mod_span in iterate_modified( diff_layer, 'span_status' ):
        assert len( mod_span.annotations ) > 1, f'(!) Unexpected number of annotations in a modified span {mod_span}'
        # 2.1) Create separate spans for different layers 
        sorted_annotations = sorted( mod_span.annotations, key=lambda x:x['input_layer_name'])
        diff_spans = [] 
        for ann in sorted_annotations:
            if not diff_spans:
                # First annotation: Create a new span
                diff_spans.append( Span( base_span=mod_span.base_span, layer=diff_layer ) )
                new_ann = Annotation(diff_spans[-1], **{ k:ann[k] for k in ann.legal_attribute_names } )
                diff_spans[-1].add_annotation( new_ann )
            else:
                # Next annotation: either add to existing span
                # or create a new one
                prev_input_layer = diff_spans[-1].annotations[0]['input_layer_name']
                cur_input_layer  = ann['input_layer_name']
                if prev_input_layer == cur_input_layer:
                    new_ann = Annotation(diff_spans[-1], **{ k:ann[k] for k in ann.legal_attribute_names } )
                    diff_spans[-1].add_annotation( new_ann )
                else:
                    # Create new span for new layer
                    diff_spans.append( Span( base_span=mod_span.base_span, layer=diff_layer ) )
                    new_ann = Annotation(diff_spans[-1], **{ k:ann[k] for k in ann.legal_attribute_names } )
                    diff_spans[-1].add_annotation( new_ann )
        assert len( diff_spans ) == 2, f'(!) Unexpected number of spans created in {diff_spans}'
        # Create new conflict item
        conflict_loc = (mod_span.start, mod_span.end)
        a_name = diff_spans[0].annotations[0]['input_layer_name']
        b_name = diff_spans[1].annotations[0]['input_layer_name']
        assert a_name != '__loc', f'(!) Invalid layer name: {a_name}. Please use different name.'
        assert b_name != '__loc', f'(!) Invalid layer name: {b_name}. Please use different name.'
        new_mod_conflict = { '__loc': conflict_loc, a_name : [diff_spans[0]], 
                                                    b_name : [diff_spans[1]] }
        grouped_conflicts.append( new_mod_conflict )
    #
    # 3) Collect missing spans (spans in a, but not in b)
    #
    for missing_span in iterate_missing( diff_layer, 'span_status' ):
        conflict_loc = (missing_span.start, missing_span.end)
        if conflict_loc in recorded_conflicts_a:
            # Skip the span that was already in seen conflicting spans
            continue
        new_mod_conflict = { '__loc': conflict_loc, 
                             in_layer_a : [missing_span], 
                             in_layer_b : [] }
        grouped_conflicts.append( new_mod_conflict )
    #
    # 4) Collect extra spans (spans in b, but not in a)
    #
    for extra_span in iterate_extra( diff_layer, 'span_status' ):
        conflict_loc = (extra_span.start, extra_span.end)
        if conflict_loc in recorded_conflicts_b:
            # Skip the span that was already seen in conflicting spans
            continue
        new_mod_conflict = { '__loc': conflict_loc, 
                             in_layer_a : [], 
                             in_layer_b : [extra_span] }
        grouped_conflicts.append( new_mod_conflict )
    #
    #  5)  Order conflicts by their locations
    #
    grouped_conflicts = sorted(grouped_conflicts, key=lambda x : x['__loc'] )
    return grouped_conflicts


def _text_snippet( text_obj, start, end ):
    '''Takes a snippet out of the text, assuring that text boundaries are not exceeded.'''
    start = 0 if start < 0 else start
    start = len(text_obj.text) if start > len(text_obj.text) else start
    end   = len(text_obj.text) if end > len(text_obj.text)   else end
    end   = 0 if end < 0 else end
    snippet = text_obj.text[start:end]
    snippet = snippet.replace('\n', '\\n')
    return snippet


def format_grouped_diffs_as_string( text_obj, text_cat, fname_stub, grouped_diffs, layer_a, layer_b, 
                                    gap_counter=0, label_attr='nertag', format='vertical' ):
    '''Formats grouped differences as human-readable text snippets.'''
    if format.lower() == 'v':
        format = 'vertical'
    elif format.lower() == 'h':
        format = 'horizontal'
    assert format in ['horizontal', 'vertical'], '(!) Unexpected format:'+str( format )
    if not len( grouped_diffs ) > 0:
        return '', gap_counter
    if format == 'vertical':
        N = 40
        output_lines = []
        max_len = max(len(layer_a), len(layer_b))
        for gid, group in enumerate( grouped_diffs ):
            conflict_loc = group['__loc']
            output_a = [(' {:'+str(max_len)+'}   ').format(layer_a)]
            output_b = [(' {:'+str(max_len)+'}   ').format(layer_b)]
            a_spans = [(a.start, a.end) for a in group[layer_a]]
            b_spans = [(b.start, b.end) for b in group[layer_b]]
            before_a = '...'+_text_snippet( text_obj, conflict_loc[0]-N, conflict_loc[0] )
            before_b = '...'+_text_snippet( text_obj, conflict_loc[0]-N, conflict_loc[0] )
            if a_spans:
                before_a = '...'+_text_snippet( text_obj, a_spans[0][0]-N, a_spans[0][0] )
            if b_spans:
                before_b = '...'+_text_snippet( text_obj, b_spans[0][0]-N, b_spans[0][0] )
            output_a.append(before_a)
            output_b.append(before_b)
            last_span = None
            for aid, (start,end) in enumerate(a_spans):
                annotation = group[layer_a][aid].annotations[0]
                if last_span:
                    if last_span[1] != start:
                        output_a.append( _text_snippet( text_obj,last_span[1],start ) )
                output_a.append( '{'+_text_snippet( text_obj,start,end )+'} /'+annotation[label_attr] )
                last_span = (start,end)
            last_span = None
            for bid, (start,end) in enumerate(b_spans):
                annotation = group[layer_b][bid].annotations[0]
                if last_span:
                    if last_span[1] != start:
                        output_b.append( _text_snippet( text_obj,last_span[1],start ) )
                output_b.append( '{'+_text_snippet( text_obj,start,end )+'} /'+annotation[label_attr] )
                last_span = (start,end)
            after_a = _text_snippet( text_obj, conflict_loc[0], conflict_loc[1]+N )+'...'
            after_b = _text_snippet( text_obj, conflict_loc[0], conflict_loc[1]+N )+'...'
            if a_spans:
                after_a = _text_snippet( text_obj, a_spans[-1][1], a_spans[-1][1]+N )+'...'
            if b_spans:
                after_b = _text_snippet( text_obj, b_spans[-1][1], b_spans[-1][1]+N )+'...'
            output_a.append(after_a)
            output_b.append(after_b)
            if gid == 0:
                output_lines.append('='*85)
            else:
                output_lines.append('='*10)
            output_lines.append('')
            if text_cat:
                output_lines.append('  '+text_cat+'::'+fname_stub+'::'+str(gap_counter))
            else:
                output_lines.append('  '+fname_stub+'::'+str(gap_counter))
            output_lines.append('')
            output_lines.append( ''.join(output_a) )
            output_lines.append( ''.join(output_b) )
            output_lines.append('')
            gap_counter += 1
        return ('\n'.join(output_lines))+'\n', gap_counter
    elif format == 'horizontal':
        N = 70
        output_lines = []
        max_len = max(len(layer_a), len(layer_b))
        a_name = (' {:'+str(max_len)+'}   ').format(layer_a)
        b_name = (' {:'+str(max_len)+'}   ').format(layer_b)
        blank  = (' {:'+str(max_len)+'}   ').format(' ')
        for gid, group in enumerate( grouped_diffs ):
            conflict_loc = group['__loc']
            a_spans = [(a.start, a.end) for a in group[layer_a]]
            b_spans = [(b.start, b.end) for b in group[layer_b]]
            output_lines.append('='*85)
            output_lines.append('')
            if text_cat:
                output_lines.append('  '+text_cat+'::'+fname_stub+'::'+str(gap_counter))
            else:
                output_lines.append('  '+fname_stub+'::'+str(gap_counter))
            output_lines.append('')
            # 1) Context before
            before_a = '...'+_text_snippet( text_obj, conflict_loc[0]-N, conflict_loc[0] )
            before_b = '...'+_text_snippet( text_obj, conflict_loc[0]-N, conflict_loc[0] )
            if a_spans:
                before_a = '...'+_text_snippet( text_obj, a_spans[0][0]-N, a_spans[0][0] )
            if b_spans:
                before_b = '...'+_text_snippet( text_obj, b_spans[0][0]-N, b_spans[0][0] )
            extra = '' if before_a == before_b else '*** '
            if a_spans:
                output_lines.append(blank+extra+before_a)
            else:
                output_lines.append(blank+extra+before_b)
            # 2) Output difference
            output_lines.append('-'*25)
            last_span = None
            for aid, (start,end) in enumerate(a_spans):
                annotation = group[layer_a][aid].annotations[0]
                if last_span:
                    if last_span[1] != start:
                        in_between_str = _text_snippet( text_obj,last_span[1],start )
                        if not re.match(r'^\s*$', in_between_str):
                            output_lines.append( blank+in_between_str )
                output_lines.append( a_name+_text_snippet( text_obj,start,end )+' // '+annotation[label_attr] )
                last_span = (start,end)
            if not a_spans:
                output_lines.append( a_name+'' )
            output_lines.append('-'*25)
            last_span = None
            for bid, (start,end) in enumerate(b_spans):
                annotation = group[layer_b][bid].annotations[0]
                if last_span:
                    if last_span[1] != start:
                        in_between_str = _text_snippet( text_obj,last_span[1],start )
                        if not re.match(r'^\s*$', in_between_str):
                            output_lines.append( blank+in_between_str )
                output_lines.append( b_name+_text_snippet( text_obj,start,end )+' // '+annotation[label_attr] )
                last_span = (start,end)
            if not b_spans:
                output_lines.append( b_name+'' )
            output_lines.append('-'*25)
            # 3) Context after
            after_a = _text_snippet( text_obj, conflict_loc[0], conflict_loc[1]+N )+'...'
            after_b = _text_snippet( text_obj, conflict_loc[0], conflict_loc[1]+N )+'...'
            if a_spans:
                after_a = _text_snippet( text_obj, a_spans[-1][1], a_spans[-1][1]+N )+'...'
            if b_spans:
                after_b = _text_snippet( text_obj, b_spans[-1][1], b_spans[-1][1]+N )+'...'
            extra = '' if after_a == after_b else ' ***'
            if a_spans:
                output_lines.append(blank+after_a+extra)
            else:
                output_lines.append(blank+after_b+extra)
            output_lines.append('')
            gap_counter += 1
        return ('\n'.join(output_lines))+'\n', gap_counter
    return None, gap_counter


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


# ==========================================
#  All together: finding groups of          
#   annotation differences                  
# ==========================================

class XDiffFinder:
    '''Finds differences between two span layers, and groups differences in 
       a way that consecutive differences form a single group.
       Note: in principle, this should also work on layers that have gaps 
       within its spans (such as the clauses layer); however, there is no 
       guarantee that found differences will be grouped correctly in such 
       cases. 
    '''
    
    def __init__( self, old_layer: str, 
                        new_layer: str,
                        old_layer_attr: str = 'nertag',
                        new_layer_attr: str = 'nertag',
                        output_format:  str = 'vertical',
                        flat_layer_suffix: str = '_flat', 
                        old_layer_gaps_strategy: str = None,
                        new_layer_gaps_strategy: str = None):
        """Initiates XDiffFinder. A specification of comparable layers
           must be provided.
        
           :param old_layer: str
               Name of the old span layer.
           :param new_layer: str
               Name of the new span layer.
           :param old_layer_attr: str
               Name of the attribute to be compared in the old layer. 
               Value of this attribute will also be shown as the tag 
               of the annotation in the output. 
               Defaults to 'nertag';
           :param new_layer_attr: str
               Name of the attribute to be compared in the new layer. 
               Value of this attribute will also be shown as the tag 
               of the annotation in the output. 
               Defaults to 'nertag';
           :param output_format: str
               Whether the differences are aligned in the output string vertically 
               or horizontally.
               Possible values: 'vertical' (default), 'horizontal'.
           :param flat_layer_suffix: str
               Flat layers will be created from comparable layers before the 
               comparison, and this is the suffix that will be added to both
               flat layers. Defaults to '_flat';
           :param old_layer_gaps_strategy: str
               Specifies strategy for handling gaps inside enveloping spans 
               of the old layer. By default, no strategy is applied. 
               If you set `gaps_strategy='cut_out'`, then gaps inside spans 
               (signalled by a non-whitspace string between two sub-spans) 
               will be cut out, splitting spans correspondingly. 
           :param new_layer_gaps_strategy: str
               Specifies strategy for handling gaps inside enveloping spans 
               of the new layer. By default, no strategy is applied. 
               If you set `gaps_strategy='cut_out'`, then gaps inside spans 
               (signalled by a non-whitspace string between two sub-spans) 
               will be cut out, splitting spans correspondingly. 
        """
        self._flat_layer_suffix = '_flat'
        self.old_layer   = old_layer
        self.new_layer   = new_layer
        self.old_layer_attr = old_layer_attr
        self.new_layer_attr = new_layer_attr
        self.old_layer_gaps_strategy = old_layer_gaps_strategy
        self.new_layer_gaps_strategy = new_layer_gaps_strategy
        self.common_label    = '__label'
        self.ner_diff_tagger = DiffTagger( layer_a = old_layer+self._flat_layer_suffix,
                                           layer_b = new_layer+self._flat_layer_suffix,
                                           output_layer='ner_diff_layer',
                                           output_attributes=('span_status', self.common_label),
                                           span_status_attribute='span_status' )
        self.output_format = output_format
        self.gap_counter = 0
        self.doc_counter = 0


    def find_difference( self, text, fname, text_cat='', start_new_doc=True ):
        """Finds differences between old layer and new layer in given text, 
           and returns as a tuple (diff_layer, formatted_diffs_str, total_diff_gaps).
        
           :param text: `Text` object
               `Text` object in which differences will be found. Must contain
               `old_layer` and `new_layer`.
           :param fname: str
               Name of the file or document corresponding to the `Text` object.
               The name appears in formatted output (formatted_diffs_str) as 
               a part of the identifier of each difference.
           :param text_cat: str
               Name of the genre or subcorpus where the `Text` object belongs to.
               The name appears in formatted output (formatted_diffs_str) as 
               a part of the identifier of each difference. Defaults to '';
           :param start_new_doc: 
               Whether this `Text` object starts a new document or continues
               an existing document.
               If `True` (default), then it starts a new document and document 
               count will be increased.
           
           :return tuple
               A tuple `(diff_layer, formatted_diffs_str, total_diff_gaps)`:
               * `diff_layer` -- `Layer` of differences created by DiffTagger.
                                 contains differences between old layer and new 
                                 layer.
               * `formatted_diffs_str` -- output string showing grouped differences 
                                          along with their contexts.
               * `total_diff_gaps` -- integer: total number of grouped differences.
        """
        # Check input layers
        assert self.old_layer in text.layers, f'(!) Input text is missing "{self.old_layer}" layer.'
        assert self.new_layer in text.layers, f'(!) Input text is missing "{self.new_layer}" layer.'
        assert self.old_layer_attr in text[self.old_layer].attributes, \
               f'(!) Attribute "{self.old_layer_attr}" is missing from layer "{self.old_layer}".'
        assert self.new_layer_attr in text[self.new_layer].attributes, \
               f'(!) Attribute "{self.new_layer_attr}" is missing from layer "{self.new_layer}".'
        assert self.common_label not in text[self.old_layer].attributes, \
               f'(!) Invalid attribute name "{self.common_label}" used in layer "{self.old_layer}".'
        assert self.common_label not in text[self.new_layer].attributes, \
               f'(!) Invalid attribute name "{self.common_label}" used in layer "{self.new_layer}".'
        # Flatten labels + unify attribute names
        flat_a_name = self.old_layer+self._flat_layer_suffix
        flat_b_name = self.new_layer+self._flat_layer_suffix
        old_layer_flat = flatten( text[self.old_layer], flat_a_name, 
                                  output_attributes=[self.common_label], 
                                  attribute_mapping=[(self.old_layer_attr, self.common_label)],
                                  gaps_strategy = self.old_layer_gaps_strategy )
        new_layer_flat = flatten( text[self.new_layer], flat_b_name, 
                                  output_attributes=[self.common_label],  
                                  attribute_mapping=[(self.new_layer_attr, self.common_label)], 
                                  gaps_strategy = self.new_layer_gaps_strategy )
        # Find raw differences
        diff_layer = self.ner_diff_tagger.make_layer( text, { flat_a_name:old_layer_flat,
                                                              flat_b_name:new_layer_flat } )
        # Group differences for better readability
        grouped_diffs = group_continuous_differences( diff_layer, flat_a_name, flat_b_name )
        formatted_str, new_gap_count = format_grouped_diffs_as_string( text, text_cat, fname, 
                                                                       grouped_diffs, 
                                                                       flat_a_name,
                                                                       flat_b_name,
                                                                       label_attr=self.common_label,
                                                                       gap_counter = self.gap_counter, 
                                                                       format = self.output_format )
        total_diff_gaps = new_gap_count - self.gap_counter
        self.gap_counter = new_gap_count
        if start_new_doc:
            self.doc_counter += 1
        return diff_layer, formatted_str, grouped_diffs, total_diff_gaps


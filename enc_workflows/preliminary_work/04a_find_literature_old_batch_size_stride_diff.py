#
#   Compares Bert named entities layers in 'literature_old' texts which 
#   have large texts chunked in two different ways: 
#   1) setting batch_size=1500; 
#   2) setting use_fast=True, aggregation_strategy=simple and stride=128; 
#
#   Assumes input document subfolders contain "doc_bs_1500.json" 
#   and "doc_stride_128.json" files with comparable span layers
#   ('estbertner_bs_1500' and 'estbertner_stride_128').
#
#   Saves all found differences into the file:
#   '_bs_1500_vs_stride_128__ner_diffs.txt';
# 
#   Requires estnltk v1.7.5+
#
import os, os.path, re, sys, warnings

from datetime import datetime

from tqdm import tqdm

from estnltk import Text
from estnltk.converters import json_to_text

sys.path.insert(0, '..')
from x_utils import collect_collection_subdirs
from x_diff_utils import XDiffFinder

# Remove accidentally double-annotated docs, e.g. 
# "doc_bs_1500_stride_128.json" 
remove_redundant_files = True

if __name__ == '__main__':
    input_dir = os.path.abspath("../literature_old")
    assert os.path.exists( input_dir ), f'(!) Missing input dir: {input_dir}'

    ner_differ = XDiffFinder(old_layer = f'estbertner_bs_1500', 
                             new_layer = f'estbertner_stride_128', 
                             old_layer_attr = 'nertag',
                             new_layer_attr = 'nertag')

    output_fname = '_bs_1500_vs_stride_128__ner_diffs.txt'
    with open(output_fname, 'w', encoding='utf-8') as out_f:
        pass

    # Iterate over all vert subdirs and all document subdirs within these subdirs
    total_start_time = datetime.now()
    docs_processed = 0
    doc_diffs_found = 0
    doc_diffs_calculated = 0
    doc_redundant_removed = 0
    vert_subdirs = collect_collection_subdirs(input_dir, only_first_level=True, full_paths=False)
    if len(vert_subdirs) == 0:
        warnings.warn(f'(!) No document subdirectories found from collection dir {input_dir!r}')
    for vert_subdir in vert_subdirs:
        full_subdir = os.path.join(input_dir, vert_subdir)
        print(f'Processing {vert_subdir} ...')
        # Fetch all the document subdirs
        document_subdirs = collect_collection_subdirs(full_subdir, only_first_level=False, full_paths=True)
        for doc_subdir in tqdm(document_subdirs, ascii=True):
            document_id = int( doc_subdir.split(os.path.sep)[-1] )
            # Collect document json files
            v1_fpath = None
            v2_fpath = None
            for fname in os.listdir(doc_subdir):
                if fname == 'doc_bs_1500.json':
                    v1_fpath = os.path.join(doc_subdir, fname)
                    assert os.path.exists(v1_fpath), f'{v1_fpath!r}'
                elif fname == 'doc_stride_128.json':
                    v2_fpath = os.path.join(doc_subdir, fname)
                    assert os.path.exists(v2_fpath)
                if remove_redundant_files:
                    if fname == 'doc_bs_1500_stride_128.json' or \
                       fname == 'doc_stride_128_bs_1500.json':
                        v3_fpath = os.path.join(doc_subdir, fname)
                        os.remove(v3_fpath)
                        assert not os.path.exists(v3_fpath)
                        doc_redundant_removed += 1
            if v1_fpath is not None and v2_fpath is not None:
                text_v1 = json_to_text( file = v1_fpath )
                text_v2 = json_to_text( file = v2_fpath )
                has_diffs = False
                assert 'estbertner_bs_1500' in text_v1.layers
                assert 'estbertner_stride_128' in text_v2.layers
                v1_layer = text_v1.pop_layer('estbertner_bs_1500')
                v2_layer = text_v2.pop_layer('estbertner_stride_128')
                assert v1_layer.name != v2_layer.name
                #v1_layer.name = f'{v1_layer.name}'
                #v2_layer.name = f'{v2_layer.name}'
                v1_layer.text_object = text_v1
                v2_layer.text_object = text_v1
                text_v1.add_layer(v1_layer)
                text_v1.add_layer(v2_layer)
                diff_finder = ner_differ
                assert diff_finder is not None
                assert output_fname is not None
                diff_layer, formatted_diffs_str, grouped_diffs, total_diff_gaps = \
                                diff_finder.find_difference( text_v1, os.path.join(doc_subdir, 'doc.json') )
                if len(formatted_diffs_str) > 0 and not formatted_diffs_str.isspace():
                    has_diffs = True
                    with open(output_fname, 'a', encoding='utf-8') as out_f:
                        out_f.write(formatted_diffs_str)
                        out_f.write('\n')
                if has_diffs:
                    doc_diffs_found += 1
                doc_diffs_calculated += 1
            else:
                warnings.warn(f'(!) Files "doc_normalized_entities.json" & "doc_unnormalized_entities.json" are missing from {doc_subdir}.')
            docs_processed += 1

    print()
    print(f'  Diffs calculated for docs:  {doc_diffs_calculated} / {docs_processed}')
    print(f'  Diffs found for docs:       {doc_diffs_found} / {docs_processed}')
    if doc_redundant_removed > 0:
        print(f'  Redundant docs removed:     {doc_redundant_removed} / {docs_processed}')
    print(f'  Total time elapsed:         {datetime.now()-total_start_time}')
    print()
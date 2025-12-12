#
#   Compares clauses, timexes and named entities layers in unnormalized 
#   and normalized ('w' changed to 'v', such as 'Jüripäew' -> 'Jüripäev') 
#   'literature_old' texts. 
#
#   Assumes input document subfolders contain "doc_unnormalized_entities.json" 
#   and "doc_normalized_entities.json" files with all comparable span layers
#   ('clauses', 'timexes', 'named_entities').
#
#   Saves all found differences into files:
#   * '_norm_vs_unnorm__ner_diffs.txt';
#   * '_norm_vs_unnorm__timexes_diffs.txt';
#   * '_norm_vs_unnorm__clauses_diffs.txt';
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

if __name__ == '__main__':
    input_dir = os.path.abspath("../literature_old")
    assert os.path.exists( input_dir ), f'(!) Missing input dir: {input_dir}'

    ner_differ = XDiffFinder(old_layer = f'named_entities_unnormalized', 
                             new_layer = f'named_entities_normalized', 
                             old_layer_attr = 'nertag',
                             new_layer_attr = 'nertag')
    clauses_differ = XDiffFinder(old_layer = f'clauses_unnormalized', 
                                 new_layer = f'clauses_normalized', 
                                 old_layer_attr = 'clause_type',
                                 new_layer_attr = 'clause_type')
    timexes_differ = XDiffFinder(old_layer = f'timexes_unnormalized', 
                                 new_layer = f'timexes_normalized', 
                                 old_layer_attr = 'type',
                                 new_layer_attr = 'type')

    with open('_norm_vs_unnorm__ner_diffs.txt', 'w', encoding='utf-8') as out_f:
        pass
    with open('_norm_vs_unnorm__timexes_diffs.txt', 'w', encoding='utf-8') as out_f:
        pass
    with open('_norm_vs_unnorm__clauses_diffs.txt', 'w', encoding='utf-8') as out_f:
        pass

    # Iterate over all vert subdirs and all document subdirs within these subdirs
    total_start_time = datetime.now()
    docs_processed = 0
    doc_diffs_found = 0
    doc_diffs_calculated = 0
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
            norm_fpath = None
            unnorm_fpath = None
            for fname in os.listdir(doc_subdir):
                if fname == 'doc_normalized_entities.json':
                    norm_fpath = os.path.join( doc_subdir, fname)
                    assert os.path.exists(norm_fpath), f'{norm_fpath!r}'
                elif fname == 'doc_unnormalized_entities.json':
                    unnorm_fpath = os.path.join( doc_subdir, fname)
                    assert os.path.exists(unnorm_fpath)
            if norm_fpath is not None and unnorm_fpath is not None:
                text_norm   = json_to_text( file = norm_fpath )
                text_unnorm = json_to_text( file = unnorm_fpath )
                has_diffs = False
                for layer in ['named_entities', 'clauses', 'timexes']:
                    assert layer in text_norm.layers
                    assert layer in text_unnorm.layers
                    normalized_layer   = text_norm.pop_layer(layer)
                    unnormalized_layer = text_unnorm.pop_layer(layer)
                    normalized_layer.name   = f'{normalized_layer.name}_normalized'
                    unnormalized_layer.name = f'{unnormalized_layer.name}_unnormalized'
                    normalized_layer.text_object   = text_norm
                    unnormalized_layer.text_object = text_norm
                    text_norm.add_layer(normalized_layer)
                    text_norm.add_layer(unnormalized_layer)
                    diff_finder = None
                    output_fname = None
                    if layer == 'named_entities':
                        diff_finder = ner_differ
                        output_fname = '_norm_vs_unnorm__ner_diffs.txt'
                    elif layer == 'clauses':
                        diff_finder = clauses_differ
                        output_fname = '_norm_vs_unnorm__clauses_diffs.txt'
                    elif layer == 'timexes':
                        diff_finder = timexes_differ
                        output_fname = '_norm_vs_unnorm__timexes_diffs.txt'
                    assert diff_finder is not None
                    assert output_fname is not None
                    diff_layer, formatted_diffs_str, grouped_diffs, total_diff_gaps = \
                                    diff_finder.find_difference( text_norm, os.path.join(doc_subdir, 'doc.json') )
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
    print(f'  Total time elapsed:         {datetime.now()-total_start_time}')
    print()
#
#   This module contains improvements of StanzaSyntaxTagger.
#
#   StanzaSyntaxTaggerWithChunking: An improved version of StanzaSyntaxTagger 
#   which can process extra long sentences: sentences exceeding the given length 
#   limit (1000 words by default) will be cut into 1000 word chunks and analysed 
#   chunk by chunk;
#   Based on: 
#   https://github.com/estnltk/estnltk/blob/devel_1.7/estnltk_neural/estnltk_neural/taggers/syntax/stanza_tagger/stanza_tagger.py 
#   ( commit 90bfc1482b6efd40093c068d519eb3481ecc3fe9 ) 
#
#   DualStanzaSyntaxTagger: An improved version of StanzaSyntaxTagger which uses 2 
#   instances of stanza models: GPU/CPU model for parsing texts with normal length 
#   sentences, and a CPU based-model for processing texts with very long sentences 
#   (sentence length more than 1000 words). 
#   Based on: 
#   https://github.com/estnltk/estnltk/blob/main/estnltk_neural/estnltk_neural/taggers/syntax/stanza_tagger/stanza_tagger.py 
#   https://github.com/estnltk/estnltk/blob/devel_1.7/estnltk_neural/estnltk_neural/taggers/syntax/stanza_tagger/stanza_tagger.py 
#   ( commit 90bfc1482b6efd40093c068d519eb3481ecc3fe9 )
#

import os
from collections import OrderedDict
from random import Random

from estnltk import Layer
from estnltk.taggers.standard.syntax.syntax_dependency_retagger import SyntaxDependencyRetagger
from estnltk.taggers.standard.syntax.ud_validation.deprel_agreement_retagger import DeprelAgreementRetagger
from estnltk.taggers.standard.syntax.ud_validation.ud_validation_retagger import UDValidationRetagger
from estnltk.taggers import Tagger
from estnltk.converters.serialisation_modules import syntax_v0
from estnltk.downloader import get_resource_paths

from estnltk_neural.taggers.syntax.stanza_tagger.common_utils import prepare_input_doc
from estnltk_neural.taggers.syntax.stanza_tagger.common_utils import feats_to_ordereddict


class StanzaSyntaxTaggerWithChunking(Tagger):
    """
    Tags dependency syntactic analysis with Stanza.
    
    This version of StanzaSyntaxTagger can process extra long sentences: sentences exceeding the given length 
    limit 'max_words_in_sentence' (1000 words by default) will be cut into 1000 word chunks and analysed 
    chunk by chunk. This avoids running into "CUDA out of memory" error when processing very long sentences 
    with GPU.
    
    Note that stanza also has parameter 'depparse_batch_size' which controls "the maximum number of words to 
    process as a minibatch for efficient processing." According to stanza's documentation: "This parameter 
    should be set larger than the number of words in the longest sentence in your input document, or you might 
    run into unexpected behaviors." ( https://stanfordnlp.github.io/stanza/depparse.html )
    While the default value of stanza's depparse_batch_size is 5000, such large batch size can also cause 
    "CUDA out of memory" errors if the device does not have enough memory. Therefore, StanzaSyntaxTaggerWithChunking 
    sets depparse_batch_size value to 1500 by default. 
    
    The tagger assumes that the segmentation to sentences and words is completed before. Morphological analysis
    can be used, too.

    The tagger assumes that morph analysis is completed with VabaMorf module and follows EstMorph tagset.
    For using extended analysis, basic morph analysis layer must first exist.

    The tagger creates a syntax layer that features Universal Dependencies dependency-tags in attribute 'deprel'.
    When using only sentences for prediction, features and UPOS-tags from UD-tagset are used and displayed.
    Otherwise UPOS is the same as VabaMorf's part of speech tag and feats is based on VabaMorf's forms.

    An optional input_type flag allows choosing layer type to use as the base of prediction. Default is 'morph_analysis',
    which expects 'morph_analysis' as input. Values 'morph_extended' and 'sentences' can also be chosen. When using
    one of the morphological layers, the name of the morphological layer to use must be declared in input_morph_layer
    parameter (default 'morph_analysis'). Possible configurations (with default layer names):
        1) input_type='sentences', input_morph_layer=None;
           uses only 'sentences' from estnltk, and the lingustic processing is done with stanza's models;
           (value of input_morph_layer is irrelevant)
           ( currently not implemented )
        
        2) input_type='morph_analysis', input_morph_layer='morph_analysis';
           uses a model trained on Vabamorf's layer ('morph_analysis'); input_morph_layer 
           must point to the name of the Vabamorf's layer;
           
        3) input_type='morph_extended', input_morph_layer='morph_extended';
           uses a model trained on the extended Vabamorf's layer ('morph_extended'); input_morph_layer 
           must point to the name of the layer;

    Names of layers to use can be changed using parameters sentences_layer, words_layer and input_morph_layer,
    if needed. To use GPU for parsing, parameter use_gpu must be set to True. 
    Parameter add_parents_and_children adds attributes that contain the parent and children of a word. 
    
    The input morph analysis layer can be ambiguous. In that case, StanzaSyntaxTagger picks randomly one 
    morph analysis for each ambiguous word, and predicts from "unambiguous" input. 
    Important: as a result, by default, the output will not be deterministic: for ambiguous words, you will 
    get different 'lemma', 'upostag', 'xpostag', 'feats' values on each run, and this also affects the results 
    of dependency parsing. 
    How to make the output deterministic: you can pass a seed value for the random generator via constructor 
    parameter random_pick_seed (int, default value: None). Note that the seed value is fixed once at creating 
    a new instance of StanzaSyntaxTagger, and you only get deterministic / repeatable results if you tag texts 
    in exactly the same order.
    Note: if you want to get the same deterministic output as in previous versions of the tagger, use 
    random_pick_seed=4.
    
    Tutorial:
    https://github.com/estnltk/estnltk/blob/main/tutorials/nlp_pipeline/C_syntax/03_syntactic_analysis_with_stanza.ipynb
    """

    conf_param = ['model_path', 'add_parent_and_children', 'syntax_dependency_retagger',
                  'input_type', 'dir', 'mark_syntax_error', 'mark_agreement_error', 'agreement_error_retagger',
                  'ud_validation_retagger', 'nlp', 'use_gpu', 'max_words_in_sentence', 'random_pick_seed', 
                  '_random']

    def __init__(self,
                 output_layer='stanza_syntax',
                 sentences_layer='sentences',
                 words_layer='words',
                 input_morph_layer='morph_analysis',
                 input_type='morph_analysis',  # or 'morph_extended', 'sentences'
                 random_pick_seed=None,
                 add_parent_and_children=False,
                 depparse_path=None,
                 resources_path=None,
                 mark_syntax_error=False,
                 mark_agreement_error=False,
                 use_gpu=False,
                 max_words_in_sentence=1000,
                 depparse_batch_size=1500,
                 ):
        # Make an internal import to avoid explicit stanza dependency
        import stanza

        self.add_parent_and_children = add_parent_and_children
        self.mark_syntax_error = mark_syntax_error
        self.mark_agreement_error = mark_agreement_error
        self.output_layer = output_layer
        self.output_attributes = ('id', 'lemma', 'upostag', 'xpostag', 'feats', 'head', 'deprel', 'deps', 'misc')
        self.input_type = input_type
        self.use_gpu = use_gpu
        self.random_pick_seed = random_pick_seed
        self._random = Random()
        if isinstance(self.random_pick_seed, int):
            self._random.seed(self.random_pick_seed)

        # We may run into "CUDA out of memory" error when processing very long sentences 
        # with GPU.
        # Set a reasonable default for max sentence length: if that gets exceeded, then a 
        # long sentences will be cut into smaller ones before processing
        self.max_words_in_sentence = max_words_in_sentence

        if not resources_path:
            # Try to get the resources path for stanzasyntaxtagger. Attempt to download resources, if missing
            self.dir = get_resource_paths("stanzasyntaxtagger", only_latest=True, download_missing=True)
        else:
            self.dir = resources_path
        # Check that resources path has been set
        if self.dir is None:
            raise Exception('Models of StanzaSyntaxTagger are missing. '+\
                            'Please use estnltk.download("stanzasyntaxtagger") to download the models.')

        self.syntax_dependency_retagger = None
        if add_parent_and_children:
            self.syntax_dependency_retagger = SyntaxDependencyRetagger(syntax_layer=output_layer)
            self.output_attributes += ('parent_span', 'children')

        self.ud_validation_retagger = None
        if mark_syntax_error:
            self.ud_validation_retagger = UDValidationRetagger(output_layer=output_layer)
            self.output_attributes += ('syntax_error', 'error_message')

        self.agreement_error_retagger = None
        if mark_agreement_error:
            if not add_parent_and_children:
                raise ValueError('`add_parent_and_children` must be True for marking agreement errors.')
            else:
                self.agreement_error_retagger = DeprelAgreementRetagger(output_layer=output_layer)
                self.output_attributes += ('agreement_deprel',)

        if self.input_type not in ['sentences', 'morph_analysis', 'morph_extended']:
            raise ValueError('Invalid input type {}'.format(input_type))

        # Check for illegal parameter combinations (mismatching input type and layer):
        if input_type=='morph_analysis' and input_morph_layer=='morph_extended':
            raise ValueError( ('Invalid parameter combination: input_type={!r} and input_morph_layer={!r}. '+\
                              'Mismatching input type and layer.').format(input_type, input_morph_layer))
        elif input_type=='morph_extended' and input_morph_layer=='morph_analysis':
            raise ValueError( ('Invalid parameter combination: input_type={!r} and input_morph_layer={!r}. '+\
                              'Mismatching input type and layer.').format(input_type, input_morph_layer))

        if depparse_path and not os.path.isfile(depparse_path):
            raise ValueError('Invalid path: {}'.format(depparse_path))
        elif depparse_path and os.path.isfile(depparse_path):
            self.model_path = depparse_path
        else:
            if input_type == 'morph_analysis':
                self.model_path = os.path.join(self.dir, 'et', 'depparse', 'morph_analysis.pt')
            if input_type == 'morph_extended':
                self.model_path = os.path.join(self.dir, 'et', 'depparse', 'morph_extended.pt')
            if input_type == 'sentences':
                self.model_path = os.path.join(self.dir, 'et', 'depparse', 'stanza_depparse.pt')

        if not os.path.isfile(self.model_path):
            raise FileNotFoundError('Necessary models missing, download from https://entu.keeleressursid.ee/public-document/entity-9791 '
                             'and extract folders `depparse` and `pretrain` to root directory defining '
                             'StanzaSyntaxTagger under the subdirectory `stanza_resources/et (or set )`')

        if input_type == 'sentences':
            if not os.path.isfile(os.path.join(self.dir, 'et', 'pretrain', 'edt.pt')):
                raise FileNotFoundError(
                    'Necessary pretrain model missing, download from https://entu.keeleressursid.ee/public-document/entity-9791 '
                    'and extract folder `pretrain` to root directory defining '
                    'StanzaSyntaxTagger under the subdirectory `stanza_resources/et`')

        if self.input_type == 'sentences':
            # Stanza default pipeline on EstNLTK's pretagged tokens/sentences
            self.input_layers = [sentences_layer, words_layer]
            self.nlp = stanza.Pipeline(lang='et', processors='tokenize,pos,lemma,depparse',
                                       dir=self.dir,
                                       tokenize_pretokenized=True,
                                       depparse_model_path=self.model_path,
                                       use_gpu=self.use_gpu,
                                       logging_level='WARN')  # Logging level chosen so it does not display
            # information about downloading model

        elif self.input_type in ['morph_analysis', 'morph_extended']:
            self.input_layers = [sentences_layer, input_morph_layer, words_layer]
            depparse_conf = {}
            depparse_conf['lang'] = 'et'
            depparse_conf['processors'] = 'depparse'
            depparse_conf['dir'] = self.dir
            depparse_conf['depparse_pretagged'] = True
            depparse_conf['depparse_model_path'] = self.model_path
            depparse_conf['use_gpu'] = self.use_gpu
            depparse_conf['logging_level'] = 'WARN'
            if isinstance(depparse_batch_size, int):
                depparse_conf['depparse_batch_size'] = depparse_batch_size
            self.nlp = stanza.Pipeline( **depparse_conf )

    def _make_layer_template(self):
        """Creates and returns a template of the layer."""
        layer = Layer(name=self.output_layer,
                      text_object=None,
                      attributes=self.output_attributes,
                      parent=self.input_layers[1],
                      ambiguous=False )
        if self.add_parent_and_children:
            layer.serialisation_module = syntax_v0.__version__
        return layer


    def _chunk_long_sentences(self, text_data, max_words_in_sentence):
        '''Chunks extra long sentences into smaller ones by allowing 
           at most max_words_in_sentence.
           Returns tuple (chunked_text_data, chunked_flags, sentence_ends).
        '''
        assert isinstance(max_words_in_sentence, int)
        chunked_text_data = []
        chunked_sentence_status = []
        sentence_ends = []
        for sentence in text_data:
            if len(sentence) > max_words_in_sentence:
                # Very loooong sentence
                i = 0
                temp_sent = []
                temp_wid = 1
                while i < len(sentence):
                    word = sentence[i]
                    word['id'] = temp_wid
                    if len(temp_sent) < max_words_in_sentence:
                        temp_sent.append( word )
                        temp_wid += 1
                    else:
                        # Flush "sentence buffer"
                        if temp_sent:
                            chunked_text_data.append( temp_sent )
                            chunked_sentence_status.append( True )
                            sentence_ends.append( False )
                        temp_sent = []
                        temp_wid = 1
                        # Add next word
                        word['id'] = temp_wid
                        temp_sent.append( word )
                        temp_wid += 1
                    i += 1
                # Flush "sentence buffer"
                if temp_sent:
                    chunked_text_data.append( temp_sent )
                    chunked_sentence_status.append( True )
                    sentence_ends.append( False )
                sentence_ends[-1] = True
            else:
                # Normal length sentence
                chunked_text_data.append( sentence )
                chunked_sentence_status.append( False )
                sentence_ends.append( True )
        assert len(chunked_text_data) == len(chunked_sentence_status)
        assert len(chunked_text_data) == len(sentence_ends)
        return chunked_text_data, chunked_sentence_status, sentence_ends


    def _merge_chunked_sentences(self, doc_dict, chunk_flags, end_flags):
        '''Merges chunked sentences back into full ones by fixing word 
           and head id-s.
        '''
        assert len(doc_dict) == len(chunk_flags), \
            f'(!) Mismatching lengths: doc_dict {len(doc_dict)!r} vs chunk_flags {len(chunk_flags)!r}'
        assert len(doc_dict) == len(end_flags), \
            f'(!) Mismatching lengths: doc_dict {len(doc_dict)!r} vs end_flags {len(end_flags)!r}'
        new_doc_dict = []
        j = 0
        while j < len(doc_dict):
            is_chunked = chunk_flags[j]
            if is_chunked:
                temp_doc_dicts = []
                # Collect all parts of the chunked sentence
                i = j
                while i < len(doc_dict):
                    if chunk_flags[i]:
                        temp_doc_dicts.append(doc_dict[i])
                    if end_flags[i]:
                        break
                    i += 1
                # Map word id-s from old to new global ones
                global_wid = 1
                wid_maps = []
                for temp_doc in temp_doc_dicts:
                    wid_maps.append({})
                    for analysis in temp_doc:
                        wid = analysis['id']
                        wid_maps[-1][wid] = global_wid
                        global_wid += 1
                # Rewrite chunked sentence into one, fix word ids
                new_sentence = []
                for temp_doc, w_map in zip(temp_doc_dicts, wid_maps):
                    for analysis in temp_doc:
                        analysis['id'] = w_map.get(analysis['id'])
                        analysis['head'] = w_map.get(analysis['head'], analysis['head'])
                        new_sentence.append(analysis)
                new_doc_dict.append( new_sentence )
                # Finally, skip merged sentences
                j = i
            else:
                # Not chunked, proceed as normal
                new_doc_dict.append( doc_dict[j] )
            j += 1
        return new_doc_dict


    def _make_layer(self, text, layers, status=None):
        # Make an internal import to avoid explicit stanza dependency
        from stanza.models.common.doc import Document

        chunk_flags = []
        ending_flags = []
        if self.input_type in ['morph_analysis', 'morph_extended']:
            # Input: EstNLTK's tokenization and morphological features
            sentences = self.input_layers[0]
            morph_analysis = self.input_layers[1]
            text_data = prepare_input_doc(layers, sentences, morph_analysis, 
                                          random_picker=self._random)
            if self.max_words_in_sentence is not None:
                # Chunk sentences are too long
                text_data, chunk_flags, ending_flags = \
                    self._chunk_long_sentences( \
                                    text_data, \
                                    max_words_in_sentence = self.max_words_in_sentence )
            document = Document(text_data)
        else:
            # Input: EstNLTK's tokenization only
            raise NotImplementedError('(!) long sentence chunking not implemented for input_type=sentences')

        parent_layer = layers[self.input_layers[1]]

        layer = self._make_layer_template()
        layer.text_object=text

        doc = self.nlp(document)

        doc_dict = doc.to_dict()
        if len(chunk_flags) > 0 and any(chunk_flags):
            # If any of the sentences were chunked into smaller ones, 
            # we need to merge these back into full ones by fixing word id-s
            doc_dict = \
                self._merge_chunked_sentences(doc_dict, chunk_flags, ending_flags)

        extracted_data = [analysis for sentence in doc_dict for analysis in sentence]

        assert len(extracted_data) == len(parent_layer), \
            f'(!) {parent_layer.name!r} length {len(parent_layer)} != output words {len(extracted_data)}'
        for line, span in zip(extracted_data, parent_layer):
            wid = line['id']
            lemma = line['lemma']
            upostag = line['upos']
            xpostag = line['xpos']
            feats = OrderedDict()  # Stays this way if word has no features.
            if 'feats' in line.keys():
                feats = feats_to_ordereddict(line['feats'])
            head = line['head']
            deprel = line['deprel']

            attributes = {'id': wid, 'lemma': lemma, 'upostag': upostag, 'xpostag': xpostag, 'feats': feats,
                          'head': head, 'deprel': deprel, 'deps': '_', 'misc': '_'}

            layer.add_annotation(span, **attributes)

        if self.add_parent_and_children:
            # Add 'parent_span' & 'children' to the syntax layer.
            self.syntax_dependency_retagger.change_layer(text, {self.output_layer: layer})

        if self.mark_syntax_error:
            # Add 'syntax_error' & 'error_message' to the layer.
            self.ud_validation_retagger.change_layer(text, {self.output_layer: layer})

        if self.mark_agreement_error:
            # Add 'agreement_deprel' to the layer.
            self.agreement_error_retagger.change_layer(text, {self.output_layer: layer})

        return layer



class DualStanzaSyntaxTagger(Tagger):
    """
    Tags dependency syntactic analysis with Stanza. 
    This version of StanzaSyntaxTagger uses 2 instances of stanza models: GPU model for parsing texts with 
    normal sentences, and a CPU based-model for processing texts with very long sentences (sentence length 
    more than 1000 words). The reason is that we may run into "CUDA out of memory" error when processing 
    very long sentences with GPU.
    
    The tagger assumes that the segmentation to sentences and words is completed before. Morphological analysis
    can be used, too.

    The tagger assumes that morph analysis is completed with VabaMorf module and follows EstMorph tagset.
    For using extended analysis, basic morph analysis layer must first exist.

    The tagger creates a syntax layer that features Universal Dependencies dependency-tags in attribute 'deprel'.
    When using only sentences for prediction, features and UPOS-tags from UD-tagset are used and displayed.
    Otherwise UPOS is the same as VabaMorf's part of speech tag and feats is based on VabaMorf's forms.

    An optional input_type flag allows choosing layer type to use as the base of prediction. Default is 'morph_analysis',
    which expects 'morph_analysis' as input. Values 'morph_extended' and 'sentences' can also be chosen. When using
    one of the morphological layers, the name of the morphological layer to use must be declared in input_morph_layer
    parameter (default 'morph_analysis'). Possible configurations (with default layer names):
        1) input_type='sentences', input_morph_layer=None;
           uses only 'sentences' from estnltk, and the lingustic processing is done with stanza's models;
           (value of input_morph_layer is irrelevant)
        
        2) input_type='morph_analysis', input_morph_layer='morph_analysis';
           uses a model trained on Vabamorf's layer ('morph_analysis'); input_morph_layer 
           must point to the name of the Vabamorf's layer;
           
        3) input_type='morph_extended', input_morph_layer='morph_extended';
           uses a model trained on the extended Vabamorf's layer ('morph_extended'); input_morph_layer 
           must point to the name of the layer;

    Names of layers to use can be changed using parameters sentences_layer, words_layer and input_morph_layer,
    if needed. To use GPU for parsing, parameter use_gpu must be set to True. 
    Parameter add_parents_and_children adds attributes that contain the parent and children of a word. 
    
    The input morph analysis layer can be ambiguous. In that case, StanzaSyntaxTagger picks randomly one 
    morph analysis for each ambiguous word, and predicts from "unambiguous" input. 
    Important: as a result, by default, the output will not be deterministic: for ambiguous words, you will 
    get different 'lemma', 'upostag', 'xpostag', 'feats' values on each run, and this also affects the results 
    of dependency parsing. 
    How to make the output deterministic: you can pass a seed value for the random generator via constructor 
    parameter random_pick_seed (int, default value: None). Note that the seed value is fixed once at creating 
    a new instance of StanzaSyntaxTagger, and you only get deterministic / repeatable results if you tag texts 
    in exactly the same order.
    Note: if you want to get the same deterministic output as in previous versions of the tagger, use 
    random_pick_seed=4.
    
    Tutorial:
    https://github.com/estnltk/estnltk/blob/main/tutorials/nlp_pipeline/C_syntax/03_syntactic_analysis_with_stanza.ipynb
    """

    conf_param = ['model_path', 'add_parent_and_children', 'syntax_dependency_retagger',
                  'input_type', 'dir', 'mark_syntax_error', 'mark_agreement_error', 'agreement_error_retagger',
                  'ud_validation_retagger', 'nlp', 'nlp_non_gpu', 'use_gpu', 'gpu_max_words_in_sentence', 'random_pick_seed', 
                  '_random']

    def __init__(self,
                 output_layer='stanza_syntax',
                 sentences_layer='sentences',
                 words_layer='words',
                 input_morph_layer='morph_analysis',
                 input_type='morph_analysis',  # or 'morph_extended', 'sentences'
                 random_pick_seed=None,
                 add_parent_and_children=False,
                 depparse_path=None,
                 resources_path=None,
                 mark_syntax_error=False,
                 mark_agreement_error=False,
                 use_gpu=False,
                 gpu_max_words_in_sentence=1000
                 ):
        # Make an internal import to avoid explicit stanza dependency
        import stanza

        self.add_parent_and_children = add_parent_and_children
        self.mark_syntax_error = mark_syntax_error
        self.mark_agreement_error = mark_agreement_error
        self.output_layer = output_layer
        self.output_attributes = ('id', 'lemma', 'upostag', 'xpostag', 'feats', 'head', 'deprel', 'deps', 'misc')
        self.input_type = input_type
        self.use_gpu = use_gpu
        self.random_pick_seed = random_pick_seed
        self._random = Random()
        if isinstance(self.random_pick_seed, int):
            self._random.seed(self.random_pick_seed)

        # We may run into "CUDA out of memory" error when processing very long sentences 
        # with GPU.
        # Set a reasonable default for max sentence length: if that gets exceeded, 
        # then use CPU instead of GPU for processing
        self.gpu_max_words_in_sentence = gpu_max_words_in_sentence

        if not resources_path:
            # Try to get the resources path for stanzasyntaxtagger. Attempt to download resources, if missing
            self.dir = get_resource_paths("stanzasyntaxtagger", only_latest=True, download_missing=True)
        else:
            self.dir = resources_path
        # Check that resources path has been set
        if self.dir is None:
            raise Exception('Models of StanzaSyntaxTagger are missing. '+\
                            'Please use estnltk.download("stanzasyntaxtagger") to download the models.')

        self.syntax_dependency_retagger = None
        if add_parent_and_children:
            self.syntax_dependency_retagger = SyntaxDependencyRetagger(syntax_layer=output_layer)
            self.output_attributes += ('parent_span', 'children')

        self.ud_validation_retagger = None
        if mark_syntax_error:
            self.ud_validation_retagger = UDValidationRetagger(output_layer=output_layer)
            self.output_attributes += ('syntax_error', 'error_message')

        self.agreement_error_retagger = None
        if mark_agreement_error:
            if not add_parent_and_children:
                raise ValueError('`add_parent_and_children` must be True for marking agreement errors.')
            else:
                self.agreement_error_retagger = DeprelAgreementRetagger(output_layer=output_layer)
                self.output_attributes += ('agreement_deprel',)

        if self.input_type not in ['sentences', 'morph_analysis', 'morph_extended']:
            raise ValueError('Invalid input type {}'.format(input_type))

        # Check for illegal parameter combinations (mismatching input type and layer):
        if input_type=='morph_analysis' and input_morph_layer=='morph_extended':
            raise ValueError( ('Invalid parameter combination: input_type={!r} and input_morph_layer={!r}. '+\
                              'Mismatching input type and layer.').format(input_type, input_morph_layer))
        elif input_type=='morph_extended' and input_morph_layer=='morph_analysis':
            raise ValueError( ('Invalid parameter combination: input_type={!r} and input_morph_layer={!r}. '+\
                              'Mismatching input type and layer.').format(input_type, input_morph_layer))

        if depparse_path and not os.path.isfile(depparse_path):
            raise ValueError('Invalid path: {}'.format(depparse_path))
        elif depparse_path and os.path.isfile(depparse_path):
            self.model_path = depparse_path
        else:
            if input_type == 'morph_analysis':
                self.model_path = os.path.join(self.dir, 'et', 'depparse', 'morph_analysis.pt')
            if input_type == 'morph_extended':
                self.model_path = os.path.join(self.dir, 'et', 'depparse', 'morph_extended.pt')
            if input_type == 'sentences':
                self.model_path = os.path.join(self.dir, 'et', 'depparse', 'stanza_depparse.pt')

        if not os.path.isfile(self.model_path):
            raise FileNotFoundError('Necessary models missing, download from https://entu.keeleressursid.ee/public-document/entity-9791 '
                             'and extract folders `depparse` and `pretrain` to root directory defining '
                             'StanzaSyntaxTagger under the subdirectory `stanza_resources/et (or set )`')

        if input_type == 'sentences':
            if not os.path.isfile(os.path.join(self.dir, 'et', 'pretrain', 'edt.pt')):
                raise FileNotFoundError(
                    'Necessary pretrain model missing, download from https://entu.keeleressursid.ee/public-document/entity-9791 '
                    'and extract folder `pretrain` to root directory defining '
                    'StanzaSyntaxTagger under the subdirectory `stanza_resources/et`')

        if self.input_type == 'sentences':
            # Stanza default pipeline on EstNLTK's pretagged tokens/sentences
            self.input_layers = [sentences_layer, words_layer]
            self.nlp = stanza.Pipeline(lang='et', processors='tokenize,pos,lemma,depparse',
                                       dir=self.dir,
                                       tokenize_pretokenized=True,
                                       depparse_model_path=self.model_path,
                                       use_gpu=self.use_gpu,
                                       logging_level='WARN')  # Logging level chosen so it does not display
            # information about downloading model
            self.nlp_non_gpu = None
            if self.use_gpu:
                # Create non-gpu tagger (for tagging documents that are extremely large)
                self.nlp_non_gpu = stanza.Pipeline(lang='et', processors='tokenize,pos,lemma,depparse',
                                                   dir=self.dir,
                                                   tokenize_pretokenized=True,
                                                   depparse_model_path=self.model_path,
                                                   use_gpu=False,
                                                   logging_level='WARN')  # Logging level chosen so it does not display
        elif self.input_type in ['morph_analysis', 'morph_extended']:
            self.input_layers = [sentences_layer, input_morph_layer, words_layer]
            self.nlp = stanza.Pipeline(lang='et', processors='depparse',
                                       dir=self.dir,
                                       depparse_pretagged=True,
                                       depparse_model_path=self.model_path,
                                       use_gpu=self.use_gpu,
                                       logging_level='WARN')
            self.nlp_non_gpu = None
            if self.use_gpu:
                # Create non-gpu parser (for parsing documents that are extremely large)
                self.nlp_non_gpu = stanza.Pipeline(lang='et', processors='depparse',
                                            dir=self.dir,
                                            depparse_pretagged=True,
                                            depparse_model_path=self.model_path,
                                            use_gpu=False,
                                            logging_level='WARN')

    def _make_layer_template(self):
        """Creates and returns a template of the layer."""
        layer = Layer(name=self.output_layer,
                      text_object=None,
                      attributes=self.output_attributes,
                      parent=self.input_layers[1],
                      ambiguous=False )
        if self.add_parent_and_children:
            layer.serialisation_module = syntax_v0.__version__
        return layer

    def _make_layer(self, text, layers, status=None):
        # Make an internal import to avoid explicit stanza dependency
        from stanza.models.common.doc import Document

        exceeds_gpu_limit = False
        if self.input_type in ['morph_analysis', 'morph_extended']:
            # Input: EstNLTK's tokenization and morphological features
            sentences = self.input_layers[0]
            morph_analysis = self.input_layers[1]
            text_data = prepare_input_doc(layers, sentences, morph_analysis, 
                                          random_picker=self._random)
            if self.use_gpu and self.gpu_max_words_in_sentence is not None:
                # Using GPU: Check that sentences are not too long (for CUDA memory)
                for sentence in text_data:
                    if len(sentence) > self.gpu_max_words_in_sentence:
                        exceeds_gpu_limit = True
            document = Document(text_data)
        else:
            # Input: EstNLTK's tokenization only
            sentences = self.input_layers[0]
            document = prepare_input_doc(layers, sentences, None, only_tokenization=True)

        parent_layer = layers[self.input_layers[1]]

        layer = self._make_layer_template()
        layer.text_object=text

        if not self.use_gpu or not exceeds_gpu_limit:
            doc = self.nlp(document)
        else:
            doc = self.nlp_non_gpu(document)

        extracted_data = [analysis for sentence in doc.to_dict() for analysis in sentence]

        for line, span in zip(extracted_data, parent_layer):
            id = line['id']
            lemma = line['lemma']
            upostag = line['upos']
            xpostag = line['xpos']
            feats = OrderedDict()  # Stays this way if word has no features.
            if 'feats' in line.keys():
                feats = feats_to_ordereddict(line['feats'])
            head = line['head']
            deprel = line['deprel']

            attributes = {'id': id, 'lemma': lemma, 'upostag': upostag, 'xpostag': xpostag, 'feats': feats,
                          'head': head, 'deprel': deprel, 'deps': '_', 'misc': '_'}

            layer.add_annotation(span, **attributes)

        if self.add_parent_and_children:
            # Add 'parent_span' & 'children' to the syntax layer.
            self.syntax_dependency_retagger.change_layer(text, {self.output_layer: layer})

        if self.mark_syntax_error:
            # Add 'syntax_error' & 'error_message' to the layer.
            self.ud_validation_retagger.change_layer(text, {self.output_layer: layer})

        if self.mark_agreement_error:
            # Add 'agreement_deprel' to the layer.
            self.agreement_error_retagger.change_layer(text, {self.output_layer: layer})

        return layer


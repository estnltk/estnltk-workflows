#
#   Composite stanza syntax taggers  which  combine  preprocessing 
#  (creation of 'morph_extended'  layer) with  syntactic  parsing 
#  (creation of 'stanza_syntax' / 'stanza_ensemble_syntax' layer).
#   The 'morph_extended' layer is a  throw-away  layer: it will be 
#   removed after the processing. 
#

import os

from estnltk.taggers import Tagger
from estnltk.taggers import MorphExtendedTagger
from estnltk_neural.taggers import StanzaSyntaxTagger
from estnltk_neural.taggers import StanzaSyntaxEnsembleTagger

class StanzaSyntaxCompositeTagger( Tagger ):
    """A syntax tagger that combines processing of MorphExtendedTagger and StanzaSyntaxTagger. 
       The output layer will be that of StanzaSyntaxTagger and the output layer's parent 
       will be rebased to morph_analysis layer. 
       Only supports StanzaSyntaxTagger's models with input_type='morph_extended'. 
    """
    conf_param = ['morph_extended_tagger', 'stanza_syntax_tagger']

    def __init__(self,
                 output_layer='stanza_syntax',
                 sentences_layer='sentences',
                 words_layer='words',
                 input_morph_layer='morph_analysis',
                 add_parent_and_children=False,
                 depparse_path=None,
                 resources_path=None,
                 mark_syntax_error=False,
                 mark_agreement_error=False,
                 use_gpu=False ):
        self.morph_extended_tagger = MorphExtendedTagger( input_morph_analysis_layer=input_morph_layer, \
                                                          output_layer='_temp_morph_extended' )
        self.stanza_syntax_tagger = StanzaSyntaxTagger( output_layer=output_layer,
                                                        input_type='morph_extended', 
                                                        input_morph_layer=self.morph_extended_tagger.output_layer, 
                                                        sentences_layer=sentences_layer,
                                                        words_layer=words_layer,
                                                        depparse_path=depparse_path,
                                                        resources_path=resources_path,
                                                        mark_syntax_error=mark_syntax_error,
                                                        mark_agreement_error=mark_agreement_error,
                                                        use_gpu=use_gpu,
                                                        add_parent_and_children=add_parent_and_children )
        self.output_layer = self.stanza_syntax_tagger.output_layer
        self.output_attributes = self.stanza_syntax_tagger.output_attributes
        self.input_layers = [ words_layer, sentences_layer, input_morph_layer ]

    def _make_layer_template(self):
        morph_extended_layer = self.morph_extended_tagger._make_layer_template()
        stanza_syntax_layer = self.stanza_syntax_tagger._make_layer_template()
        # Rebase syntax layer
        stanza_syntax_layer.parent = morph_extended_layer.parent
        return stanza_syntax_layer

    def _make_layer(self, text, layers, status=None):
        # Create layers
        morph_extended_layer = self.morph_extended_tagger.make_layer(text, layers)
        # Temporarily add morph_extended_layer to Text 
        # (... otherwise stanza_syntax_tagger won't work ...)
        text.add_layer( morph_extended_layer )
        layers[ morph_extended_layer.name ] = morph_extended_layer
        stanza_syntax_layer = self.stanza_syntax_tagger.make_layer(text, layers)
        # Rebase syntax layer
        stanza_syntax_layer.parent = morph_extended_layer.parent
        text.pop_layer( morph_extended_layer.name )
        assert morph_extended_layer.name not in text.layers
        return stanza_syntax_layer



class StanzaEnsembleSyntaxCompositeTagger( Tagger ):
    """A syntax tagger that combines processing of MorphExtendedTagger and StanzaSyntaxEnsembleTagger. 
       The output layer will be that of StanzaSyntaxEnsembleTagger and the output layer's parent 
       will be rebased to morph_analysis layer. 
    """
    conf_param = ['morph_extended_tagger', 'ensemble_syntax_tagger']

    def __init__(self,
                 output_layer='stanza_ensemble_syntax',
                 sentences_layer='sentences',
                 words_layer='words',
                 input_morph_layer='morph_analysis',
                 add_parent_and_children=False,
                 model_paths=None,
                 mark_syntax_error=False,
                 mark_agreement_error=False,
                 use_gpu=False ):
        self.morph_extended_tagger  = MorphExtendedTagger( input_morph_analysis_layer=input_morph_layer, \
                                                           output_layer='_temp_morph_extended' )
        self.ensemble_syntax_tagger = StanzaSyntaxEnsembleTagger( output_layer=output_layer,
                                                                  input_morph_layer=self.morph_extended_tagger.output_layer, 
                                                                  sentences_layer=sentences_layer,
                                                                  words_layer=words_layer,
                                                                  model_paths=model_paths,
                                                                  mark_syntax_error=mark_syntax_error,
                                                                  mark_agreement_error=mark_agreement_error,
                                                                  use_gpu=use_gpu,
                                                                  add_parent_and_children=add_parent_and_children )
        self.output_layer = self.ensemble_syntax_tagger.output_layer
        self.output_attributes = self.ensemble_syntax_tagger.output_attributes
        self.input_layers = [ words_layer, sentences_layer, input_morph_layer ]

    def _make_layer_template(self):
        morph_extended_layer = self.morph_extended_tagger._make_layer_template()
        stanza_syntax_layer = self.ensemble_syntax_tagger._make_layer_template()
        # Rebase syntax layer
        stanza_syntax_layer.parent = morph_extended_layer.parent
        return stanza_syntax_layer

    def _make_layer(self, text, layers, status=None):
        # Create layers
        morph_extended_layer = self.morph_extended_tagger.make_layer(text, layers)
        # Temporarily add morph_extended_layer to Text 
        # (... otherwise ensemble_syntax_tagger won't work ...)
        text.add_layer( morph_extended_layer )
        layers[ morph_extended_layer.name ] = morph_extended_layer
        ensemble_syntax_layer = self.ensemble_syntax_tagger.make_layer(text, layers)
        # Rebase syntax layer
        ensemble_syntax_layer.parent = morph_extended_layer.parent
        text.pop_layer( morph_extended_layer.name )
        assert morph_extended_layer.name not in text.layers
        return ensemble_syntax_layer



class StanzaPOSbasedSyntaxCompositeTagger( Tagger ):
    """A syntax tagger that combines processing of MorphExtendedTagger and StanzaSyntaxEnsembleTagger with POS-based syntax models. 
       The output layer will be that of StanzaSyntaxEnsembleTagger and the output layer's parent 
       will be rebased to morph_analysis layer. 
    """
    conf_param = ['morph_extended_tagger', 'ensemble_syntax_tagger']

    def __init__(self,
                 output_layer='stanza_pos_based_syntax',
                 sentences_layer='sentences',
                 words_layer='words',
                 input_morph_layer='morph_analysis',
                 add_parent_and_children=False,
                 model_paths=None,
                 mark_syntax_error=False,
                 mark_agreement_error=False,
                 use_gpu=False ):
        self.morph_extended_tagger  = MorphExtendedTagger( input_morph_analysis_layer=input_morph_layer, \
                                                           output_layer='_temp_morph_extended' )
        self.ensemble_syntax_tagger = StanzaSyntaxEnsembleTagger( output_layer=output_layer,
                                                                  input_morph_layer=self.morph_extended_tagger.output_layer, 
                                                                  sentences_layer=sentences_layer,
                                                                  remove_fields = ['lemma', 'upos', 'xpos'],
                                                                  replace_fields = (['text'], '---'),
                                                                  words_layer=words_layer,
                                                                  model_paths=model_paths,
                                                                  mark_syntax_error=mark_syntax_error,
                                                                  mark_agreement_error=mark_agreement_error,
                                                                  use_gpu=use_gpu,
                                                                  add_parent_and_children=add_parent_and_children )
        self.output_layer = self.ensemble_syntax_tagger.output_layer
        self.output_attributes = self.ensemble_syntax_tagger.output_attributes
        self.input_layers = [ words_layer, sentences_layer, input_morph_layer ]

    def _make_layer_template(self):
        morph_extended_layer = self.morph_extended_tagger._make_layer_template()
        stanza_syntax_layer = self.ensemble_syntax_tagger._make_layer_template()
        # Rebase syntax layer
        stanza_syntax_layer.parent = morph_extended_layer.parent
        return stanza_syntax_layer

    def _make_layer(self, text, layers, status=None):
        # Create layers
        morph_extended_layer = self.morph_extended_tagger.make_layer(text, layers)
        # Temporarily add morph_extended_layer to Text 
        # (... otherwise ensemble_syntax_tagger won't work ...)
        text.add_layer( morph_extended_layer )
        layers[ morph_extended_layer.name ] = morph_extended_layer
        ensemble_syntax_layer = self.ensemble_syntax_tagger.make_layer(text, layers)
        # Rebase syntax layer
        ensemble_syntax_layer.parent = morph_extended_layer.parent
        text.pop_layer( morph_extended_layer.name )
        assert morph_extended_layer.name not in text.layers
        return ensemble_syntax_layer


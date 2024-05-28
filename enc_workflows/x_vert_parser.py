#
#  This module contains:
#
#   * SimpleVertFileParser -- a simplified version of VertXMLFileParser for 
#     document-wise parsing of .vert files;
#
#   * SyntaxVertFileWriter -- a class for augmenting documents read by  
#     SimpleVertFileParser with syntactic annotations and writing into 
#     new .vert files;
#
#

from typing import List, Union, Any

import re, sys
import os, os.path
import warnings

from estnltk import Text
from estnltk.corpus_processing.parse_enc import parse_tag_attributes


def collect_sentence_tokens(document_lines: List[Any], sentence_start:int):
    '''Collects all tokens belonging to the sentence starting at the `sentence_start` 
       in `document_lines`. Returns list of tokens (strings).
       
       `document_lines` should be a list of document content lines yielded by 
       `SimpleVertFileParser`.
    '''
    sentence_tokens = []
    if document_lines[sentence_start][0] != '<s>': # sanity check
        raise IndexError(f'(!) document_line at {sentence_start} is not '+\
                         f'sentence start <s>, but {document_lines[sentence_start]!r}.')
    i = sentence_start
    while i < len(document_lines):
        line = document_lines[i]
        line_type  = line[0]
        line_token = line[1]
        if line_type == 'TOKEN':
            sentence_tokens.append(line_token)
        elif line_type == '</s>':
            # end this sentence
            break
        i += 1
    return sentence_tokens



class SimpleVertFileParser:
    """ A simple parser for document by document parsing of vert XML files. 
        
        Use `vert_parser = SimpleVertFileParser(vert_file)` to create a new 
        document parser and call `next(vert_parser)` to retrieve the content and 
        metadata of the next document until all documents have been yielded. 
        If `vert_parser.parsing_finished == False`, then there are still documents 
        left to be parsed. 
        
        The `vert_parser` is a generator, which yields tuples 
        `(document_content_lines:list, document_metadata:dict)`. 
        
        This class is based on VertXMLFileParser from:
        https://github.com/estnltk/estnltk/blob/main/estnltk/estnltk/corpus_processing/parse_enc.py
        https://github.com/estnltk/estnltk/blob/devel_1.7/estnltk/estnltk/corpus_processing/parse_enc.py
    """

    def __init__(self, vert_file:str):
        '''Initializes the parser for parsing the given `vert_file`.'''
        assert os.path.isfile(vert_file), \
            f'(!) Invalid or missing input vert file: {vert_file}'
        self.vert_file           = vert_file
        # Initialize the state of parsing
        self.parsing_finished    = False
        self.lines               = 0  # internal line counter used by _parse_next_line(...)
        self.total_lines         = 0  # external line counter used by _document_collector(...)
        self.document_metadata   = {} # metadata of the document
        self.document_lines      = [] # lines of the document in the format: ( line_type, line )
        self.document_id         = -1 # index of the document in the vert file, starting from 0
        self.document_start_line = -1 # line on which the document started, starting from 1
        self.document_end_line   = -1 # line on which the document ended, starting from 1
        self.last_was_glue  = False
        self.doc_end_passed = False
        # Patterns for detecting tags
        self.enc_doc_tag_start  = re.compile(r"^<doc[^<>]+>\s*$")
        self.enc_doc_tag_end    = re.compile(r"^</doc>\s*$")
        self.enc_p_tag_start    = re.compile(r"^<p( [^<>]+)?>\s*$")
        self.enc_p_tag_end      = re.compile(r"^</p>\s*$")
        self.enc_s_tag_start    = re.compile(r"^<s( [^<>]+)?>\s*$")
        self.enc_s_tag_end      = re.compile(r"^</s>\s*$")
        self.enc_glue_tag       = re.compile("^<g/>$")
        self.enc_unknown_tag    = re.compile("^<([^<>]+)>$")
        # Initialize a new generator
        self._document_collector_inst = self._document_collector()


    def _document_collector( self ):
        '''Creates a generator function for collecting document content 
           lines and metadata from the vert file. 
           Upon iteration or calling next( vert_parser ), yields a 
           tuple ( document_content_lines:list, document_metadata:dict ) 
           until each document of the vert file has been yielded. 
        '''
        with open( self.vert_file, mode='r', encoding='utf-8' ) as in_f:
            for line in in_f:
                self.total_lines += 1
                result = self._parse_next_line( line )
                if result is not None:
                    # Return next document
                    yield result
        self.parsing_finished = True
        # Yield the last document
        yield self._finish_parsing()


    def __next__(self):
        '''Yields a tuple (document_content_lines, document_metadata) 
           until each document of the input vert file has been yielded. 
        '''
        return next(self._document_collector_inst)


    def _parse_next_line( self, line: str ):
        '''Parses a next line from the XML content of ENC vert file.
           
           Collects document content and metadata on the process. 
           If a start of a new document is reached with the line, 
           and a preceding document has been collected, then returns 
           the content and metadata of the preceding document.
           Normally, if the line only continues the document content, 
           then returns None.
           
           In order to complete the last document, please call method 
           _finish_parsing() after parsing the last line of the vert 
           file.
        '''
        stripped_line = line.strip()
        lt_escaped = False
        gt_escaped = False
        if stripped_line.startswith('<doc'):
            # Problem:  sometimes <doc>-tag contains more than one 
            #           < or >, and thus escapes from detection
            # Solution: replace < and > with &lt; and &gt; inside 
            #           the tag
            if stripped_line.count('<') > 1:
                stripped_line = '<'+(stripped_line[1:]).replace('<', '&lt;')
                lt_escaped = True
            if stripped_line.count('>') > 1:
                stripped_line = (stripped_line[:-1]).replace('>', '&gt;')+'>'
                gt_escaped = True
        m_doc_start = self.enc_doc_tag_start.match(stripped_line)
        m_doc_end   = self.enc_doc_tag_end.match(stripped_line)
        # *** Start of a new document
        if m_doc_start and stripped_line.startswith('<doc '):
            # Copy the old document
            old_document_metadata = None
            old_document_lines    = None
            old_document_line_count = 0
            if len(self.document_lines) > 0:
                # Add vert file indexing information
                self.document_metadata['_doc_id'] = self.document_id
                assert self.document_start_line > -1
                self.document_metadata['_doc_start_line'] = self.document_start_line
                # Note: ending tag </doc> can also be missing, like in 
                # nc23_Literature_Contemporary.vert <doc id="739539" ...
                # Then we need to update self.document_end_line on the fly.
                if not (self.document_end_line > -1):
                    self.document_end_line = (self.lines + 1)
                assert self.document_end_line > -1, \
                    f'{self.document_lines[:10]}...{self.document_lines[-10:]} {self.lines} {self.document_id}'
                self.document_metadata['_doc_end_line']   = self.document_end_line
                # Copy the old document
                old_document_metadata = self.document_metadata.copy()
                old_document_lines = self.document_lines.copy()
                old_document_line_count = len(old_document_lines)
            # Replace back &lt; and &gt;
            if lt_escaped:
                stripped_line = stripped_line.replace('&lt;', '<')
            if gt_escaped:
                stripped_line = stripped_line.replace('&gt;', '>')
            # Clear doc content for storing new document
            self.document_metadata.clear()
            self.document_lines.clear()
            if old_document_lines is not None:
                assert len(old_document_metadata.keys()) > 0
                assert old_document_line_count == len(old_document_lines)
            # Advance doc index, reset start & end
            self.document_id += 1
            assert self.document_id > -1
            self.document_start_line = (self.lines + 1)
            self.document_end_line   = -1
            # Carry over attributes
            attribs = parse_tag_attributes( stripped_line, logger=None )
            for key, value in attribs.items():
                self.document_metadata[key] = value
            if 'id' not in self.document_metadata:
                if 'doaj_id' in self.document_metadata:
                    # Use "doaj_id" instead of "id" (Fix for 'nc21_DOAJ.vert')
                    self.self.document_metadata['id'] = self.document_metadata['doaj_id']
                elif 'ISBN' in self.document_metadata:
                    # Use "ISBN" instead of "id" (Fix for 'nc21_Fiction.vert')
                    self.document_metadata['id'] = self.document_metadata['ISBN']
                elif 'url' in self.document_metadata and \
                     'src' in self.document_metadata and \
                     self.document_metadata['src'] in ['Feeds 2014–2021', 'Timestamped 2014–2023']:
                    # Use "url"+"line number" instead of "id" (Fix for 'nc21_Feeds.vert')
                    # ("url" itself is not enough, as there can be duplicate urls)
                    self.document_metadata['id'] = \
                        self.document_metadata['url']+('(doc@line:{})'.format(self.lines))
                else:
                    # No 'id' provided: fall back to using line number as id
                    self.document_metadata['id'] = '(doc@line:{})'.format(self.lines)
            if 'src' not in self.document_metadata and 'id' in self.document_metadata:
                print('WARNING: Document with id={} misses src attribute'.format(self.document_metadata['id']))
            self.document_lines.append( ('<doc>', line.strip()) )
            self.doc_end_passed = False
            if old_document_lines is not None:
                return old_document_lines, old_document_metadata
            else:
                return None
        # *** End of a document
        if m_doc_end:
            # Remember that we've passed document ending
            self.doc_end_passed = True
            self.document_lines.append( ('</doc>', line.strip()) )
            self.document_end_line = (self.lines + 1)
            return None
        # Sanity check : is there an unexpected continuation after document ending?
        if self.doc_end_passed and self.lines + 1 > self.document_end_line:
            if not m_doc_end and not m_doc_start:
                # Note: this problem is frequent to 'etnc19_doaj.vert'
                #print( ('WARNING: Unexpected content line {}:{!r} after document '+\
                #        'ending tag. Content outside documents will be skipped.').format(self.lines, stripped_line) )
                pass
            # shift the document ending forward
            self.document_end_line = (self.lines + 1)
        # Next patterns to be checked
        m_par_start   = self.enc_p_tag_start.match(stripped_line)
        m_par_end     = self.enc_p_tag_end.match(stripped_line)
        m_s_start     = self.enc_s_tag_start.match(stripped_line)
        m_s_end       = self.enc_s_tag_end.match(stripped_line)
        m_glue        = self.enc_glue_tag.match(stripped_line)
        m_unk_tag     = self.enc_unknown_tag.match(stripped_line)
        p_or_s_tag    = False
        # *** New paragraph
        if m_par_start:
            # Add a new paragraph start
            self.document_lines.append( ('<p>', line.strip()) )
            p_or_s_tag = True
        # *** Paragraph's end
        if m_par_end:
            self.document_lines.append( ('</p>', line.strip()) )
            p_or_s_tag = True
        # *** New sentence
        if m_s_start:
            # Add a new sentence start
            self.document_lines.append( ('<s>', line.strip()) )
            p_or_s_tag = True
        # *** Sentence end
        if m_s_end:
            self.document_lines.append( ('</s>', line.strip()) )
            p_or_s_tag = True
        # *** The glue tag or an unknown tag
        if not p_or_s_tag and (m_glue or m_unk_tag):
            self.document_lines.append( ('<TAG>', line.strip()) )
            if m_glue:
                self.last_was_glue = True
        # *** Text content + morph analysis inside sentence or paragraph
        elif len( stripped_line ) > 0 and '\t' in stripped_line:
            # Add word (could be a malformed word also)
            items = stripped_line.split('\t')
            token = items[0]
            self.document_lines.append( ('TOKEN', token, len(items), line.rstrip('\n')) )
        self.lines += 1
        return None


    def _finish_parsing( self ):
        '''Finishes the vert file parsing and returns the last document lines and metadata. 
           Call this method after reaching to the end of the file. 
        '''
        if len(self.document_lines) > 0:
            old_document_metadata = None
            old_document_lines    = None
            old_document_line_count = 0
            # Add vert file indexing information
            self.document_metadata['_doc_id'] = self.document_id
            assert self.document_start_line > -1
            self.document_metadata['_doc_start_line'] = self.document_start_line
            if not (self.document_end_line > -1):
                self.document_end_line = (self.lines + 1)
            assert self.document_end_line > -1
            self.document_metadata['_doc_end_line']   = self.document_end_line
            # Copy the old document
            old_document_lines = self.document_lines.copy()
            old_document_line_count = len(old_document_lines)
            old_document_metadata = self.document_metadata.copy()
            # Clear doc content for storing new document
            self.document_metadata.clear()
            self.document_lines.clear()
            assert len(old_document_metadata.keys()) > 0
            assert old_document_line_count == len(old_document_lines)
            return old_document_lines, old_document_metadata
        return None


    def status_str(self):
        '''Returns the parsing status message: completed/in progress and the number of lines parsed.'''
        msg_str = 'Parsing completed.' if self.parsing_finished else 'Parsing in progress.'
        return f'Total {self.total_lines} lines read from {self.vert_file!r}. {msg_str}'



class SyntaxVertFileWriter:
    '''A file writer for adding syntactic annotations to a vert file. 
       Syntactic annotations will be added to the vert content extracted by SimpleVertFileParser.
    '''

    def __init__(self, vert_file:str, vert_file_dir:str=None):
        '''Initializes the parser for writing into the `vert_file`. 
           Optionally, `vert_file_dir` can be used to specify output dir. 
        '''
        # Remember paths
        self.vert_file = vert_file
        if vert_file_dir is not None:
            os.makedirs(vert_file_dir, exist_ok=True)
        self.vert_file_dir = vert_file_dir
        self.vert_file_path = os.path.join(vert_file_dir, vert_file) \
            if vert_file_dir is not None else vert_file
        # Initialize file (erase the old content)
        with open(self.vert_file_path, mode='w', encoding='utf-8') as out_f:
            pass
        # Initialize line buffer
        self.line_buffer = []
        self.max_buffer_size = 100000
        self.total_lines_written = 0
        self.completed = False


    def write_sentence_start(self, vert_token: List[Any], sentence_hash:str=None, hash_attr:str='sha256'):
        '''Writes out sentence start tag <s> with added `hash_attr="sentence_hash"` attribute.
           This hash is a fingerprint of the sentence tokenization 
           (for details, see `x_utils.get_sentence_hash(...)`).
        '''
        assert vert_token[0] == '<s>'
        if sentence_hash is not None:
            assert isinstance(hash_attr, str)
            assert isinstance(sentence_hash, str)
            self._write_line(vert_token, modified_token=f'<s {hash_attr}="{sentence_hash}">')
        else:
            self._write_line(vert_token, modified_token=None)


    def write_tag(self, vert_token: List[Any]):
        '''Writes out a tag. 
           If the tag has (unexpectedly) \t-separated annotations, then auguments 
           tag's annotation fields with empty values to meet the exact length of a 
           syntactically annotated line.'''
        assert vert_token[0] != 'TOKEN'
        original_line  = vert_token[1]
        modified_token = None
        # Check if the tag has (unexpectedly) \t-separated annotations
        if '\t' in original_line:
            items = original_line.split('\t')
            if len(items) != 13:
                msg_str = 'Padding with empty values.' if len(items) < 13 else 'Truncating excessive values.'
                warnings.warn(f'(!) Unexpected number of features {len(items)} in the vert line {original_line!r}. {msg_str}')
            while len(items) != 13:
                if len(items) < 13:
                    # Add empty value
                    items.append('')
                elif len(items) > 13:
                    # Remove the last value
                    items.pop(-1)
            # Add empty values for syntactic annotations
            items.extend(['']*8)
            assert len(items) == 21
            # Construct new output token
            modified_token = '\t'.join(items)
        self._write_line(vert_token, modified_token=modified_token)


    def write_syntax_token(self, vert_token: List[Any], syntax_word_span: 'Span'):
        '''Writes out a vert token with added syntactic annotations.'''
        assert vert_token[0] == 'TOKEN'
        #
        # The output format is exemplified in the notebook:
        #   https://github.com/estnltk/estnltk/blob/1.6.9.1b0_devel/scribbles/enc_processing.ipynb
        #
        
        # Reminder: the syntax layer should have the following attributes:
        #('id', 'lemma', 'root_tokens', 'clitic', 'xpostag', 'feats', 'extended_feats', 'head', 'deprel'),
        
        # Syntactic info from stanza
        syn_id   = syntax_word_span.annotations[0]['id']
        syn_head = syntax_word_span.annotations[0]['head']
        syn_rel  = syntax_word_span.annotations[0]['deprel']

        # Finally, add information about the syntactic head / parent
        # (if the word is syntactic root, these fields will be empty)
        head_word = ''
        head_lemma = ''
        head_tag = ''
        head_features = ''
        head_syn_rel = ''
        if syntax_word_span.annotations[0]['parent_span'] is not None:
            # Get parent token
            parent_token = syntax_word_span.annotations[0]['parent_span']
            # Get features of the parent token:
            parent_analysis = parent_token.annotations[0]
            head_word = parent_token.text
            head_lemma = parent_analysis['lemma']
            head_tag   = parent_analysis['xpostag']
            head_features = '_'.join(parent_analysis['feats'].split())
            head_syn_rel = parent_analysis['deprel']

        original_line = vert_token[3]
        assert isinstance(original_line, str)
        items = original_line.split('\t')
        if len(items) != 13:
            # The original line from vert file should contain exactly 13 feature values. 
            # If not, then the original line is malformed. Add empty values or truncate excessive values.
            msg_str = 'Padding with empty values.' if len(items) < 13 else 'Truncating excessive values.'
            warnings.warn(f'(!) Unexpected number of features {len(items)} {items!r} in the vert line {original_line!r}. {msg_str}')
            while len(items) != 13:
                if len(items) < 13:
                    # Add empty value
                    items.append('')
                elif len(items) > 13:
                    # Remove the last value
                    items.pop(-1)
            assert len(items) == 13
            # Construct new original_line
            original_line = '\t'.join(items)
        modified_line = f'{original_line}\t{syn_id}\t{syn_head}\t{syn_rel}\t{head_word}\t{head_lemma}\t{head_tag}\t{head_features}\t{head_syn_rel}'
        modified_line = modified_line.replace('\n', ' ')
        self._write_line(vert_token, modified_token=modified_line)


    def _write_line(self, vert_token: List[Any], modified_token:str=None):
        '''Writes `vert_token` into the file (buffer). 
           If `modified_token` is not `None`, then writes `modified_token` instead of the 
           `vert_token` value.
        '''
        if vert_token[0] == 'TOKEN':
            # Write token
            original_line = vert_token[3]
            self.line_buffer.append(original_line if modified_token is None else modified_token)
        else:
            # Write tag
            original_line = vert_token[1]
            self.line_buffer.append(original_line if modified_token is None else modified_token)
        if len(self.line_buffer) > self.max_buffer_size: # Flush the buffer (if needed)
            self._write_out_buffer()
            self.line_buffer = []


    def finish_writing(self, add_newline=True):
        '''Completes the writing process: writes remaining contents of the buffer into the file.
           If `add_newline` is True (default), then adds newline at the end of the file.
        '''
        if len(self.line_buffer) > 0:
            self._write_out_buffer()
            self.line_buffer = []
        if add_newline:
            with open(self.vert_file_path, mode='a', encoding='utf-8') as out_f:
                out_f.write('\n')
        self.completed = True


    def _write_out_buffer(self):
        '''Writes contents of the line buffer into the vert file.'''
        if len(self.line_buffer) > 0:
            with open(self.vert_file_path, mode='a', encoding='utf-8') as out_f:
                if self.total_lines_written > 0:
                    # Continue writing: separate the last and the first line with newline
                    out_f.write('\n')
                for lid, line in enumerate(self.line_buffer):
                    out_f.write(line)
                    self.total_lines_written += 1
                    if lid < len(self.line_buffer)-1:
                        # Only add newline if this is not the last line
                        out_f.write('\n')


    def status_str(self):
        '''Returns the writing status message: completed/in progress and the number of lines written.'''
        msg_str = 'Writing completed.' if self.completed else 'Writing in progress.'
        return f'Total {self.total_lines_written} lines written into {self.vert_file!r}. {msg_str}'
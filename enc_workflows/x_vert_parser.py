#
#  This module contains:
#
#   * SimpleVertFileParser -- a simplified version of VertXMLFileParser for 
#     document-wise parsing of .vert files;
#
#

from typing import List, Union

import re, sys
import os, os.path

from estnltk import Text
from estnltk.corpus_processing.parse_enc import parse_tag_attributes

class SimpleVertFileParser:
    """ A simple parser for document by document parsing of vert XML files. 
        
        Document by document parsing (recommended):
        Use `vert_parser = SimpleVertFileParser(vert_file)` to create a new 
        document parser and call `next(vert_parser)` to retrieve the content and 
        metadata of the next document until all documents have been yielded. 
        If `vert_parser.parsing_finished == False`, then there are still documents 
        left to be parsed. The generator yields tuples `(document_content_lines:list, 
        document_metadata:dict)`. 
        
        Line by line parsing (advanced):
        Each line of a vert file should be passed to the method `parse_next_line()`, 
        which returns document content and metadata after a full document has been 
        collected, or `None` if collecting document content is in progress. 
        After the last line of the file has been parsed, method `finish_parsing()` 
        needs to be called to get the content and metadata of the last document. 
        
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
        self.lines               = 0
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
        self._document_collector = self.document_collector()


    def document_collector( self ):
        '''Creates a generator function for collecting document content 
           lines and metadata from the vert file. 
           Upon iteration or calling next( vert_parser ), yields a 
           tuple ( document_content_lines:list, document_metadata:dict ) 
           until each document of the vert file has been yielded. 
        '''
        with open( self.vert_file, mode='r', encoding='utf-8' ) as in_f:
            for line in in_f:
                result = self.parse_next_line( line )
                if result is not None:
                    # Return next document
                    yield result
        self.parsing_finished = True
        # Yield the last document
        yield self.finish_parsing()


    def __next__(self):
        '''Yields a tuple (document_content_lines, document_metadata) 
           until each document of the input vert file has been yielded. 
        '''
        return next(self._document_collector)


    def parse_next_line( self, line: str ):
        '''Parses a next line from the XML content of ENC vert file.
           
           Collects document content and metadata on the process. 
           If a start of a new document is reached with the line, 
           and a preceding document has been collected, then returns 
           the content and metadata of the preceding document.
           Normally, if the line only continues the document content, 
           then returns None.
           
           In order to complete the last document, please call method 
           finish_parsing() after parsing the last line of the vert 
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
                assert self.document_end_line > -1, f'{self.document_lines[:10]}...{self.document_lines[-10:]} {self.lines} {self.document_id}'
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
        # *** New paragraph
        if m_par_start:
            # Add a new paragraph start
            self.document_lines.append( ('<p>', line.strip()) )
        # *** Paragraph's end
        if m_par_end:
            self.document_lines.append( ('</p>', line.strip()) )
        # *** New sentence
        if m_s_start:
            # Add a new sentence start
            self.document_lines.append( ('<s>', line.strip()) )
        # *** Sentence end
        if m_s_end:
            self.document_lines.append( ('</s>', line.strip()) )
        # *** The glue tag or an unknown tag
        if m_glue or m_unk_tag:
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


    def finish_parsing( self ):
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
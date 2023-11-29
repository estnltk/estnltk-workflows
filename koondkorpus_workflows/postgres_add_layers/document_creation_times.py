#
#  Robust document creation time detection for Koondkorpus' documents. 
#  Document creation times are needed by TimexTagger for correctly 
#  resolving relative time expressions, such as 'eile' ('yesterday') 
#  or 'eelmisel aastal' ('last year').
#
import re
import json
import os, os.path
import warnings

from datetime import datetime

from tqdm import tqdm
from bs4 import BeautifulSoup

from estnltk.corpus_processing.parse_koondkorpus import unpack_zipped_xml_files_iterator


class KoondkorpusDCTFinder:
    '''
    A robust document creation time (dct) finder for Koondkorpus' documents. 
    
    Uses two strategies: first attempts to find creation times from document 
    metadata (e.g. file names or titles). If that fails or produces incomplete 
    date information, then tries to find creation dates from the original XML 
    contents of the document. This requires specific preprocessing that needs 
    to be completed before initializing KoondkorpusDCTFinder (see the function 
    extract_reference_times_from_xml_archives() below for details). 
    
    Note that this soultion is robust and works best for newspaper articles. 
    For many other types of documents, a more fine-grained approach is 
    desirable. For instance, in case of new media documents (e.g. forum or 
    newsfeed postings), the original document often contained many postings 
    made on different times; however, we assign a single creation time to 
    the whole document, based on the first posting, and ignore creation times 
    of other postings.
    '''

    def __init__( self, extracted_dcts_file='extracted_dcts.tsv'):
        '''Initializes KoondkorpusDCTFinder.'''
        self.file_to_dct = {}
        if isinstance(extracted_dcts_file, str):
            if os.path.exists(extracted_dcts_file):
                self._load_file_to_dct( extracted_dcts_file )
            else:
                warnings.warn(f'(!) Missing document creation times file {extracted_dcts_file!r}.')
    
    def _load_file_to_dct(self, extracted_dcts_file, separator='____'):
        '''Loads extracted document creation times from the input file.
           The input file must be a file created by the pre-processing 
           function extract_reference_times_from_xml_archives(), see 
           below for details.
           Assigns loaded mapping to self.file_to_dct.
        '''
        assert os.path.exists(extracted_dcts_file)
        cur_file_to_dct = {}
        with open(extracted_dcts_file, 'r', encoding='utf-8') as in_f:
            for line in in_f:
                line = line.strip()
                if len(line) > 0 and '\t' in line:
                    xml_file, dcts = line.split('\t')
                    if separator in xml_file:
                        xml_file, forum_topic = xml_file.split(separator)
                        if xml_file not in cur_file_to_dct:
                            cur_file_to_dct[xml_file] = {}
                        if forum_topic not in cur_file_to_dct[xml_file]:
                            cur_file_to_dct[xml_file][forum_topic] = []
                        if ';' not in dcts:
                            dcts = [dcts]
                        for dct in dcts:
                            cur_file_to_dct[xml_file][forum_topic].append(dct)
                    else:
                        if xml_file not in cur_file_to_dct:
                            cur_file_to_dct[xml_file] = []
                        if ';' not in dcts:
                            dcts = [dcts]
                        for dct in dcts:
                            cur_file_to_dct[xml_file].append(dct)
                elif len(line) > 0:
                    #warnings.warn(f'(!) Unexpected dct entry: {line!r}')
                    pass
        self.file_to_dct = cur_file_to_dct
    
    def find_dct( self, meta, pick_first=True ):
        '''Attempts to find document creation time based on document metadata.
           First, tries to parse creation time straightly from metadata values. 
           If that fails or the information is incomplete in metadata, then 
           falls back to fetching creation time from file_to_dct mapping (if 
           extracted dct-s have been loaded).
           
           Returns document creation time as an ISO format datetime string 
           (yyyy-mm-dd or yyyy-mm-ddTHH:MM). 
           Note that incomplete/missing values in the string can be replaced 
           by Xs (e.g. '2003-XX-XX' if only the creation year was found, and 
           'XXXX-XX-XX' if no creation time could be found).
           
           If pick_first=False (default: True), then returns a list of 
           creation times if a file contained with multiple documents. 
           Otherwise, in case of an ambiguity (multiple documents), the 
           (temporally) first creation time is returned.
        '''
        # First, attempt to detect document creation time from metadata
        meta_dct = detect_reference_time_from_meta( meta )
        if 'file' in meta.keys() and (meta_dct is None or 'X' in meta_dct):
            # Second, try to use document creation times extracted from 
            # XML file contents. This assumes that the extraction has been 
            # already performed. 
            xml_file = meta['file']
            if xml_file in self.file_to_dct:
                if isinstance(self.file_to_dct[xml_file], list):
                    if pick_first and self.file_to_dct[xml_file]:
                        return self.file_to_dct[xml_file][0]
                    else:
                        return self.file_to_dct[xml_file]
                elif isinstance(self.file_to_dct[xml_file], dict):
                    # Specific to Planet forum postings
                    title = meta.get('title', '')
                    if title in self.file_to_dct[xml_file]:
                        if pick_first and self.file_to_dct[xml_file][title]:
                            return self.file_to_dct[xml_file][title][0]
                        else:
                            return self.file_to_dct[xml_file][title]
        return meta_dct if meta_dct is not None else 'XXXX-XX-XX'

# ==============================================================================
# ==============================================================================
#   Extract creation dates from metadata of Koondkorpus documents.
# ==============================================================================
# ==============================================================================

def month_name_to_number( month_name: str ):
    '''
    Converts common Estonian month names (and month name abbreviations) to numbers.
    '''
    month_names_long = ["jaanuar", "veebruar", "m.rts", "aprill", "mai", "juuni", 
                        "juuli", "august", "september", "okto+ber", "november", 
                        "detsember"];
    for m_id, m_name in enumerate(month_names_long):
        if re.match(m_name, month_name):
            month_number = f'{int(m_id+1):02d}'
            return month_number
    month_names_short = ["jaan", "veebr?", "m.rts", "apr(ill)?", "mai", "juuni", 
                         "juuli", "aug", "sept", "okt", "nov", "dets" ];
    for m_id, m_name in enumerate(month_names_short):
        if re.match(m_name, month_name):
            month_number = f'{int(m_id+1):02d}'
            return month_number
    raise ValueError(f'(!) Unknown month name: {month_name!r}')


def detect_reference_time_from_meta( meta: dict ) -> str:
    '''
    Parses reference time from metadata of the Koondkorpus document.
    
    Full date (yyyy-mm-dd) reference times can be parsed from metadata 
    in the following sub corpora:
    * EPL (newspaper Eesti Päevaleht);
    * PM (newspaper Postimees);
    * LE (newspaper Lääne Elu);
    * KR (magazine Kroonika);
    * ML (newspaper Maaleht);
    * VM (newspaper Valgamaalane);
    * Chatroom recordings (new media corpus);
    
    Partial date (yyyy-XX-XX or yyyy-mm-XX) reference times can be 
    parsed from metadata in the following sub corpora:
    * EE (newspaper Eesti Ekspress);
    * Riigikogu stenogrammid (Parliament transcripts);
    * AA (magazine Arvutustehnika & Andmetöötlus);
    * Magazine Eesti Arst;
    * Magazine Agraarteadus;
    * Magazine Horisont;
    * (few) Scientific articles which dates can be derived;
    
    Assigns unknown date (XXXX-XX-XX) reference times to 
    documents from the following sub corpora:
    * Fiction;
    * Estonian and European legal documents;
    * (most) Scientific articles;
    
    Returns metadata as an ISO date string (yyyy-mm-dd), where 
    missing/incomplete values will be marked with Xs.
    Returns None if no information about reference time could be 
    derived from metadata alone. 
    '''
    if 'file' in meta.keys():
        #
        # Full date reference times from file names of newspapers
        #
        m1 = re.match('.*aja_EPL_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['file'])
        if m1:
            return f'{m1.group(1)}-{int(m1.group(2)):02d}-{int(m1.group(3)):02d}'
        m2 = re.match('.*aja_sloleht_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['file'])
        if m2:
            return f'{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}'
        m3 = re.match('.*aja_pm_(\d\d\d\d)_(\d+)_(\d+)(Extra|e|a)?\..*', meta['file'])
        if m3:
            return f'{m3.group(1)}-{int(m3.group(2)):02d}-{int(m3.group(3)):02d}'
        m4 = re.match('.*aja_le_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['file'])
        if m4:
            return f'{m4.group(1)}-{int(m4.group(2)):02d}-{int(m4.group(3)):02d}'
        m5 = re.match('.*aja_kr_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['file'])
        if m5:
            return f'{m5.group(1)}-{int(m5.group(2)):02d}-{int(m5.group(3)):02d}'
        m6 = re.match('.*aja_vm_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['file'])
        if m6:
            return f'{m6.group(1)}-{int(m6.group(2)):02d}-{int(m6.group(3)):02d}'
        m7 = re.match('.*aja_luup_(\d\d\d\d)_(\d+)\..*', meta['file'])
        if m7:
            year = m7.group(1)
            if 'ajakirjanumber' in meta.keys():
                ajakirjanumber = meta['ajakirjanumber']
                # Fix format: '1.aprill' --> '1. aprill'
                ajakirjanumber = re.sub(', (\d+\.)([a-z])', ', \\1 \\2', ajakirjanumber)
                # Fix format: '19 jaanuar' --> '19. jaanuar'
                ajakirjanumber = re.sub(', (\d+) ([a-z])', ', \\1. \\2', ajakirjanumber)
                # 1st format:  "Luup Nr . 9 ( 118 ) , 29. aprill 2000"
                full_date = re.search('[,;] (\d+)\. (\S+) (\d\d\d\d)', ajakirjanumber)
                if full_date:
                    assert full_date.group(3) == year
                    month = month_name_to_number( (full_date.group(2)).lower() )
                    return f'{full_date.group(3)}-{month}-{int(full_date.group(1)):02d}'
                else:
                    # 2nd format:  "Luup Nr . 12 ( 139 ) , detsember 2001"
                    par_date = re.search('[,;] (\S+) (\d\d\d\d)', ajakirjanumber)
                    if par_date:
                        assert par_date.group(2) == year
                        month = month_name_to_number( (par_date.group(1)).lower() )
                        return f'{par_date.group(2)}-{month}-XX'
                    else:
                        warnings.warn(f'(!) Failed to parse Luup doc creation date from {meta["ajakirjanumber"]!r}')
            else:
                warnings.warn(f'(!) Failed to parse Luup doc creation date from {meta!r}')
        m8 = re.match('.*aja_ml_(\d\d\d\d)_(\d+)\..*', meta['file'])
        if m8:
            year = m8.group(1)
            if 'ajalehenumber' in meta.keys():
                ajakirjanumber = meta['ajalehenumber']
                # Format:  'Maaleht 02.10.2003 ( number 40/2003 ( 834 ) )'
                full_date = re.search('Maaleht (\d+)\.(\d+)\.(\d\d\d\d)', ajakirjanumber)
                if full_date:
                    assert year == full_date.group(3)
                    return f'{full_date.group(3)}-{int(full_date.group(2)):02d}-{int(full_date.group(1)):02d}'
                else:
                    warnings.warn(f'(!) Failed to parse Maaleht doc creation date from {meta["ajalehenumber"]!r}')
            else:
                warnings.warn(f'(!) Failed to parse Maaleht doc creation date from {meta!r}')
        if meta.get('subcorpus', '') == "jututoavestlus":
            #
            # Full date reference times from file names of chat room recordings. 
            #
            # Example format:
            #    'AdiChat151103.xml' -->
            #    <title> #AdiChat   Sat Nov 15 00:00:00 2003 kuni Sun Nov 16 00:00:00 2003 </title> 
            #
            # Note that the file name only contains starting date of the recording 
            # and recordings can span over the following dates, so the given 
            # date is an approximation.
            #
            chatroom_name = ['AdiChat', 'ALsPlace.', 'armastusesaal', 'armutuba', \
                'blue', 'C4U.', 'c4u', 'creative', 'delfi', 'elutuba', 'hinnavaatlus', \
                'karmtuba', 'kuressaare', 'kpk', 'kreisiraadio', 'metal', 'nak', \
                'noortekas', 'paevateema', 'ringfm', 'rullnokad', 'seks', 'toru']
            chatroom_name_str = '|'.join(chatroom_name)
            chat_record = re.match(f'.*({chatroom_name_str})(\d\d)(\d\d)(\d\d)\..*', meta['file'])
            if chat_record:
                day = chat_record.group(2)
                month = chat_record.group(3)
                year = chat_record.group(4)
                if year.startswith('0'):
                    year = '20'+year
                return f'{year}-{month}-{day}'
        # 
        #  The following files do not have dates in the title/header part (or dates are broken). 
        #  However, correct dates were manually looked from the document. Use hardcoded values:
        #
        if meta['file'].startswith('aja_ee_2001_48.'):
            return "2001-11-29";
        # Discard finding creation times of law texts altogether.
        #if meta['file'].startswith('sea_eesti_x2056.'):
        #    return "1997-11-19";
        #elif meta['file'].startswith('sea_eesti_x2064.'):
        #    return "1997-10-24";
        #elif meta['file'].startswith('sea_eesti_x50007.'):
        #    # NB! This is uncertain
        #    return "2000-02-01";
        #elif meta['file'].startswith('sea_eesti_x50016.'):
        #    return "2000-12-08";
        #elif meta['file'].startswith('sea_eesti_x50017k1.'):
        #    return "2002-02-01";

        #
        # Partial date reference times from file names & other fields
        #
        aja_ee = re.match('.*aja_ee_(\d\d\d\d)_(\d+)\..*', meta['file'])
        if aja_ee:
            year = aja_ee.group(1)
            return f'{year}-XX-XX'
        rkogu = re.match('.*rkogu_(\d\d\d\d)_(\d+)\..*', meta['file'])
        if rkogu:
            return f'{rkogu.group(1)}-{int(rkogu.group(2)):02d}-XX'
        eesti_arst = re.match('.*tea_eesti_arst_(\d\d\d\d)\..*', meta['file'])
        if eesti_arst:
            year = eesti_arst.group(1)
            month = 'XX'
            if 'title' in meta.keys():
                title = re.match('Nr (\d+) (\S+) (\d\d\d\d)', meta['title'])
                if title:
                    assert title.group(3) == year
                    month = month_name_to_number( (title.group(2)).lower() )
            else:
                warnings.warn(f'(!) Failed to parse Eesti Arst doc creation date from {meta["title"]!r}')
            return f'{year}-{month}-XX'
        mtaa = re.match('.*tea_AA_(\d\d)_(\d+)\..*', meta['file'])
        if mtaa:
            year = mtaa.group(1)
            if int(year) >= 90:
                year = f'19{year}'
            else:
                year = f'20{year}'
            number = mtaa.group(2)
            month = 'XX'
            # Numbrite ilmumiskuud ajakirja koduleheküljelt (http://deepzone0.ttu.ee/aa/)
            if int(number) == 1:
                month = '02'
            elif int(number) == 2:
                month = '04'
            elif int(number) == 3:
                month = '06'
            elif int(number) == 4:
                month = '08'
            elif int(number) == 5:
                month = '10'
            elif int(number) == 6:
                month = '12'
            return f'{year}-{month}-XX'
        mtagr = re.match('.*agraar_(\d\d\d\d)\..*', meta['file'])
        if mtagr:
            year = mtagr.group(1)
            return f'{year}-XX-XX'
        hor = re.match('.*horisont_(\d\d\d\d)\..*', meta['file'])
        if hor:
            year = hor.group(1)
            return f'{year}-XX-XX'
        #
        #  Scientific articles. Hard to detect dates, were manually looked up from the document:
        #
        if meta['file'].startswith('tea_EMS_1997.'):
            return "1997-XX-XX";
        elif meta['file'].startswith('tea_EMS_2001.'):
            return "2001-XX-XX";
        elif meta['file'].startswith('tea_ESA_49.'):
            return "2003-XX-XX";
        elif meta['file'].startswith('tea_ESA_50.'):
            return "2004-XX-XX";
        elif meta['file'].startswith('tea_draama.'):
            return "1992-XX-XX";
        elif meta['file'].startswith('tea_draama2.'):
            return "1994-XX-XX";
        elif meta['file'].startswith('tea_'):
            return "XXXX-XX-XX";
        #
        # Missing date reference times for literature & law texts
        #
        if meta['file'].startswith('ilu_') or \
           meta['file'].startswith('sea_') :
            return 'XXXX-XX-XX'
    return None


# ==============================================================================
# ==============================================================================
#   Extract creation dates from Koondkorpus XML file contents.
#   Note: this is a pre-processing step which 
#   needs to be performed before using KoondkorpusDCTFinder;
# ==============================================================================
# ==============================================================================

def get_EE_reference_times_from_packed_XML(input_file:str='Ekspress.zip'):
    '''
    Parses Eesti Ekspress document creation times from XML archive.
    Returns a mapping from XML file names  to  ISO format document 
    creation dates. 
    '''
    assert isinstance(input_file, str) and os.path.exists(input_file)
    assert input_file.endswith('.zip')
    fname_to_dct = {}
    progress = tqdm(desc="Extracting {}".format(input_file))
    for (full_fnm, content) in unpack_zipped_xml_files_iterator(input_file, test_only=False):
        path_head, path_tail = os.path.split(full_fnm)
        soup = BeautifulSoup(content, 'html5lib')
        title = soup.find_all('title')[0].string
        # Example format:  "Eesti Ekspress 20. jaanuar 2000"
        full_date = re.search('Eesti Ekspress (\d+)\.? ([^0-9 ]+) (\d\d\d\d)', title)
        if full_date:
            month = month_name_to_number( (full_date.group(2)).lower() )
            dct = f'{full_date.group(3)}-{month}-{int(full_date.group(1)):02d}'
            fname_to_dct[path_tail] = dct
        else:
            warnings.warn(f'(!) Failed to parse Eesti Ekspress doc creation date from {title!r}')
        progress.update(1)
    progress.close()
    return fname_to_dct


def convert_datetime_to_iso_format(datetime_str:str) -> str:
    '''
    Attempts to convert commonly used new media datetime formats 
    to ISO format strings. Note that this method is not locale-aware,
    so any locality information will be discarded during the 
    conversion.
    
    Returns datetime as an ISO string (yyyy-mm-ddTHH:MM) if the 
    conversion is successful. Otherwise, raises a warning and 
    returns None.
    '''
    # Example format #0:  '30.09.2003 09:48'
    full_date0 = re.search('(\d\d)\.(\d\d)\.(\d\d\d\d) (\d\d):(\d\d)', datetime_str)
    # Example format #1:  'Wed , 24 Oct 2001 16:28:08 +0200 ( EET )'
    full_date1 = re.search(', (\d+) ([^0-9 ]+) (\d\d\d\d) (\d\d)\s*:\s*(\d\d)', datetime_str)
    # Example format #2:  '11 Feb 2003 11:31:39 +0200'
    full_date2 = re.search('(\d+) ([^0-9 ]+) (\d\d\d\d) (\d\d)\s*:\s*(\d+)', datetime_str)
    # Example format #3:  '25. veebruar 2004. a. 18:06:27'
    full_date3 = re.search('(\d+)\. ([^0-9 ]+) (\d\d\d\d)\. a\. (\d+)\s*:\s*(\d+)', datetime_str)
    # Example format #4:  'Thu , 04 Dec 03 01:51:31 GMT'
    full_date4 = re.search(', (\d+) ([^0-9 ]+) (\d\d) (\d\d)\s*:\s*(\d\d)', datetime_str)
    # Example format #5:  '21-06-2003 , 16:48'
    full_date5 = re.search('(\d+)-(\d+)-(\d\d\d\d) , (\d\d):(\d\d)', datetime_str)
    # Example format #6:  '29.12.2003'
    full_date6 = re.search('(\d+)\.(\d+)\.(\d\d\d\d)', datetime_str)
    if full_date0:
        # Simplify datetime
        simplified_date = full_date0.group(0)
        datetime_format = '%d.%m.%Y %H:%M'
        datetime_object = datetime.strptime(simplified_date, datetime_format)
        return datetime_object.isoformat()[:-3]
    elif full_date1:
        # Example format #1:  'Wed , 24 Oct 2001 16:28:08 +0200 ( EET )'
        # Simplify datetime
        # (note: this results in a loss of information!)
        simplified_date = full_date1.group(0)
        # Fix the time format so we can parse with strptime()
        simplified_date = re.sub('([0-9]+) : ([0-9]+)', '\\1:\\2', simplified_date)
        datetime_format = ', %d %b %Y %H:%M'
        datetime_object = datetime.strptime(simplified_date, datetime_format)
        return datetime_object.isoformat()[:-3]
    elif full_date2:
        # Example format #2:  '11 Feb 2003 11:31:39 +0200'
        # Simplify datetime
        # (note: this results in a loss of information!)
        simplified_date = full_date2.group(0)
        # Fix the time format so we can parse with strptime()
        simplified_date = re.sub('([0-9]+) : ([0-9]+)', '\\1:\\2', simplified_date)
        datetime_format = '%d %b %Y %H:%M'
        datetime_object = datetime.strptime(simplified_date, datetime_format)
        return datetime_object.isoformat()[:-3]
    elif full_date3:
        # Example format #3:  '25. veebruar 2004. a. 18:06:27'
        #                     '30. oktoober 2002. a. 9 : 44'
        date = full_date3.group(1)
        month = None
        try:
            month = month_name_to_number( (full_date3.group(2)).lower() )
        except Exception as error:
            warnings.warn(f'{error}')
            warnings.warn(f'(!) Unable to convert {datetime_str!r} to ISO datetime format.')
            return None
        year = full_date3.group(3)
        hours = full_date3.group(4)
        minutes = full_date3.group(5)
        return \
            f'{year}-{month}-{int(date):02d}T{int(hours):02d}:{int(minutes):02d}'
    elif full_date4:
        # Example format #4:  'Thu , 04 Dec 03 01:51:31 GMT'
        # Simplify datetime
        # (note: this results in a loss of information!)
        simplified_date = full_date4.group(0)
        # Fix the time format so we can parse with strptime()
        simplified_date = re.sub('([0-9]+) : ([0-9]+)', '\\1:\\2', simplified_date)
        datetime_format = ', %d %b %y %H:%M'
        datetime_object = datetime.strptime(simplified_date, datetime_format)
        return datetime_object.isoformat()[:-3]
    elif full_date5:
        # Example format #5:  '21-06-2003 , 16:48'
        # Simplify datetime
        simplified_date = full_date5.group(0)
        datetime_format = '%d-%m-%Y , %H:%M'
        datetime_object = datetime.strptime(simplified_date, datetime_format)
        return datetime_object.isoformat()[:-3]
    elif full_date6:
        # Example format #6:  '29.12.2003'
        date = full_date6.group(1)
        month = full_date6.group(2)
        year = full_date6.group(3)
        return f'{year}-{int(month):02d}-{int(date):02d}TXX:XX'
    else:
        warnings.warn(f'(!) Unable to convert {datetime_str!r} to ISO datetime format.')
        return None


def get_new_media_reference_times_from_packed_XML(input_file:str='foorum_uudisgrupp_kommentaar.zip',
                                                  only_first_and_last_date:bool=False,
                                                  separator:str='____'):
    '''
    Parses new media (forums, news feeds and commentaries) document creation times 
    from an XML archive. Returns a dict mapping from XML file names to a list of ISO 
    format document creation dates. 
    
    If only_first_and_last_date is set, then sorts creation times of each file and 
    returns associates each file only with the first and the last creation time.
    
    In case of Planet forum postings, the mapping key will be formatted as 
    f'{filename}{separator}{title}', because multiple documents were extracted 
    from a single file.
    '''
    assert isinstance(input_file, str) and os.path.exists(input_file)
    assert input_file.endswith('.zip')
    fname_to_dct = {}
    progress = tqdm(desc="Extracting {}".format(input_file))
    for (full_fnm, content) in unpack_zipped_xml_files_iterator(input_file, test_only=False):
        path_head, path_tail = os.path.split(full_fnm)
        if path_head.endswith('kommentaarid') and path_tail.startswith('delfi'):
            #
            # Collect creation dates from delfi commentaries
            #
            soup = BeautifulSoup(content, 'html5lib')
            datetimes = []
            for div2 in soup.find_all('div2'):
                for datetime_tag in div2.find_all('time'):
                    # Example format:  '30.09.2003 09:48'
                    datetime_str = datetime_tag.string.strip()
                    if ',' in datetime_str:
                        for sub_datetime_str in datetime_str.split(','):
                            datetime_iso_str = convert_datetime_to_iso_format(sub_datetime_str.strip())
                            if datetime_iso_str is not None:
                                datetimes.append(datetime_iso_str)
                            break
                    else:
                        datetime_iso_str = convert_datetime_to_iso_format(datetime_str.strip())
                        if datetime_iso_str is not None:
                            datetimes.append(datetime_iso_str)
            if only_first_and_last_date and datetimes:
                datetimes = sorted(datetimes)
                datetimes = [datetimes[0], datetimes[-1]]
            fname_to_dct[path_tail] = datetimes
        elif path_head.endswith('uudisgrupid'):
            #
            # Collect creation dates from news feeds
            #
            soup = BeautifulSoup(content, 'html5lib')
            datetimes = []
            for div2 in soup.find_all('div2'):
                for datetime_tag in div2.find_all('time'):
                    datetime_str = datetime_tag.string.strip()
                    datetime_iso_str = convert_datetime_to_iso_format( datetime_str )
                    if datetime_iso_str is not None:
                        datetimes.append(datetime_iso_str)
            if only_first_and_last_date and datetimes:
                datetimes = sorted(datetimes)
                datetimes = [datetimes[0], datetimes[-1]]
            fname_to_dct[path_tail] = datetimes
        elif path_head.endswith('foorumid'):
            #
            # Collect creation dates from Planet forum postings
            #
            soup = BeautifulSoup(content, 'html5lib')
            for div2 in soup.find_all('div2'):
                title = list(div2.children)[0].string.strip()
                assert separator not in title
                key = f'{path_tail}{separator}{title}'
                datetimes = []
                for div3 in div2.find_all('div3'):
                    for datetime_tag in div3.find_all('time'):
                        datetime_str = datetime_tag.string.strip()
                        datetime_iso_str = convert_datetime_to_iso_format( datetime_str )
                        if datetime_iso_str is not None:
                            datetimes.append(datetime_iso_str)
                if only_first_and_last_date and datetimes:
                    datetimes = sorted(datetimes)
                    datetimes = [datetimes[0], datetimes[-1]]
                fname_to_dct[key] = datetimes
        progress.update(1)
    progress.close()
    return fname_to_dct


def extract_reference_times_from_xml_archives(express_archive:str='Ekspress.zip',
                                              new_media_archive:str='foorum_uudisgrupp_kommentaar.zip',
                                              output_file:str='extracted_dcts.tsv'):
    '''
    Extracts reference times (dct-s) from packed Koondkorpus XML files. 
    
    Currently, the dct extraction has been implemented for the following archives:
    *) express_archive (default: 'Ekspress.zip');
    *) new_media_archive (default: 'foorum_uudisgrupp_kommentaar.zip');
    Please download the required .zip files from: 
    https://www.cl.ut.ee/korpused/segakorpus/index.php?lang=en
    
    Extracted reference times will be saved into output_file (default: 
    'extracted_dcts.tsv'). First item in each row will be XML file name, 
    and the second item will be list of ISO format document creation times 
    (multiple values separated by ';').
    
    In case of Planet forum postings, the file name will be formatted as 
    f'{xml_file}____{title}' so that sub documents with specific titles 
    can be separated.
    '''
    ee_ref_times = dict()
    new_media_ref_times = dict()
    if not os.path.exists(express_archive):
        warnings.warn(f'(!) Unable to locate EE archive {express_archive!r}. '+\
                       'Please download the corresponding XML archive from https://www.cl.ut.ee/korpused/segakorpus/ .')
    else:
        ee_ref_times = get_EE_reference_times_from_packed_XML(input_file=express_archive)
    if not os.path.exists(new_media_archive):
        warnings.warn(f'(!) Unable to locate EE archive {new_media_archive!r}. '+\
                       'Please download the corresponding XML archive from https://www.cl.ut.ee/korpused/segakorpus/ .')
    else:
        new_media_ref_times = get_new_media_reference_times_from_packed_XML(input_file=new_media_archive, 
                                                                            only_first_and_last_date=True)
    # Merge results 
    ee_ref_times.update(new_media_ref_times)
    if len(ee_ref_times.keys()) > 0:
        # Save results into file
        with open(output_file, 'w', encoding='utf-8') as out_f:
            for k, v in ee_ref_times.items():
                if isinstance(v, str):
                    out_f.write(f'{k}\t{v}\n')
                elif isinstance(v, list):
                    out_f.write(f'{k}\t{";".join(v)}\n')
                else:
                    warnings.warns(f'(!) Unexpcted dct type {type(v)}, expected str.')
    else:
        print('(!) No dct-s found. Please make sure the archives have been downloaded.')


# ==============================================================================
# ==============================================================================
#   Testing
# ==============================================================================
# ==============================================================================

def test_apply_dct_finder_on_koondkorpus_metadata():
    # The following file should contain metadata of all Koondkorpus' documents.
    # Metadata of each file should be on a separate line as a JSON object.
    input_file = 'koondkorpus_metadata.jsonl'
    if os.path.exists(input_file):
        ref_finder = KoondkorpusDCTFinder()
        total_lines = 0
        ref_times_found = 0
        full_dates = 0
        incomplete_dates = 0
        unknown_dates = 0
        with open(input_file, 'r', encoding='utf-8') as in_f:
            for line in in_f:
                if len(line) > 0:
                    doc_meta = json.loads(line)
                    ref = ref_finder.find_dct( doc_meta )
                    if ref is not None:
                        ref_times_found+=1
                        if ref.endswith('-XX') and ref[0].isnumeric():
                            incomplete_dates += 1
                        elif ref[-1].isnumeric():
                            full_dates += 1
                        elif ref == 'XXXX-XX-XX' or ref == 'XXXX-XX-XXTXX:XX':
                            unknown_dates += 1
                    total_lines += 1
        percentage = ref_times_found/total_lines*100.0
        print('Reference times found:  {} ({:.2f}%)'.format(ref_times_found, percentage))
        percentage = full_dates/ref_times_found*100.0
        print('           Full dates:  {} ({:.2f}%)'.format(full_dates, percentage))
        percentage = incomplete_dates/ref_times_found*100.0
        print('        Partial dates:  {} ({:.2f}%)'.format(incomplete_dates, percentage))
        percentage = unknown_dates/ref_times_found*100.0
        print('        Unknown dates:  {} ({:.2f}%)'.format(unknown_dates, percentage))
        print('Total entries:          {}'.format(total_lines))
        assert total_lines == full_dates + incomplete_dates + unknown_dates
        assert total_lines == 705356
        assert full_dates  == 697870
        assert incomplete_dates == 1122
        assert unknown_dates    == 6364
    else:
        warnings.warn(f'(!) Missing test input file {input_file}. Cannot perform test.')


if __name__ == '__main__':
    test_apply_dct_finder_on_koondkorpus_metadata()
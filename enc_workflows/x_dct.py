#
#  Robust document creation time (dct) detection for ENC's documents. 
#  Document creation times are needed by TimexTagger for correctly 
#  resolving relative time expressions, such as 'eile' ('yesterday') 
#  or 'eelmisel aastal' ('last year').
#  
#  This code builds upon:
#   https://github.com/estnltk/estnltk-workflows/blob/enc_add_new_layers/koondkorpus_workflows/postgres_add_layers/document_creation_times.py
#

import re
import warnings

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
    Parses reference time from metadata of the ENC document.
    
    Full date (yyyy-mm-dd) reference times can be parsed from metadata 
    in the following sub corpora:
    * EPL (newspaper Eesti Päevaleht);
    * PM (newspaper Postimees);
    * LE (newspaper Lääne Elu);
    * KR (magazine Kroonika);
    * ML (newspaper Maaleht);
    * VM (newspaper Valgamaalane);
    * Timestamped (articles from online news feeds);
    
    Partial date (yyyy-XX-XX or yyyy-mm-XX) reference times can be 
    parsed from metadata in the following sub corpora:
    * EE (newspaper Eesti Ekspress);
    
    Returns metadata as an ISO date string (yyyy-mm-dd), where 
    missing/incomplete values will be marked with Xs.
    Returns 'XXXX-XX-XX' if no information about reference time 
    could be derived from metadata alone. 
    '''
    if (meta.get('src', '')).startswith( ('Balanced Corpus', 'Reference Corpus') ):
        if 'filename' in meta.keys():
            #
            # Full date reference times from file names of newspapers
            #
            m1 = re.match(r'(?i).*aja_EPL_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['filename'])
            if m1:
                return f'{m1.group(1)}-{int(m1.group(2)):02d}-{int(m1.group(3)):02d}'
            m2 = re.match(r'(?i).*aja_sloleht_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['filename'])
            if m2:
                return f'{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}'
            m3 = re.match(r'(?i).*aja_pm_(\d\d\d\d)_(\d+)_(\d+)(Extra|e|a)?\..*', meta['filename'])
            if m3:
                return f'{m3.group(1)}-{int(m3.group(2)):02d}-{int(m3.group(3)):02d}'
            m4 = re.match(r'(?i).*aja_le_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['filename'])
            if m4:
                return f'{m4.group(1)}-{int(m4.group(2)):02d}-{int(m4.group(3)):02d}'
            m5 = re.match(r'(?i).*aja_kr_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['filename'])
            if m5:
                return f'{m5.group(1)}-{int(m5.group(2)):02d}-{int(m5.group(3)):02d}'
            m6 = re.match(r'(?i).*aja_vm_(\d\d\d\d)_(\d+)_(\d+)\..*', meta['filename'])
            if m6:
                return f'{m6.group(1)}-{int(m6.group(2)):02d}-{int(m6.group(3)):02d}'
            m7 = re.match(r'(?i).*aja_luup_(\d\d\d\d)_(\d+)\..*', meta['filename'])
            if m7:
                year = m7.group(1)
                if 'newspaperNumber' in meta.keys():
                    ajakirjanumber = meta['newspaperNumber']
                    # Fix format: '1.aprill' --> '1. aprill'
                    ajakirjanumber = re.sub(r', (\d+\.)([a-z])', ', \\1 \\2', ajakirjanumber)
                    # Fix format: '19 jaanuar' --> '19. jaanuar'
                    ajakirjanumber = re.sub(r', (\d+) ([a-z])', ', \\1. \\2', ajakirjanumber)
                    # 1st format:  "Luup Nr . 9 ( 118 ) , 29. aprill 2000"
                    full_date = re.search(r'[,;] (\d+)\. (\S+) (\d\d\d\d)', ajakirjanumber)
                    if full_date:
                        assert full_date.group(3) == year
                        month = month_name_to_number( (full_date.group(2)).lower() )
                        return f'{full_date.group(3)}-{month}-{int(full_date.group(1)):02d}'
                    else:
                        # 2nd format:  "Luup Nr . 12 ( 139 ) , detsember 2001"
                        par_date = re.search(r'[,;] (\S+) (\d\d\d\d)', ajakirjanumber)
                        if par_date:
                            assert par_date.group(2) == year
                            month = month_name_to_number( (par_date.group(1)).lower() )
                            return f'{par_date.group(2)}-{month}-XX'
                        else:
                            warnings.warn(f'(!) Failed to parse Luup doc creation date from {meta["newspaperNumber"]!r}')
                else:
                    warnings.warn(f'(!) Failed to parse Luup doc creation date from {meta!r}')
            m8 = re.match(r'(?i).*aja_ml_(\d\d\d\d)_(\d+)\..*', meta['filename'])
            if m8:
                year = m8.group(1)
                if 'newspaperNumber' in meta.keys():
                    ajakirjanumber = meta['newspaperNumber']
                    # Format:  'Maaleht 02.10.2003 ( number 40/2003 ( 834 ) )'
                    full_date = re.search(r'Maaleht (\d+)\.(\d+)\.(\d\d\d\d)', ajakirjanumber)
                    if full_date:
                        assert year == full_date.group(3)
                        return f'{full_date.group(3)}-{int(full_date.group(2)):02d}-{int(full_date.group(1)):02d}'
                    else:
                        warnings.warn(f'(!) Failed to parse Maaleht doc creation date from {meta["newspaperNumber"]!r}')
                else:
                    warnings.warn(f'(!) Failed to parse Maaleht doc creation date from {meta!r}')
            #
            # Partial date reference times from file names & other fields
            #
            aja_ee = re.match(r'(?i).*aja_ee_(\d\d\d\d)_(\d+)\..*', meta['filename'])
            if aja_ee:
                year = aja_ee.group(1)
                return f'{year}-XX-XX'
            
    elif (meta.get('src', '')).startswith( ('Timestamped') ):
        if 'timestamp_date' in meta.keys():
            #
            # Full date reference times from document timestamp
            #
            m1 = re.match(r'(\d\d\d\d)-(\d+)-(\d+).*', meta['timestamp_date'])
            if m1:
                return f'{m1.group(1)}-{int(m1.group(2)):02d}-{int(m1.group(3)):02d}'
    # No document creation time found
    return 'XXXX-XX-XX'


def get_reference_time_type( ref_time: str ) -> str:
    r'''
    Detects type of the reference time found from the metadata. 
    Type of the reference time has 3 possible values:
    * 'full_date' -- if reference time matches '(\d\d\d\d)-(\d+)-(\d+)';
    * 'partial_date' -- if reference time matches '(\d\d\d\d)-(XX-XX|\d+-XX)';
    * 'unknown_date' -- all remaining reference times;
    '''
    if re.match(r'(\d\d\d\d)-(\d+)-(\d+)', ref_time):
        return 'full_date'
    elif re.match(r'(\d\d\d\d)-(XX-XX|\d+-XX)', ref_time):
        return 'partial_date'
    else:
        return 'unknown_date'

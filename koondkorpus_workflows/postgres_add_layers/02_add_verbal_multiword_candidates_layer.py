#
#  Add verbal multiword expression candidates to a collection.
#  Command line arguments:
#  * CONFIGURATION_INI -- [database] -> (schema, role, pgpass_file(optional)),
#                         [tagger] -> (collection, output_layer, input layers and flags).
#  * PGPASS_FILE(optional) -- connection file, overridden by pgpass_file from INI.
#  * MODULE,REMAINDER(optional) -- process one block of documents.
#  * "APPEND"(optional) -- continue an existing layer or block.
#

import logging
import re, sys
import os, os.path
import configparser
from datetime import datetime

from estnltk import logger
from estnltk.taggers import VerbalMultiWordExprPreTagger
from estnltk.storage.postgres import PostgresStorage


def init_logger( level, block ):
    logger.setLevel( level )
    if block is None:
        logfile = sys.argv[0]+'.log'
    else:
        logfile = sys.argv[0]+'__block'+str(block[0])+'_'+str(block[1])+'.log'
    f_handler = logging.FileHandler( logfile, mode='w', encoding='utf-8' )
    f_handler.setLevel( level )
    f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    return logger


if __name__ == '__main__':
    # =============================================
    #   Parse configuration
    # =============================================
    pgpass_file = None
    conf_file   = None
    block = None
    mode = None
    for item in sys.argv[1:]:
        if item.lower().endswith('.ini'):
            conf_file = item
        elif item.upper() == 'APPEND':
            mode = 'append'
        elif re.fullmatch(r'\d+[,:;]\d+', item):
            block = tuple(map(int, re.split('[,:;]', item)))
            if not 0 <= block[1] < block[0]:
                raise ValueError('Expected a block N,I with 0 <= I < N.')
        else:
            pgpass_file = item
    if conf_file is None:
        raise ValueError('Missing command line argument: name of the configuration file.')

    config = configparser.ConfigParser()
    config_file_full = os.path.abspath(os.path.expanduser(os.path.expandvars(conf_file)))
    if len(config.read(config_file_full, encoding='utf-8')) != 1:
        raise ValueError('File {} is not accessible or is not in valid INI format'.format(config_file_full))
    for section_name, options in [('database', ['schema', 'role']),
                                  ('tagger', ['collection', 'output_layer'])]:
        if not config.has_section(section_name):
            raise ValueError('Error in file {}: missing section [{}]'.format(config_file_full, section_name))
        for option in options:
            if not config.has_option(section_name, option):
                raise ValueError('Error in file {}: missing option {} in section [{}]'.format(
                    config_file_full, option, section_name))

    pgpass_file = config['database'].get('pgpass_file', pgpass_file)
    if pgpass_file is None:
        raise ValueError('Missing parameter: name of the pgpass file, e.g. "pgpass.txt"')
    pgpass_file = os.path.expanduser(os.path.expandvars(pgpass_file))
    if not os.path.isfile(pgpass_file):
        raise FileNotFoundError('Non-existent pgpass file: {}'.format(pgpass_file))

    settings = config['tagger']
    sparse = settings.getboolean('sparse', False)
    dry_run = settings.getboolean('dry_run', False)

    # =============================================
    #  Initialize tagger
    # =============================================
    tagger = VerbalMultiWordExprPreTagger(
        output_layer=settings['output_layer'],
        words_layer=settings.get('words_layer', 'words'),
        sentences_layer=settings.get('sentences_layer', 'sentences'),
        clauses_layer=settings.get('clauses_layer', 'clauses'),
        syntax_layer=settings.get('syntax_layer', 'stanza_syntax'),
        include_nonfinite=settings.getboolean('include_nonfinite', False),
        include_compound_verbs=settings.getboolean('include_compound_verbs', False),
        include_copula_heads=settings.getboolean('include_copula_heads', True),
        lexicon_path=settings.get('lexicon_path', None),
        max_components=settings.getint('max_components', 5),
    )

    # =============================================
    #  Logging and database connection
    # =============================================
    log = init_logger( logging.DEBUG, block )
    storage = PostgresStorage(pgpass_file=pgpass_file,
                              schema=config['database']['schema'],
                              role=config['database']['role'],
                              options='-c default_transaction_read_only=on' if dry_run else '')
    try:
        collection = storage[settings['collection']]
        missing = set(tagger.input_layers) - set(collection.layers)
        if missing:
            raise ValueError('Collection {!r} misses input layers {!r}.'.format(collection.name, sorted(missing)))
        log.info(' Collection {!r}: layer {!r}, sparse={!r}, block={!r}.'.format(
            collection.name, tagger.output_layer, sparse, block))
        startTime = datetime.now()
        if not dry_run:
            if tagger.output_layer not in collection.layers and (block is not None or mode == 'append'):
                log.info(" Initializing collection's layer")
                collection.add_layer(tagger.get_layer_template(), sparse=sparse)
            if block is not None:
                collection.create_layer_block(tagger, block, mode=mode)
            else:
                collection.create_layer(tagger=tagger, progressbar='ascii', mode=mode, sparse=sparse)
        else:
            log.info(' [dry_run mode]: configuration and input layers checked.')
        log.info('Total processing time: {}'.format(datetime.now() - startTime))
    finally:
        storage.close()

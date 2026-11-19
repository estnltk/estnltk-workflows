#
#  Create a missing GIN index for a detached layer of a collection.
#  Command line arguments:
#  * CONFIGURATION_INI -- name of the configuration INI file, provides info about:
#                         [database] -> (schema, role, pgpass_file(optional)),
#                         [layer]    -> (collection, name)
#  * PGPASS_FILE(optional) -- pgpass file providing "host:port:dbname:user:password".
#                             this will be overridden by pgpass_file from INI file.
#

import logging
import sys
import os, os.path
import configparser
from datetime import datetime

from estnltk import logger
from estnltk.storage.postgres import PostgresStorage
from estnltk.storage.postgres import get_index_name_hash, index_exists, layer_table_name


def init_logger( level ):
    logger.setLevel( level )
    logfile = sys.argv[0]+'.log'
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
    for item in sys.argv[1:]:
        if item.lower().endswith('.ini'):
            conf_file = item
        else:
            pgpass_file = item
    if conf_file is None:
        raise ValueError('Missing command line argument: name of the configuration file, e.g. "configuration.ini"')

    config = configparser.ConfigParser()
    config_file_full = os.path.abspath(os.path.expanduser(os.path.expandvars(conf_file)))
    if len(config.read(config_file_full, encoding='utf-8')) != 1:
        raise ValueError('File {} is not accessible or is not in valid INI format'.format(config_file_full))
    for section_name, options in [('database', ['schema', 'role']),
                                  ('layer', ['collection', 'name'])]:
        if not config.has_section(section_name):
            raise ValueError('Error in file {}: missing section [{}]'.format(config_file_full, section_name))
        for option in options:
            if not config.has_option(section_name, option):
                raise ValueError('Error in file {}: missing option {} in section [{}]'.format(
                    config_file_full, option, section_name))

    # Override the command line pgpass file with the file from configuration.
    pgpass_file = config['database'].get('pgpass_file', pgpass_file)
    if pgpass_file is None:
        raise ValueError('Missing parameter: name of the pgpass file, e.g. "pgpass.txt"')
    pgpass_file = os.path.expanduser(os.path.expandvars(pgpass_file))
    if not os.path.isfile(pgpass_file):
        raise FileNotFoundError('Non-existent pgpass file: {}'.format(pgpass_file))

    # =============================================
    #  Logging
    # =============================================
    log = init_logger( logging.DEBUG )

    # =============================================
    #  Connect DB
    # =============================================
    storage = PostgresStorage(pgpass_file=pgpass_file,
                              schema=config['database']['schema'],
                              role=config['database']['role'])
    try:
        target_collection = config['layer']['collection']
        collection = storage[target_collection]
        layer_name = config['layer']['name']
        if collection.structure[layer_name]['layer_type'] != 'detached':
            raise ValueError('Expected a detached layer: {!r}.'.format(layer_name))

        # =============================================
        #  Create layer index
        # =============================================
        table = layer_table_name(target_collection, layer_name)
        suffix = 'relations' if collection.is_relation_layer(layer_name) else 'spans'
        index_name = get_index_name_hash('{}_{}'.format(table, suffix))
        if index_exists(storage, index_name):
            log.info(' Collection {!r}: index for {!r} already exists.'.format(
                target_collection, layer_name))
        else:
            log.info(' Collection {!r}: creating index for {!r}.'.format(
                target_collection, layer_name))
            startTime = datetime.now()
            collection.create_layer_index(layer_name)
            log.info(' Collection {!r}: index for {!r} is ready.'.format(
                target_collection, layer_name))
            log.info('Total processing time: {}'.format(datetime.now() - startTime))
    finally:
        storage.close()

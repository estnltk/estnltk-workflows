import os, sys
import os.path
import logging

# A temporary solution getting the info out of the pgpass file
# ( because passing pgpass_file to PostgresStorage does not work currently ... )
def read_info_from_pgpass_file( fname ):
    assert os.path.exists(fname), '(!) pgpass file {!r} not found.'.format(fname)
    with open( fname, 'r', encoding='utf-8' ) as f:
        content = f.read()
        host, port, dbname, user, passwd = content.split(':')
        mapping = {}
        mapping['host'] = host
        mapping['port'] = port
        mapping['dbname'] = dbname
        mapping['user'] = user
        mapping['passwd'] = passwd
        return mapping
    return None


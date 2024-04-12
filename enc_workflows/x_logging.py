#
#   Logger creation and handling. 
# 

import logging
from estnltk import logger as estnltk_logger

def init_logger( log_file_name, level, block ):
    estnltk_logger.setLevel( level )
    if block is None:
        logfile = log_file_name+'.log'
    else:
        logfile = log_file_name+'__block'+str(block[0])+'_'+str(block[1])+'.log'
    f_handler = logging.FileHandler( logfile, mode='w', encoding='utf-8' )
    f_handler.setLevel( level )
    f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')
    f_handler.setFormatter(f_format)
    estnltk_logger.addHandler(f_handler)
    return estnltk_logger


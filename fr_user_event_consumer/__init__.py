import logging

DEFAULT_CONFIG_FILENAMES = [ './config.yaml', '/etc/fr_user_events_consumer/config.yaml' ]

LOG_FORMAT = '{levelname} {msg} ({filename} line {lineno})'

def setup_logging( debug ):
    logging.basicConfig( format = LOG_FORMAT, style = '{' )

    if debug:
        logging.root.setLevel( level = logging.DEBUG ) 
    else:
        logging.root.setLevel( level = logging.INFO )

    # TODO Make INFO go to stdout, or find some other solution for sending messages there

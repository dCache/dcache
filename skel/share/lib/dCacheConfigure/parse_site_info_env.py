import os
import logging
logger = logging.getLogger("dCacheConfigure.parse_site_info_env")

def parse_env():
    output = {}
    for param in os.environ.keys():
        output[param] = os.environ[param]
    return output

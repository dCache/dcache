import logging
logger = logging.getLogger("dCacheConfigure.validate_site_info")

class evalidate(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


var_layout_required = set([
    'DCACHE_ADMIN',
    'DCACHE_POOLS',
])

var_layout_optional_default_admin = set([
    'DCACHE_NAME_SERVER',
    'DCACHE_HTTPD',
    'DCACHE_DOOR_SRM',
])
var_layout_optional_default_pool = set([
    'DCACHE_DOOR_DCAP',
    'DCACHE_DOOR_GSIDCAP',
    'DCACHE_DOOR_GSIFTP',
    'DCACHE_DOOR_LDAP',
    'DCACHE_DOOR_XROOTD',
    'DCACHE_PROVIDER_INFO',
    'DCACHE_PROVIDER_WEB',
])

def required_present(config):
    for i in var_layout_required:
        if not i in config:
            msg = "a value for '%s' was not found in the site-info.def" % (i)
            raise evalidate(msg)
    return True

def decompose_site_info_var(inputstring):
    cleaned = []
    decomposed = inputstring.split(' ')
    for hostdef in decomposed:
        if hostdef != '':
            pobj = hostdef.strip().split(':')
            if len(pobj) == 2:
                cleaned.append([pobj[0].strip(), pobj[1].strip()])
            if len(pobj) == 1:
                cleaned.append([pobj[0].strip(),None])
    #print "cleaned=%s" % (cleaned)
    return cleaned

def default_dcache_vars(config):
    for i in var_layout_required:
        if not i in config:
            raise siteinfodef("Required variable '%s' not found." % (i))
    if not "DCACHE_NAME_SERVER" in config:
        if "DCACHE_PNFS_SERVER" in config:
            config["DCACHE_NAME_SERVER"] = config["DCACHE_PNFS_SERVER"]
        if "DCACHE_CHIMERA_SERVER" in config:
            config["DCACHE_NAME_SERVER"] = config["DCACHE_CHIMERA_SERVER"]

    for i in var_layout_optional_default_admin:
        if not i in config:
            config[i] = config['DCACHE_ADMIN']

    parsedline = decompose_site_info_var(config['DCACHE_POOLS'])
    defaultpoolsstr = " "
    for i in parsedline:
        defaultpoolsstr += i[0] + " "
    defaultpoolsstr = defaultpoolsstr.strip()
    for i in var_layout_optional_default_pool:
        if not i in config:
            config[i] = defaultpoolsstr
    if ("DCACHE_CHIMERA_SERVER" in config) and ("DCACHE_PNFS_SERVER" in config):
        msg = "Both 'DCACHE_CHIMERA_SERVER' and 'DCACHE_PNFS_SERVER' where found in the configuration this is not allowed."
        logger.error(msg)
        raise evalidate(msg)
    if not "DCACHE_NAME_SERVER" in config:
        msg = "No value for 'DCACHE_NAME_SERVER' was found in the configuration."
        logger.error(msg)
        raise evalidate(msg)
    if (not "DCACHE_PNFS_SERVER" in config) and (not "DCACHE_CHIMERA_SERVER" in config) and ("DCACHE_NAME_SERVER" in config):
        # Default "DCACHE_CHIMERA_SERVER" to "DCACHE_NAME_SERVER" if possible
        config["DCACHE_CHIMERA_SERVER"] = config["DCACHE_NAME_SERVER"]
    return config




def validate(config):
    required_present(config)
    validated = default_dcache_vars(config)
    return validated

import logging

logger = logging.getLogger("dCacheConfigure.putils.org_dcache_cfg_default")
misc = set([
    'DCACHE_PORT_RANGE_PROTOCOLS_CLIENT_GSIFTP',
    'DCACHE_PORT_RANGE_PROTOCOLS_CLIENT_GSIFTP',
    'DCACHE_PORT_RANGE_PROTOCOLS_SERVER_GSIFTP',
    'DCACHE_PORT_RANGE_PROTOCOLS_SERVER_MISC',
    'PNFSROOT',
    'POSTGRESQL_HOME',
    'POSTGRESQL_LOG',
    'RESET_DCACHE_CONFIGURATION',
    'RESET_DCACHE_RDBMS',
    'DCACHE_PNFS_VO_DIR',
    'DCACHE_CHIMERA_SERVER',
    'DCACHE_PNFS_SERVER',

    ])

var_layout_required = set([
    'DCACHE_ADMIN',
    'DCACHE_POOLS',
])

var_layout_optional_default_admin = set([
    'DCACHE_NAME_SERVER',
    'DCACHE_HTTPD',
    'DCACHE_DOOR_SRM',
    'DCACHE_PROVIDER_INFO',
    'DCACHE_PROVIDER_WEB',
])

var_layout_optional_default_pool = set([
    'DCACHE_DOOR_DCAP',
    'DCACHE_DOOR_GSIDCAP',
    'DCACHE_DOOR_GSIFTP',
    'DCACHE_DOOR_LDAP',
    'DCACHE_DOOR_XROOTD',
    'DCACHE_DOOR_WEBDAV',
])

var_layout_optional_default_namespace = set([
    'DCACHE_PNFSMANAGER',
    'DCACHE_CLEANER',
    'DCACHE_ACL',
])


var_layout_optional = var_layout_optional_default_admin.union(var_layout_optional_default_pool)

var_layout = var_layout_required.union(var_layout_optional)
var_layout = var_layout.union(var_layout_optional_default_namespace)


var_grid = set([
    'VOS',
])

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
    return cleaned


def config_default(config):
    found_req = True
    for i in var_layout_required:
        if not i in config:
            logger.warning("Required variable '%s' not found." % (i))
            found_req = False
    if not found_req:
        return config
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
        logger.critical("you cannot specifiy chimera and pnfs in one site.info.def")
        raise "misconfiguration"
    if not "DCACHE_NAME_SERVER" in config:
        raise "misconfiguration"
    if not "DCACHE_PNFS_SERVER" in config:
        if not "DCACHE_CHIMERA_SERVER" in config:
            if "DCACHE_NAME_SERVER" in config:
                config["DCACHE_CHIMERA_SERVER"] = config["DCACHE_NAME_SERVER"]
    else:
        if "DCACHE_CHIMERA_SERVER" in config:
            pass
    return config

def get_host_set(config):
    hosts_set = set([])
    for i in var_layout:
        if i in config.keys():
            config_list = decompose_site_info_var(config[i])
            for hostconf in config_list:
                if len(hostconf) > 0:
                    hosts_set.add(hostconf[0])
    return hosts_set

def get_services_set(config,hostname):
    services = set([])
    for i,j in config.iteritems():
        parsed = decompose_site_info_var(j)
        for k in parsed:
            if len(k) > 0:
                thishost = k[0]
                if thishost == hostname:
                    services.add(i)
    return services


import os.path
import logging

logger = logging.getLogger("dCacheConfigure.putils.org_dcache_cfg_query")

## Code to be moved to a util module

def get_dcache_path_home(cfg):
    output = '/opt/d-cache/'
    return output

def get_dcache_path_launcher(cfg):
    prefix = get_dcache_path_home(cfg)
    return os.path.join(prefix,'bin/dcache')

def get_dcache_path_keys(cfg):
    prefix = get_dcache_path_home(cfg)
    return os.path.join(prefix,'config')

def get_dcache_path_chimera_cli(cfg):
    prefix = get_dcache_path_home(cfg)
    return os.path.join(prefix,'libexec/chimera/chimera-cli.sh')

def get_dcap_doors_port_default():
    return 22125

def get_gsidcap_doors_port_default():
    return 22128

def get_gridftp_doors_port_default():
    return 2811

def get_srm_doors_port_default():
    return 8443

def get_webdav_doors_port_default():
    return 2880

def get_xrootd_doors_port_default():
    # What is this port
    return 1094

## Code to be move to postgresql util module


def get_postgresql_path_home(cfg):
    ret = '/var/lib/pgsql'
    if 'POSTGRESQL_HOME' in cfg.keys():
        ret = cfg['POSTGRESQL_HOME'].strip()
    return ret

def get_postgresql_path_log(cfg):
    if 'POSTGRESQL_LOG' in cfg.keys():
        return cfg['POSTGRESQL_LOG'].strip()
    return get_postgresql_path_home(cfg) + '/pgstartup.log'

## Code to query if we should reset things.



## Code to query which services are existing

def get_servers_by_key(cfg,key):
    output = []
    if key in cfg.keys():
        raw_list_of_doors = cfg[key].split(' ')
        for item in raw_list_of_doors:
            cleaned_item = item.strip()
            if len(cleaned_item) == 0:
                continue
            host = None
            output.append([cleaned_item])
    return output

def get_admin_servers(cfg):
    output = []
    for item in get_servers_by_key(cfg,'DCACHE_ADMIN'):
        details = {'host':item[0]}
        output.append(details)
    return output


def get_abstract_doors(cfg,door_type,siteinfo_filter):
    output = []
    if siteinfo_filter in cfg.keys():
        raw_list_of_doors = cfg[siteinfo_filter].split(' ')
        for item in raw_list_of_doors:
            cleaned_item = item.strip()
            if len(cleaned_item) == 0:
                continue
            item_split = cleaned_item.split(':')
            if len(item_split) == 2:
                output.append({'type':door_type,'host':item_split[0],'port':item_split[1]})
            if len(item_split) == 1:
                output.append({'type':door_type,'host':item_split[0]})
    return output

def get_srm_doors(cfg):
    return get_abstract_doors(cfg,'srm','DCACHE_DOOR_SRM')

def get_dcap_doors(cfg):
    return get_abstract_doors(cfg,'dcap','DCACHE_DOOR_DCAP')

def get_gsidcap_doors(cfg):
    return get_abstract_doors(cfg,'gsidcap','DCACHE_DOOR_GSIDCAP')

def get_gridftp_doors(cfg):
    return get_abstract_doors(cfg,'gridftp','DCACHE_DOOR_GSIFTP')

def get_xrootd_doors(cfg):
    return get_abstract_doors(cfg,'xrootd','DCACHE_DOOR_XROOTD')

def get_webdav_doors(cfg):
    return get_abstract_doors(cfg,'webdav','DCACHE_DOOR_WEBDAV')

def get_chimera_servers(cfg):
    output = []
    for item in get_servers_by_key(cfg,'DCACHE_CHIMERA_SERVER'):
        details = {'host':item[0]}
        output.append(details)
    return output

def get_pnfs_servers(cfg):
    output = []
    for item in get_servers_by_key(cfg,'DCACHE_PNFS_SERVER'):
        details = {'host':item[0]}
        output.append(details)
    return output

def get_name_servers(cfg):
    nameservers = get_servers_by_key(cfg,'DCACHE_NAME_SERVER')
    if len(nameservers) > 0:
        return nameservers
    chimera_servers = get_chimera_servers(cfg)
    pnfs_servers = get_pnfs_servers(cfg)
    if (len(chimera_servers) > 0) and (len(pnfs_servers) > 0):
        logger.critical("Only DCACHE_PNFS_SERVER or DCACHE_NAME_SERVER can be used for dCache both are specified")
        sys.exit(1)
    if (len(chimera_servers) > 0):
        return chimera_servers
    return pnfs_servers


def get_pool_servers(cfg):
    # Returns a list of lists of pools in format
    # [{'host':host,'path':path,'size':size},{'host':host,'path':path}]
    # Note size is not garenteed to exist
    output = []
    poolserver_raw = get_servers_by_key(cfg,'DCACHE_POOLS')
    if len(poolserver_raw) == 0:
        return output
    for pool_raw_item in poolserver_raw:
        pool_details = None
        pool_decomposed = pool_raw_item[0].split(':')
        if len(pool_decomposed) == 3:
            pool_details = {'type':'pool','host':pool_decomposed[0],'path':pool_decomposed[2],'size':pool_decomposed[1]}
        if len(pool_decomposed) == 2:
            pool_details = {'type':'pool','host':pool_decomposed[0],'path':pool_decomposed[1]}
        if pool_details != None:
            output.append(pool_details)
        else:
            logger.warning("Warning: ignoring configuration of DCACHE_POOLS for '%s'" % (pool_raw_item))
            continue
    return output


def is_admin(cfg,hostname):
    rc = False
    doors = get_admin_servers(cfg)
    for door in doors:
        if door['host'] == hostname:
            rc = True
            break
    return rc


def is_srm_door(cfg,hostname):
    rc = False
    doors = get_srm_doors(cfg)
    for door in doors:
        if door['host'] == hostname:
            rc = True
            break
    return rc


def is_name_server(cfg,hostname):
    rc = False
    servers = get_name_servers(cfg)
    for server in servers:
        if server[0] == hostname:
            rc = True
            break
    return rc

def is_chimera_server(cfg,hostname):
    rc = False
    servers = get_chimera_servers(cfg)
    for server in servers:
        if server['host'] == hostname:
            rc = True
            break
    return rc

def is_pnfs_server(cfg,hostname):
    rc = False
    servers = get_pnfs_servers(cfg)
    for server in servers:
        if server['host'] == hostname:
            rc = True
            break
    return rc

def is_info_server(cfg,hostname):
    should_default = True
    usekey = ''
    if 'DCACHE_DOOR_LDAP' in cfg.keys():
        usekey = 'DCACHE_DOOR_LDAP'
    if 'DCACHE_PROVIDER_INFO' in cfg.keys():
        usekey = 'DCACHE_PROVIDER_INFO'
    if usekey == "":
        return is_admin(cfg,hostname)
    if usekey in cfg.keys():
        info_split = cfg[usekey].strip().split(' ')
        for item in info_split:
            if item.strip() == hostname:
                return True
    return False

def is_dbserver(cfg,hostname):
    if is_srm_door(cfg,hostname):
        return True
    if is_name_server(cfg,hostname):
        return True
    return False




def should_reset_config(cfg):
    rc = False
    if 'RESET_DCACHE_CONFIGURATION' in cfg.keys():
        out = cfg['RESET_DCACHE_CONFIGURATION'].strip().upper()
        if (out == 'YES') or (out == 'Y'):
            rc = True
    return rc

def should_reset_db(cfg):
    rc = False
    if 'RESET_DCACHE_RDBMS' in cfg.keys():
        out = cfg['RESET_DCACHE_RDBMS'].strip().upper()
        if (out == 'YES') or (out == 'Y'):
            rc = True
    return rc

def should_reset_chimera(cfg):
    rc = False
    if 'RESET_DCACHE_CHIMERA' in cfg.keys():
        out = cfg['RESET_DCACHE_CHIMERA'].strip().upper()
        if (out == 'YES') or (out == 'Y'):
            rc = True
    return rc

def namespace_dir_get_root_vo(cfg):
    if 'DCACHE_CHIMERA_VO_DIR' in cfg.keys():
        return cfg['DCACHE_CHIMERA_VO_DIR']
    if 'DCACHE_PNFS_VO_DIR' in cfg.keys():
        return cfg['DCACHE_PNFS_VO_DIR']
    return "/pnfs/%s/data" % (cfg['MY_DOMAIN'])

def get_chimera_cli_path(cfg):
    prefix = get_dcache_path_home(cfg)
    return os.path.join(prefix,"libexec/chimera/chimera-cli.sh")


def get_supported_vos(cfg):
    if not 'VOS' in cfg.keys():
        return []
    volist = cfg['VOS'].split(' ')
    return volist
    output = []
    for vo in volist:
        if ((len(vo) > 0) and (not vo in output)):
            output.append(vo)
    return vo

def get_site_name(cfg):
    if 'SITE_NAME' in cfg.keys():
        return cfg['SITE_NAME']
    return 'mysitename'

def get_site_uid(cfg):
    if 'SITE_UID' in cfg.keys():
        return cfg['SITE_UID']
    if 'SITE_NAME' in cfg.keys():
        return cfg['SITE_NAME']
    return 'mysitename'

def get_glue_se_site_name(cfg):
    if 'DCACHE_GLUE_SENAME' in cfg.keys():
        return cfg['DCACHE_GLUE_SENAME']
    return None


def get_glue_se_GlueSEUniqueID(cfg,hostname):
    if 'DCACHE_GLUE_SENAME' in cfg.keys():
        return cfg['DCACHE_GLUE_SENAME']
    adminservers = get_admin_servers(cfg)
    if len(adminservers) > 0:
        return "dcache@%s:SRM" % adminservers[0]['host']
    else:
        return None


def get_glue_se_GlueSEStatus(cfg):
    if 'DCACHE_GLUE_SESTATUS' in cfg.keys():
        return cfg['DCACHE_GLUE_SESTATUS']
    return "Production"

def get_glue_se_GlueSEArchitecture(cfg):
    if 'DCACHE_GLUE_SEARCHITECTURE' in cfg.keys():
        return cfg['DCACHE_GLUE_SEARCHITECTURE']
    return "disk"

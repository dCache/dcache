import dCacheConfigure.putils.org_dcache_cfg_default as dcache_cfg_default
import dCacheConfigure.putils.org_dcache_cfg_layout as layout
import dCacheConfigure.putils.org_file_cfg as org_file_cfg
import dCacheConfigure.putils.org_dcache_cfg_query as org_dcache_cfg_query

import time
from datetime import datetime
import os.path
import shutil
import sys
import logging

logger = logging.getLogger("dCacheConfigure.pmods.org_dcache_cfg_layout")


def layout_filename_generate(cfg,hostname):
    rootdir = org_dcache_cfg_query.get_dcache_path_home(cfg)
    new_hostname = hostname
    new_hostname.replace('.', '_')
    layout_cfg = 'etc/layouts/dCacheConfigure-%s.conf' % (new_hostname)
    return os.path.join(rootdir,layout_cfg)

def dcache_conf_filename(cfg):
    rootdir = org_dcache_cfg_query.get_dcache_path_home(cfg)
    extention = "etc/dcache.conf"
    return os.path.join(rootdir,extention)

def poolid_from_hostid(hostid,path):
    dname = "%s%s" % (hostid,path)
    for illegalchar in ['/',':','@']:
        dname = dname.replace(illegalchar,'_')
    return dname

def service_dict_to_domain_name_default(service,hostid):
    """ Generate the default domain name for a service.
    service is a dictionary of all the properties
    hostid is a string identifier for the host
    """
    if not service.has_key("type"):
        # Return nothing if no matching type
        return None
    service_type = service["type"]
    service_domain_mapping = {
        'admin'           : 'dCacheDomain',
        'broadcast'       : 'dCacheDomain',
        'topo'            : 'dCacheDomain',
        'pnfsmanager'     : 'nameserver',
        'poolmanager'     : 'nameserver',
        'dir'             : 'nameserver',
        'acl'             : 'nameserver',
        'nfsv3'           : 'nameserver',
        'cleaner'         : 'nameserver',
        'gplazma'         : 'dCacheDomain',
        'httpd'           : 'dCacheDomain',
        'loginbroker'     : 'dCacheDomain',
        'replica'         : 'replica',
        'info'            : 'info',
        'statistics'      : 'statistics',
        'webadmin'        : 'webadmin',
        'srm-loginbroker' : 'srm',
        'srm'             : 'srm',
        'spacemanager'    : 'srm',
        'pinmanager'      : 'srm',
        'transfermanagers' : 'srm',
        }
    if service_domain_mapping.has_key(service_type):
        return service_domain_mapping[service_type]
    # Now we know its not a simple name

    host_name_comains = [
        'dcap',
        'gsidcap',
        'gridftp',
        'xrootd',
        'webdav',
        'httpdoor',
        'kerberosdcap',
        'kerberosftp',
        ]
    if service_type in host_name_comains:
        return str("%s-domain-%s" % (service_type,hostid))
    if service_type == "pool":
        if not service.has_key('path'):
            return None
        dname = "pool-domain-%s" % (poolid_from_hostid(hostid,service['path']))
        for illegalchar in ['/',':','@']:
            dname = dname.replace(illegalchar,'_')
        return dname
    return None


def get_service_for_host(cfg,hostname):
    output = []
    if org_dcache_cfg_query.is_admin(cfg,hostname):
        output.extend([
            {'type':'gplazma'},
            {'type':'httpd'},
            {'type':'loginbroker'},
            {'type':'admin'},
            {'type':'broadcast'},
            {'type':'topo'},
            {'type':'webadmin'},
            ])
    if org_dcache_cfg_query.is_name_server(cfg,hostname):
        output.extend([
            {'type':'pnfsmanager'},
            {'type':'poolmanager'},
            {'type':'acl'},
            {'type':'dir'},
            {'type':'cleaner'},
            ])
    if org_dcache_cfg_query.is_pnfs_server(cfg,hostname):
        output.extend([
            ])
    if org_dcache_cfg_query.is_chimera_server(cfg,hostname):
        output.extend([
            {'type':'nfsv3'},
            ])
    if org_dcache_cfg_query.is_srm_door(cfg,hostname):
        output.extend([
            {'type':'pinmanager'},
            {'type':'spacemanager'},
            {'type':'srm-loginbroker'},
            {'type':'srm'},
            {'type':'transfermanagers'},
            ])
    if org_dcache_cfg_query.is_info_server(cfg,hostname):
        output.extend([
            {'type':'info'},
            ])
    doors = org_dcache_cfg_query.get_dcap_doors(cfg)
    doors.extend(org_dcache_cfg_query.get_gsidcap_doors(cfg))
    doors.extend(org_dcache_cfg_query.get_gridftp_doors(cfg))
    doors.extend(org_dcache_cfg_query.get_xrootd_doors(cfg))
    doors.extend(org_dcache_cfg_query.get_webdav_doors(cfg))
    pools = org_dcache_cfg_query.get_pool_servers(cfg)
    for pool in pools:
        if (not pool.has_key('name')) and (pool.has_key('path')):
            pool['name'] = poolid_from_hostid(hostname,pool['path'])
            doors.append(pool)
    for door in doors:
        if door.has_key('host'):
            if door['host'] == hostname:
                if not door in output:
                    output.append(door)
    return output

def service_clean(service):
    output = {}
    for key in service.keys():
        if not key in ['host','type']:
            output[key] = service[key]
    return output

def services_match(service1, service2):
    obj1 = service_clean(service1)
    obj2 = service_clean(service2)
    for key in obj1.keys():
        if not key in ['host','type']:
            if not obj2.has_key(key):
                return False
            if obj1[key] != obj2[key]:
                return False
    for key in obj2.keys():
        if not key in ['host','type']:
            if not obj1.has_key(key):
                return False
    return True

def config_layout(layout_obj,cfg,hostname):
    all_service_types = ['admin', 'broadcast', 'topo', 'pnfsmanager',
        'poolmanager', 'dir', 'acl', 'nfsv3', 'cleaner',
        'gplazma', 'httpd', 'loginbroker', 'info', 'srm-loginbroker',
        'srm', 'spacemanager', 'pinmanager', 'dcap', 'gsidcap', 'gridftp',
        'xrootd', 'webdav', 'pool','transfermanagers','webadmin']
    hostId = hostname.split('.')[0]
    defaulted = dcache_cfg_default.config_default(cfg)
    wanted_services = get_service_for_host(defaulted,hostname)
    # Create a list of service types we want
    wanted_service_types = []
    for service in wanted_services:
        service_type = service['type']
        if not service_type in wanted_service_types:
            wanted_service_types.append(service_type)
    # Process each service type
    found_services = []
    unmatched_services = []
    for service_type in all_service_types:
        # Get list of defined services
        defined_services_properties = layout_obj.get_service_dict_by_type(service_type)
        if service_type in wanted_service_types:
            for defined_service_properties in defined_services_properties:
                found = False
                for wanted_service in wanted_services:
                    if wanted_service['type'] != service_type:
                        continue
                    if services_match(wanted_service, defined_service_properties):
                        found_services.append(wanted_service)
                        found = True

                if not found:
                    # We know this service is not matching a wanted service
                    # in its properties
                    clean = {'type' : service_type}
                    for key in defined_service_properties:
                        clean[key] = defined_service_properties[key]
                    unmatched_services.append(clean)
        else:
            # Remove all services of the types we dont want
            for this_unwanted in defined_services_properties:
                clean = {'type' : service_type}
                layout_obj.comment_service_dict_by_type_properties(clean['type'],service_clean(clean))

    # Check the properties of all services we do want and store found comment others.
    for service in unmatched_services:
        defined_services_properties = layout_obj.get_service_dict_by_type(service['type'])
        for defined_service_properties in defined_services_properties:
            if services_match(service, defined_service_properties):
                layout_obj.comment_service_dict_by_type_properties(service['type'],service_clean(service))
    for service in wanted_services:
        if not service in found_services:
            domain_name = service_dict_to_domain_name_default(service,hostId)
            layout_obj.create_service_dict_by_type_properties(domain_name,service['type'],service_clean(service))
    return layout_obj


def org_dcache_cfg_layout_check(cfg,hostname):
    defaulted = dcache_cfg_default.config_default(cfg)

def org_dcache_cfg_layout_run(cfg,hostname):
    host_name = hostname.replace('.', '_')
    defaulted = dcache_cfg_default.config_default(cfg)
    layout_cfg_path = layout_filename_generate(defaulted,host_name)

    layout_cfg = org_file_cfg.file_cfg(layout_cfg_path)
    layout_cfg.wc_open()

    # Load Layout controler
    layoutobj = layout.lcntrl()
    layout_cfg_file = layout_cfg.wc_get()
    if os.path.isfile(layout_cfg_file):
        logger.debug("Loading %s" % layout_cfg_file)
        layoutobj.load(layout_cfg_file)
    layoutobj = config_layout(layoutobj,defaulted,hostname)
    layoutobj.save(layout_cfg_file)
    layout_cfg.wc_close()
    conffilepath = dcache_conf_filename(defaulted)
    conf_file_path , conf_file_ext = os.path.splitext(layout_cfg_path)

    layout_cfg_path_dir, layout_cfg_path_file = os.path.split(conf_file_path)


    dcache_conf = org_file_cfg.file_cfg_kv(conffilepath)

    dcache_conf.wc_open()
    dcache_conf.kv_set("dcache.layout",layout_cfg_path_file)
    adminlist = org_dcache_cfg_query.get_admin_servers(defaulted)
    if len(adminlist) > 0:
        dcache_conf.kv_set("serviceLocatorHost",adminlist[0]['host'])
        dcache_conf.kv_set("httpHost",adminlist[0]['host'])
    dcache_conf.wc_close()
    return True

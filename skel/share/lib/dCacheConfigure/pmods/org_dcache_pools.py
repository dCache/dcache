import dCacheConfigure.putils.org_dcache_cfg_default as dcache_cfg_default
import dCacheConfigure.putils.org_dcache_cfg_query as org_dcache_cfg_query
import os
import commands
import logging

logger = logging.getLogger("dCacheConfigure.pmods.org_dcache_pools")

def size_by_path(path):
    workingpath = path
    while not os.path.isdir(workingpath):
        workingpath =  os.path.dirname(workingpath)

    # Get available disk space by path
    diskdetails = os.statvfs(workingpath)
    # so we need to multiply by the block size to get the space free in bytes
    capacity = diskdetails.f_bsize * diskdetails.f_blocks
    available = diskdetails.f_bsize * diskdetails.f_bavail
    # Now the available GB
    available_gb = available/1.073741824e9
    return int(round(available_gb))

def pool_check_path(path):
    # Checks if a pool exists. True if a Pool
    #print "checking '%s'" % (path)
    if os.path.isdir(path):
        return True
    return False



def pool_install(cfg,pool_def_list):
    path = pool_def_list['path']
    if not pool_check_path(path):
        size = None
        if 'size' in pool_def_list:
            size = pool_def_list['size']
        if size == None:
            # Get the size of the disk and specify pool size 1Gb smaller.
            size = size_by_path(path) -1
        if size < 4:
            return False
        executable = org_dcache_cfg_query.get_dcache_path_launcher(cfg)
        cmd = "%s pool create %sG %s" % (executable,size,path)
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            logger.error("creating pool with cmd='%s'" % (cmd))
            logger.warning(output)
            return False
    return True

def org_dcache_pools_check(cfg,hostname):
    return True

def org_dcache_pools_run(cfg,hostname):
    defaulted = dcache_cfg_default.config_default(cfg)
    pool_servers = org_dcache_cfg_query.get_pool_servers(cfg)
    for pool_server in pool_servers:
        if not 'host' in pool_server:
            logger.warning("ignoring host '%s'" % (pool_server))
            continue
        if (pool_server['host']) != str(hostname):
            continue
        rc = pool_install(cfg,pool_server)
        if rc != True:
            logger.error("Failed to create pool '%s'" % (pool_server))
            return False
    return True

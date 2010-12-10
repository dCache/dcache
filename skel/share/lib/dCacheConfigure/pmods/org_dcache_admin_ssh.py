import dCacheConfigure.putils.org_dcache_cfg_query as org_dcache_cfg_query
import os.path
import commands
import logging

# this module set up the keys for logging in to the admin interface.

logger = logging.getLogger("dCacheConfigure.pmods.org_dcache_admin_ssh")

def path_for_host_key():
    return "/etc/ssh/ssh_host_key"


def org_dcache_admin_ssh_check(cfg,hostname):
    return org_dcache_cfg_query.is_admin(cfg,hostname)

def org_dcache_admin_ssh_run(cfg,hostname):
    if not org_dcache_cfg_query.is_admin(cfg,hostname):
        return True
    hostdir = org_dcache_cfg_query.get_dcache_path_keys(cfg)
    serverkeypath = os.path.join(hostdir,"server_key")
    if not os.path.isfile(serverkeypath):
        cmd = 'ssh-keygen -b 768 -t rsa1 -f %s -N ""' % (serverkeypath)
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            logger.error("Failed to create serverkey with '%s'" % (cmd))
            logger.info(output)
            return False
    hostkey = os.path.join(hostdir,"host_key")
    if not os.path.isfile(hostkey):
        os.symlink(path_for_host_key(),hostkey)
    return True


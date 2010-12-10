import dCacheConfigure.putils.org_dcache_cfg_query as org_dcache_cfg_query
import pwd
import grp
import os.path
import os
import dCacheConfigure.putils.org_file_cfg as org_file_cfg
import commands
import logging
logger = logging.getLogger("dCacheConfigure.pmods.org_dcache_info")

def info_users_exist():
    for user in ['edguser','edginfo']:
        try:
            pw = pwd.getpwnam(user)
        except:
            logger.critical("User '%s' does not exist." % (user))
            return False
    for group in ['edguser']:
        try:
            gp = grp.getgrnam(group)
        except:
            logger.critical("Group '%s' does not exist." % (group))
            return False
    return True

def config_gip_dcache_info_setup(cfg,hostname):
    info_helper_cms_app = '/opt/d-cache/libexec/infoprovidercms.rb'
    info_helper_cms_in = '/opt/d-cache/etc/glue-1.3.xml.template'
    info_helper_cms_out = '/opt/d-cache/etc/glue-1.3.xml'
    if not os.path.isfile(info_helper_cms_app):
        logger.error("File '%s' does not exist." % (info_helper_cms_app))
        return False

    if os.path.isfile(info_helper_cms_out):
        if org_dcache_cfg_query.should_reset_config(cfg):
            os.remove(info_helper_cms_out)
    if not os.path.isfile(info_helper_cms_in):
        logger.error("Template file '%s' does not exist." % (info_helper_cms_app))
        return False
    out_file_holder = org_file_cfg.file_cfg(info_helper_cms_out)
    working_out = out_file_holder.wc_open()

    cmd = "%s --input %s --output %s " % (info_helper_cms_app,info_helper_cms_in,working_out)
    supported_vos = org_dcache_cfg_query.get_supported_vos(cfg)
    if len(supported_vos) > 0:
        cmd += "--vos %s " % (','.join(supported_vos))
    site_name = org_dcache_cfg_query.get_site_uid(cfg)
    if site_name != None:
        cmd += "--site-unique-id %s " % (site_name)

    se_uid = org_dcache_cfg_query.get_glue_se_GlueSEUniqueID(cfg,hostname)
    if se_uid != None:
        cmd += "--se-unique-id %s " % (se_uid)
    se_name = org_dcache_cfg_query.get_glue_se_site_name(cfg)
    if se_name != None:
        cmd += "--se-name %s " % (se_name)
    se_status = org_dcache_cfg_query.get_glue_se_GlueSEStatus(cfg)
    if se_status != None:
        cmd += "--dcache-status %s " % (se_status)
    se_architecture = org_dcache_cfg_query.get_glue_se_GlueSEArchitecture(cfg)
    if se_architecture != None:
        cmd += "--dcache-architecture %s " % (se_architecture)

    namespace_root = org_dcache_cfg_query.namespace_dir_get_root_vo(cfg)
    if namespace_root != None:
        cmd += "--name-space-prefix %s " % (namespace_root)

    unit2path = []
    unit2vo = []
    vo2path = []
    for vo in supported_vos:
        pathvo = os.path.join(namespace_root,vo)
        pathvogen = os.path.join(pathvo,"generated")
        unit2path.append("%s:STATIC@osm^%s" % (vo,pathvo))
        unit2path.append("%s:GENERATED@osm^%s" % (vo,pathvogen))
        unit2vo.append("%s:STATIC@osm^%s" % (vo,vo))
        unit2vo.append("%s:GENERATED@osm^%s" % (vo,vo))
        vo2path.append("%s^%s" % (vo,pathvo))
    if len(unit2path) > 0:
        cmd += "--unit2path %s " % (",".join(unit2path))
    if len(unit2vo) > 0:
        cmd += "--unit2vo %s " % (",".join(unit2vo))
    if len(vo2path) > 0:
        cmd += "--vo2path %s " % (",".join(vo2path))
    logger.debug("site_name=%s" % (site_name))
    logger.debug("namespace_root=%s" % (namespace_root))
    logger.debug("supported_vos=%s" % (','.join(supported_vos)))
    logger.debug("cmd=%s" % (cmd))
    logger.info("running command '%s'" % (cmd))
    status,output = commands.getstatusoutput(cmd)
    logger.debug("DEBUG:output %s" % (output))
    if status != 0:
        logger.error("command '%s' failed" % (cmd))
        logger.info(output)
        return False
    out_file_holder.wc_close()
    return True

def config_gip_dcache_info_link():
    link_src = "/opt/d-cache/libexec/infoProvider/info-based-infoProvider.sh"
    link_target = "/opt/glite/etc/gip/provider/info-based-infoProvider.sh"
    if os.path.islink(link_target):
        return True
    try:
        os.symlink(link_src,link_target)
    except:
        logger.error("Could not soft link %s to %s" % (link_target,link_src))
        return False
    return True
def org_dcache_info_check(cfg,hostname):
    return org_dcache_cfg_query.is_info_server(cfg,hostname)

def org_dcache_info_run(cfg,hostname):
    # These next two lines should be done by the infrastructure
    if not org_dcache_info_check(cfg,hostname):
        return True
    if not info_users_exist():
        return False
    if not config_gip_dcache_info_setup(cfg,hostname):
        return False
    if not config_gip_dcache_info_link():
        return False

    return True

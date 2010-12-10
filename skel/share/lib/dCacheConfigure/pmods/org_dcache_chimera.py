import dCacheConfigure.putils.org_dcache_cfg_query as org_dcache_cfg_query
import os.path
import commands
import dCacheConfigure.putils.org_pgsql_cfg as org_pgsql_cfg
import dCacheConfigure.putils.org_glite_userhandling as userhandling
import logging

logger = logging.getLogger("dCacheConfigure.pmods.org_dcache_chimera")

def chimera_stop():
    return True

def chimera_start():
    return True

def chimera_database_drop():
    if not 'chimera' in org_pgsql_cfg.postgresql_databases_list():
        # Chimera database already exists
        return True
    #dropdb -U postgres chimera
    cmd = 'dropdb -U postgres chimera'
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        logger.warning("Failed to create serverkey with '%s'" % (cmd))
        logger.info(output)
        return False
    return True

def chimera_database_create(cfg,hostname):
    if 'chimera' in org_pgsql_cfg.postgresql_databases_list():
        # Chimera database already exists
        return
    ddsql = ['libexec/chimera/sql/create.sql','libexec/chimera/sql/pgsql-procedures.sql']
    prefix = org_dcache_cfg_query.get_dcache_path_home(cfg)
    absddsql = []
    for sql in ddsql:
        absddsql.append(os.path.join(prefix,sql))
    org_pgsql_cfg.postgresql_databases_create('chimera','postgres',absddsql,"plpgsql")


def chimera_namespace_setup(cfg,hostname):
    chimeraclicmd = org_dcache_cfg_query.get_chimera_cli_path(cfg)
    root_vo_dir = org_dcache_cfg_query.namespace_dir_get_root_vo(cfg)
    splitname = root_vo_dir.split(os.sep)
    if len(splitname) < 1:
        logger.error("The Vo root dir is set incorectly")
        return False
    testedpath = "/"
    for option in splitname:
        testedpath = os.path.join(testedpath,option)
        cmd = "%s Ls %s" % (chimeraclicmd,testedpath)
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            cmd = "%s Mkdir %s" % (chimeraclicmd,testedpath)
            status,output = commands.getstatusoutput(cmd)
            if status == 0:
                logger.debug("Made directory '%s'" % (testedpath))
            else:
                logger.error("could not make directory '%s'" % (testedpath))
                logger.error("failed executing command '%s'" % (cmd))
                return False
    users = userhandling.user_handling(cfg['USERS_CONF'])
    for vo in org_dcache_cfg_query.get_supported_vos(cfg):
        vousers = users.get_by_vo(vo)
        if len(vousers) == 0:
            logger.error("could not find any users in VO '%s' skipping setting up names space" % (vo))
            continue
        voprimaryuser = vousers[0]
        voprimaryUID = voprimaryuser['UID']
        voprimaryGID = voprimaryuser['GIDS'][0]
        voroot = os.path.join(root_vo_dir,vo)
        cmd = "%s Ls %s" % (chimeraclicmd,voroot)
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            cmd = "%s Mkdir %s" % (chimeraclicmd,voroot)
            status,output = commands.getstatusoutput(cmd)
            if status == 0:
                logger.debug("Made directory '%s'" % (voroot))
            else:
                logger.error("could not make directory '%s'" % (voroot))
                logger.error("failed executing command '%s'" % (cmd))
                logger.info(output)
                return False
            cmd="%s Chown %s %s" % (chimeraclicmd,voroot,voprimaryUID)
            status,output = commands.getstatusoutput(cmd)
            if status == 0:
                logger.debug("Chown directory '%s' '%s'" % (voroot,voprimaryUID))
            else:
                logger.error("could not chown directory '%s'" % (voroot))
                logger.error("failed executing command '%s'" % (cmd))
                logger.info(output)
                return False
            cmd="%s Chgrp %s %s" % (chimeraclicmd,voroot,voprimaryGID)
            status,output = commands.getstatusoutput(cmd)
            if status == 0:
                logger.debug("Chgrp directory '%s' '%s'" % (voroot,voprimaryGID))
            else:
                logger.error("could not chgrp directory '%s'" % (voroot))
                logger.error("failed executing command '%s'" % (cmd))
                logger.info(output)
                return False
            cmd="%s Chmod %s %s" % (chimeraclicmd,voroot,775)
            status,output = commands.getstatusoutput(cmd)
            if status == 0:
                logger.debug("Chmod directory '%s' '775'" % (voroot))
            else:
                logger.error("could not chmod directory '%s'" % (voroot))
                logger.error("failed executing command '%s'" % (cmd))
                logger.info(output)
                return False
            tags = {"sGroup":"STATIC","OSMTemplate":"StoreName %s"% (vo)}
            for key in tags.keys():
                cmd="echo %s | %s Writetag %s %s" % (tags[key],chimeraclicmd,voroot,key)
                status,output = commands.getstatusoutput(cmd)
                if status == 0:
                    logger.debug("Wrote tag '%s', with value '%s' to directory '%s'" % (key,tags[key],voroot))
                else:
                    logger.error("could not tag directory '%s'" % (voroot))
                    logger.error("failed executing command '%s'" % (cmd))
                    logger.info(output)
                    return False
            '''
            Note I have removed the checking tags using tag_command
            "%s %s %s %s" % (chimeraclicmd,tag_command,voroot,voprimaryUID)
            as the output is too messy. Other values of tag_command can be
            Readtag
            Lstag
            '''

def cfg_chimera(cfg,hostname):
    if org_dcache_cfg_query.should_reset_chimera(cfg):
        chimera_database_drop()
    chimera_database_create(cfg,hostname)
    chimera_namespace_setup(cfg,hostname)
    return True

def org_dcache_chimera_check(cfg,hostname):
    return org_dcache_cfg_query.is_chimera_server(cfg,hostname)

def org_dcache_chimera_run(cfg,hostname):
    # These next two lines should be done by the infrastructure
    if not org_dcache_chimera_check(cfg,hostname):
        return True
    return cfg_chimera(cfg,hostname)



import sys
import dCacheConfigure.putils.org_dcache_cfg_default as dcache_cfg_default
import dCacheConfigure.putils.org_dcache_cfg_query as org_dcache_cfg_query
import dCacheConfigure.putils.org_pgsql_cfg as org_pgsql_cfg
import commands
import re
import logging
import os.path

logger = logging.getLogger("dCacheConfigure.pmods.org_dcache_pgsql")

def nameserver_unmount():
    cmd = "mount"
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        logger.error("failed running '%s'" % (cmd))
    expresion = re.compile('.*\/pnfs.*')
    found = False
    for line in output:
        if None != expresion.match(line):
            found = True
    if found:
        cmd = "umount /pnfs"
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            logger.error("Command '%s' failed with errorcode '%s' and output '%s'" % (cmd,status,output))
            return False
    return True


def dcache_pgsql_databases_list(cfg,hostname):
    output = []
    if org_dcache_cfg_query.is_pnfs_server(cfg,hostname):
        output.append('companion')
    if org_dcache_cfg_query.is_srm_door(cfg,hostname):
        output.append('dcache')
    # This used to be for an admin node Im not sure this is correct
    if org_dcache_cfg_query.is_name_server(cfg,hostname):
        output.append('dcache')
        output.append('replicas')
        output.append('billing')
    logger.debug("Local databases '%s'" % (output))
    return output



def setup_dcache_pgsql(cfg,hostname):
    for username in ['pnfsserver','srmdcache','bill']:
        org_pgsql_cfg.postgresql_user_add(username)
    db_defs = {
        'companion':'share/pnfs/psql_install_companion.sql',
        'replicas':'share/replica/psql_install_replicas.sql'
    }
    dCache_home = org_dcache_cfg_query.get_dcache_path_home(cfg)
    for db in dcache_pgsql_databases_list(cfg,hostname):
        if db in org_pgsql_cfg.postgresql_databases_list():
            logger.debug("Databases '%s' already exists." % (db))
            continue
        definition = []
        if db in db_defs.keys():
            definition_sql_path = os.path.join(dCache_home,db_defs[db])
            if not os.path.isfile(definition_sql_path):
                logger.error("The definition file '%s' does not exist for database '%s'" % (definition_sql_path,db))
                sys.exit(1)
            definition = [definition_sql_path]
        rc = org_pgsql_cfg.postgresql_databases_create(db,'srmdcache',definition)
        if not rc:
            sys.exit(1)

def configure_postgresql(cfg,hostname):
    pgsql_home = org_dcache_cfg_query.get_postgresql_path_home(cfg)
    pgsql_log = org_dcache_cfg_query.get_postgresql_path_log(cfg)
    if org_dcache_cfg_query.should_reset_db(cfg):
        logger.warning("Reseting the data base")
        if org_dcache_cfg_query.is_name_server(cfg,hostname):
            if not nameserver_unmount():
                logger.error("Failed to unset the database")
                return False
        org_pgsql_cfg.reset_postgresql(cfg,hostname,pgsql_home,pgsql_log)
    if not org_pgsql_cfg.postgresql_start():
        logger.critical("Failed to start postgresql")
        return False
    setup_dcache_pgsql(cfg,hostname)
    return True

def org_dcache_pgsql_check(cfg,hostname):
    if not org_pgsql_cfg.is_dbserver(cfg,hostname):
        logger.debug("Postgresql is not needed on this host.")
        return True
    if not have_postgresql_uid():
        logger.error("No postgresql UID")
        return False
    return True

def org_dcache_pgsql_run(cfg,hostname):
    defaulted = dcache_cfg_default.config_default(cfg)
    if not org_dcache_cfg_query.is_dbserver(cfg,hostname):
        logger.debug("Postgresql is not needed on this host.")
        return True
    rc = configure_postgresql(cfg,hostname)
    return rc

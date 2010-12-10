import commands
import os
import pwd
import re
import shutil
import sys
import logging

logger = logging.getLogger("dCacheConfigure.putils.org_pgsql_cfg")

def have_postgresql_uid():
    rc = True
    try:
        pwd.getpwnam("postgres")
    except:
        rc = False
    return rc

def postgresql_start():
    cmd = "/etc/init.d/postgresql status"
    status,output = commands.getstatusoutput(cmd)
    if status == 0:
        return True
    cmd = "/etc/init.d/postgresql start"
    status,output = commands.getstatusoutput(cmd)
    if status == 0:
        return True
    return False

def postgresql_stop():

    cmd = "/etc/init.d/postgresql status"
    status,output = commands.getstatusoutput(cmd)
    if status == 0:
        cmd = "/etc/init.d/postgresql stop"
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            logger.error("Could not stop PostGreSQL.")
    else:
        logger.info(output)
    return

def postgresql_user_add(username):
    user_exists = re.compile('^[ \t]*%s' % (username))
    cmd = 'psql  -U postgres  -c " SELECT * FROM pg_shadow;  "'
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        logger.critical("Could not list PostGreSQL users.")
        sys.exit(1)
    found_user = False
    for line in output.split('\n'):
        if None != user_exists.match(line):
            found_user = True
            break
    if not found_user:
        cmd = "createuser -U postgres --no-adduser -r --createdb %s" % (username)
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            # Try to create user without the -r option
            # which is not supported on postgres 8.0.4
            cmd = "createuser -U postgres --no-adduser --createdb %s" % (username)
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                logger.critical("User '%s' could not be added to PostGreSQL." % (username))
                sys.exit(1)

def postgresql_databases_list():
    results = []
    cmd = 'psql  -U postgres  -l'
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        logger.critical("Could not list PostGreSQL databases.")
        logger.info(output)
        sys.exit(1)
    databases_raw = output.split('\n')
    if len(databases_raw) < 3:
        logger.critical("Could not parse list of PostGreSQL databases.")
        logger.info(output)
        sys.exit(1)
    for raw_line in databases_raw[3:]:
        split_line = raw_line.split('|')
        if len(split_line) > 2:
            results.append(split_line[0].strip())
    return results

def postgresql_databases_create(dbname,dbuser,datadefinitionlist = [],language=None):
    cmd = "createdb -U %s %s" % (dbuser,dbname)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        logger.error("Failed to create database '%s' with user '%s'" % (dbname,dbuser))
        logger.info(output)
        return False
    if language != None:
        cmd = "createlang -U %s %s %s" % (dbuser,language,dbname)
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            logger.error("Failed to add language '%s' to database '%s'" % (language,db))
            logger.info(output)
            return False
    for dbscheamer in datadefinitionlist:
        cmd = "psql -U %s %s -f %s" % (dbuser,dbname,dbscheamer)
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            logger.error("Failed to create database '%s'" % (db))
            logger.info(output)
            return False
    return True


def postgresql_init(cfg,hostname,pgsql_home,pgsql_log):
    if not os.path.isdir(pgsql_home):
        currentdir = '/'
        for this_dir in pgsql_home.split('/'):
            currentdir = os.path.join(currentdir,this_dir)
            if not os.path.isdir(currentdir):
                os.mkdir(currentdir,0755)
    name,un1,uid,gid,un2,un3,shell =  pwd.getpwnam("postgres")
    os.chown(pgsql_home,uid,gid)
    currentdir = '/'
    for this_dir in pgsql_log.split('/')[:-1]:
        currentdir = os.path.join(currentdir,this_dir)
        if not os.path.isdir(currentdir):
            os.mkdir(currentdir,0755)
    os.chown(currentdir,uid,gid)
    cmd = '/etc/init.d/postgresql start'
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        cmd = "service postgresql initdb"
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            logger.error("Initialising PostGreSQL database failed retrying.")
            logger.info(cmd)
            logger.info(output)

            sudo_runner = 'su'
            cmd = '''%s -l postgres -c " /usr/bin/initdb --pgdata='%s' --auth='ident sameuser' " >> "%s" 2>&1 < /dev/null''' % (sudo_runner, pgsql_home,pgsql_log)
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                logger.error("Initialising PostGreSQL database failed")
                logger.info(cmd)
                logger.info(output)
    checkfile = os.path.join(pgsql_home,'data/PG_VERSION')

    if not os.path.isfile(checkfile):
        logger.critical("Initialising PostGreSQL database failed")
        logger.info("File '%s' does not exist" % (checkfile))
        sys.exit(1)

    #


    # Now we configure the access (pg_hba.conf)

    file_pg_hba = os.path.join(pgsql_home,'data/pg_hba.conf')
    if not os.path.isfile(file_pg_hba):
        logger.critical("File '%s' is missing" % (file_pg_hba))
        sys.exit(1)
    file_pg_hba_new = file_pg_hba + '.bak'
    cmd = 'host %s' % (hostname)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        logger.error("Failed to get ip address for %s" % (hostname))
        logger.info(output)
        return False
    splitout = output.split(' ')
    ipaddres = splitout[-1:][0]
    while len(ipaddres) < 15:
        ipaddres += ' '
    commented = re.compile('^#')
    local = re.compile('^local')
    host = re.compile('^host')
    fp_pg_hba_org = open(file_pg_hba, 'r')
    fp_pg_hba_new = open(file_pg_hba_new, 'w')
    for line in fp_pg_hba_org.readlines():
        if None != commented.match(line):
            fp_pg_hba_new.write(line)
            continue
        if len(line.strip()) == 0:
            fp_pg_hba_new.write(line)
            continue
        if None != local.match(line):
            fp_pg_hba_new.write("local   all         all                                             trust\n")
        if None != host.match(line):
            fp_pg_hba_new.write("host    all         all         127.0.0.1         255.255.255.255   trust\n")
            fp_pg_hba_new.write("host    all         all         %s   255.255.255.255   trust\n" %(ipaddres))
    fp_pg_hba_org.close()
    fp_pg_hba_new.close()

    shutil.move(file_pg_hba_new,file_pg_hba )
    postgresql_stop()
    postgresql_start()

def reset_postgresql(cfg,hostname,pgsql_home,pgsql_log):
    postgresql_stop()
    if os.path.isdir(pgsql_home):
        cmd = "rm -rf %s" % (pgsql_home)
        status,output = commands.getstatusoutput(cmd)

    postgresql_init(cfg,hostname,pgsql_home,pgsql_log)
    return True

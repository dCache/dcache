#!/usr/bin/env python

# ___________________________________________________________________________
# COPYRIGHT STATUS:
# Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
# software are sponsored by the U.S. Department of Energy under Contract No.
# DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
# non-exclusive, royalty-free license to publish or reproduce these documents
# and software for U.S. Government purposes.  All documents and software
# available from this server are protected under the U.S. and Foreign
# Copyright Laws, and FNAL reserves all rights.
#
# Distribution of the software available from this server is free of
# charge subject to the user following the terms of the Fermitools
# Software Legal Information.
#
# Redistribution and/or modification of the software shall be accompanied
# by the Fermitools Software Legal Information  (including the copyright
#                                               notice).
#
# The user is asked to feed back problems, benefits, and/or suggestions
# about the software to the Fermilab Software Providers.
#
# Neither the name of Fermilab, the  URA, nor the names of the contributors
# may be used to endorse or promote products derived from this software
# without specific prior written permission.
#
# DISCLAIMER OF LIABILITY (BSD):
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
# OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
# FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
# OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.
#
# Liabilities of the Government:
#
# This software is provided by URA, independent from its Prime Contract
# with the U.S. Department of Energy. URA is acting independently from
# the Government and in its own private capacity and is not acting on
# behalf of the U.S. Government, nor as its contractor nor its agent.
# Correspondingly, it is understood and agreed that the U.S. Government
# has no connection to this software and in no manner whatsoever shall
# be liable for nor assume any responsibility or obligation for any claim,
# cost, or damages arising out of or resulting from the use of the software
# available from this server.
#
# Export Control:
#
# All documents and software available from this server are subject to U.S.
# export control laws.  Anyone downloading information from this server is
# obligated to secure any necessary Government licenses before exporting
# documents or software obtained from this server.
# ___________________________________________________________________________

##
#  repman-migration.py
#
#  This script is for the chimera database part of the migration procedure to
#  move from a dCache installation which uses the old replica manager to one
#  enabled for the new resilience service.
#
#  Note that this script assumes chimera 2.16+, in which the primary keys
#  for all tables were changed to numeric (from string), and in which
#  iaccesslatency and iretentionpolicy were denormalized into the t_inodes table.
#
#  NOTE: We do not provide a script for previous chimera versions as the new
#  resilience system is incompatible with them (it has internal queries which
#  depend on the new schema).
#
#  Required input is a file (-i or --ifile=) containing a line-separated
#  list of all the pools which used to belong to the replica manager's
#  ResilientPools group.
#
#  The script does two things:
#
#       A.  changes the attributes iaccesslatency to 1 (=ONLINE)
#           and iretentionpolicy to 2 (=REPLICA) for all the files on
#           pools belonging to the specified list.
#       B.  changes AccessLatency tags to ONLINE and RetentionPolicy tags to
#           REPLICA on all directories which are ancestors of those
#           files and constitute the tags' 'origin'.
#       C.  prints out a list of the paths of all the parent directories
#           for the files on those pools.  This is for convenience, in
#           case additional storage classification tags need to be added
#           to one or more of those directories.
#
#  There are several other things which need to be done to complete the
#  migration but which do not involve manipulation of the database.  Please
#  read the online documentation at:
#
#  https://github.com/dCache/dcache/wiki/Resilience#migrating-from-the-old-replica-manager-to-the-new-service
#
##

import sys
import os
import time
import getopt
import psycopg2

ACCESS_LATENCY      =   'AccessLatency'
ONLINE              =   'ONLINE\n'
RETENTION_POLICY    =   'RetentionPolicy'
REPLICA             =   'REPLICA\n'


UPDATE_FILE_AL_FOR_LOCATION="""
    UPDATE t_inodes i SET iaccess_latency = 1
    FROM t_locationinfo l
    WHERE i.inumber = l.inumber AND l.ilocation = %s
    """

UPDATE_FILE_RP_FOR_LOCATION="""
    UPDATE t_inodes i SET iretention_policy = 2
    FROM t_locationinfo l
    WHERE i.inumber = l.inumber AND l.ilocation = %s
    """

ORIGIN_IDS_FOR_LOCATION="""
    WITH RECURSIVE visited (inumber, itagid, isorign) AS
    (
    SELECT d_leaf.iparent, t_leaf.itagid, t_leaf.isorign
    FROM t_dirs d_leaf, t_tags t_leaf, t_locationinfo l_leaf
    WHERE d_leaf.iparent = t_leaf.inumber AND d_leaf.ichild = l_leaf.inumber
    AND t_leaf.itagname = %s and l_leaf.ilocation = %s
    UNION ALL
    SELECT d_ancstr.iparent, t_ancstr.itagid, t_ancstr.isorign
    FROM visited v, t_tags t_ancstr, t_dirs d_ancstr
    WHERE d_ancstr.iparent = t_ancstr.inumber AND d_ancstr.ichild = v.inumber
    AND t_ancstr.itagname = %s
    )
    SELECT distinct(inumber)
    FROM visited
    WHERE isorign = 1;
    """

UPDATE_ORIGIN_TAG="""
    UPDATE t_tags_inodes i SET ivalue = decode(%s,'escape'), isize = %s
    FROM t_tags t
    WHERE t.itagid = i.itagid AND t.isorign = 1 AND t.itagname = %s
    """

DIRECTORY_PATHS_FOR_LOCATION="""
    SELECT distinct(inumber2path(d.iparent))
    FROM t_dirs d, t_locationinfo l
    WHERE d.ichild = l.inumber and l.ilocation = %s
    """

##
#   Given a pool location, runs an update query to change the
#   Access Latency to 'ONLINE' and its Retention Policy to 'REPLICA'
#   of all files it contains.
#
#   @param pool location of files (pool)
#   @param db connection to database
##
def update_files( pool, db ):
    if pool == None or pool == '':
        raise Exception('update_files', 'no pool defined')

    cursor = db.cursor()

    try:
        cursor.execute(UPDATE_FILE_AL_FOR_LOCATION, (pool,))
        cursor.execute(UPDATE_FILE_RP_FOR_LOCATION, (pool,))
    except Exception as e:
        print_error("Could not execute file update for %s." % (pool))
        raise e


##
#   Given a pool location, runs a query to find
#   all ancestors of the files on the given pool where the ancestor
#   is the 'origin' directory for the given tag.
#
#   @param pool location of files (pool)
#   @param tagname tag for which to find the origin
#   @param origins accumulator set of origin directories
#   @param db connection to database
##
def find_origins( pool, tagname, origins, db ):
    if pool == None or pool == '':
        raise Exception('find_origins', 'no pool defined')

    if origins == None or origins == '':
        raise Exception('find_origins', 'no set to hold origins defined')

    if tagname == None or tagname == '':
        raise Exception('find_origins', 'no tag name defined')

    cursor = db.cursor()

    try:
        cursor.execute(ORIGIN_IDS_FOR_LOCATION, (tagname, pool, tagname))
        result = cursor.fetchall()
        for tuple in result:
            origins.add(tuple[0])
    except Exception as e:
        print_error("Could not execute query to find %s origins for %s." % (tagname, pool))
        raise e


##
#   Runs an update query to change the tags with the given tagname to the given
#   value for all matching tags in the set of directories where the directory is
#   the tag origin.
#
#   @param tagname tag for which to set
#   @param tagvalue tag for which to find the origin
#   @param origins set of origin directories
#   @param db connection to database
##
def update_origins( tagname, tagvalue, origins, db ):
    if origins == None or origins == '':
        raise Exception('update_origins', 'no set of origins defined')

    if tagname == None or tagname == '':
        raise Exception('update_origins', 'no tag name defined')

    cursor = db.cursor()

    try:
        cursor.execute(get_in_clause(origins), (tagvalue, len(tagvalue), tagname))
    except Exception as e:
        print_error("Could not execute origin update %s to '%s' IN %s." % (tagname, tagvalue, origins))
        raise e


##
#   Gets the corresponding namespace path of the directory node.
#
#   @param dir directory id list
#   @param paths set accumulator for directory paths
#   @param db connection to database
##
def get_paths( pool, paths, db ):
    if pool == None or pool == '':
        raise Exception('get_paths', 'no pool defined')

    if paths == None or paths == '':
        raise Exception('get_paths', 'no set to hold paths defined')

    cursor = db.cursor()

    try:
        cursor.execute(DIRECTORY_PATHS_FOR_LOCATION, (pool,))
        result = cursor.fetchall()
        for tuple in result:
            paths.add(tuple[0])
    except Exception as e:
        print_error("Could not execute get paths for %s." % pool)
        raise e


##
#   Construct query string representing the values in the IN (...) clause
#   to the origin update query.
#
#   @param list of values to use
##
def get_in_clause( values ):
    if values == None:
        raise Exception('get_in_clause', 'no value list defined')

    if len(values) == 0:
        return UPDATE_ORIGIN_TAG

    clause = "AND t.inumber IN ('%s%s')" % (values[0], ", ".join(values[1:]))

    return '%s %s' % (UPDATE_ORIGIN_TAG, clause)


##
#   Simple auxiliary function.
#
#   @param input path
#   @return list of lines
##
def read_list_from_file( path ):
    if path == None:
        raise Exception('read_list_from_file', 'no path to input defined')

    with open(path, 'r') as f:
        return map(lambda l: l.rstrip(), f)


##
#   Simple auxiliary function.
#
#   @param items list to print, one item per line, to the given file
#   @param path of output file
##
def print_list_to_file( items, path ):
    if path == None:
        raise Exception('print_list_to_file', 'no path to input defined')

    if items == None:
        raise Exception('print_list_to_file', 'no list of items defined')

    with open(path, 'w') as f:
        map(lambda l: f.write(l + '\n'), items)
        f.flush()


##
#   Simple auxiliary function.
#
#   @param items list to print, one item per line, to stdout
##
def print_list_to_stdout( items ):

    if items == None:
        raise Exception('print_list_to_stdout', 'no list of items defined')

    for item in items:
        sys.stdout.write(item + '\n')

    sys.stdout.flush()


##
#   Error handling.
#
#   @param text error message string
##
def print_error(text):
    if text == None:
        text = 'no error message'

    sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())) + " : " + text + "\n")
    sys.stderr.flush()

##
#   Help
#
def print_help(argv):
    print 'Usage:', argv[0], ' -i <inputfile> -o <outputfile> {-H <host>[localhost] -D <dbname>[chimera] -U <user>[dcache] -P <port>[5432]}'

##
#   Updates files, directories, and writes out directory paths in order.
##
def main(argv):

    try:
        opts, args = getopt.getopt(argv,"hi:o:D:U:P:H",["ifile=","ofile=","dbname=","user=","port=","host="])
    except getopt.GetoptError:
        print 'Problem parsing options'
        print_help(argv)
        sys.exit(1)

    inputfile   =   None
    outputfile  =   None

    _host       =   'localhost'
    _dbname     =   'chimera'
    _user       =   'dcache'
    _port       =   5432

    for opt, arg in opts:
        if opt == '-h':
            print_help(argv)
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
        elif opt in ("-H", "--host"):
            _host = arg
        elif opt in ("-D", "--dbname"):
            _dbname = arg
        elif opt in ("-U", "--user"):
            _user = arg
        elif opt in ("-P", "--port"):
            _port = arg

    if inputfile == None:
        print 'Please provide the path to a file containing a line-separated list of pools'
        print_help(argv)
        sys.exit(2)

    if outputfile == None:
        print 'No output file given'
        print_help(argv)
        print 'Writing results to stdout'


    pools   = read_list_from_file(inputfile)
    print 'Processing the following pools: ', pools

    db      = None

    al_origins = set()
    rp_origins = set()
    paths      = set()

    try:
        db  = psycopg2.connect(database=_dbname, host=_host, user=_user, port=_port )

        for pool in pools:
            update_files(pool, db)

            find_origins(pool, ACCESS_LATENCY, al_origins, db)
            find_origins(pool, RETENTION_POLICY, rp_origins, db)

            get_paths(pool, paths, db)

        update_origins(ACCESS_LATENCY, ONLINE, list(al_origins), db);
        update_origins(RETENTION_POLICY, REPLICA, list(rp_origins), db);

        db.commit()

    except Exception as e:
        print_error(str(e))
        db.rollback()
        print_error("Rollback called")
    finally:
        if db != None:
            db.close

    path_list = list(paths)
    path_list.sort()

    if outputfile != None:
        print_list_to_file(path_list, outputfile)
    else:
        print_list_to_stdout(path_list)


##  ------------------------------------------------------------------------ ##
##  Python entry point

if __name__ == "__main__":
    main(sys.argv[1:])

import os
import pydcap
import time
from pydcap import *
from stat import *


def type(stat):

    mode_str = []

    # mode
    mode = stat[ST_MODE]
    if S_ISDIR(mode):
        mode_str.append( 'd' )
    elif S_IFREG(mode):
        mode_str.append( '-' )
    elif S_IFIFO(mode):
        mode_str.append( 'p' )
    elif S_IFSOCK(mode):
        mode_str.append( 's' )
    elif S_IFCHR(mode):
        mode_str.append( 'c' )
    elif S_IFBLK(mode):
        mode_str.append( 'b' )
    else:
        mode_str.append( '?' )


    # permissions owner

    if S_IRUSR & mode:
        mode_str.append('r')
    else:
        mode_str.append('-')

    if S_IWUSR  & mode:
        mode_str.append('w')
    else:
        mode_str.append('-')

    if S_IXUSR & mode:
        mode_str.append('x')
    else:
        mode_str.append('-')

    # permissions group

    if S_IRGRP & mode:
        mode_str.append('r')
    else:
        mode_str.append('-')

    if S_IWGRP & mode:
        mode_str.append('w')
    else:
        mode_str.append('-')

    if S_IXGRP & mode:
        mode_str.append('x')
    else:
        mode_str.append('-')

    # permissions other

    if S_IROTH & mode:
        mode_str.append('r')
    else:
        mode_str.append('-')

    if S_IWOTH & mode:
        mode_str.append('w')
    else:
        mode_str.append('-')

    if S_IXOTH & mode:
        mode_str.append('x')
    else:
        mode_str.append('-')


    return ''.join(mode_str)

path = 'dcap://h1repro2:22125/pnfs/desy.de/'
dir_fd = pydcap.dc_opendir(path)
if not dir_fd == None:
    print 'mode  owner:group  size name mtime'
    print '-------------------------------'
    while 1:
        entry = pydcap.dc_readdir64(dir_fd)
        if entry is None:
            break
        filepath = "%s/%s" % (path, entry)
        statbuf = pydcap.dc_stat64(filepath)
        if statbuf[0] != 0:
            continue
        stat = statbuf[1]
        print "%s %d:%d %d %s %s" % (type(stat) ,stat[ST_UID], stat[ST_GID], stat[ST_SIZE] , entry, time.strftime("%m/%d/%Y %I:%M:%S %p",time.localtime(stat[ST_MTIME])))

    pydcap.dc_closedir(dir_fd)

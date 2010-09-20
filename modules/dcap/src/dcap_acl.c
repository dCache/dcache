/*
*   DCAP - dCache Access Protocol client interface
*
*   Copyright (C) 2000,2004 DESY Hamburg DMG-Division.
*
*   AUTHOR: Tigran Mkrtchayn (tigran.mkrtchyan@desy.de)
*
*   This program can be distributed under the terms of the GNU LGPL.
*   See the file COPYING.LIB
*
*/

#include <errno.h>

#include "dcap.h"
#include "gettrace.h"
#include "node_plays.h"

#if defined(HAVE_ACL) || defined(HAVE_FACL)

/* This code is only needed to preload ACL's */


#ifndef EINVAL
#define EINVAL  22
#endif

#ifndef ENOSYS
#define ENOSYS  89
#endif

#ifndef GETACL
#define GETACL 1
#endif

#ifndef SETACL
#define SETACL 2
#endif

#ifndef GETACLCNT
#define GETACLCNT 3
#endif

#ifndef ACE_SETACL
#define ACE_SETACL 4
#endif


#ifndef ACE_GETACL
#define ACE_GETACL 5
#endif


#ifndef ACE_GETACLCNT
#define ACE_GETACLCNT 6
#endif

static int dc_acl_dummy(int cmd);


int dc_acl_dummy(int cmd)
{
    int rc = -1;
    switch(cmd)
    {
        case GETACL:
        case GETACLCNT:
        case ACE_GETACL:
        case ACE_GETACLCNT:
            /* Get ACL details */
        case SETACL:
        case ACE_SETACL:
            /* Set ACL details */
            errno = ENOSYS;
            break;

        default :
            errno = EINVAL;
            break;
    }
    return rc;
}
#ifdef HAVE_ACL
int dc_acl(const char *path, int cmd, int nentries, void *aclbufp)
{
    dcap_url *url;
    struct vsp_node *node;
    int rc;
    url = (dcap_url *)dc_getURL(path);
    if( url == NULL )
    {
        dc_debug(DC_INFO, "Using system native chown for %s.", path);
        return system_acl(path, cmd, nentries, aclbufp);
    }
    else
    {
        free(url->file);
        free(url->host);
        if( url->prefix != NULL )
        {
            free(url->prefix);
        }
        free(url);
    }
    return dc_acl_dummy(cmd);
}
#endif /* HAVE_ACL */

#ifdef HAVE_FACL
int dc_facl(int fd, int cmd, int nentries, void *aclbufp)
{
    struct vsp_node *node;
    int rc;
    char *path;
    off64_t size;
    #ifdef DC_CALL_TRACE
    showTraceBack();
    #endif
    node = get_vsp_node( fd );
    if( node == NULL ) {
        dc_debug(DC_INFO, "Using system native fstat64 for %d.", fd);
        return system_facl(fd, cmd, nentries, aclbufp);
    }
    return dc_acl_dummy(cmd);
}
#endif /* HAVE_FACL */


#endif /* HAVE_ACL || HAVE_FACL */

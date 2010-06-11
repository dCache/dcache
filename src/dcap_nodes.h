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


/*
 * $Id: dcap_nodes.h,v 1.6 2004-11-01 19:33:29 tigran Exp $
 */
#ifndef DCAP_NODES_H
#define DCAP_NODES_H

#include "dcap_types.h"


extern int node_init(struct vsp_node *node, const char *path);
extern struct vsp_node *new_vsp_node(const char *path);
extern struct vsp_node *delete_vsp_node(int fd);
extern struct vsp_node *get_vsp_node(int fd);
extern void node_destroy( struct vsp_node *node);
extern void node_attach_fd( struct vsp_node *node, int);
extern void node_detach_fd( struct vsp_node *node, int);
extern void node_unplug( struct vsp_node *node);
extern void node_dupToAll( struct vsp_node *node, int);

#endif

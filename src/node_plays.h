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
 * $Id: node_plays.c,v 1.34 2006-09-22 13:25:46 tigran Exp $
 */

#include "dcap_types.h"

void node_dupToAll(struct vsp_node *node, int fd);
void node_attach_fd( struct vsp_node *node, int fd);
void node_detach_fd( struct vsp_node *node, int fd);
struct vsp_node *new_vsp_node(const char *path);
struct vsp_node *delete_vsp_node(int fd);
struct vsp_node *get_vsp_node(int fd);
void node_destroy( struct vsp_node *node);
void node_unplug( struct vsp_node *node);
fdList getAllFD();

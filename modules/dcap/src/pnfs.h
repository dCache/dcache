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
 * $Id: pnfs.h,v 1.5 2004-11-01 19:33:30 tigran Exp $
 */
#ifndef PNFS_H
#define PNFS_H

extern int isPnfs(const char *);
extern int create_pnfs_entry(const char *, mode_t);
extern int getPnfsID(struct vsp_node *);
extern char *getPnfsIDbyPath(const char *);

#endif

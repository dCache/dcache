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
 * $Id: tunnelManager.h,v 1.3 2004-11-01 19:33:30 tigran Exp $
 */


#ifndef TUNNEL_MANAGER_H
#define TUNNEL_MANAGER_H

extern int setTunnelPair(int sock, ioTunnel *tunnel);
extern ioTunnel * getTunnelPair(int sock);
extern ioTunnel *addIoPlugin(const char *libname);

#endif /* TUNNEL_MANAGER_H */

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
 * $Id: array.h,v 1.4 2004-11-01 19:33:28 tigran Exp $
 */
#ifndef ARRAY_H
#define ARRAY_H

extern unsigned short isMember(char *);
extern void addMember(char *, int );
extern int getMember(char *);
extern void deleteMember(char *);
extern void deleteMemberByValue(int);
extern void lockMember();
extern void unlockMember();
#endif

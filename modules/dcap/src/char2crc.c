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

#include "char2crc.h"

/*
 * $Id: char2crc.c,v 1.6 2004-11-01 19:33:28 tigran Exp $
 */
#define HASHSIZE 997

unsigned long char2crc( const unsigned char *name)
{
    unsigned long   h = 0, g;
    static int M = HASHSIZE;

    while (*name)
        {
        h = ( h << 4 ) + *name++;
        if ( (g = (h & 0xF0000000L)) != 0 )
            h ^= g >> 24;

        h &= ~g;
        }
    return h % M;
}

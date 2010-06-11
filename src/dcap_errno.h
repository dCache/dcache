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
 * $Id: dcap_errno.h,v 1.8 2004-11-01 19:33:29 tigran Exp $
 */

#ifndef _DC_ERROR_NUM_H
#define _DC_ERROR_NUM_H

#define DEOK		0
#define DERESOLVE	8
#define DEAD		11
#define DENE		12
#define DENPNFS		13
#define DEEVAL		14
#define DEEPNFS		15
#define DEPNFSID	16
#define DENODE		17
#define DECC		18
#define DECONF		19
#define DEPARSER	20
#define DESYS		21
#define DECONFF		22
#define DENOCONF	23
#define DESOCKET	24
#define DECONNECT	25
#define DEHELLO		26
#define DEBIND		27
#define DEMALLOC	28
#define DEFLAGS		29
#define DESRVMSG	30
#define DENCACHED   31
#define DEURL       32
#define DEUOF       33
#define DEREAD      34

#define DEMAXERRORNUM 34


#endif

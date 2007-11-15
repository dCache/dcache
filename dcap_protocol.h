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
 * $Id: dcap_protocol.h,v 1.21 2007-07-06 16:10:53 tigran Exp $
 */
 
#ifndef DCAP_PROTOCOL_H
#define DCAP_PROTOCOL_H

/* command ids transfered over data channel */
#define IOCMD_ERROR         -1
#define IOCMD_WRITE          1
#define IOCMD_READ           2
#define IOCMD_SEEK           3
#define IOCMD_CLOSE          4
#define IOCMD_INTERRUPT      5
#define IOCMD_ACK            6
#define IOCMD_FIN            7
#define IOCMD_DATA           8
#define IOCMD_LOCATE         9
#define IOCMD_SEEK_READ      11
#define IOCMD_SEEK_WRITE     12
#define IOCMD_READV          13

#define IOCMD_SEEK_SET       0
#define IOCMD_SEEK_CURRENT   1
#define IOCMD_SEEK_END       2



/* replies from server transferred over ASCII channel */
#define ASCII_ERROR     -1	/* command not recognized */
#define ASCII_NULL       0	/* it is just placeholder for sent_message(), when you do not expect any confirmation */
#define ASCII_WELCOME    1	/* 0 0 server welcome <majorVersion> <minorVersion> */
#define ASCII_REJECTED   2	/* 0 0 server rejected <errorCode> <errorMessage> */
#define ASCII_BYE        3	/* 0 0 server|client byebye */
#define ASCII_OK         4	/* returned upon successfull close */
#define ASCII_FAILED     5	/* failed to open the file */
#define ASCII_RETRY      6  /* request to retry on the pool failure */
#define ASCII_PING       7  /* ping reply via control connection */
#define ASCII_STAT       8  /* stat reply via control line */
#define ASCII_LSTAT      9  /* stat reply via control line */
#define ASCII_SHUTDOWN  10  /* shutdown of control line */
#define ASCII_CONNECT   11  /* passive dcap - client have to connect to a pool */

/* replies by send_command */
#define COMM_SENT     0x00
#define COMM_ACKERR   0x01
#define COMM_BYE      0x02
#define COMM_VIOL     0x04
#define COMM_TRASH    0x08
#define COMM_OTHER    0x10
#define COMM_NACK     0x20
#define COMM_NCONF    0x40
#define COMM_NSENT    0x80

/* commands send over control line */

#define DCAP_CMD_OPEN   1
#define DCAP_CMD_STAGE  2
#define DCAP_CMD_CHECK  3
#define DCAP_CMD_STAT   4
#define DCAP_CMD_FSTAT  5
#define DCAP_CMD_LSTAT  6
#define DCAP_CMD_TRUNC  7
#define DCAP_CMD_UNLINK 8
#define DCAP_CMD_RMDIR  9
#define DCAP_CMD_MKDIR  10
#define DCAP_CMD_CHMOD  11
#define DCAP_CMD_OPENDIR  12
#define DCAP_CMD_RENAME 13
#define DCAP_CMD_CHOWN 14

#define DCAP_CMD_SIZE 1024



/*  optional data on close */
#define DCAP_DATA_SUM 1

extern const char *asciiCommand( int );

#endif				/* DCAP_PROTOCOL_H */

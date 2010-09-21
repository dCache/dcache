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
 * $Id: dcap.c,v 1.268 2006-09-26 07:40:28 tigran Exp $
 */

#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>

#ifdef WIN32
#   include <io.h>
#   include <time.h>
#   include <Winsock2.h>
#   include <process.h>
#   include <mmsystem.h>
#   include "dcap_unix2win.h"
#else
#   include <sys/time.h>
#   include <sys/times.h>
#   include <unistd.h>
#   include <sys/socket.h>
#   include <netinet/in.h>
#   include <arpa/inet.h>
#   include <netdb.h>
#endif
#include <string.h>

#include "system_io.h"

#include "dcap.h"
#include "dcap_debug.h"
#include "dcap_error.h"
#include "dcap_types.h"
#include "dcap_protocol.h"
#include "dcap_poll.h"
#include "dcap_mqueue.h"
#include "dcap_str_util.h"
#include "dcap_reconnect.h"
#include "dcap_functions.h"
#include "array.h"
#include "pnfs.h"
#include "io.h"
#include "sysdep.h"
#include "links.h"
#include "dcap_url.h"
#include "dcap_accept.h"
#include "socket_nio.h"
#include "dcap_reconnect.h"
#include "ioTunnel.h"
#include "tunnelManager.h"
#include "lineparser.h"
#include "xutil.h"
#include "node_plays.h"


static char    *hostName;
static int      callBackSocket = -1;
static unsigned short    callBackPort = 0;
static unsigned short    callBackPortRange = 1;

static char      *tunnel; /* tunnel provider ( library name )*/
static char      *tunnelType; /* tunnel id ( krb5, ssl, gsi ....) */

static int32_t   rqReceiveBuffer = 0;
static int32_t   rqSendBuffer = 0;

/* Send extra option in open converstation */
static char *extraOption = NULL;

/* Advanced user control */
#undef onErrorRetry
#undef onErrorFail
#undef onErrorDefault

#define onErrorRetry 1
#define onErrorFail  0
#define onErrorDefault -1

static int  onError = onErrorDefault;
static long openTimeOut = -1;

#define PNFS_DC_CONF "/.(config)(dCache)/dcache.conf"
#define PNFS_DC_LOCK "/.(config)(dCache)/dcap.LOCK"
#define DC_LOCK_TIME 60 /* time to sleep if dcap.LOCK file exist */
#define DC_STAGE (O_RDONLY | O_NONBLOCK)
#ifndef MAXHOSTNAMELEN
#define		MAXHOSTNAMELEN  64
#endif /* MAXHOSTNAMELEN */


static int activeClient = 0;

/* Local funcion prototypes */
static int initControlLine(struct vsp_node *);
static int serverConnect(struct vsp_node *);
static server *parseConfig(const char *);
static int cache_connect(server *);
static int sayHello(int, ioTunnel *);
static int create_data_socket(int *, unsigned short *);
static int ascii_open_conversation(struct vsp_node *);
static int getDataMessage(struct vsp_node *);
static void getRevision( revision * );
static int init_hostname();
static void getPortRange();
static int isActive();
static ConfirmationBlock get_reply(int);
static int name_invalid(char *);

#ifndef HAVE_HTONLL
    uint64_t  htonll(uint64_t);
#endif
#ifndef HAVE_NTOHLL
    uint64_t  ntohll(uint64_t);
#endif

static MUTEX(couterLock);
static unsigned int connectionCounter = 0;
static unsigned int newCounter();

static MUTEX(bindLock);
static MUTEX(acceptLock);

/* Utilities functions */

int name_invalid(char *p)
{
       /* Check input strings for characters which will cause protocol problems.  Currently
       * newlines and double-quotes are problematic */
       if (!p)
               return 1;
       for (; *p; ++p) {
               switch( *p ){
                       case '\n':
                       case '"':
                               return 1;
               }
       }
       return 0;
}



int init_hostname()
{
    struct hostent *he;

	if(hostName != NULL) return 0;

	hostName = getenv("DCACHE_REPLY");

	if (hostName == NULL) {
		hostName = (char *) malloc(MAXHOSTNAMELEN + 1);
		if (hostName == NULL) {
			dc_errno = DEMALLOC;
			return -1;
		}
		hostName[MAXHOSTNAMELEN] = '\0';

		if (gethostname(hostName, MAXHOSTNAMELEN) < 0) {
			dc_debug(DC_ERROR, "Failed to get local host name.");
			return -1;
		}

		/* trying to get full fully-qualified domain name. */
		he = (struct hostent *) gethostbyname((const char *) hostName);
		if (he != NULL) {
			/* if we successed to get it, let use it */
		    free(hostName);
			hostName = (char *)strdup(he->h_name);
		}else{
			dc_debug(DC_INFO, "Unable to get FQDN for host %s.", hostName);
		}

		dc_debug(DC_INFO, "Setting hostname to %s.", hostName);
	}

	return 0;
}


int
cache_open(vsp_Node * node )
{

	int old_fd;
	int new_fd;
	int rc;

	/* if node->dataFd != -1 - we need just to reconnect */
	if(node->dataFd != -1) {
		old_fd = node->dataFd;

		deleteQueue(node->queueID);
		node->queueID = newCounter();

		if( newQueue(node->queueID) == NULL) {
			dc_debug(DC_ERROR, "Failed to create new message queue.");
			return -1;
		}

		if (ascii_open_conversation(node) < 0) {
			return -1;
		}

		shutdown(old_fd, SHUT_RDWR);
		new_fd = node->dataFd;
		node->dataFd = dup2(node->dataFd, old_fd);
		if( node->dataFd != old_fd) {
			node->dataFd = old_fd;
			dc_debug(DC_ERROR, "dup2 failed. Reconnection impossible.");
			return -1;
		}

		system_close(new_fd);

		/* ascii_open_conversation attaches new file descriptor to node */
		node_detach_fd(node, new_fd);
		/* node_detach_fd sets new_fd as node->dataFd */
		node->dataFd = old_fd;

		/* correct all descriptors */
		node_dupToAll(node, node->dataFd);
		return 0;
	}

	node->queueID = newCounter();

	if( newQueue(node->queueID) == NULL) {
		dc_debug(DC_ERROR, "Failed to create new message queue.");
		return -1;
	}

	if ( initControlLine(node) < 0) {
		return -1;
	}

	/* find out our host anme if we need it */
	if( (node->asciiCommand == DCAP_CMD_OPEN) ||
		(node->asciiCommand == DCAP_CMD_TRUNC) ||
		(node->asciiCommand == DCAP_CMD_STAGE) ||
		(node->asciiCommand == DCAP_CMD_CHECK) ||
		(node->asciiCommand == DCAP_CMD_OPENDIR) ) {

		m_lock(&bindLock);
		rc = init_hostname();
		m_unlock(&bindLock);

		if(rc < 0 ) {
			return -1;
		}
	}

	/* if callback socket not created yet - do it! */
	/* only one thread have to bind callback socket */
	/* create data socket only for IO operatios ( open ) */

	if( (node->asciiCommand == DCAP_CMD_OPEN ) ||
		(node->asciiCommand == DCAP_CMD_OPENDIR ) ||
		(node->asciiCommand == DCAP_CMD_TRUNC) ) {

		m_lock(&bindLock);
		if( callBackSocket == -1 ) {
			if ( create_data_socket(&callBackSocket, &callBackPort) < 0) {
				dc_debug(DC_ERROR, "Callback socket not created.");
				m_unlock(&bindLock);
				return -1;
			}
		}
		m_unlock(&bindLock);

		node->data_port = callBackPort;
	}

	if (ascii_open_conversation(node) < 0) {
		return -1;
	}

	dc_debug(DC_TRACE, "cache_open -> OK");
	return 0;
}


int
initControlLine(struct vsp_node * node)
{

	int             ret;

	ret = serverConnect( node);
	if (ret < 0) {		/* we got an error */
		dc_debug(DC_ERROR, "Failed to create a control line");
		return -1;
	}

	return 0;
}


int
serverConnect(struct vsp_node * node)
{
	char           *dcache_host;
	char           *conf_file;
	FILE           *cf;
	int             len;
	char            buffer[MAXHOSTNAMELEN + 1];
	server         *srv = NULL;
	server        **allServers = NULL;
	server        **tmp = NULL;
	int             serversNumber = 0;
	int             i;
	int             isLocked = 0;

	if(node->url != NULL) {
		dcache_host = node->url->host;
	}else{
		dcache_host = getenv("DCACHE_DOOR");
		/* to be backward compatible */
		if (dcache_host == NULL) {
			dcache_host = getenv("DCACHE_HOST");
		}
	}

	if (dcache_host == NULL) {

		/* check for </pnfs/../dcap.LOCK file, and wait untill it will gone */
		/* use config_file as temporary store for lock file name */
		len = strlen(node->directory) + strlen(PNFS_DC_LOCK) + 1;	/* dir/file */
		conf_file = malloc(len + 1);
		if (conf_file == NULL) {
			dc_errno = DESYS;
			return -1;
		}
		sprintf(conf_file, "%s%s", node->directory, PNFS_DC_LOCK);
		while(access(conf_file, F_OK) == 0 ) {
			/* if lock file exist, wait a bit and try again */
			if(!isLocked) {
				/* print message only once*/
				dc_debug(DC_INFO, "DCAP Locked. Waiting for unLock");
				isLocked = 1;
			}
#ifndef WIN32
			sleep(DC_LOCK_TIME);
#else
			Sleep(DC_LOCK_TIME*1000);
#endif /* WIN32 */
		}

		if(isLocked) {
			dc_debug(DC_INFO, "DCAP unLocked.");
		}
		free(conf_file);

		len = strlen(node->directory) + strlen(PNFS_DC_CONF) + 1;	/* dir/file */
		conf_file = malloc(len + 1);
		if (conf_file == NULL) {
			dc_errno = DESYS;
			return -1;
		}
		sprintf(conf_file, "%s%s", node->directory, PNFS_DC_CONF);
		dc_debug(DC_INFO, "Using config file %s",conf_file);

		/* open config file and read line by line ... */
		cf = system_fopen(conf_file, "r");
		if (cf == NULL) {
			dc_errno = DECONFF;
			dc_debug(DC_ERROR, "Failed to open config file %s",conf_file);
			free(conf_file);
			return -1;
		}

		/* take exclusive access to user servers list */
		lockMember();
		while (system_fgets(buffer, MAXHOSTNAMELEN, cf) != NULL) {
			buffer[MAXHOSTNAMELEN] = '\0';
			if (buffer[0] == '#') {
				continue;
			}

			srv = (server *)parseConfig(buffer);
			if(srv == NULL) {
				continue;
			}

			buffer[0] = '\0';
			sprintf(buffer, "%s:%d", srv->hostname, srv->port);

			if ((node->fd = getMember(buffer)) != -1) {

				/* If dcap was locked by LOCK file in pnfs, check status of
				   old  control line connections */

				if(isLocked) {
					if( ping_pong( node ) == 0 ) {
						dc_debug(DC_INFO, "Existing control connection to %s:%d DOWN, skeeping.",
							srv->hostname, srv->port);

						/* remove file descriptor from the list of control lines in use */
						deleteMemberByValue(node->fd);

						/* we are no longer interesting in the messages which cames
													via old control line*/
						pollDelete(node->fd);

						/* file descriptor can be reused by system */
						system_close(node->fd);
						node->fd = -1;
						continue;
					}

				}

				dc_debug(DC_INFO, "Using existing control connection to %s:%d.",
					srv->hostname, srv->port);

				/* existing connection + tunnel */
				node->tunnel = srv->tunnel;
				free(srv->hostname);
				free(srv);

				unlockMember();
				system_fclose(cf);
				free(conf_file);

				/* cleanup memory used by Door selection mechanism */
				if(serversNumber) {
					for(i=0; i< serversNumber; i++) {
						free(allServers[i]->hostname);
						free(allServers[i]);
					}
					free(allServers);
				}

				return 1;
			}else{
				tmp = realloc(allServers, sizeof(server *)*(serversNumber + 1));
				if(tmp == NULL ) {
					dc_debug(DC_ERROR, "Memory allocation failed.");

					/* if there are some servers in the list, then try to yse them */
					if(!serversNumber) {
						return -1;
					}else{
						break;
					}
				}

				tmp[serversNumber] = srv;
				serversNumber++;
				allServers = tmp;
			}
		}

		if(serversNumber == 0) {
			dc_debug(DC_ERROR, "No doors available.");
		}else{
			dc_debug(DC_TRACE, "Totaly %d doors entries found", serversNumber );
#ifdef WIN32
			srand(time(NULL));
#else
			srandom(time(NULL));
#endif
			newQueue(0);

			while(serversNumber) {
				/* try to choose server randomly to simulate
					the load balancing */
#ifdef WIN32
				i = rand()%serversNumber;
#else
				i = random()%serversNumber;
#endif
				dc_debug(DC_INFO, "Creating a new control connection to %s:%d.",
					allServers[i]->hostname, allServers[i]->port);

				if( dc_errno == DECONNECT ) {
					/* for the next iteration,
					 * we do not care about old connect problem */
					dc_errno = DEOK;
				}
				node->fd = cache_connect(allServers[i]);
				if(node->fd < 0) {
					dc_debug(DC_INFO, "Connection failed to %s:%d.",
						allServers[i]->hostname, allServers[i]->port);

					free(allServers[i]->hostname);
					free(allServers[i]);
					allServers[i] = allServers[--serversNumber];
				}else{
					dc_debug(DC_INFO, "Established control connection to %s:%d.",
						allServers[i]->hostname, allServers[i]->port);

					/* add entry in the servers list */
					buffer[0] = '\0';
					sprintf(buffer, "%s:%d", allServers[i]->hostname,allServers[i]->port);

					addMember(buffer, node->fd);

					/* keep tunnel */
					node->tunnel = allServers[i]->tunnel;

					/* cleanup memory used by Door selection mechanism */
					for(i=0; i< serversNumber; i++) {
						free(allServers[i]->hostname);
						free(allServers[i]);
					}
					free(allServers);
					break;
				}
			}
		}

		unlockMember();

		system_fclose(cf);
		free(conf_file);
		if (node->fd < 0) {
			return -1;
		}
		return 0;

	} else {

		dc_debug(DC_TRACE, "Using environment variable as configuration");
		lockMember();
		if ((node->fd = getMember(dcache_host)) != -1) {

			srv = parseConfig(node->url == NULL ? dcache_host : url2config(buffer, sizeof(buffer), node->url) );
			if (srv == NULL) {
				unlockMember();
				return -1;
			}
			node->tunnel = srv->tunnel;

			dc_debug(DC_INFO, "Using existing control connection to %s.", dcache_host);
			unlockMember();

			free(srv->hostname);
			free(srv);

			return 1;
		}

		newQueue(0);
		dc_debug(DC_INFO, "Creating a new control connection to %s.",dcache_host );
		srv = parseConfig(node->url == NULL ? dcache_host : url2config(buffer, sizeof(buffer), node->url) );

		if (srv == NULL) {
			unlockMember();
			return -1;
		}
		node->fd = cache_connect(srv);

		addMember(dcache_host, node->fd);
		unlockMember();
		if (node->fd < 0) {
			dc_debug(DC_INFO, "Failed to connect to %s:%d", srv->hostname, srv->port);
			free(srv->hostname);
			free(srv);
			return -1;
		}
		dc_debug(DC_INFO, "Connected to %s:%d", srv->hostname, srv->port);
		node->tunnel = srv->tunnel;
		free(srv->hostname);
		free(srv);

		return 0;
	}

}

/* current config line format:
    [tunnelType@]host[:port[:tunnelProvider[:tunnelType] ] ]
*/

server *
parseConfig(const char *str)
{
	server         *srv;
	char          **arg;
	char          **tt;
	int               i;
	char             *s;
	char            *tT;
	char    *configType = NULL;
	char   *configTunnel = NULL;

	if (str == NULL) {
		return NULL;
	}
	srv = (server *) malloc(sizeof(server));
	if (srv == NULL) {
		dc_errno = DESYS;
		return NULL;
	}

	/* initial state */
	srv->hostname = NULL;
	srv->port = -1;
	srv->tunnel = NULL;

	arg = lineParser(str, ":");
	if( (arg == NULL) || (arg[0] == NULL) ) {
		free(srv);
		return NULL;
	}

	tt = lineParser(arg[0], "@");
	if( (tt == NULL ) || (tt[0] == NULL) ) {
		/* unknown situation! problem with memmory ? */
		srv->hostname = strdup(arg[0]);
	}else{

		/* if tunnel type specified, then hostname is a second field */
		if( tt[1] != NULL ) {
			srv->hostname = tt[1];
			configType = tt[0];
		}else{
			srv->hostname = tt[0];

		}
	}

	if( arg[1] != NULL ) {
		srv->port = atoi(arg[1]);
		configTunnel = arg[2];
	}else{
		srv->port = DEFAULT_DOOR_PORT;
		configTunnel = NULL;
	}

	if( (configTunnel != NULL) || (getenv("DCACHE_IO_TUNNEL") != NULL) || (tunnel != NULL)) {

		/* ENV -> command line -> config file */
		s = getenv("DCACHE_IO_TUNNEL");
		if(s == NULL) {
			s = tunnel != NULL? tunnel : arg[2];
		}

		tT = getenv("DCACHE_IO_TUNNEL_TYPE");
		if(tT == NULL) {
			tT = tunnelType;
		}

		if( configType == NULL ) {
			configType = arg[3];
		}

		if( (tT == NULL ) || ( ( configType != NULL) && ( strcmp( tT , configType ) == 0 ) ) ) {

			srv->tunnel = addIoPlugin(s);
			if(srv->tunnel == NULL ) {
				dc_debug(DC_INFO,"Tunnel %s empty or unavailable, using plain.", s);
			}else{
				dc_debug(DC_INFO, "Added IO tunneling plugin %s for %s:%d.", s , srv->hostname, srv->port);
			}

		}else{
			dc_debug(DC_INFO, "Tunnel type missmatch: requested [%s] provided [%s]. Skipping...",
					tT, configType == NULL ? "null" : configType );

			if(srv->hostname!= NULL) {
				free(srv->hostname);
			}
			if(srv->tunnel != NULL ) {
				free(srv->tunnel);
			}

			free(srv);
			srv = NULL;
		}

	}else{
		dc_debug(DC_INFO, "No IO tunneling plugin specified for %s:%d.", srv->hostname, srv->port);
	}


	/* cleanup */
	for(i = 0; arg[i] != NULL; i++ ) {
		free(arg[i]);
	}
	free(arg);

	/* if tunnelType and hostname specidied, cleanup tunnelType */
	if( tt[1] != NULL ) {
		free(tt[0]);
	}

	return srv;
}


int
cache_connect(server * srv)
{

	int             fd;
	struct sockaddr_in serv_addr;
	struct hostent *hp;

#ifdef WIN32
	initWinSock();
#endif /* WIN32 */

	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		dc_errno = DESOCKET;
		return fd;
	}
	memset((char *) &serv_addr, 0, sizeof(serv_addr));

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(srv->port);

	/* first try  by host name, then by address */
	hp = (struct hostent *) gethostbyname(srv->hostname);
	if (hp == NULL) {
		if ((serv_addr.sin_addr.s_addr = inet_addr(srv->hostname)) < 0) {
			system_close(fd);
			dc_errno = DERESOLVE;
			return -1;
		}
	} else {
		memcpy( &serv_addr.sin_addr.s_addr, hp->h_addr_list[0], hp->h_length);
	}

	if (nio_connect(fd, (struct sockaddr *) & serv_addr, sizeof(serv_addr), 20) != 0) {
		system_close(fd);
		dc_errno = DECONNECT;
		return -1;
	}

	if(srv->tunnel != NULL) {
		srv->tunnel->eInit(fd);
	}

	setTunnelPair(fd, srv->tunnel);

	if( sayHello(fd, srv->tunnel) < 0 ) {
		system_close(fd);
		dc_errno = DEHELLO;
		return -1;
	}

	return fd;
}

int
sayHello(int fd, ioTunnel *en)
{

	char            helloStr[64];
	revision        rev;
	int             pid;
	int             uid;
	int             gid;
	asciiMessage   *aM;

#ifdef WIN32
	pid = _getpid();
	uid = 17;
	gid = 18;
#else
	pid = (int)getpid();
	uid = (int)getuid();
	gid = (int)getgid();
#endif

    getRevision(&rev);
	helloStr[0] = '\0';
	sprintf(helloStr, "0 0 client hello 0 0 %d %d -uid=%d -pid=%d -gid=%d\n", rev.Maj, rev.Min, uid, pid, gid);

	if (sendControlMessage(fd, helloStr, strlen(helloStr), en) < 0) {
		dc_debug(DC_ERROR, "Failed to send Hello fd=%d", fd);
		errno = EIO;
		return -1;
	}
	pollAdd(fd);

	aM = getControlMessage(HAVETO, NULL);
	if( aM == NULL ) {
		pollDelete(fd);
		errno = EIO;
		return -1;
	}

	/* FIXME: implement result recognition */
	free(aM);

	return 0;
}

int
create_data_socket(int *dataFd, unsigned short *cbPort)
{

	struct sockaddr_in me;
#if defined(__linux__) || defined(__GNU__) || defined(__FreeBSD_kernel__)
	socklen_t       addrSize;
#else
	size_t          addrSize;
#endif
	int             bindResult;
	int             i;

	*dataFd = socket(AF_INET, SOCK_STREAM, 0);
	if (*dataFd < 0) {
		dc_errno = DESOCKET;
		return *dataFd;
	}

	memset((char *) &me, 0, sizeof(me));
	me.sin_family = AF_INET;
	me.sin_addr.s_addr = htonl(INADDR_ANY);



	/* get port range from environment */
	getPortRange();


	/* try to get free slot in range of TCP ports */
	for( i = 0 ; i < callBackPortRange; i++) {

		*cbPort += i;
		me.sin_port = htons(*cbPort + i);
		addrSize = sizeof(me);
		bindResult = bind(*dataFd, (struct sockaddr *) & me, addrSize);
		if( bindResult == 0) break;

	}

	if (bindResult < 0) {
		dc_errno = DEBIND;
		system_close(*dataFd);
		*dataFd = -1;
		return -1;
	}

	/* get our TCP port number as it can be redefined at bind time */
#if defined(__linux__) || defined(__GNU__) || defined(__FreeBSD_kernel__)
	getsockname(*dataFd, (struct sockaddr *) & me, (socklen_t *) &addrSize);
#else
	getsockname(*dataFd, (struct sockaddr *) & me, (int *) &addrSize);
#endif
	*cbPort = ntohs(me.sin_port);

	listen(*dataFd, 512);
	return 1;

}

#define APPEND_TO_OPENSTR(STRING) dc_safe_strncat(openStr, DCAP_CMD_SIZE, STRING)

int
ascii_open_conversation(struct vsp_node * node)
{

	int             len;
	int             uid;
	short           invalid_flag = 1;
	asciiMessage   *aM;
	char_buf_t    *context;
	char           *outStr;
	context = dc_char_buf_create();
	if (context == NULL) {
		dc_errno = DEMALLOC;
		return -1;
	}

#ifdef WIN32
	uid = 17;
#else
	uid = (int)getuid();
#endif
	outStr = dc_char_buf_sprintf(context,"%d 0 client %s \"%s\"", node->queueID,
	                                          asciiCommand(node->asciiCommand),
	                                          node->asciiCommand == DCAP_CMD_TRUNC ? node->ipc : node->pnfsId );
	
	if (outStr == NULL){
		goto out_of_mem_exit;
	}
	switch( node->asciiCommand ) {
		case DCAP_CMD_OPEN:
		case DCAP_CMD_TRUNC:
#ifdef WIN32
            if ((node->flags == O_RDONLY) || (node->flags & O_WRONLY) || (node->flags & O_RDWR) || (node->flags == O_BINARY)){
#else
            if ((node->flags == O_RDONLY) || (node->flags & O_WRONLY) || (node->flags & O_RDWR)	){
#endif /* WIN32 */
                invalid_flag = 0;
                if ((node->file_name != NULL) && (name_invalid(node->file_name))) {
                    dc_debug(DC_ERROR, "File '%s' contains a currently invalid dcap protocol character." , node->file_name);
                    goto parser_exit;
                }
                if ((node->directory != NULL ) && (name_invalid(node->directory))) {
                    dc_debug(DC_ERROR, "Directory '%s' contains a currently invalid dcap protocol character." , node->directory);
                    goto parser_exit;
                }
            }
#ifdef WIN32
			if( (node->flags == O_RDONLY) || (node->flags == O_BINARY)) {
#else
			if( node->flags == O_RDONLY) {
#endif /* WIN32 */

				outStr = dc_char_buf_sprintf(context,
					              "%s r", outStr);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
				if( (node->url == NULL) && (node->directory != NULL ) && (node->file_name != NULL) ) {
					outStr = dc_char_buf_sprintf(context,
					              "%s -path=%s/%s", outStr, node->directory, node->file_name);
					if (outStr == NULL) {
						goto out_of_mem_exit;
					}
				}
			}

			if (node->flags & O_WRONLY) {
				outStr = dc_char_buf_sprintf(context,
					              "%s w", outStr);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
				if( (node->url == NULL) && (node->directory != NULL ) && (node->file_name != NULL) ) {
					outStr = dc_char_buf_sprintf(context,
					              "%s -path=%s/%s", outStr, node->directory, node->file_name);
					if (outStr == NULL) {
						goto out_of_mem_exit;
					}
				}
			}

			if (node->flags & O_RDWR) {
				outStr = dc_char_buf_sprintf(context,
					              "%s rw", outStr);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
				if( (node->url == NULL) && (node->directory != NULL ) && (node->file_name != NULL) ) {
					outStr = dc_char_buf_sprintf(context,
					              "%s -path=%s/%s", outStr, node->directory, node->file_name);
					if (outStr == NULL) {
						goto out_of_mem_exit;
					}
				}
			}


			/*
			 *  send file permissions in case of create with URL syntax
			 */
			if( (node->flags & O_CREAT) && (node->url != NULL) ) {
				outStr = dc_char_buf_sprintf(context,
				             "%s -mode=0%o", outStr, node->mode);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
			}

			if ( node->asciiCommand == DCAP_CMD_TRUNC ) {
				if( node->url == NULL ) {
					outStr = dc_char_buf_sprintf(context,
						     "%s -truncate=\"%s\"", outStr, node->pnfsId);
				}else{
					outStr = dc_char_buf_sprintf(context,
						     "%s -truncate", outStr);
				}
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
			}
			outStr = dc_char_buf_sprintf(context,
				"%s %s %u -timeout=%ld",
					outStr, hostName, node->data_port, openTimeOut);
			if (outStr == NULL) {
				goto out_of_mem_exit;
			}
			switch( onError) {
			case onErrorFail:
				outStr = dc_char_buf_sprintf(context,
					"%s -onerror=fail", outStr);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
				break;
			case onErrorDefault:
				outStr = dc_char_buf_sprintf(context,
					"%s -onerror=default", outStr);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
				break;
			default:
			case onErrorRetry:
				outStr = dc_char_buf_sprintf(context,
					"%s -onerror=retry", outStr);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
				break;
			}

			if( rqReceiveBuffer != 0 ) {
				outStr = dc_char_buf_sprintf(context,
					"%s -send=%d", outStr, rqReceiveBuffer);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
			}

			if( rqSendBuffer!= 0 ) {
				outStr = dc_char_buf_sprintf(context,
					"%s -receive=%d", outStr, rqSendBuffer);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
			}


			if( isActive() ) {
				outStr = dc_char_buf_sprintf(context,
					"%s -passive", outStr);
				if (outStr == NULL) {
					goto out_of_mem_exit;
				}
			}

			break;
		case DCAP_CMD_STAGE:
		case DCAP_CMD_CHECK:
			outStr = dc_char_buf_sprintf(context,
				"%s -stagetime=%ld -location=%s",
					outStr,
					node->atime,
					node->stagelocation == NULL ? hostName : node->stagelocation);
			if (outStr == NULL) {
				goto out_of_mem_exit;
			}
			invalid_flag = 0;
			break;
		case DCAP_CMD_MKDIR:
		case DCAP_CMD_CHMOD:
			outStr = dc_char_buf_sprintf(context,
				"%s -mode=%d", outStr, node->mode);
			if (outStr == NULL) {
				goto out_of_mem_exit;
			}
			invalid_flag = 0;
			break;
		case DCAP_CMD_CHOWN:
			outStr = dc_char_buf_sprintf(context,
				"%s -owner=%d:%d", outStr, node->uid, node->gid);
			if (outStr == NULL) {
				goto out_of_mem_exit;
			}
			invalid_flag = 0;
			break;
		case DCAP_CMD_STAT:
		case DCAP_CMD_LSTAT:
		case DCAP_CMD_FSTAT:
		case DCAP_CMD_UNLINK:
		case DCAP_CMD_RMDIR:
			invalid_flag = 0;
			break;
		case DCAP_CMD_OPENDIR:
			outStr = dc_char_buf_sprintf(context,
				"%s %s %u", outStr, hostName, node->data_port);
			if (outStr == NULL) {
				goto out_of_mem_exit;
			}
			invalid_flag = 0;
			break;
		case DCAP_CMD_RENAME:
			outStr = dc_char_buf_sprintf(context,
				"%s %s", outStr, node->ipc);
			if (outStr == NULL) {
				goto out_of_mem_exit;
			}
			invalid_flag = 0;
			break;
		default:
			dc_debug(DC_ERROR, "Invalid DCAP command %d", node->asciiCommand);
			break;
	}

	if (invalid_flag) {
		dc_char_buf_free(context);
		dc_errno = DEFLAGS;
		return -1;
	}

	if(extraOption != NULL) {
		outStr = dc_char_buf_sprintf(context,
				"%s %s", outStr, extraOption);
		free(extraOption);
		extraOption = NULL;
		if (outStr == NULL) {
			goto out_of_mem_exit;
		}
	}
	outStr = dc_char_buf_sprintf(context,
				"%s -uid=%d\n", outStr, uid);
	if (outStr == NULL) {
		goto out_of_mem_exit;
	}
	len = strlen(outStr);
	sendControlMessage(node->fd, outStr, len, node->tunnel);
	/* getControlMessage(MAYBE, NULL); */
	dc_char_buf_free(context);

	if ( (node->asciiCommand == DCAP_CMD_OPEN ) ||
		(node->asciiCommand == DCAP_CMD_OPENDIR ) ||
		(node->asciiCommand == DCAP_CMD_TRUNC ) ) {

    	if(data_hello_conversation(node) < 0) {
    		return -1;
		}

	}else{

		aM = getControlMessage(HAVETO, node);
		if( (aM == NULL) || ( aM->type == ASCII_FAILED )) {

			if(aM != NULL) {
				if(aM->msg != NULL) {
					free(aM->msg);
				}
				free(aM);
			}

			return -1;
		}

		switch( aM->type ) {

			case ASCII_STAT:
				node->ipc = aM->msg;
				break;
			default:
				free(aM->msg);
				break;
		}

		free(aM);
	}

	return 0;
out_of_mem_exit:
	dc_char_buf_free(context);
	dc_errno = DEMALLOC;
	return -1;
parser_exit:
	dc_char_buf_free(context);
	dc_errno = DEPARSER;
	errno = EINVAL;
	return -1;
}

int
close_data_socket(int dataFd)
{
	return system_close(dataFd);
}

int sendControlMessage(int to, const char *buff, size_t len, ioTunnel *en)
{
	int n;
	struct pollfd  pfd;
	char *debugMessage;

	pfd.fd = to;
	pfd.events = POLLOUT;

	n = poll(&pfd, 1, 1000*10); /* 10 seconds */

	if( (n == 1) && ( (pfd.revents & POLLERR) || (pfd.revents & POLLHUP) ) ) {
		dc_debug(DC_ERROR, "Unable to send control message, line [%d] is down", to);
		n = -1;
	}else{
		m_lock(&bindLock);
		debugMessage = xstrndup(buff, len);
		debugMessage[len-1] = '\0';
        dc_debug(DC_INFO, "Sending control message: %s (len=%d)", debugMessage, len);
		free(debugMessage);
		n = writen(to, buff, len, en);
		m_unlock(&bindLock);
	}
	return n;
}

asciiMessage *getControlMessage(int mode, struct vsp_node *node)
{

	asciiMessage *aM = NULL;
	int rc;
	int pass = 0;
	int queueID;

	if( node == NULL ) {
		queueID = 0;
	}else{
		queueID = node->queueID;
	}

	while(1){
		m_lock(&bindLock);
		rc = queueGetMessage(queueID, &aM);
		if((rc == 0) || ( ((!queueID ) || (mode == MAYBE)) && (pass)) || isIOFailed) {
			m_unlock(&bindLock);
			return aM;
		}

		if( dcap_poll(mode, node, POLL_CONTROL) < 0 ) {
			dc_debug(DC_ERROR, "getControlMessage: poll fail.");
			m_unlock(&bindLock);
			return NULL;
		}

		m_unlock(&bindLock);
		pass++;
	}

}

int getDataMessage(struct vsp_node *node)
{
   return dcap_poll(HAVETO, node, POLL_DATA);
}


/*
 * this routine receives the hello block over data channel it assumes that
 * some data are avaliable otherwise it will block!!!
 */
int
data_hello_conversation(struct vsp_node * node)
{
	struct sockaddr_in him;
	int             newFd;
#if defined(__linux__) || defined(__GNU__) || defined(__FreeBSD_kernel__)
	socklen_t       addrSize;
#else
	size_t          addrSize;
#endif
	struct in_addr *addr;
	struct hostent *hostEnt;
	u_short         remotePort;
	char           *hostname;
	int32_t         sessionId, challengeSize;
	int             tmp;

	while(1) {

		m_lock(&acceptLock);

		newFd = queueGetAccepted(node->queueID);
		if(newFd >= 0) {
			node_attach_fd(node, newFd);
			m_unlock(&acceptLock);
			return 0;
		}

		node->dataFd =  callBackSocket;

		if( getDataMessage(node) < 0 ) {
			node->dataFd = -1;
			m_unlock(&acceptLock);
			return -1;
		}


		/* check that we did not connect to the pool ( passive mode) */
		if( node->isPassive ) {
			m_unlock(&acceptLock);
			return 0;
		}

		addrSize = sizeof(him);
	#if defined(__linux__) || defined(__GNU__) || defined(__FreeBSD_kernel__)
		newFd = accept(callBackSocket, (struct sockaddr *) & him, (socklen_t *) &addrSize);
	#else
		newFd = accept(callBackSocket, (struct sockaddr *) & him, (int *) &addrSize);
	#endif

		if(newFd < 0) {
			dc_debug(DC_ERROR, "Accept failed.");
			node->dataFd = -1;
			m_unlock(&acceptLock);
			return -1;
		}

		addr = (struct in_addr *) & (him.sin_addr);
		hostEnt = (struct hostent *) gethostbyaddr((const char *) addr, sizeof(struct in_addr), AF_INET);
		remotePort = ntohs(him.sin_port);

		if (hostEnt != NULL) {
			hostname = hostEnt->h_name;
		} /* else -> address not resolved */

	/* change send/receive buffer size prior any write/read operation */
#ifdef  SO_RCVBUF
		if( (rqReceiveBuffer != 0) && (node->rcvBuf == 0) ) {
			/*
				tune the socket buffer size
			*/
			node->rcvBuf = rqReceiveBuffer > 4096 ? rqReceiveBuffer : 4096;
			while ( (node->rcvBuf > 4096) &&
					(setsockopt(newFd, SOL_SOCKET, SO_RCVBUF,
							(char *)&(node->rcvBuf), sizeof (node->rcvBuf)) < 0 )) {
					node->rcvBuf -= 4096;
			}

			dc_debug(DC_INFO, "Socket RECEIVE buffer size changed to %d", node->rcvBuf);
		}
#endif /* SO_RCVBUF */

#ifdef  SO_SNDBUF
		if( (rqSendBuffer != 0) && (node->sndBuf==0) ) {
			/*
				tune the socket buffer size
			*/

			node->sndBuf = rqSendBuffer > 4096 ? rqSendBuffer : 4096 ;
			while ( (node->sndBuf > 4096) &&
					(setsockopt(newFd, SOL_SOCKET, SO_SNDBUF,
							(char *)&node->sndBuf, sizeof (node->sndBuf)) < 0 )) {
					node->sndBuf -= 4096;
			}

			dc_debug(DC_INFO, "Socket SEND buffer size changed to %d", node->sndBuf);
		}
#endif /* SO_SNDBUF */

		tmp = readn(newFd, (char *) &sessionId, sizeof(sessionId), NULL);
		sessionId = ntohl(sessionId);

		tmp = readn(newFd, (char *) &challengeSize, sizeof(challengeSize), NULL);
		challengeSize = ntohl(challengeSize);

		dc_debug(DC_INFO, "Got callback connection from %s:%d for session %d, myID %d.",
												hostname, remotePort, sessionId, node->queueID);

		if( sessionId == (int32_t)node->queueID ) {
			node_attach_fd(node, newFd);
			m_unlock(&acceptLock);
			return 0;
		}

		queueAddAccepted(sessionId, newFd);
		m_unlock(&acceptLock);

	}
}

int writeOK(int fd)
{
#ifdef WIN32
	return 1;
#else
	struct pollfd pd[1];

	pd[0].fd = fd;
	pd[0].events = POLLOUT;

	poll(pd, (unsigned long)1, -1);


	if( (pd[0].revents &  POLLHUP ) || (pd[0].revents & POLLERR) ) {
		dc_debug(DC_ERROR, "[%d] socket in %s state", fd,
				pd[0].revents == POLLHUP? "HANGUP" : "ERROR");
		return 0;
	}

	if(pd[0].revents & POLLOUT) {
		return 1;
	}

	dc_debug(DC_ERROR, "[%d] socket in UNKNOWN(%d) state", fd, pd[0].revents);

	/* it's safe to indicate error condition on unkown state */
	return 0;
#endif /* WIN32 */
}

int sendDataMessage(struct vsp_node *node, char *message, int sizeOfMessage, int asciiConfirm, ConfirmationBlock *result)
{
	int ret;
	asciiMessage *aM;
	int try = 0;
	int rc = 0;
	int err = 0;

	dc_debug(DC_CALLS, "Entered sendDataMessage.");
again:
	if( (rc == -1) || (!writeOK(node->dataFd)) ) {
		dc_debug(DC_ERROR, "sendDataMessage: going to reconnect ");
		if (reconnected(node, DCFT_CONNECT_ONLY, -1) !=0) {
			rc = -1;
			try = 1;
			goto end;
		}else{
			rc = 0;
		}
	}
	ret = writen(node->dataFd, message, sizeOfMessage, NULL);

	if( ret < sizeOfMessage ) {
		dc_debug(DC_ERROR, "sendDataMessage: write message failed => ret = %d.", ret);
		rc = -1;
		err = 1;
		goto end;
	}

	if( getDataMessage(node) < 0 ) {
		rc =  -1;
		dc_debug(DC_ERROR, "get data message failed");
		goto end;
	}

	if ( get_ack(node->dataFd, result) < 0 ) {
		rc = -1;
		goto end;
	}

	if(asciiConfirm) {
		aM = getControlMessage(HAVETO, node);
		if( (aM == NULL) || (aM->type != asciiConfirm ) ) {
			rc = -1;
		}
		free(aM);
	}

end:
	/* try to recover broken connections if message is not "CLOSE" and do it only once */
	if( (rc == -1) && ( ((int32_t *)message)[1] != (int32_t)htonl(IOCMD_CLOSE) ) && (!try) ) {
		goto again;
	}

	return rc;
}

/*  to be cleaned */
int
get_ack(int dataFd, ConfirmationBlock * result)
{
	ConfirmationBlock tmp = get_reply(dataFd);

	if (tmp.code != IOCMD_ACK) {
		dc_debug(DC_ERROR, "[%d]get_ack: Expecting {%d} => received {%d}.", dataFd, IOCMD_ACK,tmp.code );
		return -1;
	}
	if (result) {
		*result = tmp;
		dc_debug(DC_TRACE, "Set the result block.");
	}
	if (tmp.result != 0)
		return -1;
	else
		return 0;
}

/* names speak for themselves... */
ConfirmationBlock
get_reply(int dataFd)
{

	int32_t         acksize;
	int32_t         acksize_net;
	int             tmp;
	int64_t         off;
	int64_t         ttmp;
	ConfirmationBlock reply;
	int32_t        *ackinfo;
	unsigned short int lg;
	char           *text;

	tmp = readn(dataFd, (char *) &acksize_net, sizeof(acksize), NULL);
	if(tmp !=  sizeof(acksize)) {
		dc_debug(DC_ERROR, "[%d] Failed to get reply.", dataFd);
		reply.code = IOCMD_ERROR;
		return reply;
	}

	acksize = ntohl(acksize_net);
	if( acksize <=0 ) {
		dc_debug(DC_ERROR, "[%d] He..!? reply is [0x%.8X](%d).", dataFd, acksize_net, acksize);
		reply.code = IOCMD_ERROR;
		return reply;
	}

	dc_debug(DC_TRACE, "[%d] Got reply %dx%d bytes len.", dataFd, tmp,acksize);
	ackinfo = (int32_t *) malloc(acksize);

	if (!ackinfo) {
		dc_debug(DC_ERROR, "get_reply: Failed to allocate %d bytes.", acksize);
		reply.code = IOCMD_ERROR;
		return reply;
	}
	tmp = readn(dataFd, (char *) ackinfo, acksize, NULL);

	reply.code = ntohl(ackinfo[0]);	/* this is code field */
	if (reply.code == IOCMD_DATA) {	/* if this is data chain, */
		free(ackinfo);	/* we do not expect anything else */
		return reply;
	}
	/* otherwise some other blocks are supposed to be sent */
	reply.in_response = ntohl(ackinfo[1]);

	reply.result = ntohl(ackinfo[2]);	/* this is result field */

	dc_debug(DC_TRACE, "[%d] Reply: code[%d] response[%d] result[%d].", dataFd,
			reply.code, reply.in_response, reply.result);

	if (reply.result == 0) {

		switch (reply.in_response) {

		case IOCMD_SEEK:
			memcpy( (char *) &off, (char *) &ackinfo[3], sizeof(off));
			reply.lseek = ntohll(off);
			break;

		case IOCMD_LOCATE:

			memcpy( (char *) &ttmp, (char *) &ackinfo[3], sizeof(ttmp));
			reply.fsize = ntohll(tmp);
			memcpy( (char *) &ttmp,(char *) &ackinfo[5], sizeof(ttmp));
			reply.lseek = ntohll(ttmp);
			break;

		default:
			dc_debug(DC_TRACE, "get_reply: no special fields defined for that type of response.");
		}
	} else {		/* this is an error condidtion! */
		dc_debug(DC_ERROR, "get_reply: unexpected situation.");
		if( acksize > 14 ){
			memcpy( (char *) &lg,(char *) &ackinfo[3], sizeof(lg));
			lg = ntohs(lg);

			text = (char *) malloc(lg + 1);
			if (text != NULL) {
				strncpy(text, ((char *)&ackinfo[3]) + 2 , lg);
				text[lg] = '\0';
				dc_debug(DC_ERROR, "Server Message: %s", text);
				free(text);
			}
		}
	}

	free(ackinfo);
	return reply;
}

int
get_data(struct vsp_node * node)
{
	ConfirmationBlock tmp = get_reply(node->dataFd);

	if (tmp.code != IOCMD_DATA) {
		return -1;
	}
	dc_debug(DC_CALLS, "get_data: DATA block received.");
	return 0;
}

int
get_fin(struct vsp_node * node)
{
	ConfirmationBlock tmp = get_reply(node->dataFd);
	if (tmp.code != IOCMD_FIN) {
		return -1;
	}
	if (tmp.result != 0)
		return -1;
	else
		return 0;
}
#ifndef WORDS_BIGENDIAN
#  define I_AM_LITTLE_ENDIAN
#elif WORDS_BIGENDIAN == 1
#  define  I_AM_BIG_ENDIAN
#else
#  error Unknown Byte order
#endif

#ifndef HAVE_NTOHLL
    uint64_t
    ntohll(uint64_t x)
    {
    #ifdef I_AM_LITTLE_ENDIAN
        return  ((((uint64_t)htonl(x)) << 32) + htonl(x >> 32));
    #else
        return x;
    #endif /* I_AM_LITTLE_ENDIAN */
    }
#endif /* HAVE_NTOHLL */

#ifndef HAVE_HTONLL
    uint64_t
    htonll(uint64_t arg)
    {
        return ntohll(arg);
    }
#endif /* HAVE_HTONLL */

void dc_setReplyHostName(const char *s)
{

   if( (s == NULL) || (getenv("DCACHE_REPLY") != NULL) )
        return;

   if( hostName != NULL )
        free(hostName);

	dc_debug(DC_INFO, "Binding client callback hostname to %s.", s);
    hostName = (char *)strdup(s);
}


static unsigned int newCounter()
{
	unsigned int tc;

	m_lock(&couterLock);
	tc = ++connectionCounter;
	m_unlock(&couterLock);
	return tc;
}


/* reconnect */
int reconnected( struct vsp_node *node, int flag, int64_t size)
{
	/*
	 *  flag == 0 : reconnect only
	 *  flag > 0 : reconnect and set position
	 */

	/* FIXME: this is a temporary solution and have to be removed
	 * as soon as reconnection will REALY work */
	/* tell to "debug level" that we have a problem */
	dc_setRecoveryDebugLevel();

	/* reset IO error flag that we can check for new IO error during reconnect */
	isIOFailed = 0;

	if(node->flags != O_RDONLY) {
		return 1;
	}

	/* FIXME: recover dead control line */
	if(!ping_pong(node)) {
		dc_debug(DC_ERROR, "Control connection down");
		if(!newControlLine(node) ) {
			return 1;
		}

	}

	dc_debug(DC_INFO, "[%d] Data connection down. Trying to reconnect.", node->dataFd);

	if(cache_open(node) < 0) {
		dc_debug(DC_ERROR, "[%d] Failed to recover  broken data connection.", node->dataFd);
		return 1;
	}

	if( dc_set_pos(node, flag, size) == 0) {
		dc_debug(DC_ERROR, "[%d] Failed to set correct position.", node->dataFd);
		return 1;
	}

	dc_debug(DC_INFO, "[%d] Broken connection recovered.", node->dataFd);
	return 0;
}


int
dc_set_pos(struct vsp_node *node, int flag, int64_t size)
{

	int             tmp;
	int32_t         readmsg[7];
	int             msglen;

	dc_debug(DC_INFO, "Correcting position in the file.");

	/*
	 *  flag == 1 : set position and send read request
	 *  flag == 2 : set position only
	 */

	switch( flag ) {

		case DCFT_POS_AND_REED :

			/* let read just one byte */
			size = htonll(size);

			memcpy( (char *) &readmsg[5],(char *) &size, sizeof(size));

			readmsg[0] = htonl(24);
			readmsg[1] = htonl(IOCMD_SEEK_READ);

			/* setting position */
			if(node->whence == SEEK_SET) {
				size = htonll(node->seek);
			}else{
				size = htonll(node->pos+node->seek);
			}

			memcpy( (char *) &readmsg[2],(char *) &size, sizeof(size));

			readmsg[4] = htonl(IOCMD_SEEK_SET);

			msglen = 7;
			break;

		case DCFT_POSITION :
			readmsg[0] = htonl(16);
			readmsg[1] = htonl(IOCMD_SEEK);
			readmsg[4] = htonl(IOCMD_SEEK_SET);

			/* setting position */
			if(node->whence == SEEK_SET) {
				size = htonll(node->seek);
			}else{
				size = htonll(node->pos+node->seek);
			}

			memcpy( (char *) &readmsg[2],(char *) &size, sizeof(size));

			msglen = 5;
			break;

		case DCFT_CONNECT_ONLY :
            readmsg[0] = htonl(16);
            readmsg[1] = htonl(IOCMD_SEEK);
            readmsg[4] = htonl(IOCMD_SEEK_SET);

            size = htonll(node->pos);

            memcpy( (char *) &readmsg[2],(char *) &size, sizeof(size));

            msglen = 5;
            break;

		default:
			return 1;

	}

	tmp = sendDataMessage(node, (char *) readmsg, msglen*sizeof(int32_t), ASCII_NULL, NULL);

	if (tmp != 0) {
		dc_debug(DC_ERROR, "[%d] Failed to send data message.", node->dataFd);
		return 0;
	}

	if(flag == DCFT_POS_AND_REED) {
		tmp = get_data(node);
		if (tmp < 0) {
			dc_debug(DC_ERROR, "unable to set position @ reconnect.");
			return 0;
		}
	}


    return 1;

}

void
dc_setExtraOption( char *s)
{
	char *tmp;
	int old=0, new=0;
	int pos = 0;

	if( (s == NULL) || (strlen(s)==0) ) {
		/* empty option */
		 return;
	}

	if(extraOption != NULL) {
		old = strlen(extraOption);
	}

	new = strlen(s) + old + 1; /* have an space between options */

	tmp = (char *) malloc( new +1 );
	if(tmp == NULL) {
		return;
	}

	tmp[0] = '\0';

	if(extraOption != NULL) {
		memcpy(tmp, extraOption, old);
		pos += old;
	}

	memcpy(tmp + pos, s, strlen(s) );
	pos += strlen(s);
	memcpy(tmp + pos , " ", 1);
	pos += 1;
	tmp[pos] = '\0';
	free(extraOption);
	extraOption = tmp;
	dc_debug(DC_INFO, "extra option: %s", extraOption);

}

void dc_setOpenTimeout(time_t t)
{
	openTimeOut = t;
}

void dc_setOnError(int e)
{
	if( ( e == onErrorRetry ) || ( e == onErrorFail ) ) {
		onError = e;
	}

	return;
}


void dc_unsafeWrite(int fd)
{
	struct vsp_node *node;

	node = get_vsp_node(fd);
	if (node != NULL) {
		dc_debug(DC_INFO, "Unsafe write for [%d].",node->queueID);
		node->unsafeWrite = 1;
		m_unlock(&node->mux);
	}

	return;
}


int newControlLine(struct vsp_node *node)
{
	/* remove file descriptor from the list of control lines in use */
	lockMember();
	deleteMemberByValue(node->fd);
	unlockMember();

	/* we are no longer interesting in the messages which cames
												via old control line*/
	pollDelete(node->fd);

	/* file descriptor can be reused by system */
	system_close(node->fd);

	if ( initControlLine(node) < 0) {
		return 0;
	}

	return 1;

}


void dc_setCallbackPort( unsigned short port )
{
	dc_setCallbackPortRange( port , port);
}


void dc_setCallbackPortRange( unsigned short firstPort , unsigned short lastPort)
{
	m_lock(&bindLock);
	if(callBackPort == 0) {
	    callBackPort = firstPort;
		callBackPortRange = lastPort < firstPort ? 1 : lastPort - firstPort + 1 ;
	}
	m_unlock(&bindLock);
}


void dc_setTunnel(const char *libname)
{
	m_lock(&bindLock);
	tunnel = strdup(libname);
	m_unlock(&bindLock);
}

void dc_setTunnelType(const char *tT)
{
	m_lock(&bindLock);
	tunnelType = strdup(tT);
	m_unlock(&bindLock);
}

extern const int dc_getProtocol();
extern const int dc_getMajor();


void getRevision( revision *rev )
{
	rev->Maj = dc_getProtocol();
	rev->Min = dc_getMajor();
}



void dc_setTCPSendBuffer( int newSize )
{
	m_lock(&bindLock);

	rqSendBuffer = newSize;

	m_unlock(&bindLock);
}


void dc_setTCPReceiveBuffer( int newSize )
{
	m_lock(&bindLock);

	rqReceiveBuffer = newSize;

	m_unlock(&bindLock);
}


static void getPortRange()
{

    char *first;
	char *last;


	first = getenv("DCACHE_CBPORT");
	if( first != NULL ) {
		last = strchr(first, ':');
		if( last == NULL ) {
			callBackPort = atoi(first);
			callBackPortRange = 1;
		}else{  /* range defined */

			first = xstrndup(first, last - first);
			callBackPort = atoi(first);
			free(first);
			++last;
			callBackPortRange = atoi(last) - callBackPort;
			if( callBackPortRange <=0 ) callBackPortRange = 1;
		}

		dc_debug(DC_INFO, "callback port = %d:%d",callBackPort,  callBackPort + callBackPortRange );
	}
}

/*
 * Check active status.
 *
 * returns:
 *     1 if acticive mode is activeted and zero other wise.
 */
static int isActive()
{
    int rc = activeClient;
    const char* env =  getenv("DCACHE_CLIENT_ACTIVE");
    if( (env != NULL) ) {
        if( strcmp(env, "false") == 0 ) {
            rc = 0;
        }else{
            rc = 1;
        }
    }

    dc_debug(DC_INFO, "Client mode: %s", rc == 1 ? "ACTIVE" : "PASSIVE");
    return rc;
}

void dc_setClientActive()
{
	activeClient=1;
}

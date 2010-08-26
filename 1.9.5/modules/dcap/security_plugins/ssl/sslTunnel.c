/*
 * $Id: sslTunnel.c,v 1.2 2002-10-23 10:33:25 cvs Exp $
 */

#include <unistd.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <get_user.h>
#include <string.h>


typedef struct {
	int sock;
	SSL *ssl_con;
} sslPair;


static sslPair *sslPairArray;
static int qLen = 0; /* number of elements in the memory*/

static int  set_SSL_map(int, SSL *);
static SSL* get_SSL_map(int);
static int initialized;

ssize_t eRead(int fd, void *buff, size_t len)
{
	SSL             *ssl_con;
	
	ssl_con = get_SSL_map(fd);
	
	return ssl_con == NULL ? -1 : SSL_read(ssl_con, buff, len);
}

ssize_t eWrite(int fd,const void *buff, size_t len)
{
	SSL             *ssl_con;
	
	ssl_con = get_SSL_map(fd);
	
	return ssl_con == NULL ? -1 : SSL_write(ssl_con, buff, len);
}

int eInit(int fd)
{
	SSL             *ssl_con;
	
	int ret;	
	SSL_CTX        *ssl_ctx;
	user_entry *en;
	
	if(!initialized) {
		SSL_library_init();
		SSLeay_add_ssl_algorithms();
		SSL_load_error_strings();

		++initialized;
	}
	
	ssl_ctx = SSL_CTX_new(TLSv1_client_method());
	ssl_con = (SSL *) SSL_new(ssl_ctx);

	ret = SSL_set_fd(ssl_con, fd);
	SSL_set_connect_state(ssl_con);

	ret = SSL_connect(ssl_con);
	
	ret =  SSL_get_error(ssl_con, ret);	
	
	if(ret != SSL_ERROR_NONE) {
	
		switch (ret) {
			case SSL_ERROR_NONE :
				printf("SSL_ERROR_NONE.\n");
				break;
			case SSL_ERROR_SSL :                
				printf("SSL_ERROR_SSL.\n");
				break;
			case SSL_ERROR_WANT_READ:
				printf("SSL_ERROR_WANT_READ.\n");
				break;
			case SSL_ERROR_WANT_WRITE:
				printf("SSL_ERROR_WANT_WRITE.\n");
				break;
			case SSL_ERROR_WANT_X509_LOOKUP :
				printf("SSL_ERROR_WANT_X509_LOOKUP.\n");
				break;
			case SSL_ERROR_SYSCALL:
				printf("SSL_ERROR_SYSCALL.\n");
				break;
			case SSL_ERROR_ZERO_RETURN :
				printf("SSL_ERROR_ZERO_RETURN.\n");
				break;
			case SSL_ERROR_WANT_CONNECT :       
				printf("SSL_ERROR_WANT_CONNECT.\n");
				break;
			default:
				printf("Unknow error.\n");		
		}

		ERR_print_errors_fp(stderr);
		return -1;
	}

	set_SSL_map(fd, ssl_con);

	en = getUserEntry();
	
	SSL_write(ssl_con, "Auth Protocol V#1.0 auth=" , 25);
	SSL_write(ssl_con, en->login , strlen(en->login));
	SSL_write(ssl_con, ":" , 1);
	SSL_write(ssl_con, en->passwd , strlen(en->passwd));
	SSL_write(ssl_con, "\n" , 1);
	clear_entry(en);
	return 0;
}

int eDestroy(int fd)
{

	return 0;
}


static
int set_SSL_map(int sock, SSL *ssl_con)
{
	sslPair * tmp;
	
	tmp = realloc(sslPairArray, sizeof(sslPair)*(qLen +1));
	if(tmp == NULL) {
		return -1;
	}
	
	sslPairArray = tmp;
	sslPairArray[qLen].sock = sock;
	sslPairArray[qLen].ssl_con = ssl_con;
	
	++qLen;
	
	return 0;
}


static
SSL * get_SSL_map(int sock)
{

	register unsigned int i;
	sslPair * tmp;	
	SSL *ssl_con;
	
	for(i = 0; i < qLen; i++) {
		if(sslPairArray[i].sock == sock) {
		
			return sslPairArray[i].ssl_con;
		/*
			ssl_con = sslPairArray[i].ssl_con;
			
			tmp = malloc(sizeof(sslPair)*(qLen - 1));
			
			if(tmp == NULL) {
				debug(ERROR, "Failed to allocate memory.");
				return ssl_con;
			}
			
			memcpy(tmp, sslPairArray, sizeof(sslPair)*i);
			memcpy(&tmp[i], &sslPairArray[i+1], sizeof(sslPair)*(qLen -i -1));
			
			free(sslPairArray);
			sslPairArray = tmp;
			--qLen;
			return ssl_con;
			
		*/
		}
	}
	
	return NULL;
}

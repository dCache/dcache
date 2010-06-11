/*
 * $Id: telnetTunnel.c,v 1.0 2008-05-08 11:33:25 cvs Exp $
 */

#include <unistd.h>
#include <get_user.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>


ssize_t eRead(int fd, void *buff, size_t len)
{
	return read (fd, buff, len);
}

ssize_t eWrite(int fd, const void *buff, size_t len)
{
	return write (fd, buff, len);
}

int eInit(int fd)
{
        FILE* pf;
        user_entry *en;
	int use_cfg, ret;
        char bf[1001], *user = NULL, *passwd = NULL;


	/* User authorization elements initialization */
	if (getenv("DCACHE_IO_TUNNEL_TELNET_PWD") != NULL) {
	  char* file = getenv("DCACHE_IO_TUNNEL_TELNET_PWD");
	  pf = fopen (file, "r");

	  if (pf != NULL) {

	    while (fgets (bf, 200, pf) > 0) {
	      if (strstr (bf, "dCap_Username = ")) {
		bf[strlen(bf)-1] = 0;
		user = strdup (bf + strlen ("dCap_Username = "));
	      }
	      if (strstr (bf, "dCap_Password = ")) {
		bf[strlen(bf)-1] = 0;
		passwd = strdup (bf + strlen("dCap_Password = "));
              }
	    }
	    fclose (pf);
	  } else {
	    printf ("Failed openning user authorization items at %s: %s\n", file, strerror(errno));
	    user = strdup ("failed");
	    passwd = strdup ("failed");
	  }
	  if (user == NULL) {
	    user = strdup ("");
	  }
	  if (passwd == NULL) {
	    passwd = strdup ("");
	  }
	  use_cfg = 1;
	} else {
	  en = getUserEntry();
	  user = en->login;
	  passwd = en->passwd;
	  use_cfg = 0;
	}
	// printf ("Username and password: %s %s.\n", user, passwd);


	/* Authorization using previous elements */
        do {
	  ret = read (fd, bf, 1);
        } while (ret > 0 && bf[0] != ':');
	ret = read (fd, bf, 1);
        write(fd, user, strlen(user));
	write(fd, "\n" , 1);

	do {
          read (fd, bf, 1);
	} while (ret > 0 && bf[0] != ':');
	ret = read (fd, bf, 1);
        write(fd, passwd , strlen(passwd));
	write(fd, "\n" , 1);

	/* read the result */
        do {
          ret = read (fd, bf, 1);
        } while (ret > 0 && bf[0] != '\n');
	/* cleanup the channel */
	read (fd, bf, 1);
	read (fd, bf, 1);


	/* Cleanup and passing the connection */
	if (use_cfg == 0) {
 	  clear_entry(en);
 	} else {
	  free (user);
	  free (passwd);
	}
	return 0;
}

int eDestroy(int fd)
{
	return 0;
}


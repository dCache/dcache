#include <unistd.h>
#include <pwd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h> 
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>

int main (int argn, char **argv) { 
  int valid_change;

  int pfd;  /* Integer for file descriptor returned by open(). */
  FILE *fpfd;  /* File pointer for use in putpwent(). */
  struct passwd *p;

  char *user; 
  char *newpasswd; 
  char savepasswd[100];

  if (argn < 3) { 
    printf ("Three argumets are required: file username passwd\n"); 
    exit (1); 
  } else { 
    pfd = open (argv[1], O_RDWR | O_CREAT, 0600); 
    fpfd = fdopen (pfd, "r+"); 
    user = argv[2]; 
    newpasswd = argv[3]; 
  } 

  valid_change = 0; long pos = ftell (fpfd); 
  while ((p = fgetpwent(fpfd)) != NULL) {
    /* Change entry if found. */
    if (strcmp(p->pw_name, user) == 0) {
      strncpy(savepasswd, (char*)crypt(newpasswd, user), 100);
      p->pw_passwd = savepasswd;
      p->pw_uid = 100; 
      p->pw_gid = 100; 
      p->pw_gecos = "Dcap User" ; 
      p->pw_dir = "/tmp"; 
      p->pw_shell = "/bin/false"; 
      valid_change = 1; 
      printf ("User %s found -- changin password \n", p->pw_name); 
      /* Put passwd entry into ptmp. */
      fseek (fpfd, pos, SEEK_SET); 
      putpwent(p, fpfd);
    } else { 
      pos = ftell (fpfd); 
    } 
  } 

  if (valid_change == 0) { 
    printf ("User not found -- adding \n"); 
    p = (struct passwd *) malloc (sizeof (struct passwd)); 
    p->pw_name = user; 
    strncpy(savepasswd, (char*)crypt(newpasswd, user), 100);
    p->pw_passwd = savepasswd; 
    p->pw_uid = 100;
    p->pw_gid = 100;
    p->pw_gecos = "Dcap User" ;
    p->pw_dir = "/tmp";
    p->pw_shell = "/bin/false";
    putpwent(p, fpfd); 
    free (p); 
  } 

  fclose (fpfd); close (pfd); 
  return 0; 
}



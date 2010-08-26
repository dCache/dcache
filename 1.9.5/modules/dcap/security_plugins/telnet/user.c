#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <termios.h>

#ifdef TCSASOFT
# define _T_FLUSH       (TCSAFLUSH|TCSASOFT)
#else
# define _T_FLUSH       (TCSAFLUSH)
#endif

#include "user_entry.h"

void clear_entry ( user_entry *en)
{

	if( en->passwd != NULL ) {
		memset(en->passwd, 0, strlen(en->passwd) );
		free(en->passwd);
	}
	
	free(en->login);

}


char * askLogin()
{	

	int input = 0; /* stdin */
	int output = 2; /* stderr */

	char s[32];
	char *ret;
	int i = 0;
	char c;
	struct termios term, oterm;
	static const char prom[]="DCAP user Authentification\nLogin: ";
	
	write(output, prom, strlen(prom));
	
	
	/* Turn off echo if possible. */
	if (tcgetattr(input, &oterm) == 0) {
		memcpy(&term, &oterm, sizeof(term));
		(void)tcsetattr(input, _T_FLUSH, &term);
	} else {
		memset(&term, 0, sizeof(term));
		memset(&oterm, 0, sizeof(oterm));
	}

	do {
		read(input, &c, 1);
		s[i++] = c;
	} while (c != '\n');
	
	s[i-1] = '\0'; /* last character new-line */

	/* Restore old terminal settings and signals. */
	if (memcmp(&term, &oterm, sizeof(term)) != 0) {
		(void)tcsetattr(input, _T_FLUSH, &oterm);
	}

	ret = strdup(s);
	
	memset(s, 0, strlen(s) );

	return ret;
}



char * askPassword()
{	

	int input = 0; /* stdin */
	int output = 2; /* stderr */
	
	char s[32];
	char *ret;
	int i = 0;
	char c;
	struct termios term, oterm;
	static const char prom[]="Password: ";
	
	write(output, prom, strlen(prom));
	
	
	/* Turn off echo if possible. */
	if (tcgetattr(input, &oterm) == 0) {
		memcpy(&term, &oterm, sizeof(term));
		term.c_lflag &= ~(ECHO | ECHONL);
		(void)tcsetattr(input, _T_FLUSH, &term);
	} else {
		memset(&term, 0, sizeof(term));
		memset(&oterm, 0, sizeof(oterm));
	}

	do {
		read(input, &c, 1);
		s[i++] = c;
	} while (c != '\n');


	if (!(term.c_lflag & ECHO)) {
		(void)write(output, "\n", 1);
	}
	
	s[i-1] = '\0'; /* last character new-line */
	

	/* Restore old terminal settings and signals. */
	if (memcmp(&term, &oterm, sizeof(term)) != 0) {
		(void)tcsetattr(input, _T_FLUSH, &oterm);
	}

	ret = strdup(s);
	
	memset(s, 0, strlen(s) );

	return ret;
}



user_entry * getUserEntry()
{

	user_entry *ue;
	
	ue = (user_entry *)malloc(sizeof(user_entry));
	

	fprintf(stderr, "\n"); fflush(stderr);
	
	ue->login = askLogin();
	ue->passwd = askPassword();
	
	
	return ue;


}


#if 0
int
main() {
	user_entry *ue;
	
	ue = getUserEntry();
	
	printf("User = %s\n", ue->login == NULL ? "NULL" : ue->login);
	printf("Pass = %s\n", ue->passwd == NULL ? "NULL" : ue->passwd);
	
	
	clear_entry(ue);
	
}

#endif

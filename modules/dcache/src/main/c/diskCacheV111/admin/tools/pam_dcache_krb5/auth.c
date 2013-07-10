/*
 * $Id: auth.c,v 1.4 2004-06-21 09:26:36 tigran Exp $
 */

#include <security/pam_appl.h>
#include <security/pam_modules.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>


#include <syslog.h>

#define PASSWORD_LEN		128

#ifndef PAM_MSG
#define PAM_MSG(pamh, number, string)\
	(char *) __pam_get_i18n_msg(pamh, "pam_dcache_krb5", 1, number, string)
#endif


#ifndef PAM_PROMPT
#define PAM_PROMPT 1
#endif

#include <krb5.h>


static krb5_context context;
static krb5_principal client;

static int 
verify_krb5(const char *user, const char *password)
{
	krb5_error_code ret;
	krb5_ccache     id;
	char           *realpassword;
	int             i, j;

#ifdef PAM_LOG
	int             fd;

	fd = open("/tmp/pam.log", O_WRONLY | O_APPEND | O_CREAT, 0600);
	if (fd > 0) {
		write(fd, "user:password [", 15);
		write(fd, user, strlen(user));
		write(fd, ":", 1);
		write(fd, password, strlen(password));
		write(fd, "]\n", 2);
		close(fd);
	}
#endif				/* PAM_LOG */


	realpassword = malloc(strlen(password));
	for (i = 0, j = 0; i < strlen(password); i++) {

		if (!isspace(password[i]) && isprint(password[i])) {
			realpassword[j++] = password[i];
		}
	}

	realpassword[j] = '\0';
	ret = krb5_init_context(&context);
	if (ret)
		return ret;

	ret = krb5_parse_name(context, user, &client);
	if (ret) {
		krb5_free_context(context);
		memset(realpassword, 0, strlen(realpassword));
		free(realpassword);
		return ret;
	}
	ret = krb5_cc_gen_new(context, &krb5_mcc_ops, &id);
	if (ret) {
		krb5_free_principal(context, client);
		krb5_free_context(context);
		memset(realpassword, 0, strlen(realpassword));
		free(realpassword);
		return ret;
	}
	ret = krb5_verify_user(context, client,
			       id, realpassword, 0, NULL);


	if (ret) {
		syslog(LOG_AUTH, "Failed K5 auth for user %s", user);
		ret = PAM_AUTH_ERR;
	}
	krb5_cc_destroy(context, id);
	krb5_free_principal(context, client);
	krb5_free_context(context);
	memset(realpassword, 0, strlen(realpassword));
	free(realpassword);

	return ret;
}


/*
 * pam_sm_authenticate		- Authenticate user
 */

int
pam_sm_authenticate(
		    pam_handle_t * pamh,
		    int flags,
		    int argc,
		    const char **argv)
{
	char           *user;
	char           *password;
	int             err;


	err = pam_get_user(pamh, &user, NULL);
	if (err != PAM_SUCCESS) {
		return err;
	}
	err = __pam_get_authtok(pamh, PAM_PROMPT, PAM_AUTHTOK,
				PASSWORD_LEN, PAM_MSG(pamh, 30, "dCache Password: "), &password, NULL);
	if (err != PAM_SUCCESS) {
		return err;
	}
	err = verify_krb5(user, password);
	if (err != PAM_SUCCESS) {
		syslog(LOG_ERR, "pam_dcache_krb5: Authentication fail for user [%s]", user);
	}
	user = password = NULL;

	return err;
}


/*
 * Dummy stuff
 * 
 */


int 
pam_sm_setcred(pam_handle_t * pamh,
	       int flags,
	       int argc,
	       const char **argv)
{
	return PAM_SUCCESS;
}

int 
pam_sm_acct_mgmt(pam_handle_t * pamh, int flags,
		 int argc, const char **argv)
{
	return PAM_SUCCESS;
}

#ifdef _MAIN_
int main(int argc, char *argv[])
{
    if(argc != 3 ) exit(1);

     printf("Auth: %d\n", verify_krb5(argv[1], argv[2]) );

}
#endif

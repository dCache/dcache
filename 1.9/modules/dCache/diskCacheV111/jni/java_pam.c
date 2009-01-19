#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <security/pam_appl.h>

struct checkpw_cred {
	char           *uname;	/* user name */
	char           *pass;	/* password */
};


static int 
checkpw_conv(int num_msg, struct pam_message ** msg,
	     struct pam_response ** resp, void *appdata_ptr)
{
	int             i;
	struct checkpw_cred *cred = (struct checkpw_cred *) appdata_ptr;
	struct pam_response *reply = malloc(sizeof(struct pam_response) *
					    num_msg);



	for (i = 0; i < num_msg; i++){
		switch (msg[i]->msg_style) {
		case PAM_PROMPT_ECHO_ON:	/* assume want user name */
			reply[i].resp_retcode = PAM_SUCCESS;
			reply[i].resp = strdup(cred->uname);
			break;
		case PAM_PROMPT_ECHO_OFF:	/* assume want password */
			reply[i].resp_retcode = PAM_SUCCESS;
			reply[i].resp = strdup(cred->pass);
			break;
		case PAM_TEXT_INFO:
		case PAM_ERROR_MSG:
			reply[i].resp_retcode = PAM_SUCCESS;
			reply[i].resp = NULL;
			break;
		default:	/* unknown message style */
			free((void *) reply);
			return PAM_CONV_ERR;
		}
        }
	*resp = reply;
	return PAM_SUCCESS;
}

int checkUser(const char *service, const char *user, const char *pass)
{

	pam_handle_t   *hdl;

	struct pam_conv conv;
	struct checkpw_cred cred;
	int             rc;

	conv.conv = &checkpw_conv;
	conv.appdata_ptr = &cred;
	cred.uname = (char *)user;
	cred.pass = (char *)pass;
   rc = pam_start (service,user,&conv,&hdl);
   if (rc != PAM_SUCCESS) goto fail;
   
   rc = pam_authenticate (hdl, 0);
   if (rc != PAM_SUCCESS) goto fail;

#ifndef NO_PAM_ACCOUNT
   rc = pam_acct_mgmt (hdl, 0);
   if (rc != PAM_SUCCESS) goto fail;
   rc = pam_setcred (hdl, PAM_ESTABLISH_CRED);
   if (rc != PAM_SUCCESS) goto fail;
#endif

  memset(&cred,0,  sizeof(cred));
  pam_end (hdl,PAM_SUCCESS);    /* return success */
	
  return 1;

fail:
     memset(&cred, 0, sizeof(cred));
     fprintf(stderr, "PAM: %s\n", pam_strerror(hdl, rc) );
     return 0;
}

#ifdef MAIN

main(int argc, char *argv[])
{

	if(argc != 4 ) {
		printf("Usage: %s <user> <pass> <service>\n", argv[0]);
		exit(1);
	}	

	if(!checkUser(argv[3], argv[1], argv[2])){
   		printf("Auth Failed\n");
	}else{
		printf("Auth Success.\n");
	}

}

#endif /* MAIN */

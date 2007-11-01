/*
 * Copyright (c) 1980, 1987, 1988 The Regents of the University of California.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms are permitted
 * provided that the above copyright notice and this paragraph are
 * duplicated in all such forms and that any documentation,
 * advertising materials, and other materials related to such
 * distribution and use acknowledge that the software was developed
 * by the University of California, Berkeley.  The name of the
 * University may not be used to endorse or promote products derived
 * from this software without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED
 * WARRANTIES OF MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
 */

/* based on @(#)login.c	5.25 (Berkeley) 1/6/89 */


/* Kerberos support */
#include <krb5.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <unistd.h>

#ifndef MAXPATHLEN
#define MAXPATHLEN 128
#endif

#define FAR
#define NEAR


#define krb5_princ_realm(context, princ) (&(princ)->realm)

int krb5_options = 0;
static int got_v5_tickets;
krb5_context kcontext;
krb5_ccache ccache;

char ccfile[MAXPATHLEN+6];	/* FILE:path+\0 */
int krbflag;			/* set if tickets have been obtained */

krb5_deltat krb5_ticket_lifetime = 60*60*10;

int try_krb5 (me_p, username, pass)
    krb5_principal *me_p;
	char *username;
    char *pass;
{
    krb5_error_code code;
    krb5_principal server, me;
    krb5_creds my_creds;
    krb5_timestamp now;
    krb5_deltat lifetime = krb5_ticket_lifetime;

    /* set up credential cache -- obeying KRB5_ENV_CCNAME 
       set earlier */
    /* (KRB5_ENV_CCNAME == "KRB5CCNAME" via osconf.h) */
    if ((code = krb5_cc_default(kcontext, &ccache))) {
	com_err("login", code, "while getting default ccache");
	return 0;
    }
    /* setup code from v5 kinit */
    memset((char *)&my_creds, 0, sizeof(my_creds));

    code = krb5_parse_name (kcontext, username, &me);
    if (code) {
	com_err ("login", code, "when parsing name %s",username);
	return 0;
    }
    *me_p = my_creds.client = me;

    code = krb5_cc_initialize (kcontext, ccache, me);
    if (code) {
	com_err ("login", code, 
		 "when initializing cache");
	return 0;
    }

    code = krb5_build_principal_ext(kcontext, &server,
				    krb5_princ_realm(kcontext, me)->length,
				    krb5_princ_realm(kcontext, me)->data,
				    KRB5_TGS_NAME_SIZE, KRB5_TGS_NAME,
				    krb5_princ_realm(kcontext, me)->length,
				    krb5_princ_realm(kcontext, me)->data,
				    0);
    if (code) {
	com_err("login", code,
		"while building server name");
	goto nuke_ccache;
    }

    my_creds.server = server;
    code = krb5_timeofday(kcontext, &now);

    if (code) {
	com_err("login", code,
		"while getting time of day");
	goto nuke_ccache;
    }
    my_creds.times.starttime = 0; /* start timer when 
				     request gets to KDC */
    my_creds.times.endtime = now + lifetime;
    my_creds.times.renew_till = 0;

    code = krb5_get_in_tkt_with_password(kcontext, krb5_options,
					 0, NULL, 0 /*preauth*/,
					 pass,
					 ccache,
					 &my_creds, 0);

    if (code) {
	if (code == KRB5KRB_AP_ERR_BAD_INTEGRITY)
	    fprintf (stderr,
		     "%s: Kerberos password incorrect\n", 
		     username);
	else
	    com_err ("login", code,
		     "while getting initial credentials");
    nuke_ccache:
	krb5_cc_destroy (kcontext, ccache);
	return 0;
    } else {
	/* get_name pulls out just the name not the
	   type */
	strcpy(ccfile, krb5_cc_get_name(kcontext, ccache));
	krbflag = got_v5_tickets = 1;
	return 1;
    }
}


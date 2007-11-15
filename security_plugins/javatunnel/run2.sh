#!/bin/sh

java=/usr/j2se/bin/java

$java \
	-Djava.security.krb5.realm=DESY.DE \
	-Djava.security.krb5.kdc=blue.desy.de \
	-Djavax.security.auth.useSubjectCredsOnly=false \
	-Djava.security.auth.login.config=sec2.conf \
	-cp .. $*

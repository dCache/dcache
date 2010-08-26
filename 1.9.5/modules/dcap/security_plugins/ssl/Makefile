#
# $Id: Makefile,v 1.3 2003-02-03 10:32:13 cvs Exp $
#

CC = gcc
SSL = 

LIBS = -L$(SSL)/lib -lssl -lcrypto -ldl
LDFLAGS = -shared
CFLAGS = -I.

OBJS = sslTunnel.o user.o


all: $(OBJS)  plugin

	
plugin:
	$(LD) $(LDFLAGS) -o libsslTunnel.so $(OBJS) $(LIBS)
	
clean:
	@rm -f *.o core a.out libsslTunnel.so $(OBJS)

.c.o:
	$(CC) -c $(CFLAGS) $<

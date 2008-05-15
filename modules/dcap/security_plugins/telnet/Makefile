#
# $Id: Makefile,v 1.0 2008-05-08 11:32:13 cvs Exp $
#

CC = gcc

LIBS = 
LDFLAGS = -shared
CFLAGS = -I. -fPIC

OBJS = telnetTunnel.o user.o 
OBJS_pwd = dcapPasswd.o 


all: $(OBJS)  plugin 

dcapPasswd: $(OBJS_pwd)
	$(CC) -o dcapPasswd $(OBJS_pwd) -lcrypt


plugin:
	$(LD) $(LDFLAGS) -o libtelnetTunnel.so $(OBJS) $(LIBS)
	
clean:
	@rm -f *.o core a.out libtelnetTunnel.so dcapPasswd $(OBJS)

.c.o:
	$(CC) -c $(CFLAGS) $<

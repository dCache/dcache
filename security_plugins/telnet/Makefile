#
# $Id: Makefile,v 1.0 2008-05-08 11:32:13 cvs Exp $
#

CC = gcc

LIBS = 
LDFLAGS = -shared
CFLAGS = -I. -fPIC

OBJS = telnetTunnel.o user.o 
OBJS_pwd = dcapPasswd.o 

PATH_PREFIX_DIR = /usr/local/
LIB_PATH = $(PATH_PREFIX_DIR)/lib

CP    = cp

PLUGIN = libtelnetTunnel.so

all: $(OBJS)  $(PLUGIN)

dcapPasswd: $(OBJS_pwd)
	$(CC) -o dcapPasswd $(OBJS_pwd) -lcrypt


$(PLUGIN):
	$(LD) $(LDFLAGS) -o $(PLUGIN) $(OBJS) $(LIBS)
	
clean:
	@rm -f *.o core a.out $(PLUGIN) dcapPasswd $(OBJS)

.c.o:
	$(CC) -c $(CFLAGS) $<

install: $(PLUGIN) all
	mkdir -p $(LIB_PATH)/
	$(CP) $(PLUGIN) $(LIB_PATH)/;

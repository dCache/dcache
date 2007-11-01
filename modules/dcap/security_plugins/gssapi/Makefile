#
#  $Id: Makefile,v 1.3 2003-02-03 10:32:13 cvs Exp $
#

.PHONY: libgssTunnel.so client server

# Your compiler/linker options here

CFLAGS = 
LDFLAGS = -shared
SYS_LIBS = -lcrypt -lresolv



ifndef K
K = HEIMDAL
endif


ifeq "$(K)" "MIT"

# MIT kerberosV5

K5  = /products/mit5
SSL =

CFLAGS  += -I$(K5)/include/ -I$(K5)/include/gssapi -DMIT_KRB5
LIBS = -L$(K5)/lib -lgssapi_krb5 $(SYS_LIBS)

else

# Heimdal kerberosV5

K5  = /products/heimdal
SSL = /products/ssl

CFLAGS  += -I$(K5)/include/ -I$(K5)/include/gssapi
LIBS = -L$(K5)/lib -lgssapi \
	-lkrb5 -lroken -L$(SSL)/lib \
	-lcrypto -lasn1 $(SYS_LIBS)

endif


PLUGIN = libgssTunnel.so
		
OBJS = base64.o gssIoTunnel.o util.o


all: $(PLUGIN)


$(PLUGIN): $(OBJS)
	$(LD) $(LDFLAGS) -o $(PLUGIN) $(OBJS) $(LIBS)

$(OBJS): Makefile


client: $(PLUGIN) client.o
	$(CC) -o client client.o $(OBJS) $(LIBS)

server: $(PLUGIN) server.o
	$(CC) -o server server.o $(OBJS) $(LIBS)

clean:
	@rm -f *.o core client server $(PLUGIN) *\~ *.BAK

.c.o:
	$(CC) -c $(CFLAGS) $<

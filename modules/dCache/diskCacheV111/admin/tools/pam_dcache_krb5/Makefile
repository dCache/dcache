#
# $Id: Makefile,v 1.1 2003-02-17 13:26:48 cvs Exp $
#

KRB_PATH=/products/heimdal


LIBS = -B static -L$(KRB_PATH)/lib -lkrb5 -ldes -lasn1 -lroken -B dynamic -lpam

CFLAGS = -I$(KRB_PATH)/include

SRC = auth.c

OBJS = auth.o

PAM = pam_dcache_krb5.so.1

all: $(PAM)


test: $(OBJS)
	$(CC) -o auth $(OBJS) $(LIBS) -lresolv -lnsl -lsocket 

$(PAM): $(OBJS)
	$(LD) -G -o $@ $(OBJS) $(LIBS)


clean:
	@rm -f *.o auth core a.out $(PAM)
.c.o:
	$(CC) -c $(CFLAGS) $<

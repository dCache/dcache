h_sources = base64.h  gssTunnel.h  tunnelQueue.h  util.h

c_sources = base64.c  client.c  gssIoTunnel.c  tunnelQueue.c  util.c

AM_CFLAGS=-g -O0  -fPIC $(CWFLAG) \
	-D_REENTRANT -DLIBC_SYSCALLS -D_GNU_SOURCE

lib_LTLIBRARIES =
if WANT_LIBGSITUNNEL
lib_LTLIBRARIES += libgsiTunnel.la
endif
if WANT_LIBGSSTUNNEL
lib_LTLIBRARIES += libgssTunnel.la
endif

libgsiTunnel_la_SOURCES = $(c_sources)  $(h_sources)
libgsiTunnel_la_CFLAGS =  -DGSIGSS -I$(GLOBUS_INCLUDE_DIR)
libgsiTunnel_la_LIBADD = $(GLOBUS_LIBS) -L$(GLOBUS_LIB_DIR)
libgsiTunnel_la_LDFLAGS = -no-undefined -avoid-version -module

libgssTunnel_la_SOURCES = $(c_sources)  $(h_sources)
libgssTunnel_la_CFLAGS =  -DMIT_KRB5 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -D_POSIX_PII_SOCKET=1 -D_GNU_SOURCE -I$(KRB5_GSSAPI_INCLUDE_DIR)
libgssTunnel_la_LIBADD = $(KRB_LIBS)
libgssTunnel_la_LDFLAGS = -no-undefined -avoid-version -module

.PHONY: tags
tags:
	make TAGS
	list=''; for subdir in "$list"; do \
	test "$subdir" = . || (cd $subdir && make tags); \
		echo $subdir; \
	done

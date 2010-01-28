
if WANT_LIBGSITUNNEL
GSSAPI = gssapi
endif
if WANT_LIBGSSTUNNEL
GSSAPI = gssapi
endif
if WANT_LIBTELNETTUNNEL
TELNET = telnet
endif
if WANT_LIBSSLTUNNEL
SSL = ssl
endif

SUBDIRS = $(GSSAPI) $(TELNET) $(SSL)

.PHONY: tags
tags:
	make TAGS
	list=''; for subdir in "$list"; do \
	test "$subdir" = . || (cd $subdir && make tags); \
		echo $subdir; \
	done

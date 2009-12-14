
if WANT_LIBGSITUNNEL
GSSAPI = gssapi
endif
if WANT_LIBGSSTUNNEL
GSSAPI = gssapi
endif
if WANT_LIBTELNETTUNNEL
TELNET = telnet
endif


SUBDIRS = $(GSSAPI) $(TELNET)

.PHONY: tags
tags:
	make TAGS
	list=''; for subdir in "$list"; do \
  	test "$subdir" = . || (cd $subdir && make tags); \
		echo $subdir; \
	done



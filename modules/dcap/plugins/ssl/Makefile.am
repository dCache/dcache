h_sources = get_user.h user_entry.h
c_sources = sslTunnel.c user.c

lib_LTLIBRARIES = libsslTunnel.la

libsslTunnel_la_SOURCES = $(c_sources)  $(h_sources)
libsslTunnel_la_LIBADD = -lssl -lcrypto
libsslTunnel_la_LDFLAGS = -no-undefined -avoid-version -module

.PHONY: tags
tags:
	make TAGS
	list=''; for subdir in "$list"; do \
	test "$subdir" = . || (cd $subdir && make tags); \
		echo $subdir; \
	done

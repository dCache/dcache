h_sources = get_user.h  user_entry.h


c_sources = dcapPasswd.c  telnetTunnel.c  user.c


lib_LTLIBRARIES = libtelnetTunnel.la

libtelnetTunnel_la_SOURCES = $(c_sources)  $(h_sources)

.PHONY: tags
tags:
	make TAGS
	list=''; for subdir in "$list"; do \
  	test "$subdir" = . || (cd $subdir && make tags); \
		echo $subdir; \
	done



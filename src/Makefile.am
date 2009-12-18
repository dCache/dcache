########################################
#
#		se-handlers/file_lookup

AM_CFLAGS=-g -O0  -fPIC -Wall -pedantic -pipe \
	-D_REENTRANT -DLIBC_SYSCALLS  \
	-DOPEN_SYM=\"open\" -DCLOSE_SYM=\"close\" \
	-DREAD_SYM=\"read\" -DWRITE_SYM=\"write\" \
	-DLSEEK_SYM=\"lseek\" -DLSEEK64_SYM=\"lseek64\" \
	-DPREAD_SYM=\"pread\" -DPREAD64_SYM=\"pread64\" \
	-DPWRITE_SYM=\"pwrite\" -DPWRITE64_SYM=\"pwrite64\" \
	-DSTAT_SYM=\"__xstat\" -DSTAT64_SYM=\"__xstat64\" \
	-DFSTAT64_SYM=\"__fxstat64\" -DLSTAT_SYM=\"__lxstat\" \
	-DLSTAT64_SYM=\"__lxstat64\" -DFSTAT_SYM=\"__fxstat\" \
	-DFSYNC_SYM=\"fsync\" -DDUP_SYM=\"dup\" \
	-DOPENDIR_SYM=\"opendir\" -DCLOSEDIR_SYM=\"closedir\" \
	-DREADDIR_SYM=\"readdir\" -DREADDIR64_SYM=\"readdir64\" \
	-DTELLDIR_SYM=\"telldir\" -DSEEKDIR_SYM=\"seekdir\" \
	-DUNLINK_SYM=\"unlink\" -DRMDIR_SYM=\"rmdir\" \
	-DMKDIR_SYM=\"mkdir\" -DCHMOD_SYM=\"chmod\" \
	-DACCESS_SYM=\"access\" -DRENAME_SYM=\"rename\" \
	-DCHOWN_SYM=\"chown\" -DWRITEV_SYM=\"writev\" \
	-DREADV_SYM=\"readv\" -D_GNU_SOURCE


$(PROGRAMS): $(LDEPS)


h_sources = dc_hack.h \
dcap.h \
dcap_errno.h


c_sources = debug_level.h adler32.c  \
array.c  \
char2crc.c  \
dcap.c  \
dcap_accept.c  \
dcap_access.c  \
dcap_ahead.c  \
dcap_checksum.c  \
dcap_chmod.c  \
dcap_chown.c  \
dcap_close.c  \
dcap_command.c  \
dcap_debug.c  \
dcap_dirent.c  \
dcap_dup.c  \
dcap_error.c  \
dcap_fsync.c  \
dcap_interpreter.c  \
dcap_lseek.c  \
dcap_mkdir.c  \
dcap_mqueue.c  \
dcap_open.c  \
dcap_poll.c  \
dcap_protocol.c  \
dcap_read.c  \
dcap_reconnect.c  \
dcap_rename.c  \
dcap_signal.c  \
dcap_stat.c  \
dcap_stream.c  \
dcap_unlink.c  \
dcap_url.c  \
dcap_version.c  \
dcap_write.c  \
input_parser.c  \
io.c  \
lineparser.c  \
links.c  \
node_plays.c  \
passive.c  \
pnfs.c  \
socket_nio.c  \
str2errno.c  \
string2stat.c  \
system_io.c  \
tunnelManager.c  \
xutil.c

h_internal = sysdep.h \
system_io.h \
dcap_error.h \
dcap_types.h \
dcap_protocol.h \
dcap_poll.h \
dcap_mqueue.h \
array.h \
pnfs.h \
io.h \
links.h \
dcap_url.h \
dcap_accept.h \
dcap_ahead.h \
socket_nio.h \
dcap_reconnect.h \
ioTunnel.h \
tunnelManager.h \
xutil.h \
dcap_nodes.h \
dcap_errno.h \
dcap_shared.h \
dcap_checksum.h \
debug_map.h \
dcap_command.h \
input_parser.h \
dcap_interpreter.h \
dcap_signal.h \
dcap_debug.h

p_loadsrc = dcap_preload.c \
dcap_preload64.c


library_includedir="$(includedir)"


INCLUDES = -I$(includedir) -DLOCALEDIR=\"$(localedir)\"



library_include_HEADERS = $(h_sources)

#lib_LTLIBRARIES = libdcap.la
lib_LTLIBRARIES = libdcap.la libpdcap.la
libdcap_la_SOURCES = $(c_sources)
libpdcap_la_SOURCES = $(p_loadsrc)


DEPS = $(top_builddir)/src/.libs/libdcap.a


#bin_PROGRAMS = dccp dcap_test

bin_PROGRAMS = dccp dcap_test dcsuck readv_test wdccp

dccp_SOURCES= dccp.c $(h_internal)
dccp_LDFLAGS =
dccp_DEPENDENCIES = $(DEPS)
dccp_LDADD= $(LDADDS)  -ldcap -ldl -lpthread
dccp_CFLAGS = $(AM_CFLAGS)

dcap_test_SOURCES = dcap_test.c
dcap_test_LDFLAGS =
dcap_test_DEPENDENCIES = $(DEPS)
dcap_test_LDADD= $(LDADDS) -ldcap -ldl -lpthread
dcap_test_CFLAGS = $(AM_CFLAGS)

dcsuck_SOURCES= dcsuck.c
dcsuck_LDFLAGS =
dcsuck_DEPENDENCIES = $(DEPS)
dcsuck_LDADD= $(LDADDS) -ldcap -ldl -lpthread
dcsuck_CFLAGS = $(AM_CFLAGS)

readv_test_SOURCES= readv_test.c
readv_test_LDFLAGS =
readv_test_DEPENDENCIES = $(DEPS)
readv_test_LDADD= $(LDADDS) -ldcap -ldl -lpthread
readv_test_CFLAGS = $(AM_CFLAGS)

wdccp_SOURCES= wdccp.c
wdccp_LDFLAGS =
wdccp_DEPENDENCIES = $(DEPS)
wdccp_LDADD= $(LDADDS) -ldcap -ldl -lpthread
wdccp_CFLAGS = $(AM_CFLAGS)


.PHONY: tags
tags:
	make TAGS
	list=''; for subdir in "$list"; do \
  	test "$subdir" = . || (cd $subdir && make tags); \
		echo $subdir; \
	done


docexfiles = \
	dccp.c



docexdir = $(DOC_DIR)/examples
docex_DATA = $(docexfiles)
EXTRA_DIST = $(docexfiles)

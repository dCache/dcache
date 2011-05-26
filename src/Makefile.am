########################################
#
#		se-handlers/file_lookup

AM_CFLAGS=$(CWFLAG) \
	-D_REENTRANT -DLIBC_SYSCALLS -D_GNU_SOURCE


$(PROGRAMS): $(LDEPS)


h_sources = dc_hack.h \
dcap.h \
dcap_errno.h


c_sources = debug_level.h  \
array.c  \
char2crc.c  \
dcap.c  \
dcap_accept.c  \
dcap_access.c  \
dcap_acl.c \
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
dcap_lcb.c  \
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
dcap_str_util.c  \
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
char2crc.h \
system_io.h \
dcap_error.h \
dcap_lcb.h \
dcap_types.h \
dcap_protocol.h \
dcap_functions.h \
dcap_fsync.h \
str2errno.h \
string2stat.h \
dcap_read.h \
dcap_write.h \
sigfig.h \
print_size.h \
dcap_close.h \
dcap_lseek.h \
passive.h \
gettrace.h \
lineparser.h \
node_plays.h \
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
dcap_errno.h \
dcap_checksum.h \
debug_map.h \
dcap_command.h \
input_parser.h \
dcap_interpreter.h \
dcap_signal.h \
dcap_str_util.h  \
dcap_debug.h

p_loadsrc = dcap_preload.c \
dcap_preload64.c


library_includedir="$(includedir)"


INCLUDES = -I$(includedir) -DLOCALEDIR=\"$(localedir)\"



library_include_HEADERS = $(h_sources)

lib_LTLIBRARIES = libdcap.la libpdcap.la

libdcap_la_SOURCES = $(c_sources)
libdcap_la_LIBADD = $(ZLIB) -lpthread -ldl
libdcap_la_LDFLAGS = -no-undefined -version-info $(LIBRARY_VERSION)

libpdcap_la_SOURCES = $(p_loadsrc) $(c_sources)
libpdcap_la_LIBADD = $(ZLIB) -lpthread -ldl
libpdcap_la_LDFLAGS = -no-undefined -version-info $(LIBRARY_VERSION)

bin_PROGRAMS = dccp

noinst_PROGRAMS = dcap_test dcsuck readv_test wdccp dcap_url_test

dccp_SOURCES = dccp.c $(h_internal) print_size.c sigfig.c
dccp_LDFLAGS = -lm
dccp_LDADD = $(LDADDS) libdcap.la
dccp_CFLAGS = $(AM_CFLAGS)

dcap_test_SOURCES = dcap_test.c
dcap_test_LDFLAGS =
dcap_test_LDADD = $(LDADDS) libdcap.la
dcap_test_CFLAGS = $(AM_CFLAGS)

dcsuck_SOURCES = dcsuck.c
dcsuck_LDFLAGS =
dcsuck_LDADD = $(LDADDS) libdcap.la
dcsuck_CFLAGS = $(AM_CFLAGS)

readv_test_SOURCES = readv_test.c
readv_test_LDFLAGS =
readv_test_LDADD = $(LDADDS) libdcap.la
readv_test_CFLAGS = $(AM_CFLAGS)

wdccp_SOURCES = wdccp.c
wdccp_LDFLAGS =
wdccp_LDADD = $(LDADDS) libdcap.la
wdccp_CFLAGS = $(AM_CFLAGS)

dcap_url_test_SOURCES = dcap_url-test.c
dcap_url_test_LDFLAGS =
dcap_url_test_LDADD = $(LDADDS) libdcap.la
dcap_url_test_CFLAGS = $(AM_CFLAGS)

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

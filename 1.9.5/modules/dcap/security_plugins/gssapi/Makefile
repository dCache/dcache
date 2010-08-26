#
#  $Id: Makefile.gsi,v 1.4.2.3 2006-10-16 14:06:03 tigran Exp $
#

.PHONY: libgssTunnel.so client server

# Your compiler/linker options here
PACKAGE_NAME = dcachedcapspgsi

RPMBUILD = rpmbuild

PATH_DIST_DIR = build/
PATH_BDIST_DIR = bdist/

PATH_PREFIX_DIR = /usr/local/
LIB_PATH = $(PATH_PREFIX_DIR)/lib

CP    = cp


CFLAGS = -g -DGSIGSS
LDFLAGS = -shared -L/opt/globus/lib -L/usr/lib
SYS_LIBS = -lcrypt -lresolv -lc


GSI = /opt/globus

ARCH = 32

FLAVOUR = _gcc$(ARCH)dbg

SSL = /products/ssl

PATH_INCLUDE_GSI = $(GSI)/include/gcc$(ARCH)dbg
PATH_INCLUDE_LIB = $(GSI)/

CFLAGS += -fPIC -I$(PATH_INCLUDE_GSI) -DGLOBUS_BUG

LIB_FLAGS = -Bdynamic

LIBS = $(LIB_FLAGS)  \
	-lglobus_gssapi_gsi$(FLAVOUR) \
	-lglobus_gsi_callback$(FLAVOUR) \
	-lglobus_gsi_cert_utils$(FLAVOUR) \
	-lglobus_gsi_proxy_core$(FLAVOUR) \
	-lglobus_gsi_credential$(FLAVOUR) \
	-lglobus_openssl$(FLAVOUR) \
	-lglobus_gsi_sysconfig$(FLAVOUR) \
	-lglobus_openssl_error$(FLAVOUR) \
	-lglobus_oldgaa$(FLAVOUR) \
	-lglobus_proxy_ssl$(FLAVOUR) \
	-lglobus_common$(FLAVOUR) \
	-ldl \
	-L$(SSL)/lib \
	$(SYS_LIBS)
#	-lssl -lcrypto $(SYS_LIBS)

PLUGIN = libgsiTunnel.so
		
OBJS = base64.o gssIoTunnel.o util.o tunnelQueue.o


all: $(PLUGIN)


$(PLUGIN): $(OBJS) Makefile
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

dist: clean
	@mkdir -p $(PATH_DIST_DIR)
	@tar --gzip --exclude='*CVS*' --exclude='*.svn*' -cf $(PATH_DIST_DIR)/$(PACKAGE_NAME).src.tgz *

install: $(PLUGIN)
	mkdir -p $(LIB_PATH)/
	$(CP) $(PLUGIN) $(LIB_PATH)/;

bindist: all
	@rm -rf $(PATH_BDIST_DIR)/
	@mkdir -p $(PATH_DIST_DIR)
	make -f Makefile.gsi install PATH_PREFIX_DIR=$(PATH_BDIST_DIR)/${PATH_PREFIX_DIR} ARCH=$(ARCH)
	tar --gzip -cf $(PATH_DIST_DIR)/$(PACKAGE_NAME).bin.tgz -C $(PATH_BDIST_DIR)/ .

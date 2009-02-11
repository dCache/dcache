#
# $Id: Makefile,v 1.42 2005-02-04 13:58:33 tigran Exp $
#

# Top Level Makfile

BIN_PATH = /afs/.desy.de/products/dcache
RPM_STAGING_DIR	= /usr/src/packages/SOURCES

CP    = cp
RM    = rm -f
LINK  = ln -s
MV    = mv

TARGET = libdcap libpdcap dccp dcap_test depend mapfile wdccp cflags

all $(TARGET):

	@if  [ ! -f .depend ]; then \
		touch .depend;  \
		$(MAKE) depend; \
	fi
	
	@case `uname` in \
		SunOS) \
			$(MAKE) -f Makefile.sun $@ ; \
			;; \
		Linux) \
			$(MAKE) -f Makefile.linux $@ ; \
			;; \
		IRIX*) \
			$(MAKE) -f Makefile.sgi $@ ; \
			;; \
		AIX) \
			$(MAKE) -f Makefile.alpha $@ ; \
			;; \
		CYGWIN*) \
			$(MAKE) -f Makefile.w32 $@ ;\
			;; \
		*) \
			echo "Don't know your machine type" \
			echo "Have a look in Porting file" \
			;; \
	esac

release:
	@VER=`cat dcap_version.c | grep version`; \
	echo "Last version: $$VER"; \
	echo "Enter new version name:"; \
	read version; \
	$(RM) -f dcap_version.c; \
	echo "" > dcap_version.c; \
	echo "#define VER(a) #a \" \" __DATE__ \" \" __TIME__ ;" >> dcap_version.c; \
	echo "#define VERLONG(a) #a \" \" __DATE__ \" \" __TIME__ \" CFLAGS=\\\"\" __CFLAGS__ \"\\\"\";" >> dcap_version.c; \
	echo "" >> dcap_version.c; \
	echo "const char *getDcapVersion()" >> dcap_version.c; \
	echo "{" >> dcap_version.c; \
	echo "	return VER( $$version );" >> dcap_version.c; \
	echo "}" >> dcap_version.c; \
	echo "" >> dcap_version.c; \
	echo "const char *getDcapVersionLong()" >> dcap_version.c; \
	echo "{" >> dcap_version.c; \
	echo "	return VERLONG( $$version );" >> dcap_version.c; \
	echo "}" >> dcap_version.c; \
	echo "" >> dcap_version.c; \
	cvs commit; \
	cvs tag -c -b $$version;


install: rebuild dirs
	@VER=`./version.sh`; \
	$(MV) libdcap.so libdcap$$VER.so; \
	$(RM) $(BIN_PATH)/lib/libdcap.so; \
	$(LINK) libdcap$$VER.so $(BIN_PATH)/lib/libdcap.so; \
	$(CP) libdcap$$VER.so $(BIN_PATH)/lib/; \
	$(MV) libpdcap.so libpdcap$$VER.so; \
	$(RM) $(BIN_PATH)/lib/libpdcap.so; \
	$(LINK) libpdcap$$VER.so $(BIN_PATH)/lib/libpdcap.so; \
	$(CP) libpdcap$$VER.so $(BIN_PATH)/lib/; \
	$(CP) dccp $(BIN_PATH)/bin/; \
	$(CP) dcap.h $(BIN_PATH)/include/; \
	$(CP) dcap_errno.h $(BIN_PATH)/include/; \
	$(CP) dc_hack.h $(BIN_PATH)/include/; \
	$(CP) dccp.c $(BIN_PATH)/sources/;

dirs:
	./mkdirs.sh $(BIN_PATH)/bin/
	./mkdirs.sh $(BIN_PATH)/lib
	./mkdirs.sh $(BIN_PATH)/include/
	./mkdirs.sh $(BIN_PATH)/sources/


rpm:
	myver=`./version.sh`; \
	myname=`basename $$PWD`; \
	cd ..; \
	mv "$$myname" "$$myname-$$myver"; \
	tar cfz "$(RPM_STAGING_DIR)/$$myname-$$myver.tar.gz" "$$myname-$$myver"; \
	mv "$$myname-$$myver" "$$myname"; \
	cd "$$myname"; \
	rpmbuild  -ba *.spec

rebuild: cleanall all
	@echo "Compilation Done."

clean:
	@rm -f *.o *.BAK core

cleanall: clean	
	@rm -f $(TARGET) libdcap*.so libdcap*.a libpdcap*.so dcap.h \
	.depend debug_map.h  debug_level.h mapfile

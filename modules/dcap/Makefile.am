topdir=@RPM_DIR@

if PKG_DEP_VDT
GSIAPI = -with vdt
endif
if PKG_DEP_EPEL
GSIAPI = -with epel
endif
if WANT_LIBGSSTUNNEL
GSSAPI = -with krb
endif
if WANT_LIBTELNETTUNNEL
TELNETAPI = -with telnet
endif
if WANT_LIBSSLTUNNEL
SSLAPI = -with ssl
endif

AUX_DIST             =  $(ac_aux_dir)/config.guess \
                        $(ac_aux_dir)/config.sub \
                        $(ac_aux_dir)/install-sh \
                        $(ac_aux_dir)/ltmain.sh \
                        $(ac_aux_dir)/missing

MAINTAINERCLEANFILES = Makefile.in aclocal.m4 configure config-h.in \
                        stamp-h.in $(AUX_DIST)

ACLOCAL              = aclocal -I $(ac_aux_dir)


dist-hook:
	(cd $(distdir); \
	if [ -a $(ac_aux_dir) ]; then \
	   echo "config exist!"; \
	else \
	   mkdir $(ac_aux_dir); \
	fi)
	(if [ -a $(ac_aux_dir)/depcomp ]; then \
	   echo "depcomp exist!"; \
	else \
	   touch $(ac_aux_dir)/depcomp; \
	fi)
	for file in $(AUX_DIST) $(AUX_DIST_EXTRA); do \
	  cp $$file $(distdir)/$$file; \
	done



SUBDIRS = src plugins

rpm: dist @PACKAGE@.spec
	mkdir -p $(topdir)/SOURCES \
	$(topdir)/SPECS \
	$(topdir)/BUILD \
	$(topdir)/RPMS $(topdir)/SRPMS
	cp @PACKAGE@-@VERSION@.tar.gz $(topdir)/SOURCES
	cp @PACKAGE@.spec $(topdir)/SPECS
	rpmbuild --define "_topdir $(topdir)" \
	-ba $(GSSAPI) $(GSIAPI) $(SSLAPI) $(TELNETAPI) \
	@PACKAGE@.spec


docfiles = \
	AUTHORS \
	COPYING.LIB \
	LICENSE



docdir = $(DOC_DIR)
doc_DATA = $(docfiles)
EXTRA_DIST = $(docfiles)


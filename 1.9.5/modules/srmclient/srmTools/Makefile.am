# 
# Makefile.am for SRM C++ client
#
# $Id: Makefile.am,v 1.7 2003-10-07 21:09:16 cvs Exp $
#

include Makefile.conf

SERVERS  =

CLIENTS  = 

# Product:
CLIENTS += srmcp
#CLIENTS += srmget # - not supported, replaced by srmcp
#CLIENTS += srmput # - not supported, replaced by srmcp



# Tests:
#CLIENTS += srm-gsi-client
CLIENTS += SrmWsClientConfig-t
CLIENTS += SrmSoapWsClient-t

DEFINE = -DGLOBUS_LOCATION=\"$(GLOBUS_LOCATION)\" \
	-DGLOBUS_INCLUDE=\"$(GLOBUS_INCLUDE)\" \
	-DGLOBUS_LIB=\"$(GLOBUS_LIB)\" \
	-DAUTHORIZATION_FILE=\"$(AUTHORIZATION_FILE)\"
#DEFINE += -D_I_USE_GLOBUS_VERSION_2_2_4_

DEFINE_THREADS = -DGLOBUS_LOCATION=\"$(GLOBUS_LOCATION)\" \
	-DGLOBUS_INCLUDE_THREADS=\"$(GLOBUS_INCLUDE_THREADS)\" \
	-DGLOBUS_LIB=\"$(GLOBUS_LIB)\" \
	-DAUTHORIZATION_FILE=\"$(AUTHORIZATION_FILE)\"

OPT = -O3 -fomit-frame-pointer -funroll-loops

AM_CFLAGS   = $(OPT) $(DEFINE)
AM_CXXFLAGS = $(OPT) $(DEFINE) -I$(GLOBUS_INCLUDE)

AM_CFLAGS_THREADS = $(OPT) $(DEFINE_THREADS)
AM_CPPFLAGS = -I.
AM_LDFLAGS  = -L. -L$(GLOBUS_LIB)

MYCFLAGS = $(AM_CFLAGS) -I$(GLOBUS_INCLUDE)
MYCFLAGS_THREADS = $(AM_CFLAGS_THREADS) -I$(GLOBUS_INCLUDE_THREADS)

GLOBUS_LIBS = -lglobus_gram_client_$(GLOBUS_FLAVOR) -lglobus_gram_protocol_$(GLOBUS_FLAVOR) \
 -lglobus_io_$(GLOBUS_FLAVOR) -lglobus_gss_assist_$(GLOBUS_FLAVOR) \
 -lglobus_gssapi_gsi_$(GLOBUS_FLAVOR) -lglobus_gsi_proxy_core_$(GLOBUS_FLAVOR) \
 -lglobus_gsi_credential_$(GLOBUS_FLAVOR) -lglobus_gsi_callback_$(GLOBUS_FLAVOR) \
 -lglobus_oldgaa_$(GLOBUS_FLAVOR) -lglobus_gsi_sysconfig_$(GLOBUS_FLAVOR) \
 -lglobus_gsi_cert_utils_$(GLOBUS_FLAVOR) -lglobus_openssl_error_$(GLOBUS_FLAVOR) \
 -lglobus_openssl_$(GLOBUS_FLAVOR) -lglobus_proxy_ssl_$(GLOBUS_FLAVOR) \
 -lssl_$(GLOBUS_FLAVOR) -lcrypto_$(GLOBUS_FLAVOR) -lglobus_common_$(GLOBUS_FLAVOR)

GLOBUS_LIBS_THREADS = -lglobus_io_$(GLOBUS_FLAVOR_THREADS) -lglobus_gss_assist_$(GLOBUS_FLAVOR_THREADS) \
 -lglobus_gssapi_gsi_$(GLOBUS_FLAVOR_THREADS) -lglobus_gsi_proxy_core_$(GLOBUS_FLAVOR_THREADS) \
 -lglobus_gsi_credential_$(GLOBUS_FLAVOR_THREADS) -lglobus_gsi_callback_$(GLOBUS_FLAVOR_THREADS) \
 -lglobus_oldgaa_$(GLOBUS_FLAVOR_THREADS) -lglobus_gsi_sysconfig_$(GLOBUS_FLAVOR_THREADS) \
 -lglobus_gsi_cert_utils_$(GLOBUS_FLAVOR_THREADS) -lglobus_openssl_error_$(GLOBUS_FLAVOR_THREADS) \
 -lglobus_openssl_$(GLOBUS_FLAVOR_THREADS) -lglobus_proxy_ssl_$(GLOBUS_FLAVOR_THREADS) \
 -lssl_$(GLOBUS_FLAVOR_THREADS) -lcrypto_$(GLOBUS_FLAVOR_THREADS) -lglobus_common_$(GLOBUS_FLAVOR_THREADS)

#noinst_LIBRARIES = libgsigsoap.a libgsigsoapthreads.a
#libgsigsoap_a_SOURCES = stdsoap2.c gsi.c
#libgsigsoap_a_CFLAGS = $(MYCFLAGS)
#libgsigsoapthreads_a_SOURCES = stdsoap2.c gsi.c
#libgsigsoapthreads_a_CFLAGS = $(MYCFLAGS_THREADS) 

noinst_LIBRARIES = libgsigsoapcpp.a libgsigsoapcppthreads.a
libgsigsoapcpp_a_SOURCES = stdsoap2.cpp gsi.cpp
libgsigsoapcpp_a_CFLAGS = $(MYCFLAGS)
libgsigsoapcppthreads_a_SOURCES = stdsoap2.cpp gsi.cpp
libgsigsoapcppthreads_a_CFLAGS = $(MYCFLAGS_THREADS) 

XERCES_LIBS = -L$(XERCES_DIR)/lib -lxerces-c
XERCES_INCLUDE = -I$(XERCES_DIR)/include

MYLDFLAGS          = $(GLOBUS_LIBS) -lgsigsoapcpp
MYLDFLAGS_THREADS  = $(GLOBUS_LIBS_THREADS) -lgsigsoapcppthreads

sysconf_DATA = authorized_dn

serversdir = $(prefix)/servers

servers_PROGRAMS = $(SERVERS)
bin_PROGRAMS =  $(CLIENTS)


init:
	./soapcpp2 -i -psrmWS  managerv1.h
#	./soapcpp2 -i -c -psrmWS  managerv1.h   # for 'C' lang

#===============================================================================
# Products:

srm_SOURCES     = SrmSoapWsClient.cpp
srm_SOURCES    += srmWSC.cpp srmWSClient.cpp
srm_SOURCES    += SrmWsClientConfig.cpp
srm_SOURCES    += url.cpp
srm_CFLAGS      = $(MYCFLAGS)
srm_CXXFLAGS    = $(MYCFLAGS) $(XERCES_INCLUDE)
srm_LDADD       = $(MYLDFLAGS)
srm_LDADD      += -lm $(XERCES_LIBS)

#-- srmget - obsolete
#srmget_SOURCES     = srmget.cpp
#srmget_SOURCES    += $(srm_SOURCES)
#srmget_CFLAGS      = $(srm_CFLAGS)
#srmget_CXXFLAGS    = $(srm_CXXFLAGS)
#srmget_LDADD       = $(srm_LDADD)

#-- srmput - obsolete
#srmput_SOURCES     = srmput.cpp 
#srmput_SOURCES    += $(srm_SOURCES)
#srmput_CFLAGS      = $(srm_CFLAGS)
#srmput_CXXFLAGS    = $(srm_CXXFLAGS)
#srmput_LDADD       = $(srm_LDADD)

#-- srmcp
srmcp_SOURCES      = srmcp.cpp 
srmcp_SOURCES     += $(srm_SOURCES)
srmcp_CFLAGS       = $(srm_CFLAGS)
srmcp_CXXFLAGS     = $(srm_CXXFLAGS)
srmcp_LDADD        = $(srm_LDADD)

#===============================================================================
# Tests:

# obsolete , see get/put

#-- client to test srmImpl.[hc]pp class (obsolete)
#srm_gsi_client_SOURCES  = srm-gsi-client.c 
#srm_gsi_client_SOURCES += srmImpl.cpp 
#srm_gsi_client_SOURCES += srmWSC.cpp srmWSClient.cpp
#srm_gsi_client_CFLAGS  = $(MYCFLAGS)
#srm_gsi_client_LDADD   = $(MYLDFLAGS)

#-- client to test SrmSoapWsClient.[hc]pp class
SrmSoapWsClient_t_SOURCES  = SrmSoapWsClient-t.cpp
SrmSoapWsClient_t_SOURCES += SrmSoapWsClient.cpp
SrmSoapWsClient_t_SOURCES += srmWSC.cpp srmWSClient.cpp
SrmSoapWsClient_t_CFLAGS  = $(MYCFLAGS)
SrmSoapWsClient_t_LDADD   = $(MYLDFLAGS)

#-- Test program to test SrmWsClientConfig
SrmWsClientConfig_t_SOURCES     = SrmWsClientConfig-t.cpp
SrmWsClientConfig_t_SOURCES    += SrmWsClientConfig.cpp
SrmWsClientConfig_t_CXXFLAGS    = $(XERCES_INCLUDE)
SrmWsClientConfig_t_LDADD       = -lm $(XERCES_LIBS)

clean:
	rm -f $(SERVERS) $(CLIENTS) *C.c *Client.c *H.h *Proxy.h *Server.c *Stub.h \
	                            *.xml *.wsdl *.nsmap *.xsd *~

srmconfig.xml: srmconfig.xml.in
	cp srmconfig.xml.in srmconfig.xml

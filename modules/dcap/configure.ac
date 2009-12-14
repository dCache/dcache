dnl $Id: configure.in,v 1.12 2003/10/07 11:51:53 omsynge Exp $
dnl -------------------------------------------------------------------------



AC_INIT(dcap.c)
GCCVER=$EDG_RPM_RELEASE_VERSION
AC_SUBST(GCCVER)

AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER(debug_level.h)

BRUTTAG='$Name:  $'

dnl -------------------------------------------------------------------------
dnl Hack to get the current version
dnl -------------------------------------------------------------------------
CVSVERSION=`echo $BRUTTAG | cut -d':' -f2 | tr '_' '.' | tr -d 'v$ '`
if test "A" = A$CVSVERSION; then
    CVSVERSION=`date '+20%y%m%d%H'`
    CVSVERSION="cvs"$CVSVERSION
fi


GENERIC_LIBRARY_VERSION=`echo $BRUTTAG | cut -d':' -f2 | tr '_' ':' `
AC_SUBST(GENERIC_LIBRARY_VERSION)

AM_INIT_AUTOMAKE(dcache-dcap,$CVSVERSION)

AC_PROG_CC

AC_PROG_INSTALL
AM_PROG_LIBTOOL
AC_PROG_LN_S

AM_PROG_CC_C_O


AC_CHECK_FUNCS([alarm])
AC_CHECK_FUNCS([atexit])
AC_CHECK_FUNCS([bzero])
AC_CHECK_FUNCS([dup2])
AC_CHECK_FUNCS([ftruncate])
AC_CHECK_FUNCS([gethostbyaddr])
AC_CHECK_FUNCS([gethostbyname])
AC_CHECK_FUNCS([gethostname])
AC_CHECK_FUNCS([memmove])
AC_CHECK_FUNCS([memset])
AC_CHECK_FUNCS([mkdir])
AC_CHECK_FUNCS([realpath])
AC_CHECK_FUNCS([rmdir])
AC_CHECK_FUNCS([select])
AC_CHECK_FUNCS([socket])
AC_CHECK_FUNCS([strcasecmp])
AC_CHECK_FUNCS([strchr])
AC_CHECK_FUNCS([strdup])
AC_CHECK_FUNCS([strerror])
AC_CHECK_FUNCS([strndup])
AC_CHECK_FUNCS([strrchr])
AC_CHECK_FUNCS([strstr])
AC_CHECK_HEADERS([arpa/inet.h])
AC_CHECK_HEADERS([fcntl.h])
AC_CHECK_HEADERS([limits.h])
AC_CHECK_HEADERS([netdb.h])
AC_CHECK_HEADERS([netinet/in.h])
AC_CHECK_HEADERS([sys/param.h])
AC_CHECK_HEADERS([sys/socket.h])
AC_CHECK_HEADERS([sys/time.h])
AC_CHECK_HEADERS([sys/vfs.h])
AC_CHECK_HEADERS([termios.h])
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_CHECK_MEMBERS([struct stat.st_rdev])
AC_C_CONST
AC_FUNC_CHOWN
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_FORK
AC_FUNC_FSEEKO
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_REALLOC
AC_FUNC_SELECT_ARGTYPES
AC_FUNC_STAT
AC_FUNC_VPRINTF
AC_HEADER_DIRENT
AC_HEADER_TIME
AC_PROG_RANLIB
AC_STRUCT_ST_BLOCKS
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_SIGNAL
AC_TYPE_SIZE_T
AC_TYPE_UID_T


dnl === Globus stuff ===
noglobus=0
EPEL_LIBS_NOT=0

dnl Store the compile flags prior to testing the presance.

_cppflags="${CPPFLAGS}"
_ldflags="${LDFLAGS}"
_ldlibpath="${LD_LIBRARY_PATH}"

AC_ARG_WITH(globus-lib,
[ --with-globus-lib[[=DIR]] use libglobus libdir],[
if test "X${withval}" != "X" ; then
GLOBUS_LIB_DIR=$withval
LDFLAGS="${LDFLAGS} -L$withval"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${withval}"
fi
])
AC_ARG_WITH(globus-include,
[ --with-globus-include[[=DIR]] use libglobus includedir],[
if test "X${withval}" != "X" ; then
GLOBUS_INCLUDE_DIR=$withval
CPPFLAGS="${CPPFLAGS} -I$withval"
fi
])

AC_CHECK_HEADERS( [globus_config.h],[], [noglobus=1])
AC_CHECK_HEADERS([gssapi.h],[],[noglobus=1])

if test "${noglobus}" != "0" ; then
AC_MSG_RESULT([Disabling Globus plugins as header files not found.])
else

dnl Set up the flags for found libraries

GCC64PTHR_LIBS_NOT=0
EPEL_LIBS_NOT=0
GCC32PTHR_LIBS_NOT=0

AC_CHECK_LIB(globus_common, globus_version_print, [
GLOBUS_EPEL_LIBS="-lglobus_common ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_proxy_ssl, X509V3_conf_free, [
GLOBUS_EPEL_LIBS="-lglobus_proxy_ssl ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_oldgaa, internal_error_handler, [
GLOBUS_EPEL_LIBS="-lglobus_oldgaa ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl_error, globus_module_getenv, [
GLOBUS_EPEL_LIBS="-lglobus_openssl_error ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_sysconfig, globus_fifo_enqueue, [
GLOBUS_EPEL_LIBS="-lglobus_gsi_sysconfig ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl, globus_module_activate, [
GLOBUS_EPEL_LIBS="-lglobus_openssl ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_credential,globus_module_getenv, [
GLOBUS_EPEL_LIBS="-lglobus_gsi_credential ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_proxy_core,X509_sign, [
GLOBUS_EPEL_LIBS="-lglobus_gsi_proxy_core ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_cert_utils,X509_get_subject_name, [
GLOBUS_EPEL_LIBS="-lglobus_gsi_cert_utils ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_callback,X509_get_subject_name, [
GLOBUS_EPEL_LIBS="-lglobus_gsi_callback ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gssapi_gsi,gss_wrap, [
GLOBUS_EPEL_LIBS="-lglobus_gssapi_gsi ${GLOBUS_EPEL_LIBS}"
],[
EPEL_LIBS_NOT=1
])

dnl == 64pthreads libary checking version ==

AC_CHECK_LIB(globus_common_gcc64pthr, globus_version_print, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_common_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_proxy_ssl_gcc64pthr, X509V3_conf_free, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_proxy_ssl_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_oldgaa_gcc64pthr, internal_error_handler, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_oldgaa_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl_error_gcc64pthr, globus_module_getenv, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_openssl_error_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_sysconfig_gcc64pthr, globus_fifo_enqueue, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_gsi_sysconfig_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl_gcc64pthr, globus_module_activate, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_openssl_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_credential_gcc64pthr,globus_module_getenv, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_gsi_credential_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_proxy_core_gcc64pthr,X509_sign, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_gsi_proxy_core_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_cert_utils_gcc64pthr,X509_get_subject_name, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_gsi_cert_utils_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_callback_gcc64pthr,X509_get_subject_name, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_gsi_callback_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gssapi_gsi_gcc64pthr,gss_wrap, [
GLOBUS_GCC64PTHR_LIBS="-lglobus_gssapi_gsi_gcc64pthr ${GLOBUS_GCC64PTHR_LIBS}"
],[
GCC64PTHR_LIBS_NOT=1
])

dnl == 32pthreads libary checking version ==

AC_CHECK_LIB(globus_common_gcc32pthr, globus_version_print, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_common_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_proxy_ssl_gcc32pthr, X509V3_conf_free, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_proxy_ssl_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_oldgaa_gcc32pthr, internal_error_handler, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_oldgaa_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl_error_gcc32pthr, globus_module_getenv, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_openssl_error_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_sysconfig_gcc32pthr, globus_fifo_enqueue, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_gsi_sysconfig_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl_gcc32pthr, globus_module_activate, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_openssl_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_credential_gcc32pthr,globus_module_getenv, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_gsi_credential_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_proxy_core_gcc32pthr,X509_sign, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_gsi_proxy_core_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_cert_utils_gcc32pthr,X509_get_subject_name, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_gsi_cert_utils_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_callback_gcc32pthr,X509_get_subject_name, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_gsi_callback_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gssapi_gsi_gcc32pthr,gss_wrap, [
GLOBUS_GCC32PTHR_LIBS="-lglobus_gssapi_gsi_gcc32pthr ${GLOBUS_GCC32PTHR_LIBS}"
],[
GCC32PTHR_LIBS_NOT=1
])

dnl set the verion of globus fully detected and the library linking.

if test "0" = "${EPEL_LIBS_NOT}" ; then
GLOBUS_LIBS=${GLOBUS_EPEL_LIBS}
PKGDEPEPEL=1
fi

if test "0" = "${GCC64PTHR_LIBS_NOT}" ; then
GLOBUS_LIBS=${GLOBUS_GCC64PTHR_LIBS}
PKGDEPVDT=1
fi

if test "0" = "${GCC32PTHR_LIBS_NOT}" ; then
GLOBUS_LIBS=${GLOBUS_GCC32PTHR_LIBS}
PKGDEPVDT=1
fi

dnl == End checking as globus header files exist ==
fi

dnl Substitute the globus library linking variable

AC_SUBST(GLOBUS_LIBS)

dnl restore the compile flags to what they where before checking for globus.

CPPFLAGS=${_cppflags}
LDFLAGS=${_ldflags}
export LD_LIBRARY_PATH=${_ldlibpath}



dnl == MIT Kerberous ==

nokrb=0
AC_CHECK_HEADERS([gssapi_krb5.h],[],[nokrb=1])
AC_CHECK_HEADERS([gssapi_generic.h],[],[nokrb=1])


if test "${nokrb}" != "0" ; then
AC_MSG_RESULT([Disabling KRB plugins as header files not found.])
else

AC_CHECK_LIB(gssapi_krb5,gss_import_name, [
KRB_LIBS="-lgssapi_krb5 ${KRB_LIBS}"
],[
nokrb=1
])

AC_CHECK_LIB(crypto,fread, [
KRB_LIBS="-lcrypto ${KRB_LIBS}"
],[
nokrb=1
])

AC_CHECK_LIB(c,strcmp, [
KRB_LIBS="-lc ${KRB_LIBS}"
],[
nokrb=1
])

AC_CHECK_LIB(resolv,fopen, [
KRB_LIBS="-lresolv ${KRB_LIBS}"
],[
nokrb=1
])

fi
AC_SUBST(KRB_LIBS)



CPPFLAGS=${_cppflags}
LDFLAGS=${_ldflags}
export LD_LIBRARY_PATH=${_ldlibpath}

dnl =============
dnl Check RPM dir
dnl =============
RPMD=`pwd`

AC_MSG_CHECKING([for directory where to create rpms])
AC_ARG_WITH(rpm-dir,[  --with-rpm-dir=<dir> Default is \$RPMD/],
            rpm_dir="$withval", rpm_dir="$RPMD")
if test -d "$rpm_dir" ; then
    AC_MSG_RESULT([found $rpm_dir])
    RPM_DIR=$rpm_dir
else
    AC_MSG_RESULT([no such directory $rpm_dir])
    AC_MSG_RESULT([the directory $rpm_dir will be created for you if possible])
    RPM_DIR=$rpm_dir
fi



AC_SUBST(RPM_DIR)

AC_SUBST(ac_aux_dir)

DCAPPREFIX=/opt/dcache/dcap
AC_MSG_CHECKING([for dcap prefix directory])
AC_ARG_WITH(dcap-prefix,[  --with-dcap-prefix=<dir> Default is $DCAPPREFIX/],
            dcap_prefix="$withval", dcap_prefix="$DCAPPREFIX")

if test -d "$dcap_prefix" ; then
    AC_MSG_RESULT([found $dcap_prefix])
    DCAPPREFIX_DIR=$dcap_prefix
else
    AC_MSG_RESULT([no such directory $dcap_prefix])
    AC_MSG_RESULT([the directory $dcap_prefix will be created for you if possible])
    DCAPPREFIX_DIR=$dcap_prefix
fi

AC_SUBST(DCAPPREFIX_DIR)



DOCD=${DCAPPREFIX_DIR}/share/doc/${PACKAGE}-${VERSION}
AC_MSG_CHECKING([for directory where to install docs])
AC_ARG_WITH(doc-dir,[  --with-doc-dir=<dir> Default is $DOCD/],
            doc_dir="$withval", doc_dir="$DOCD")
if test -d "$doc_dir" ; then
    AC_MSG_RESULT([found $doc_dir])
    DOC_DIR=$doc_dir
else
    AC_MSG_RESULT([no such directory $doc_dir])
    AC_MSG_RESULT([the directory $doc_dir will be created for you if possible])
    DOC_DIR=$doc_dir
fi

AC_SUBST(DOC_DIR)


dnl Some debugging dcap #defines

AC_DEFINE_UNQUOTED(DC_NO_OUTPUT,0,[dcap debug level for no output])
AC_DEFINE_UNQUOTED(DC_ERROR,1,[dcap debug level for error])
AC_DEFINE_UNQUOTED(DC_INFO,2,[dcap debug level for info])
AC_DEFINE_UNQUOTED(DC_TIME,4,[dcap debug level to include time stamps])
AC_DEFINE_UNQUOTED(DC_TRACE,8,[dcap debug level to include stacks])
AC_DEFINE_UNQUOTED(DC_CALLS,16,[dcap debug level to include calls])
AC_DEFINE_UNQUOTED(DC_IO,32,[dcap debug level to include IO logging])
AC_DEFINE_UNQUOTED(DC_THREADS,64,[dcap debug level to include thread tracing])


dnl We dont test for the dependacies for the telnet plugin
dnl as their apear to be no major ones.

notelnet="0"

dnl Do the conditional compilation flags in rest of autotools.

AM_CONDITIONAL([WANT_LIBGSITUNNEL], [test "${noglobus}" = "0"])
AM_CONDITIONAL([WANT_LIBGSSTUNNEL], [test "${nokrb}" = "0"])
AM_CONDITIONAL([WANT_LIBTELNETTUNNEL], [test "${notelnet}" = "0"])
AM_CONDITIONAL([PKG_DEP_VDT], [test "${PKGDEPVDT}" = "1"])
AM_CONDITIONAL([PKG_DEP_EPEL], [test "${PKGDEPEPEL}" = "1"])



AC_SUBST(GSSAPI_TARGETS)

dnl Passing configre variables to spec file for rpm to build.

AC_SUBST(GLOBUS_INCLUDE_DIR)
AC_SUBST(GLOBUS_LIB_DIR)


AC_OUTPUT(Makefile \
  dcap.h \
  dcache-dcap.spec \
  security_plugins/Makefile \
  security_plugins/gssapi/Makefile \
  security_plugins/telnet/Makefile )

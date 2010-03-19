dnl $Id: configure.in,v 1.12 2003/10/07 11:51:53 omsynge Exp $
dnl -------------------------------------------------------------------------



AC_INIT(src/dcap.c)

AC_CONFIG_AUX_DIR(config)
AC_CANONICAL_SYSTEM
AM_CONFIG_HEADER(src/debug_level.h)


dnl uncomment when tagging
RELEASE=TRUE

VER_ARCHITECTURE=2
VER_MAJOR=45
VER_MINOR=0
VER_PATCH=0
if test "A" = A$RELEASE; then
    VER_PATCH=${VER_PATCH}_`date '+20%y%m%d%H'`
fi

AM_INIT_AUTOMAKE(dcache-dcap,$VER_ARCHITECTURE.$VER_MAJOR.$VER_MINOR,"support@dcache.org","dcache-dcap","http://www.dcache.org/")

AC_SUBST(VER_ARCHITECTURE)
AC_SUBST(VER_MAJOR)
AC_SUBST(VER_MINOR)
AC_SUBST(VER_PATCH)

LIBRARY_VERSION="$VER_ARCHITECTURE:$VER_MAJOR:1"
AC_SUBST(LIBRARY_VERSION)

AC_PROG_CC

#
# Wall
#
AC_CACHE_CHECK([for maximum warnings compiler flag],
  ac_cv_cwflag,
[case "${CC-cc}" in
  *gcc*) ac_cv_cwflag="-Wall -pedantic -pipe";;
  *)
  ;;
esac])
AC_SUBST([CWFLAG], [$ac_cv_cwflag])



AC_PROG_INSTALL
AM_PROG_LIBTOOL
AC_PROG_LN_S

AM_PROG_CC_C_O
AC_SEARCH_LIBS([gethostbyname], [nsl])
AC_SEARCH_LIBS([bind], [socket])
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
AC_CHECK_FUNCS([htonll])
AC_CHECK_FUNCS([ntohll])
AC_CHECK_FUNCS([acl])
AC_CHECK_FUNCS([facl])
AC_CHECK_FUNCS([_xstat])
AC_CHECK_FUNCS([_lxstat])
AC_CHECK_FUNCS([_fxstat])
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
AC_CHECK_HEADERS([stropts.h])
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_CHECK_MEMBERS([struct stat.st_rdev])
AC_CHECK_MEMBERS([FILE._fileno])
AC_CHECK_MEMBERS([FILE._magic])
AC_CHECK_MEMBERS([FILE._file])
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
AC_C_BIGENDIAN



dnl === openssl stuff ===

nossl=0
AC_CHECK_HEADERS([openssl/ssl.h], [], [nossl=1])
AC_CHECK_HEADERS([openssl/err.h], [], [nossl=1])
if test "${nossl}" != "0" ; then
AC_MSG_RESULT([Disabling ssl plugin as header files not found.])
else
AC_CHECK_LIB(ssl,SSL_library_init,[],[nossl=1])
fi

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


EPEL_LIBS_NOT=0
GCC64PTHR_LIBS_NOT=0
GCC64DBGPTHR_LIBS_NOT=0
GCC32PTHR_LIBS_NOT=0
GCC32DBGPTHR_LIBS_NOT=0

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


dnl == 64dbgpthr libary checking version ==

AC_CHECK_LIB(globus_common_gcc64dbgpthr, globus_version_print, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_common_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_proxy_ssl_gcc64dbgpthr, X509V3_conf_free, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_proxy_ssl_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_oldgaa_gcc64dbgpthr, internal_error_handler, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_oldgaa_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl_error_gcc64dbgpthr, globus_module_getenv, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_openssl_error_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_sysconfig_gcc64dbgpthr, globus_fifo_enqueue, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_gsi_sysconfig_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl_gcc64dbgpthr, globus_module_activate, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_openssl_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_credential_gcc64dbgpthr,globus_module_getenv, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_gsi_credential_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_proxy_core_gcc64dbgpthr,X509_sign, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_gsi_proxy_core_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_cert_utils_gcc64dbgpthr,X509_get_subject_name, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_gsi_cert_utils_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_callback_gcc64dbgpthr,X509_get_subject_name, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_gsi_callback_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gssapi_gsi_gcc64dbgpthr,gss_wrap, [
GLOBUS_GCC64DBGPTHR_LIBS="-lglobus_gssapi_gsi_gcc64dbgpthr ${GLOBUS_GCC64DBGPTHR_LIBS}"
],[
GCC64DBGPTHR_LIBS_NOT=1
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

dnl == 32dbgpthr libary checking version ==

AC_CHECK_LIB(globus_common_gcc32dbgpthr, globus_version_print, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_common_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_proxy_ssl_gcc32dbgpthr, X509V3_conf_free, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_proxy_ssl_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_oldgaa_gcc32dbgpthr, internal_error_handler, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_oldgaa_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl_error_gcc32dbgpthr, globus_module_getenv, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_openssl_error_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_sysconfig_gcc32dbgpthr, globus_fifo_enqueue, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_gsi_sysconfig_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_openssl_gcc32dbgpthr, globus_module_activate, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_openssl_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_credential_gcc32dbgpthr,globus_module_getenv, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_gsi_credential_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_proxy_core_gcc32dbgpthr,X509_sign, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_gsi_proxy_core_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_cert_utils_gcc32dbgpthr,X509_get_subject_name, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_gsi_cert_utils_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gsi_callback_gcc32dbgpthr,X509_get_subject_name, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_gsi_callback_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])

AC_CHECK_LIB(globus_gssapi_gsi_gcc32dbgpthr,gss_wrap, [
GLOBUS_GCC32DBGPTHR_LIBS="-lglobus_gssapi_gsi_gcc32dbgpthr ${GLOBUS_GCC32DBGPTHR_LIBS}"
],[
GCC32DBGPTHR_LIBS_NOT=1
])



dnl set the verion of globus fully detected and the library linking.

if test "0" = "${GCC32DBGPTHR_LIBS_NOT}" ; then
GLOBUS_LIBS=${GLOBUS_GCC32DBGPTHR_LIBS}
PKGDEPVDT=1
fi

if test "0" = "${GCC32PTHR_LIBS_NOT}" ; then
GLOBUS_LIBS=${GCC32PTHR_LIBS}
PKGDEPVDT=1
fi

if test "0" = "${GCC64DBGPTHR_LIBS_NOT}" ; then
GLOBUS_LIBS=${GLOBUS_GCC64DBGPTHR_LIBS}
PKGDEPVDT=1
fi

if test "0" = "${GCC64PTHR_LIBS_NOT}" ; then
GLOBUS_LIBS=${GLOBUS_GCC64PTHR_LIBS}
PKGDEPVDT=1
fi

if test "0" = "${EPEL_LIBS_NOT}" ; then
GLOBUS_LIBS=${GLOBUS_EPEL_LIBS}
PKGDEPEPEL=1
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

AC_ARG_WITH(krb5-gssapi-include,
[ --with-krb5-gssapi-include[[=DIR]] use krb5 gssapi includedir],[
if test "X${withval}" != "X" ; then
KRB5_GSSAPI_INCLUDE_DIR=$withval
fi
],[
KRB5_GSSAPI_INCLUDE_DIR=/usr/include/gssapi
])
AC_SUBST(KRB5_GSSAPI_INCLUDE_DIR)

nokrb=0
CPPFLAGS="${CPPFLAGS} -I${KRB5_GSSAPI_INCLUDE_DIR}"
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

dnl ====================
dnl Check and set Prefix
dnl ====================

if test "${prefix}" = NONE ; then
    dcap_prefix=/usr/local
else
    dcap_prefix=${prefix}
fi

dnl =============
dnl Check RPM dir
dnl =============

if test X = "X${RPMD}" ; then
    RPMD=`pwd`
fi
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

dnl =============
dnl Check Doc dir
dnl =============
dnl Note: as of autoconf 2.6 this is included by default

if test X = "X${DOCD}" ; then
    DOCD=${dcap_prefix}/share/doc/${PACKAGE}-${VERSION}
fi
AC_MSG_CHECKING([for directory where to install docs])
AC_ARG_WITH(doc-dir,[  --with-doc-dir=<dir> Default is \$DOCD/],
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

AC_SUBST(ac_aux_dir)

dnl Some debugging dcap #defines

AC_DEFINE_UNQUOTED(DC_NO_OUTPUT,0,[dcap debug level for no output])
AC_DEFINE_UNQUOTED(DC_ERROR,1,[dcap debug level for error])
AC_DEFINE_UNQUOTED(DC_INFO,2,[dcap debug level for info])
AC_DEFINE_UNQUOTED(DC_TIME,4,[dcap debug level to include time stamps])
AC_DEFINE_UNQUOTED(DC_TRACE,8,[dcap debug level to include stacks])
AC_DEFINE_UNQUOTED(DC_CALLS,16,[dcap debug level to include calls])
AC_DEFINE_UNQUOTED(DC_IO,32,[dcap debug level to include IO logging])
AC_DEFINE_UNQUOTED(DC_THREADS,64,[dcap debug level to include thread tracing])


dnl Set default preload options

OPEN_SYM="open"
CLOSE_SYM="close"
READ_SYM="read"
WRITE_SYM="write"
LSEEK_SYM="lseek"
LSEEK64_SYM="lseek64"
PREAD_SYM="pread"
PREAD64_SYM="pread64"
PWRITE_SYM="pwrite"
PWRITE64_SYM="pwrite64"
STAT_SYM="stat"
STAT64_SYM="stat64"
FSTAT64_SYM="fstat64"
LSTAT_SYM="lstat"
LSTAT64_SYM="lstat64"
FSTAT_SYM="fstat"
FSYNC_SYM="fsync"
DUP_SYM="dup"
OPENDIR_SYM="opendir"
CLOSEDIR_SYM="closedir"
READDIR_SYM="readdir"
READDIR64_SYM="readdir64"
TELLDIR_SYM="telldir"
SEEKDIR_SYM="seekdir"
UNLINK_SYM="unlink"
RMDIR_SYM="rmdir"
MKDIR_SYM="mkdir"
CHMOD_SYM="chmod"
ACCESS_SYM="access"
RENAME_SYM="rename"
CHOWN_SYM="chown"
WRITEV_SYM="writev"
READV_SYM="readv"
ACL_SYM="acl"
FACL_SYM="facl"
dnl Override defaults By OS
case "${target_os}" in
linux-gnu)
AC_MSG_RESULT([Setting the Linux preload methods])
STAT_SYM="__xstat"
STAT64_SYM="__xstat64"
FSTAT64_SYM="__fxstat64"
LSTAT_SYM="__lxstat"
LSTAT64_SYM="__lxstat64"
FSTAT_SYM="__fxstat"
;;
darwin*)
AC_MSG_RESULT([Setting Darwin preload methods])
;;
sgi*)
AC_MSG_RESULT([Setting SGI preload methods])
OPENDIR_SYM="_opendir"
CLOSEDIR_SYM="_closedir"
READDIR_SYM="_readdir"
READDIR64_SYM="_readdir64"
TELLDIR_SYM="_telldir"
SEEKDIR_SYM="_seekdir"
;;
solaris*)
AC_MSG_RESULT([Setting SUN preload methods])
OPENDIR_SYM="_opendir"
CLOSEDIR_SYM="_closedir"
READDIR_SYM="_readdir"
READDIR64_SYM="_readdir64"
TELLDIR_SYM="_telldir"
SEEKDIR_SYM="_seekdir"
;;
*)
AC_MSG_RESULT([Operating system not coded for explicitly so disabling preload])
CPPFLAGS=${CPPFLAGS} -DDCAP_NO_PREAD
;;
esac

AC_DEFINE_UNQUOTED(OPEN_SYM,"$OPEN_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(CLOSE_SYM,"$CLOSE_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(READ_SYM,"$READ_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(WRITE_SYM,"$WRITE_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(LSEEK_SYM,"$LSEEK_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(LSEEK64_SYM,"$LSEEK64_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(PREAD_SYM,"$PREAD_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(PREAD64_SYM,"$PREAD64_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(PWRITE_SYM,"$PWRITE_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(PWRITE64_SYM,"$PWRITE64_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(STAT_SYM,"$STAT_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(STAT64_SYM,"$STAT64_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(FSTAT64_SYM,"$FSTAT64_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(LSTAT_SYM,"$LSTAT_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(LSTAT64_SYM,"$LSTAT64_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(FSTAT_SYM,"$FSTAT_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(FSYNC_SYM,"$FSYNC_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(DUP_SYM,"$DUP_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(OPENDIR_SYM,"$OPENDIR_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(CLOSEDIR_SYM,"$CLOSEDIR_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(READDIR_SYM,"$READDIR_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(READDIR64_SYM,"$READDIR64_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(TELLDIR_SYM,"$TELLDIR_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(SEEKDIR_SYM,"$SEEKDIR_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(UNLINK_SYM,"$UNLINK_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(RMDIR_SYM,"$RMDIR_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(MKDIR_SYM,"$MKDIR_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(CHMOD_SYM,"$CHMOD_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(ACCESS_SYM,"$ACCESS_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(RENAME_SYM,"$RENAME_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(CHOWN_SYM,"$CHOWN_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(WRITEV_SYM,"$WRITEV_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(READV_SYM,"$READV_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(ACL_SYM,"$ACL_SYM",[native function for native or local file system access.])
AC_DEFINE_UNQUOTED(FACL_SYM,"$FACL_SYM",[native function for native or local file system access.])

dnl We dont test for the dependacies for the telnet plugin
dnl as their apear to be no major ones.

notelnet="0"

dnl Do the conditional compilation flags in rest of autotools.

AM_CONDITIONAL([WANT_LIBGSITUNNEL], [test "${noglobus}" = "0"])
AM_CONDITIONAL([WANT_LIBGSSTUNNEL], [test "${nokrb}" = "0"])
AM_CONDITIONAL([WANT_LIBTELNETTUNNEL], [test "${notelnet}" = "0"])
AM_CONDITIONAL([WANT_LIBSSLTUNNEL], [test "${nossl}" = "0"])
AM_CONDITIONAL([PKG_DEP_VDT], [test "${PKGDEPVDT}" = "1"])
AM_CONDITIONAL([PKG_DEP_EPEL], [test "${PKGDEPEPEL}" = "1"])



AC_SUBST(GSSAPI_TARGETS)

dnl Passing configre variables to spec file for rpm to build.

AC_SUBST(GLOBUS_INCLUDE_DIR)
AC_SUBST(GLOBUS_LIB_DIR)


AC_OUTPUT(Makefile \
  src/Makefile \
  src/dcap.h \
  src/dcap_version.c \
  plugins/Makefile \
  plugins/gssapi/Makefile \
  plugins/ssl/Makefile \
  plugins/telnet/Makefile \
  dcache-dcap.spec )

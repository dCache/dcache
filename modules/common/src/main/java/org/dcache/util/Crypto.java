package org.dcache.util;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Various useful cryptographic utility method
 */
public class Crypto {

    private enum CipherFlag {
        DISABLE_EC,
        DISABLE_RC4,
        DISABLE_RFC_7540_BANNED_CIPHERS,
        ENABLE_WEAK_CIPHERS_FOR_SL7_CLIENTS,
    }

    private static final Splitter CIPHER_FLAG_SPLITTER =
          Splitter.on(",").omitEmptyStrings().trimResults();

    private Crypto() {
    }

    /* The following is a list of cipher suites that are problematic
     * with currently suported versions of Java.
     *
     * The problem is described here:
     *
     *     https://bugs.launchpad.net/ubuntu/+source/openjdk-6/+bug/1006776
     *
     * Note that, despite the comment in the ticket, the problem is
     * also present for OpenJDK v7.
     *
     * During the SSL/TLS handshake, the client will send a list of
     * supported ciphers.  The server will choose one, based on the
     * client-supplied list and the ciphers it supports.
     *
     * The problem is that libnss3 supports only 3 EC (elliptic curve)
     * ciphers, but the Java security provider that wraps libnss3
     * believes it supports all elliptic curve ciphers.  If the
     * client's list of supported ciphers includes those based on EC
     * then the Java server may choose an EC cipher.  This SSL
     * negotiation will then fail with the 'CKR_DOMAIN_PARAMS_INVALID'
     * error.  For example, JGlobus will log the following:
     *
     *    19 Apr 2013 17:40:43 (SRM-zitpcx6184) []
     *    org.globus.common.Chained IOException: Authentication failed
     *    [Caused by: Failure unspecified at GSS-API level [Caused by:
     *    Failure unspecified at GSS-API level [Caused by:
     *    sun.security.pkcs11.wrapper.PKCS11Exception:
     *    CKR_DOMAIN_PARAMS_INVALID]]]
     *
     * Clients that do not support EC-based ciphers are not affected
     * by this bug; for example, OpenSSL prior to v1.0.0 (or there
     * abouts) and most Java-based clients.
     *
     * Some clients provide control of the cipher selection.  For
     * example, the OpenSSL sample client ('s_client') provides the
     * -cipher option.  This may be used to omit all eliptic curve
     * ciphers as a client-side work-around.  For example, the
     * following command may be used to connect to the localhost's
     * HTTPS SRM endpoint while excluding any EC-based cipher:
     *
     *     openssl s_client -connect localhost:8445 -cipher 'DEFAULT:!ECDH'
     *
     * It is also possible to disable support on the server by editing
     * the 'java.security' file to remove the security provider that
     * is supplying the (broken) eliptic curve support.  This is
     * controlled by the file 'java.security', which is located in a
     * distribution-specific directory.  For Debian, the file is in
     * the /etc/java-7-openjdk/security directory and for RHEL, it's
     * in the /usr/java/<version>/jre/lib/security directory.
     *
     * As we can't control all clients and the instructions for how to
     * disable the EC ciphers is fiddly, we have a server-side
     * work-around: dCache components (typically doors) can remove
     * these ciphers from the list of supported ciphers, preventing
     * them from being chosen during the SSL/TLS negotiation.
     *
     * The following list was generated from OpenJDK source code using
     * the command:
     */
    //    sed -n '/add.*TLS_ECDHE/s/.*add(\([^,]*\).*/        \1,/p'
    //    sun/security/ssl/CipherSuite.java | sort
    public static final List<String> EC_CIPHERS = ImmutableList.of(
          "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
          "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
          "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
          "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
          "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
          "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
          "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
          "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
          "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
          "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
          "TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_ECDHE_ECDSA_WITH_NULL_SHA",
          "TLS_ECDHE_RSA_WITH_NULL_SHA",
          "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
          "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
          "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
          "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
          "TLS_ECDHE_PSK_WITH_RC4_128_SHA",
          "TLS_ECDHE_PSK_WITH_3DES_EDE_CBC_SHA",
          "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA",
          "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA",
          "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA256",
          "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA384",
          "TLS_ECDHE_PSK_WITH_NULL_SHA",
          "TLS_ECDHE_PSK_WITH_NULL_SHA256",
          "TLS_ECDHE_PSK_WITH_NULL_SHA384");

    /**
     * A list of Ciphers that make use of the RC4 (Rivest Cipher 4) stream cipher.  RC4 has several
     * potential attacks and general recommendation seems to disable RC4 whenever possible, as
     * presented in RFC 7465.
     * <p>
     * This list was generated with the following command:
     * <p>
     * sed -n 's%^.*add( *"\([^"]*_RC4_[^"]*\)".*%            "\1",%p'
     * sun/security/ssl/CipherSuite.java|sort
     */
    public static final List<String> RC4_CIPHERS = ImmutableList.of(
          "SSL_DH_anon_EXPORT_WITH_RC4_40_MD5",
          "SSL_DH_anon_WITH_RC4_128_MD5",
          "SSL_DHE_DSS_EXPORT1024_WITH_RC4_56_SHA",
          "SSL_DHE_DSS_WITH_RC4_128_SHA",
          "SSL_RSA_EXPORT1024_WITH_RC4_56_SHA",
          "SSL_RSA_EXPORT_WITH_RC4_40_MD5",
          "SSL_RSA_WITH_RC4_128_MD5",
          "SSL_RSA_WITH_RC4_128_SHA",
          "TLS_DHE_PSK_WITH_RC4_128_SHA",
          "TLS_ECDH_anon_WITH_RC4_128_SHA",
          "TLS_ECDH_ECDSA_WITH_RC4_128_SHA",
          "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
          "TLS_ECDHE_PSK_WITH_RC4_128_SHA",
          "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
          "TLS_ECDH_RSA_WITH_RC4_128_SHA",
          "TLS_KRB5_EXPORT_WITH_RC4_40_MD5",
          "TLS_KRB5_EXPORT_WITH_RC4_40_SHA",
          "TLS_KRB5_WITH_RC4_128_MD5",
          "TLS_KRB5_WITH_RC4_128_SHA",
          "TLS_PSK_WITH_RC4_128_SHA",
          "TLS_RSA_PSK_WITH_RC4_128_SHA");

    /**
     * This is a subset of the ciphers that an Scientific Linux 7 client advertised it supported.
     * The same list will likely also work for RHEL 7 and CentOS 7 clients.  In theory, a single
     * cipher should be sufficient; multiple ciphers are included as a hedge against client
     * configuration.
     * <p>
     * The specific ciphers were selected by hand as (hopefully) the most secure of the ciphers that
     * SL7 supports.
     */
    private static final List<String> WEAK_CIPHERS_NEEDED_FOR_SL7_CLIENTS = ImmutableList.of(
          "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
          "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
          "TLS_DH_RSA_WITH_AES_128_CBC_SHA",
          "TLS_DH_RSA_WITH_AES_256_CBC_SHA",
          "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
          "TLS_DHE_RSA_WITH_AES_256_CBC_SHA");

    /* List taken from RFC 7540 */
    public static final List<String> RFC_7540_BANNED_CIPHERS = ImmutableList.of(
          "TLS_NULL_WITH_NULL_NULL",
          "TLS_RSA_WITH_NULL_MD5",
          "TLS_RSA_WITH_NULL_SHA",
          "TLS_RSA_EXPORT_WITH_RC4_40_MD5",
          "TLS_RSA_WITH_RC4_128_MD5",
          "TLS_RSA_WITH_RC4_128_SHA",
          "TLS_RSA_EXPORT_WITH_RC2_CBC_40_MD5",
          "TLS_RSA_WITH_IDEA_CBC_SHA",
          "TLS_RSA_EXPORT_WITH_DES40_CBC_SHA",
          "TLS_RSA_WITH_DES_CBC_SHA",
          "TLS_RSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_DH_DSS_EXPORT_WITH_DES40_CBC_SHA",
          "TLS_DH_DSS_WITH_DES_CBC_SHA",
          "TLS_DH_DSS_WITH_3DES_EDE_CBC_SHA",
          "TLS_DH_RSA_EXPORT_WITH_DES40_CBC_SHA",
          "TLS_DH_RSA_WITH_DES_CBC_SHA",
          "TLS_DH_RSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_DHE_DSS_EXPORT_WITH_DES40_CBC_SHA",
          "TLS_DHE_DSS_WITH_DES_CBC_SHA",
          "TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA",
          "TLS_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA",
          "TLS_DHE_RSA_WITH_DES_CBC_SHA",
          "TLS_DHE_RSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_DH_anon_EXPORT_WITH_RC4_40_MD5",
          "TLS_DH_anon_WITH_RC4_128_MD5",
          "TLS_DH_anon_EXPORT_WITH_DES40_CBC_SHA",
          "TLS_DH_anon_WITH_DES_CBC_SHA",
          "TLS_DH_anon_WITH_3DES_EDE_CBC_SHA",
          "TLS_KRB5_WITH_DES_CBC_SHA",
          "TLS_KRB5_WITH_3DES_EDE_CBC_SHA",
          "TLS_KRB5_WITH_RC4_128_SHA",
          "TLS_KRB5_WITH_IDEA_CBC_SHA",
          "TLS_KRB5_WITH_DES_CBC_MD5",
          "TLS_KRB5_WITH_3DES_EDE_CBC_MD5",
          "TLS_KRB5_WITH_RC4_128_MD5",
          "TLS_KRB5_WITH_IDEA_CBC_MD5",
          "TLS_KRB5_EXPORT_WITH_DES_CBC_40_SHA",
          "TLS_KRB5_EXPORT_WITH_RC2_CBC_40_SHA",
          "TLS_KRB5_EXPORT_WITH_RC4_40_SHA",
          "TLS_KRB5_EXPORT_WITH_DES_CBC_40_MD5",
          "TLS_KRB5_EXPORT_WITH_RC2_CBC_40_MD5",
          "TLS_KRB5_EXPORT_WITH_RC4_40_MD5",
          "TLS_PSK_WITH_NULL_SHA",
          "TLS_DHE_PSK_WITH_NULL_SHA",
          "TLS_RSA_PSK_WITH_NULL_SHA",
          "TLS_RSA_WITH_AES_128_CBC_SHA",
          "TLS_DH_DSS_WITH_AES_128_CBC_SHA",
          "TLS_DH_RSA_WITH_AES_128_CBC_SHA",
          "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",
          "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
          "TLS_DH_anon_WITH_AES_128_CBC_SHA",
          "TLS_RSA_WITH_AES_256_CBC_SHA",
          "TLS_DH_DSS_WITH_AES_256_CBC_SHA",
          "TLS_DH_RSA_WITH_AES_256_CBC_SHA",
          "TLS_DHE_DSS_WITH_AES_256_CBC_SHA",
          "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
          "TLS_DH_anon_WITH_AES_256_CBC_SHA",
          "TLS_RSA_WITH_NULL_SHA256",
          "TLS_RSA_WITH_AES_128_CBC_SHA256",
          "TLS_RSA_WITH_AES_256_CBC_SHA256",
          "TLS_DH_DSS_WITH_AES_128_CBC_SHA256",
          "TLS_DH_RSA_WITH_AES_128_CBC_SHA256",
          "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256",
          "TLS_RSA_WITH_CAMELLIA_128_CBC_SHA",
          "TLS_DH_DSS_WITH_CAMELLIA_128_CBC_SHA",
          "TLS_DH_RSA_WITH_CAMELLIA_128_CBC_SHA",
          "TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA",
          "TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA",
          "TLS_DH_anon_WITH_CAMELLIA_128_CBC_SHA",
          "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
          "TLS_DH_DSS_WITH_AES_256_CBC_SHA256",
          "TLS_DH_RSA_WITH_AES_256_CBC_SHA256",
          "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256",
          "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
          "TLS_DH_anon_WITH_AES_128_CBC_SHA256",
          "TLS_DH_anon_WITH_AES_256_CBC_SHA256",
          "TLS_RSA_WITH_CAMELLIA_256_CBC_SHA",
          "TLS_DH_DSS_WITH_CAMELLIA_256_CBC_SHA",
          "TLS_DH_RSA_WITH_CAMELLIA_256_CBC_SHA",
          "TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA",
          "TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA",
          "TLS_DH_anon_WITH_CAMELLIA_256_CBC_SHA",
          "TLS_PSK_WITH_RC4_128_SHA",
          "TLS_PSK_WITH_3DES_EDE_CBC_SHA",
          "TLS_PSK_WITH_AES_128_CBC_SHA",
          "TLS_PSK_WITH_AES_256_CBC_SHA",
          "TLS_DHE_PSK_WITH_RC4_128_SHA",
          "TLS_DHE_PSK_WITH_3DES_EDE_CBC_SHA",
          "TLS_DHE_PSK_WITH_AES_128_CBC_SHA",
          "TLS_DHE_PSK_WITH_AES_256_CBC_SHA",
          "TLS_RSA_PSK_WITH_RC4_128_SHA",
          "TLS_RSA_PSK_WITH_3DES_EDE_CBC_SHA",
          "TLS_RSA_PSK_WITH_AES_128_CBC_SHA",
          "TLS_RSA_PSK_WITH_AES_256_CBC_SHA",
          "TLS_RSA_WITH_SEED_CBC_SHA",
          "TLS_DH_DSS_WITH_SEED_CBC_SHA",
          "TLS_DH_RSA_WITH_SEED_CBC_SHA",
          "TLS_DHE_DSS_WITH_SEED_CBC_SHA",
          "TLS_DHE_RSA_WITH_SEED_CBC_SHA",
          "TLS_DH_anon_WITH_SEED_CBC_SHA",
          "TLS_RSA_WITH_AES_128_GCM_SHA256",
          "TLS_RSA_WITH_AES_256_GCM_SHA384",
          "TLS_DH_RSA_WITH_AES_128_GCM_SHA256",
          "TLS_DH_RSA_WITH_AES_256_GCM_SHA384",
          "TLS_DH_DSS_WITH_AES_128_GCM_SHA256",
          "TLS_DH_DSS_WITH_AES_256_GCM_SHA384",
          "TLS_DH_anon_WITH_AES_128_GCM_SHA256",
          "TLS_DH_anon_WITH_AES_256_GCM_SHA384",
          "TLS_PSK_WITH_AES_128_GCM_SHA256",
          "TLS_PSK_WITH_AES_256_GCM_SHA384",
          "TLS_RSA_PSK_WITH_AES_128_GCM_SHA256",
          "TLS_RSA_PSK_WITH_AES_256_GCM_SHA384",
          "TLS_PSK_WITH_AES_128_CBC_SHA256",
          "TLS_PSK_WITH_AES_256_CBC_SHA384",
          "TLS_PSK_WITH_NULL_SHA256",
          "TLS_PSK_WITH_NULL_SHA384",
          "TLS_DHE_PSK_WITH_AES_128_CBC_SHA256",
          "TLS_DHE_PSK_WITH_AES_256_CBC_SHA384",
          "TLS_DHE_PSK_WITH_NULL_SHA256",
          "TLS_DHE_PSK_WITH_NULL_SHA384",
          "TLS_RSA_PSK_WITH_AES_128_CBC_SHA256",
          "TLS_RSA_PSK_WITH_AES_256_CBC_SHA384",
          "TLS_RSA_PSK_WITH_NULL_SHA256",
          "TLS_RSA_PSK_WITH_NULL_SHA384",
          "TLS_RSA_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_DH_DSS_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_DH_RSA_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_DH_anon_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_RSA_WITH_CAMELLIA_256_CBC_SHA256",
          "TLS_DH_DSS_WITH_CAMELLIA_256_CBC_SHA256",
          "TLS_DH_RSA_WITH_CAMELLIA_256_CBC_SHA256",
          "TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA256",
          "TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA256",
          "TLS_DH_anon_WITH_CAMELLIA_256_CBC_SHA256",
          "TLS_EMPTY_RENEGOTIATION_INFO_SCSV",
          "TLS_ECDH_ECDSA_WITH_NULL_SHA",
          "TLS_ECDH_ECDSA_WITH_RC4_128_SHA",
          "TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA",
          "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA",
          "TLS_ECDHE_ECDSA_WITH_NULL_SHA",
          "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
          "TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
          "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
          "TLS_ECDH_RSA_WITH_NULL_SHA",
          "TLS_ECDH_RSA_WITH_RC4_128_SHA",
          "TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA",
          "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA",
          "TLS_ECDHE_RSA_WITH_NULL_SHA",
          "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
          "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
          "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
          "TLS_ECDH_anon_WITH_NULL_SHA",
          "TLS_ECDH_anon_WITH_RC4_128_SHA",
          "TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA",
          "TLS_ECDH_anon_WITH_AES_128_CBC_SHA",
          "TLS_ECDH_anon_WITH_AES_256_CBC_SHA",
          "TLS_SRP_SHA_WITH_3DES_EDE_CBC_SHA",
          "TLS_SRP_SHA_RSA_WITH_3DES_EDE_CBC_SHA",
          "TLS_SRP_SHA_DSS_WITH_3DES_EDE_CBC_SHA",
          "TLS_SRP_SHA_WITH_AES_128_CBC_SHA",
          "TLS_SRP_SHA_RSA_WITH_AES_128_CBC_SHA",
          "TLS_SRP_SHA_DSS_WITH_AES_128_CBC_SHA",
          "TLS_SRP_SHA_WITH_AES_256_CBC_SHA",
          "TLS_SRP_SHA_RSA_WITH_AES_256_CBC_SHA",
          "TLS_SRP_SHA_DSS_WITH_AES_256_CBC_SHA",
          "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
          "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
          "TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256",
          "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384",
          "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
          "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
          "TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256",
          "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384",
          "TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256",
          "TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384",
          "TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256",
          "TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384",
          "TLS_ECDHE_PSK_WITH_RC4_128_SHA",
          "TLS_ECDHE_PSK_WITH_3DES_EDE_CBC_SHA",
          "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA",
          "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA",
          "TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA256",
          "TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA384",
          "TLS_ECDHE_PSK_WITH_NULL_SHA",
          "TLS_ECDHE_PSK_WITH_NULL_SHA256",
          "TLS_ECDHE_PSK_WITH_NULL_SHA384",
          "TLS_RSA_WITH_ARIA_128_CBC_SHA256",
          "TLS_RSA_WITH_ARIA_256_CBC_SHA384",
          "TLS_DH_DSS_WITH_ARIA_128_CBC_SHA256",
          "TLS_DH_DSS_WITH_ARIA_256_CBC_SHA384",
          "TLS_DH_RSA_WITH_ARIA_128_CBC_SHA256",
          "TLS_DH_RSA_WITH_ARIA_256_CBC_SHA384",
          "TLS_DHE_DSS_WITH_ARIA_128_CBC_SHA256",
          "TLS_DHE_DSS_WITH_ARIA_256_CBC_SHA384",
          "TLS_DHE_RSA_WITH_ARIA_128_CBC_SHA256",
          "TLS_DHE_RSA_WITH_ARIA_256_CBC_SHA384",
          "TLS_DH_anon_WITH_ARIA_128_CBC_SHA256",
          "TLS_DH_anon_WITH_ARIA_256_CBC_SHA384",
          "TLS_ECDHE_ECDSA_WITH_ARIA_128_CBC_SHA256",
          "TLS_ECDHE_ECDSA_WITH_ARIA_256_CBC_SHA384",
          "TLS_ECDH_ECDSA_WITH_ARIA_128_CBC_SHA256",
          "TLS_ECDH_ECDSA_WITH_ARIA_256_CBC_SHA384",
          "TLS_ECDHE_RSA_WITH_ARIA_128_CBC_SHA256",
          "TLS_ECDHE_RSA_WITH_ARIA_256_CBC_SHA384",
          "TLS_ECDH_RSA_WITH_ARIA_128_CBC_SHA256",
          "TLS_ECDH_RSA_WITH_ARIA_256_CBC_SHA384",
          "TLS_RSA_WITH_ARIA_128_GCM_SHA256",
          "TLS_RSA_WITH_ARIA_256_GCM_SHA384",
          "TLS_DH_RSA_WITH_ARIA_128_GCM_SHA256",
          "TLS_DH_RSA_WITH_ARIA_256_GCM_SHA384",
          "TLS_DH_DSS_WITH_ARIA_128_GCM_SHA256",
          "TLS_DH_DSS_WITH_ARIA_256_GCM_SHA384",
          "TLS_DH_anon_WITH_ARIA_128_GCM_SHA256",
          "TLS_DH_anon_WITH_ARIA_256_GCM_SHA384",
          "TLS_ECDH_ECDSA_WITH_ARIA_128_GCM_SHA256",
          "TLS_ECDH_ECDSA_WITH_ARIA_256_GCM_SHA384",
          "TLS_ECDH_RSA_WITH_ARIA_128_GCM_SHA256",
          "TLS_ECDH_RSA_WITH_ARIA_256_GCM_SHA384",
          "TLS_PSK_WITH_ARIA_128_CBC_SHA256",
          "TLS_PSK_WITH_ARIA_256_CBC_SHA384",
          "TLS_DHE_PSK_WITH_ARIA_128_CBC_SHA256",
          "TLS_DHE_PSK_WITH_ARIA_256_CBC_SHA384",
          "TLS_RSA_PSK_WITH_ARIA_128_CBC_SHA256",
          "TLS_RSA_PSK_WITH_ARIA_256_CBC_SHA384",
          "TLS_PSK_WITH_ARIA_128_GCM_SHA256",
          "TLS_PSK_WITH_ARIA_256_GCM_SHA384",
          "TLS_RSA_PSK_WITH_ARIA_128_GCM_SHA256",
          "TLS_RSA_PSK_WITH_ARIA_256_GCM_SHA384",
          "TLS_ECDHE_PSK_WITH_ARIA_128_CBC_SHA256",
          "TLS_ECDHE_PSK_WITH_ARIA_256_CBC_SHA384",
          "TLS_ECDHE_ECDSA_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_ECDHE_ECDSA_WITH_CAMELLIA_256_CBC_SHA384",
          "TLS_ECDH_ECDSA_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_ECDH_ECDSA_WITH_CAMELLIA_256_CBC_SHA384",
          "TLS_ECDHE_RSA_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_ECDHE_RSA_WITH_CAMELLIA_256_CBC_SHA384",
          "TLS_ECDH_RSA_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_ECDH_RSA_WITH_CAMELLIA_256_CBC_SHA384",
          "TLS_RSA_WITH_CAMELLIA_128_GCM_SHA256",
          "TLS_RSA_WITH_CAMELLIA_256_GCM_SHA384",
          "TLS_DH_RSA_WITH_CAMELLIA_128_GCM_SHA256",
          "TLS_DH_RSA_WITH_CAMELLIA_256_GCM_SHA384",
          "TLS_DH_DSS_WITH_CAMELLIA_128_GCM_SHA256",
          "TLS_DH_DSS_WITH_CAMELLIA_256_GCM_SHA384",
          "TLS_DH_anon_WITH_CAMELLIA_128_GCM_SHA256",
          "TLS_DH_anon_WITH_CAMELLIA_256_GCM_SHA384",
          "TLS_ECDH_ECDSA_WITH_CAMELLIA_128_GCM_SHA256",
          "TLS_ECDH_ECDSA_WITH_CAMELLIA_256_GCM_SHA384",
          "TLS_ECDH_RSA_WITH_CAMELLIA_128_GCM_SHA256",
          "TLS_ECDH_RSA_WITH_CAMELLIA_256_GCM_SHA384",
          "TLS_PSK_WITH_CAMELLIA_128_GCM_SHA256",
          "TLS_PSK_WITH_CAMELLIA_256_GCM_SHA384",
          "TLS_RSA_PSK_WITH_CAMELLIA_128_GCM_SHA256",
          "TLS_RSA_PSK_WITH_CAMELLIA_256_GCM_SHA384",
          "TLS_PSK_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_PSK_WITH_CAMELLIA_256_CBC_SHA384",
          "TLS_DHE_PSK_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_DHE_PSK_WITH_CAMELLIA_256_CBC_SHA384",
          "TLS_RSA_PSK_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_RSA_PSK_WITH_CAMELLIA_256_CBC_SHA384",
          "TLS_ECDHE_PSK_WITH_CAMELLIA_128_CBC_SHA256",
          "TLS_ECDHE_PSK_WITH_CAMELLIA_256_CBC_SHA384",
          "TLS_RSA_WITH_AES_128_CCM",
          "TLS_RSA_WITH_AES_256_CCM",
          "TLS_RSA_WITH_AES_128_CCM_8",
          "TLS_RSA_WITH_AES_256_CCM_8",
          "TLS_PSK_WITH_AES_128_CCM",
          "TLS_PSK_WITH_AES_256_CCM",
          "TLS_PSK_WITH_AES_128_CCM_8",
          "TLS_PSK_WITH_AES_256_CCM_8");

    /**
     * @throws IllegalArgumentException if the value could not be parsed
     */
    public static String[] getBannedCipherSuitesFromConfigurationValue(String value) {
        List<String> values = Lists.newArrayList(CIPHER_FLAG_SPLITTER.split(value));
        CipherFlag[] flags = new CipherFlag[values.size()];
        for (int i = 0; i < values.size(); i++) {
            flags[i] = CipherFlag.valueOf(values.get(i));
        }
        return getBannedCipherSuites(flags);
    }

    private static String[] getBannedCipherSuites(CipherFlag[] flags) {
        Set<String> banned = new HashSet<>();
        for (CipherFlag flag : flags) {
            switch (flag) {
                case DISABLE_EC:
                    banned.addAll(EC_CIPHERS);
                    break;
                case DISABLE_RC4:
                    banned.addAll(RC4_CIPHERS);
                    break;
                case DISABLE_RFC_7540_BANNED_CIPHERS:
                    banned.addAll(RFC_7540_BANNED_CIPHERS);
                    break;
                case ENABLE_WEAK_CIPHERS_FOR_SL7_CLIENTS:
                    banned.removeAll(WEAK_CIPHERS_NEEDED_FOR_SL7_CLIENTS);
                    break;
            }
        }
        return banned.toArray(String[]::new);
    }
}

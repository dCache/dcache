Chapter 27. Transport Security
==============================

Some users, specifically those with medical data, may have data channel encryption as a requirement. dCache does not (yet) support encryption of the GridFTP data channel. At the moment, `globus-url-copy -dcpriv` *appears* to encrypt the data when talking to dCache, but in fact it decides to send the data in plain text, and it does *NOT* warn the user.

To make sure data is safely encrypted during transfer, one should do all of the following:

1. Use the WebDAV protocol
2. Configure dCache WebDAV doors not to redirect transfers, or if they do, only to HTTPS and not to HTTP
3. Have a well configured certificate or chain in each WebDAV door (and if the doors redirect to the pools through HTTPS, the pools should have host certificates too)
4. Use Java 11 or better, with a well configured java.security

There is the option `webdav.redirect.allow-https` which is `false` by default. If set to true, WebDAV doors should redirect clients to HTTPS movers on the pools. Your pools must have host certificates and have `pool.enable.encrypted-transfers = true` for this to work. This new feature should be considered experimental; test it before using it.

Please note, that IT security is an ever changing field. Make sure you follow the latest developments. If security in dCache is a priority for you, be sure to follow the dCache user mailinglist.

-----
[TOC bullet hierarchy]
-----

## Configuring a secure WebDAV door

Here is an example of a secure WebDAV door definition:

```ini
[webdav443-${host.name}Domain]
[webdav443-${host.name}Domain/webdav]
webdav.cell.name = webdav443-${host.name}
webdav.authn.protocol = https
webdav.net.port = 443
# Disable redirects because by default they send client to HTTP, not HTTPS.
webdav.redirect.on-read = false
webdav.redirect.on-write = false
# Support username/password auth
webdav.authn.basic = true
# Support X509 authentication
webdav.authn.accept-client-cert = true
```



## Configuring Java

dCache relies on Java for its transport security. Hardening Java is advised to get the maximum security. If not configured properly, Java may support ciphers and protocols that are considered weak.

From dCache 6.2 and newer, Java 11 is required. Java 11 adds support for TLSv1.3.

Java security setting can be configured in a file called java.security. On a Centos 7 system, this can be found with `find / -name java.security`. The most important setting in this file is:

    jdk.tls.disabledAlgorithms=SSLv3, TLSv1, TLSv1.1, RC4, DES, MD5withRSA, \
        DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL, \
        include jdk.disabled.namedCurves

This value is from a default Java 11 java.security file.

In recent Java 11 versions, TLS version 1.0 and 1.1 protocols have been disabled. In older versions, you can disable TLS 1.0 and TLS 1.1 by adding them to jdk.tls.disabledAlgorithms (`TLSv1, TLSv1.1`) - but be sure to test it first, because it may break older clients!

Specific cipher suites can be disabled here as well. A good method is, to run your WebDAV door through the Qualys SSLtest, look at the cipher suites that the SSLtest lists as `weak`, and list those in the `jdk.tls.disabledAlgorithms`.

Here is an example of a resulting configuration:

    jdk.tls.disabledAlgorithms=SSLv2Hello, SSLv3, TLSv1, TLSv1.1, DES, DESede, RC4, MD5withRSA, DH keySize < 1024, \
        EC keySize < 224, DES40_CBC, RC4_40, \
        TLS_RSA_WITH_AES_256_CBC_SHA256, TLS_RSA_WITH_AES_256_CBC_SHA, \
        TLS_RSA_WITH_AES_128_CBC_SHA256, TLS_RSA_WITH_AES_128_CBC_SHA, \
        TLS_RSA_WITH_AES_256_GCM_SHA384, TLS_RSA_WITH_AES_128_GCM_SHA256, \
        TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, TLS_DHE_RSA_WITH_AES_256_CBC_SHA, \
        TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, TLS_DHE_RSA_WITH_AES_128_CBC_SHA

As of October 2018, this should be sufficient to get an A rating in the Qualys SSLtest. But the Qualys test is updated regularly to match the latest security developments: you'll have to repeat the test on a regular basis to remain safe.



### The EL8 crypto-policies framework

Centos 8 and similar operating systems have the crypto-policies framework. This framework silently overrides settings in your java.security file, which can be very confusing. The currently active settings can be found in:

    /etc/crypto-policies/back-ends/java.config

To permanently override these settings, for instance to allow 1024 bit proxy authentication, you can make a file like this:


```
[root@penguin14 ~]# cat /etc/crypto-policies/local.d/java-proxy1024.config
# Imported from /etc/crypto-policies/local.d/java-proxy1024.config
jdk.certpath.disabledAlgorithms=MD2, MD5, DSA, RSA keySize < 1024
```

Then run:

```
update-crypto-policies
```

This should result in a file like this:

```
[root@penguin14 ~]# cat /etc/crypto-policies/back-ends/java.config 
jdk.tls.ephemeralDHKeySize=2048
jdk.certpath.disabledAlgorithms=MD2, MD5, DSA, RSA keySize < 2048
jdk.tls.disabledAlgorithms=DH keySize < 2048, SSLv2, SSLv3, TLSv1, TLSv1.1, DHE_DSS, RSA_EXPORT, DHE_DSS_EXPORT, DHE_RSA_EXPORT, DH_DSS_EXPORT, DH_RSA_EXPORT, DH_anon, ECDH_anon, DH_RSA, DH_DSS, ECDH, 3DES_EDE_CBC, DES_CBC, RC4_40, RC4_128, DES40_CBC, RC2, HmacMD5
jdk.tls.legacyAlgorithms=
# Imported from /etc/crypto-policies/local.d/java-proxy1024.config
jdk.certpath.disabledAlgorithms=MD2, MD5, DSA, RSA keySize < 1024
```

Then you can restart the dCache domains that needed the new setting.



### Configuring java.security settings per dCache domain

The java.security settings control all Java processes; so the disabled algorithms apply to all incoming and outgoing connections. There may be cases where different settings are needed for different Java processes. Examples:

* gPlazma may need to connect to an LDAP server that does not have the latest ciphers yet
* GridFTP doors may need to support older ciphers to support some outdated clients

It is possible to configure the java.security settings per domain, by using the `dcache.java.options.extra` setting. Example:

```ini
[webdav443-${host.name}Domain]
dcache.java.options.extra = -Djava.security.properties=/etc/dcache/maximum.java.security -Djdk.tls.ephemeralDHKeySize=2048
[webdav443-${host.name}Domain/webdav]
```

Then, in the file `/etc/dcache/maximum.java.security`, you can put a specific `jdk.tls.disabledAlgorithms` setting.



## Testing protocols and cipher suites

To test whether the WebDAV door is secure, there are some tests you can perform.



### Qualys SSL test

A famous test is the Qualys SSL test at https://www.ssllabs.com/ssltest/. This test is very thorough, but it may take a long time, and it can't scan internal networks.



### nmap

`nmap --script ssl-enum-ciphers` is a good alternative. It has the advantage that it's very fast, it can scan internal networks, and it can scan SRM doors as well as WebDAV doors. It does not test as much as the Qualys test though. You may need to download the latest version to have a reliable result. Here is an example of a test with nmap:

```console-root
nmap --script ssl-enum-ciphers -p 443 -P0 example.org
|Starting Nmap 7.70 ( https://nmap.org ) at 2018-10-16 13:07 CEST
|Nmap scan report for example.org (93.184.216.34)
|Host is up (0.090s latency).
|Other addresses for example.org (not scanned): 2606:2800:220:1:248:1893:25c8:1946
|
|PORT    STATE SERVICE
|443/tcp open  https
|| ssl-enum-ciphers:
||   TLSv1.0:
||     ciphers:
||       TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA (secp256r1) - A
||       TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA (secp256r1) - A
||       TLS_RSA_WITH_AES_256_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_CAMELLIA_256_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_AES_128_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_CAMELLIA_128_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_SEED_CBC_SHA (rsa 2048) - A
||     compressors:
||       NULL
||     cipher preference: server
||   TLSv1.1:
||     ciphers:
||       TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA (secp256r1) - A
||       TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA (secp256r1) - A
||       TLS_RSA_WITH_AES_256_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_CAMELLIA_256_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_AES_128_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_CAMELLIA_128_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_SEED_CBC_SHA (rsa 2048) - A
||     compressors:
||       NULL
||     cipher preference: server
||   TLSv1.2:
||     ciphers:
||       TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 (secp256r1) - A
||       TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 (secp256r1) - A
||       TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256 (secp256r1) - A
||       TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA (secp256r1) - A
||       TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384 (secp256r1) - A
||       TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA (secp256r1) - A
||       TLS_RSA_WITH_AES_128_GCM_SHA256 (rsa 2048) - A
||       TLS_RSA_WITH_AES_256_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_CAMELLIA_256_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_AES_128_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_CAMELLIA_128_CBC_SHA (rsa 2048) - A
||       TLS_RSA_WITH_SEED_CBC_SHA (rsa 2048) - A
||     compressors:
||       NULL
||     cipher preference: server
||_  least strength: A
|
|Nmap done: 1 IP address (1 host up) scanned in 7.49 seconds
```


### Wireshark

Wireshark is a network protocol analyzer. It will provide many details about the traffic. It can be installed on Centos with:

```console-root
yum install wireshark-gnome
```

If you test a WebDAV door on another port than 443, you may need to tell Wireshark to interpret the traffic as an `SSL` connection. You can do this in the menu at `Analysis` -> `Decode as`. With Wireshark, you can also test which ciphers are supported by the client: look for `client hello`.



### Greenbone/OpenVAS

OpenVAS is a vulnerability scanner. Greenbone is a web interface to use OpenVAS. OpenVAS can scan a system for many different vulnerabilities, including depricated ciphers and protocols. It can be downloaded from http://www.openvas.org/. It may take some effort to set it up, and scanning may take a long time, but it can detect an overwhelming amount of issues.



## HTTP header hardening

With https://securityheaders.com/, you can test how secure the HTTP headers of your WebDAV doors are. But be warned: headers that are too strict may cripple your WebDAV doors. Be sure to test it first.



### About Strict Transport Security

The Security Headers test suggests to use HTTP Strict Transport Security (HSTS). This is a header that tells clients that next time they connect they should do so over HTTPS and not HTTP. However, this may break dCache WebDAV doors that *do* redirect, as dCache may redirect to HTTP and not HTTPS. Also, from the HSTS specifications it is not entirely clear how redirection should work with non-default TCP ports. A better and safer solution to avoid people from using unencrypted WebDAV doors, is to simply not have them at all: don't configure WebDAV doors on port 80, and don't have WebDAV doors that redirect to HTTP (unless you have a good reason).



### Examples

Here is an example of safe headers that could be a good starting point:

```ini
# WebDAV security enhancements
webdav.custom-response-header!Content-Security-Policy = \
    default-src 'none' ; \
    img-src 'self' data: ; \
    style-src 'self' 'unsafe-inline' ; \
    script-src 'self'; font-src 'self'
webdav.custom-response-header!X-Frame-Options = SAMEORIGIN
webdav.custom-response-header!X-XSS-Protection = 1; mode=block
webdav.custom-response-header!X-Content-Type-Options = nosniff
webdav.custom-response-header!Referrer-Policy = strict-origin-when-cross-origin
webdav.custom-response-header!Access-Control-Allow-Origin = https://dcache-view.mydcache.org
```

The last line may be needed if you have a dCache View instance that needs this WebDAV door to provide access to data.

If you have a `dCache View` instance, it can be a bit more complicated because of various dependencies on external elements. Here is a suggested starting point:

```ini
# dCache View security enhancements
frontend.custom-response-header!Content-Security-Policy = \
    default-src 'self' data: https://webdav.mydcache.org ; \
    script-src  'self' data: 'unsafe-inline' https://fonts.googleapis.com https://www.gstatic.com/ https://fonts.gstatic.com/ ; \
    font-src    'self' data: https://fonts.googleapis.com https://www.gstatic.com/ https://fonts.gstatic.com/ ; \
    style-src   'self' 'unsafe-inline' https://fonts.googleapis.com https://www.gstatic.com/ https://fonts.gstatic.com/
frontend.custom-response-header!X-Frame-Options = SAMEORIGIN
frontend.custom-response-header!X-XSS-Protection = 1; mode=block
frontend.custom-response-header!X-Content-Type-Options = nosniff
frontend.custom-response-header!Referrer-Policy = strict-origin-when-cross-origin
frontend.custom-response-header!Access-Control-Allow-Origin = https://webdav.mydcache.org
frontend.custom-response-header!Feature-Policy = \
    accelerometer 'none' ; \
    ambient-light-sensor 'none' ; \
    camera 'none' ; \
    encrypted-media 'none' ; \
    fullscreen 'none' ; \
    geolocation 'none' ; \
    gyroscope 'none' ; \
    magnetometer 'none' ; \
    microphone 'none' ; \
    midi 'none' ; \
    payment 'none' ; \
    speaker 'none' ; \
    sync-xhr 'self' ; \
    usb 'none' ; \
    vr 'none' ; \
    picture-in-picture 'none'
```

Also here, settings that are too strict may cripple the web interface. And dCache View is under heavy development, so you may need to test and tweak the settings with every dCache upgrade.



### Troubleshooting security headers

A good way to test whether your security headers are blocking operation of a WebDAV door or a dCache View portal, is to use the Web Console in Firefox or the DevTools in Chrome. These will show all items that are blocked because of policy headers.



## DNS CAA records

CAA records indicate which CA (certificate authority) is allowed to sign certificates for a specific host name or domain. Here is an example:

    webdav	IN	CAA     0 issue "Digicert.com"
    webdav	IN	CAA     0 iodef "mailto:helpdesk@example.org"

These CAA records indicate that for the host webdav(.example.org), only Digicert is allowed to sign certificates. If an outsider asks another CA to sign a certificate for the same host, the other CA should check whether there is a CAA record, and refuse to sign the certificate when the CAA record does not authorize them.

In general CAA records are a good idea, however, Grid certificate authorities don't follow this standard (yet). So, CAA records will only stop commercial CAs, and not Grid CAs, from signing rogue certificates.

CAA records can be easily generated with this generator: https://sslmate.com/caa/. This can also generate the legacy notation that is required for older Bind clients.

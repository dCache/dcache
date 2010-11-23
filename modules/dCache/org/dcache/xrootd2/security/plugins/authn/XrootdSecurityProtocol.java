package org.dcache.xrootd2.security.plugins.authn;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class XrootdSecurityProtocol {

    // server status codes (returned in responses)
    public final static int kgST_error    = -1;      // error occured
    public final static int kgST_ok       =  0;      // ok
    public final static int kgST_more     =  1;       // need more info

    // client processing steps
    public final static int kXGC_none =     0;
    public final static int kXGC_certreq =  1000;   // 1000: request server certificate
    public final static int kXGC_cert =     1001;   // 1001: packet with (proxy) certificate
    public final static int kXGC_sigpxy =   1002;   // 1002: packet with signed proxy certificate
    public final static int kXGC_reserved = 1003;

    // server processing steps
    public final static int kXGS_none =     0;
    public final static int kXGS_init =     2000;   // 2000: fake code used the first time
    public final static int kXGS_cert =     2001;   // 2001: packet with certificate
    public final static int kXGS_pxyreq =   2002;   // 2002: packet with proxy req to be signed
    public final static int kXGS_reserved = 2003;

    // handshake options
    public final static int kOptsDlgPxy     = 1;      // 0x0001: Ask for a delegated proxy
    public final static int kOptsFwdPxy     = 2;      // 0x0002: Forward local proxy
    public final static int kOptsSigReq     = 4;      // 0x0004: Accept to sign delegated proxy
    public final static int kOptsSrvReq     = 8;      // 0x0008: Server request for delegated proxy
    public final static int kOptsPxFile     = 16;     // 0x0010: Save delegated proxies in file
    public final static int kOptsDelChn     = 32;      // 0x0020: Delete chain

    public static enum BucketType {
        kXRS_none           (0),    // end-of-vector
        kXRS_inactive       (1),    // inactive (dropped at serialization)
        kXRS_cryptomod      (3000), // Name of crypto module to use
        kXRS_main           (3001), // Main buffer
        kXRS_srv_seal       (3002), // Server secrets sent back as they are
        kXRS_clnt_seal      (3003), // Client secrets sent back as they are
        kXRS_puk            (3004), // Public Key
        kXRS_cipher         (3005), // Cipher
        kXRS_rtag           (3006), // Random Tag
        kXRS_signed_rtag    (3007), // Random Tag signed by the client
        kXRS_user           (3008), // User name
        kXRS_host           (3009), // Remote Host name
        kXRS_creds          (3010), // Credentials (password, ...)
        kXRS_message        (3011), // Message (null-terminated string)
        kXRS_srvID          (3012), // Server unique ID
        kXRS_sessionID      (3013), // Handshake session ID
        kXRS_version        (3014), // Package version
        kXRS_status         (3015), // Status code
        kXRS_localstatus    (3016), // Status code(s) saved in sealed buffer
        kXRS_othercreds     (3017), // Alternative creds (e.g. other crypto)
        kXRS_cache_idx      (3018), // Cache entry index
        kXRS_clnt_opts      (3019), // Client options, if any
        kXRS_error_code     (3020), // Error code
        kXRS_timestamp      (3021), // Time stamp
        kXRS_x509           (3022), // X509 certificate
        kXRS_issuer_hash    (3023), // Issuer hash
        kXRS_x509_req       (3024), // X509 certificate request
        kXRS_cipher_alg     (3025), // Cipher algorithm (list)
        kXRS_md_alg         (3026), // MD algorithm (list)
        kXRS_afsinfo        (3027), // AFS information
        kXRS_reserved       (3028); // Reserved

        private static final Map<Integer,BucketType> _lookup
                                        = new HashMap<Integer,BucketType>();

        static {
            for(BucketType s : EnumSet.allOf(BucketType.class)) {
                _lookup.put(s.getCode(), s);
            }
        }

        private final int _code;

        BucketType( int code ) {
            _code = code;
        }

        public int getCode() {
            return _code;
        }

        public static BucketType get(int code) {
            return _lookup.get(code);
        }
    }
}

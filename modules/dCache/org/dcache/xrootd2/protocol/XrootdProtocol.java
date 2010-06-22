package org.dcache.xrootd2.protocol;

public interface XrootdProtocol {

    /*  current supported protocol version: 2.96
     * Xrootd expects the protocol information binary encoded in an int32
     */
    public final static byte PROTOCOL_VERSION_MAJOR = 0x2;
    /* short, because bytes are signed in java and 0x96 > 0x7f */
    public final static short PROTOCOL_VERSION_MINOR =  0x96;

    public final static int  PROTOCOL_VERSION = PROTOCOL_VERSION_MAJOR << 8 | PROTOCOL_VERSION_MINOR;

    public final static byte      CLIENT_REQUEST_LEN = 24;
    public final static byte    CLIENT_HANDSHAKE_LEN = 20;
    public final static byte     SERVER_RESPONSE_LEN = 8;
    public final static int            LOAD_BALANCER = 0;
    public final static int              DATA_SERVER = 1;

    public final static byte[] HANDSHAKE_REQUEST = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 7, (byte) 220};
    /* casting the minor version to byte works, because xrootd interprets the bytes as unsigned ( and 0x96 < 0xff ) */
    public final static byte[] HANDSHAKE_RESPONSE_LOADBALANCER = {0, 0, 0, 0, 0, 0, 0, 8, 0, 0, PROTOCOL_VERSION_MAJOR, (byte) PROTOCOL_VERSION_MINOR , 0, 0, 0, LOAD_BALANCER};
    public final static byte[] HANDSHAKE_RESPONSE_DATASERVER = {0, 0, 0, 0, 0, 0, 0, 8, 0, 0, PROTOCOL_VERSION_MAJOR, (byte) PROTOCOL_VERSION_MINOR, 0, 0, 0, DATA_SERVER};

    // server response codes
    public final static int   kXR_ok       = 0;
    public final static int   kXR_oksofar  = 4000;
    public final static int   kXR_authmore = 4002;
    public final static int   kXR_error    = 4003;
    public final static int   kXR_redirect = 4004;
    public final static int   kXR_wait     = 4005;
    public final static int   kXR_waitresp = 4006;
    public final static int   kXR_noResponseYet = 10000;

    // server error codes
    public final static int   kXR_ArgInvalid     = 3000;
    public final static int   kXR_ArgMissing     = 3001;
    public final static int   kXR_ArgTooLong     = 3002;
    public final static int   kXR_FileLockedr    = 3003;
    public final static int   kXR_FileNotOpen    = 3004;
    public final static int   kXR_FSError        = 3005;
    public final static int   kXR_InvalidRequest = 3006;
    public final static int   kXR_IOError        = 3007;
    public final static int   kXR_NoMemory       = 3008;
    public final static int   kXR_NoSpace        = 3009;
    public final static int   kXR_NotAuthorized  = 3010;
    public final static int   kXR_NotFound       = 3011;
    public final static int   kXR_ServerError    = 3012;
    public final static int   kXR_Unsupported    = 3013;
    public final static int   kXR_noserver       = 3014;
    public final static int   kXR_NotFile        = 3015;
    public final static int   kXR_isDirectory    = 3016;
    public final static int   kXR_Cancelled      = 3017;
    public final static int   kXR_ChkLenErr      = 3018;
    public final static int   kXR_ChkSumErr      = 3019;
    public final static int   kXR_inProgress     = 3020;
    public final static int   kXR_noErrorYet     = 10000;

    // client's request types
    public final static int   kXR_handshake = 0;
    public final static int   kXR_auth      = 3000;
    public final static int   kXR_query     = 3001;
    public final static int   kXR_chmod     = 3002;
    public final static int   kXR_close     = 3003;
    public final static int   kXR_dirlist   = 3004;
    public final static int   kXR_getfile   = 3005;
    public final static int   kXR_protocol  = 3006;
    public final static int   kXR_login     = 3007;
    public final static int   kXR_mkdir     = 3008;
    public final static int   kXR_mv        = 3009;
    public final static int   kXR_open      = 3010;
    public final static int   kXR_ping      = 3011;
    public final static int   kXR_putfile   = 3012;
    public final static int   kXR_read      = 3013;
    public final static int   kXR_rm        = 3014;
    public final static int   kXR_rmdir     = 3015;
    public final static int   kXR_sync      = 3016;
    public final static int   kXR_stat      = 3017;
    public final static int   kXR_set       = 3018;
    public final static int   kXR_write     = 3019;
    public final static int   kXR_admin     = 3020;
    public final static int   kXR_prepare   = 3021;
    public final static int   kXR_statx     = 3022;
    public final static int   kXR_endsess   = 3023;
    public final static int   kXR_bind      = 3024;
    public final static int   kXR_readv     = 3025;
    public final static int   kXR_verifyw   = 3026;
    public final static int   kXR_locate    = 3027;
    public final static int   kXR_truncate  = 3028;

    // open mode for remote files
    public final static short kXR_ur = 0x100;
    public final static short kXR_uw = 0x080;
    public final static short kXR_ux = 0x040;
    public final static short kXR_gr = 0x020;
    public final static short kXR_gw = 0x010;
    public final static short kXR_gx = 0x008;
    public final static short kXR_or = 0x004;
    public final static short kXR_ow = 0x002;
    public final static short kXR_ox = 0x001;

    // open request options
    public final static short kXR_compress  = 1;
    public final static short kXR_delete    = 2;
    public final static short kXR_force     = 4;
    public final static short kXR_new       = 8;
    public final static short kXR_open_read = 16;
    public final static short kXR_open_updt = 32;
    public final static short kXR_async     = 64;
    public final static short kXR_refresh       = 128;
    public final static short kXR_mkpath        = 256;
    public final static short kXR_open_apnd     = 512;
    public final static short kXR_retstat       = 1024;
    public final static short kXR_replica       = 2048;
    public final static short kXR_posc          = 4096;
    public final static short kXR_nowait        = 8192;
    public final static short kXR_seqio         = 16384;

    // stat response flags
    public final static int kXR_file    =  0;
    public final static int kXR_xset    =  1;
    public final static int kXR_isDir   =  2;
    public final static int kXR_other   =  4;
    public final static int kXR_offline =  8;
    public final static int kXR_readable= 16;
    public final static int kXR_writable= 32;
    public final static int kXR_opscpend= 64;


    // attn response codes
    public final static int kXR_asyncab         = 5000;
    public final static int kXR_asyncdi         = 5001;
    public final static int kXR_asyncms         = 5002;
    public final static int kXR_asyncrd         = 5003;
    public final static int kXR_asyncwt         = 5004;
    public final static int kXR_asyncav         = 5005;
    public final static int kXR_asynunav        = 5006;
    public final static int kXR_asyncgo         = 5007;
    public final static int kXR_asynresp        = 5008;

    // prepare request options
    public final static int kXR_cancel = 1;
    public final static int kXR_notify = 2;
    public final static int kXR_noerrs = 4;
    public final static int kXR_stage  = 8;
    public final static int kXR_wmode  = 16;
    public final static int kXR_coloc  = 32;
    public final static int kXR_fresh  = 64;

    // verification options
    public final static int kXR_nocrc = 0;
    public final static int kXR_crc32 = 1;

    // query types
    public final static int kXR_QStats = 1;
    public final static int kXR_QPrep  = 2;
    public final static int kXR_Qcksum = 3;
    public final static int kXR_Qxattr = 4;
    public final static int kXR_Qspace = 5;
    public final static int kXR_Qckscan= 6;
    public final static int kXR_Qconfig= 7;
    public final static int kXR_Qvisa  = 8;
    public final static int kXR_Qopaque=16;
    public final static int kXR_Qopaquf=32;

    // dirlist options
    public final static int kXR_online = 1;

    // mkdir options
    public final static int kXR_mknone    = 0;
    public final static int kXR_mkdirpath = 1;

    // login cap version
    public final static int kXR_lcvnone   = 0;
    public final static int kXR_vermask   = 63;
    public final static int kXR_asyncap   = 128;

    // login version
    public final static int kXR_ver000 = 0; // old clients predating history
    public final static int kXR_ver001 = 1; // generally implemented 2005 prot.
    public final static int kXR_ver002 = 2; // recognizes asyncresp

    // stat options
    public final static int kXR_vfs = 1;

    // logon types
    public final static byte kXR_useruser = 0;
    public final static byte kXR_useradmin = 1;

    public final static int DEFAULT_PORT = 1094;

    /* All possible access permissions when using xrootd authZ
     * these are the possbile permission level, one file can have only one type
     * (no combinations) the granted rights increase in the order of appereance
     * (e.g. delete includes write, which includes read and write-once)
     */
    public static enum FilePerm {
        READ ("read"),
        WRITE_ONCE ("write-once"),
        WRITE ("write"),
        DELETE ("delete");

        private final String _xmlText;

        FilePerm(String xmlText) {
            _xmlText = xmlText;
        }

        public String xmlText() { return _xmlText; }
    };
}

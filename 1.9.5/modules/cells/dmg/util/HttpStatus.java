package dmg.util;

/**
 * 
 * HTTP status codes according rfc2616
 *
 */
public interface HttpStatus {
 
    public static final int CONTINUE = 100;
    public static final int SWITCHING = 101;

    public static final int OK                              = 200;
    public static final int CREATED                         = 201;
    public static final int ACCEPTED                        = 202;
    public static final int NON_AUTHORITATIVE_INFORMATION   = 203;
    public static final int NO_CONTENT                      = 204;
    public static final int RESET_CONTENT                   = 205;
    public static final int PARTIAL_CONTENT                 = 206;

    public static final int MULTIPLE_CHOICE                 = 300;
    public static final int MOVED_PERMANENTLY               = 301;
    public static final int FOUND                           = 302;
    public static final int SEE_OTHER                       = 303;
    public static final int NOT_MODIFIED                    = 304;
    public static final int USE_PROXY                       = 305;
    public static final int TEMPORARY_REDIRECT              = 307;

    public static final int BAD_REQUEST                     = 400;
    public static final int UNAUTHORIZED                    = 401;
    public static final int PAYMENT_REQUIRED                = 402;
    public static final int FORBIDDEN                       = 403;
    public static final int NOT_FOUND                       = 404;
    public static final int NOT_ALLOWED                     = 405;
    public static final int NOT_ACCEPTABLE                  = 406;
    public static final int PROXY_AUTH_REQUIRED             = 407;
    public static final int REQUEST_TIMEOUT                 = 408;
    public static final int CONFLICT                        = 409;
    public static final int GONE                            = 410;
    public static final int LENGTH_REQUIRED                 = 411;
    public static final int PRECONDITION_FAILED             = 412;
    public static final int REQUEST_ENTITY_TOO_LARGE        = 413;
    public static final int REQUEST_URI_TOO_LONG            = 414;
    public static final int UNSUPPORTED_MEDIA_TYPE          = 415;
    public static final int REQUESTED_RANGE_NOT_SATISFIABLE = 416;
    public static final int EXPECTATION_FAILED              = 417;

    public static final int INTERNAL_SERVER_ERROR           = 500;
    public static final int NOT_IMPLEMENTED                 = 501;
    public static final int BAD_GATEWAY                     = 502;
    public static final int SERVICE_UNAVAILABLE             = 503;
    public static final int GATEWAY_TIMEOUT                 = 504;
    public static final int HTTP_VERSION_NOT_SUPPORTED      = 505;

    
}

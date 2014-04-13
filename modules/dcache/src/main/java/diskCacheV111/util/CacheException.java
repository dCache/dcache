package diskCacheV111.util;

/**
 * Base class for dCache exceptions.
 *
 * @Immutable
 */
public class CacheException extends Exception
{
    private static final long serialVersionUID = 3219663683702355240L;

    /** Requested operation is disabled in pool. */
    public final static int POOL_DISABLED = 104;

    /** Disk I/O error. */
    public final static int ERROR_IO_DISK = 204;

    /** Pool already contains a replica. */
    public final static int FILE_IN_CACHE = 210;

    /** File is broken on a tape, can't be staged */
    public final static int BROKEN_ON_TAPE = 243;

    /** Usually followed by component shutdown. */
    public final static int PANIC = 10000;

    /** file not know/register in pnfs. Non existing path or pnfsid */
    public final static int FILE_NOT_FOUND = 10001;
    /** file is precious -  not flush to HSM yet */
    public final static int FILE_PRECIOUS = 10002;

    public final static int FILESIZE_UNKNOWN = 10003;

    /**
     * File size or checksum in name space does not match actual size or checksum of
     * the data file.
     */
    public final static int FILE_CORRUPTED = 10004;

    public final static int FILE_NOT_STORED = 10005;

    /** indicated a timeout in cell communication */
    public final static int TIMEOUT = 10006;

    /**
     * returned by pool in case of a request for IO on a file, which is not in
     * the pool repository
     */
    public final static int FILE_NOT_IN_REPOSITORY = 10007;

    /** returned by PnfsManager on create of existing file or directory */
    public final static int FILE_EXISTS = 10008;

    /**
     * returned in case of directory specific request ( like list ) on existing
     * not a directory path or pnfsid,
     */
    public final static int NOT_DIR = 10010;
    /** all kind of unexpected exceptions */
    public final static int UNEXPECTED_SYSTEM_EXCEPTION = 10011;

    public final static int ATTRIBUTE_FORMAT_ERROR = 10012;

    /** indicates, that HSM request to suspend current restore request */
    public final static int HSM_DELAY_ERROR = 10013;


    /**
     * returned in case of file specific request ( like read ) on existing not a
     * file path or pnfsid,
     */
    public final static int NOT_FILE = 10014;

    /** indicates that request contains invalid value */
    public final static int INVALID_ARGS = 10015;

    /** Pnfsid is not in trash directory */
    public final static int NOT_IN_TRASH = 10016;

    /**
     * There are no sufficient resources to process current request. Typically
     * returned if some limits excided.
     */
    public final static int RESOURCE = 10017;

    /** The user is not authorized to perform the action requested. */
    public final static int PERMISSION_DENIED = 10018;

    /** file not online */
    public final static int FILE_NOT_ONLINE = 10019;

    /** Target object is locked or busy and the operation was denied. */
    public final static int LOCKED = 10020;

    /**
     * The request information is suspected to be out of date. The client should
     * retry with updated request information if available.
     */
    public final static int OUT_OF_DATE = 10021;

    /**
     * An operation failed because the file is still "new", ie. has not finished
     * uploading and registered the file in the name space.
     */
    public final static int FILE_IS_NEW = 10022;

    /**
     * An operation failed because there was no service to handle the
     * corresponding message request.
     */
    public final static int SERVICE_UNAVAILABLE = 10023;


    public static final int NO_POOL_CONFIGURED = 10024;
    public static final int NO_POOL_ONLINE = 10025;

    /** Selected pool failed for third-party copy. */
    public final static int SELECTED_POOL_FAILED = 10026;

    /** Transfer between pool and remote site failed. */
    public final static int THIRD_PARTY_TRANSFER_FAILED = 10027;


    /**
     * default error code. <b>It's recommended to use more specific error
     * codes</b>
     */
    public final static int DEFAULT_ERROR_CODE = 666; // I don't like this number....

    private final int _rc;

    private static String formatMessage(String message)
    {
        if (message == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < message.length(); i++) {
            char c = message.charAt(i);
            if (c == '\n') {
                if (i != (message.length() - 1)) {
                    sb.append(';');
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Create a new CacheException with default error code and given error
     * message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     */
    public CacheException(String message)
    {
        this(message, null);
    }

    /**
     * Create a new CacheException with default error code, and the given error
     * message and cause.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public CacheException(String message, Throwable cause)
    {
        this(DEFAULT_ERROR_CODE, message, cause);
    }

    /**
     * Create a new CacheException with given error code and message.
     *
     * @param rc  error code
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     */
    public CacheException(int rc, String message)
    {
        this(rc, message, null);
    }

    /**
     * Create a new CacheException with given error code, message and cause.
     *
     * @param rc  error code
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public CacheException(int rc, String message, Throwable cause)
    {
        super(formatMessage(message), cause);
        _rc = rc;
    }

    public int getRc()
    {
        return _rc;
    }

    @Override
    public String toString()
    {
        return "CacheException(rc=" + _rc + ";msg=" + getMessage() + ")";
    }
}


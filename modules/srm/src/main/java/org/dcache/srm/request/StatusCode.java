package org.dcache.srm.request;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import org.dcache.srm.v2_2.TStatusCode;

public enum StatusCode {
    SUCCESS (TStatusCode.SRM_SUCCESS),
    SRM_FAILURE (TStatusCode.SRM_FAILURE),
    SRM_AUTHENTICATION_FAILURE (TStatusCode.SRM_AUTHENTICATION_FAILURE),
    SRM_AUTHORIZATION_FAILURE (TStatusCode.SRM_AUTHORIZATION_FAILURE),
    SRM_INVALID_REQUEST (TStatusCode.SRM_INVALID_REQUEST),
    SRM_INVALID_PATH (TStatusCode.SRM_INVALID_PATH),
    SRM_FILE_LIFETIME_EXPIRED (TStatusCode.SRM_FILE_LIFETIME_EXPIRED),
    SRM_SPACE_LIFETIME_EXPIRED (TStatusCode.SRM_SPACE_LIFETIME_EXPIRED),
    SRM_EXCEED_ALLOCATION (TStatusCode.SRM_EXCEED_ALLOCATION),
    SRM_NO_USER_SPACE (TStatusCode.SRM_NO_USER_SPACE),
    SRM_NO_FREE_SPACE (TStatusCode.SRM_NO_FREE_SPACE),
    SRM_DUPLICATION_ERROR (TStatusCode.SRM_DUPLICATION_ERROR),
    SRM_NON_EMPTY_DIRECTORY (TStatusCode.SRM_NON_EMPTY_DIRECTORY),
    SRM_TOO_MANY_RESULTS (TStatusCode.SRM_TOO_MANY_RESULTS),
    SRM_INTERNAL_ERROR (TStatusCode.SRM_INTERNAL_ERROR),
    SRM_FATAL_INTERNAL_ERROR (TStatusCode.SRM_FATAL_INTERNAL_ERROR),
    SRM_NOT_SUPPORTED (TStatusCode.SRM_NOT_SUPPORTED),
    SRM_REQUEST_QUEUED (TStatusCode.SRM_REQUEST_QUEUED),
    SRM_REQUEST_INPROGRESS (TStatusCode.SRM_REQUEST_INPROGRESS),
    SRM_REQUEST_SUSPENDED (TStatusCode.SRM_REQUEST_SUSPENDED),
    SRM_ABORTED (TStatusCode.SRM_ABORTED),
    SRM_RELEASED (TStatusCode.SRM_RELEASED),
    SRM_FILE_PINNED (TStatusCode.SRM_FILE_PINNED),
    SRM_FILE_IN_CACHE (TStatusCode.SRM_FILE_IN_CACHE),
    SRM_SPACE_AVAILABLE (TStatusCode.SRM_SPACE_AVAILABLE),
    SRM_LOWER_SPACE_GRANTED (TStatusCode.SRM_LOWER_SPACE_GRANTED),
    SRM_DONE (TStatusCode.SRM_DONE),
    SRM_PARTIAL_SUCCESS (TStatusCode.SRM_PARTIAL_SUCCESS),
    SRM_REQUEST_TIMED_OUT (TStatusCode.SRM_REQUEST_TIMED_OUT),
    SRM_LAST_COPY (TStatusCode.SRM_LAST_COPY),
    SRM_FILE_BUSY (TStatusCode.SRM_FILE_BUSY),
    SRM_FILE_LOST (TStatusCode.SRM_FILE_LOST),
    SRM_FILE_UNAVAILABLE (TStatusCode.SRM_FILE_UNAVAILABLE),
    SRM_CUSTOM_STATUS (TStatusCode.SRM_CUSTOM_STATUS);

    private final TStatusCode _status;
    private static final ImmutableMap<TStatusCode,StatusCode> MAP;
    private static final String ERROR_MESSAGE;

    private StatusCode(TStatusCode status) {
        _status = status;
    }

    static {
        StringBuilder sb = new StringBuilder();
        sb.append("Unknown StatusCode: \"%s\".");
        sb.append(" Supported values:");

        Builder<TStatusCode,StatusCode> builder = new Builder<>();
        for (StatusCode value : values()) {
                builder.put(value._status,value);
                sb.append(" \"").append(value._status).append("\"");
        }
        MAP = builder.build();
        ERROR_MESSAGE = sb.toString();
    }


    public TStatusCode toTStatusCode() {
        return _status;
    }

    public static StatusCode fromTStatusCode(TStatusCode status) {
        if ( status == null ) {
            return null;
        }
        else {
            return MAP.get(status);
        }
    }

    /**
     * this function provides wrapper of TStatusCode.fromString
     *  so that user gets better error handling
     */
    public static StatusCode fromString(String txt)
            throws IllegalArgumentException  {
            try {
                TStatusCode type = TStatusCode.fromString(txt);
                return fromTStatusCode(type);
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(ERROR_MESSAGE,
                                                                 txt));
            }
        }
}

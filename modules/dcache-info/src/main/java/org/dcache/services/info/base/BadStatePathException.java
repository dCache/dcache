package org.dcache.services.info.base;

/**
 * A generic exception, indicating that there was some problem with the
 * StatePath.
 */
public class BadStatePathException extends Exception {

    private static final long serialVersionUID = 1;

    static final String DEFAULT_MESSAGE = "Unknown error with a StatePath";

    private String _msg;

    public BadStatePathException() {
        _msg = DEFAULT_MESSAGE;
    }

    public BadStatePathException(String msg) {
        _msg = msg;
    }

    @Override
    public String toString() {
        return _msg;
    }
}

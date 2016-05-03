package org.dcache.gplazma.plugins.exceptions;

public class GplazmaParseMapFileException extends Exception {

    public GplazmaParseMapFileException() {
        super();
    }

    public GplazmaParseMapFileException(String message) {
        super(message);
    }

    public GplazmaParseMapFileException(Throwable cause) {
        super(cause);
    }

    public GplazmaParseMapFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
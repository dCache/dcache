package org.dcache.gplazma.plugins.exceptions;

import org.dcache.util.Exceptions;

public class GplazmaParseMapFileException extends Exception {

    public static void checkFormat(boolean isOK, String template, Object...args)
            throws GplazmaParseMapFileException
    {
        Exceptions.genericCheck(isOK, GplazmaParseMapFileException::new, template, args);
    }

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
package org.dcache.gplazma.configuration.parser;

import org.dcache.gplazma.GPlazmaInternalException;

/**
 * This Exception indicates there was a problem reading the structure of the
 * gPlazma configuration file.
 */
public class ParseException extends GPlazmaInternalException {

    private static final long serialVersionUID = 8146460786081822785L;

    public ParseException(String message) {
        super(message);
    }
}

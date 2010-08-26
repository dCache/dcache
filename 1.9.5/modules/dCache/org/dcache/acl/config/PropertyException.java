package org.dcache.acl.config;

/**
 * An exception occurred if a property is invalid.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class PropertyException extends Exception {

    private static final long serialVersionUID = 31031976L;

    protected String _property;

    protected String _location;

    public PropertyException(String message) {
        super(message);
    }

    public PropertyException(String message, Throwable cause) {
        super(message, cause);
    }

    public PropertyException(String message, String property, String location) {
        super(message);
        _property = property;
        _location = location;
    }
}

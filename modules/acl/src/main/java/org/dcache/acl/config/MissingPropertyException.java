package org.dcache.acl.config;

/**
 * An exception occurred if a property is missing.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class MissingPropertyException extends PropertyException {

    private static final long serialVersionUID = 31031976L;

    public MissingPropertyException(String property, String location) {
        super("Missing property '" + property + "' in: " + location);
        _property = property;
        _location = location;
    }
}

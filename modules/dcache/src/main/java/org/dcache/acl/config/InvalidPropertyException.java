package org.dcache.acl.config;

/**
 * An exception occurred if a property is illegal.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class InvalidPropertyException extends PropertyException {

    private static final long serialVersionUID = 31031976L;

    private String _value;

    public InvalidPropertyException(String property, String value, String location) {
        super("Invalid value '" + value + "' of property '" + property + "' in: " + location);
        _property = property;
        _value = value;
        _location = location;
    }

    public String getValue() {
        return _value;
    }
}

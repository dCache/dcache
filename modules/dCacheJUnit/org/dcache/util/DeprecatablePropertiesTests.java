package org.dcache.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.StringWriter;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Before;
import org.junit.Test;

public class DeprecatablePropertiesTests {

    private static final String NORMAL_PROPERTY_NAME = "normalProperty";
    private static final String NORMAL_PROPERTY_VALUE = "some normal value";

    private static final String OBSOLETE_PROPERTY_NAME = "obsoleteProperty";
    private static final String OBSOLETE_PROPERTY_KEY =
            "(obsolete)" + OBSOLETE_PROPERTY_NAME;

    private static final String FORBIDDEN_PROPERTY_NAME = "forbiddenProperty";
    private static final String FORBIDDEN_PROPERTY_KEY =
            "(forbidden)" + FORBIDDEN_PROPERTY_NAME;

    private static final String FORBIDDEN_PROPERTY_W_ERROR_NAME =
            "forbiddenPropertyWithError";
    private static final String FORBIDDEN_PROPERTY_W_ERROR_VALUE =
            "An error message";
    private static final String FORBIDDEN_PROPERTY_W_ERROR_KEY =
            "(forbidden)" + FORBIDDEN_PROPERTY_W_ERROR_NAME;

    private static final String DEPRECATED_PROPERTY_NAME = "deprecatedProperty";
    private static final String DEPRECATED_PROPERTY_VALUE =
            "some deprecated value";
    private static final String DEPRECATED_PROPERTY_KEY =
            "(deprecated)" + DEPRECATED_PROPERTY_NAME;

    private DeprecatableProperties _properties;
    private StringBuffer _log;

    @Before
    public void setUp() {
        resetProperties();
        resetLogCapture();
    }

    private void resetProperties() {
        _properties = new DeprecatableProperties( new Properties());
        _properties.put( NORMAL_PROPERTY_NAME, NORMAL_PROPERTY_VALUE);
        _properties.put( OBSOLETE_PROPERTY_KEY, "");
        _properties.put( FORBIDDEN_PROPERTY_KEY, "");
        _properties.put( FORBIDDEN_PROPERTY_W_ERROR_KEY,
                FORBIDDEN_PROPERTY_W_ERROR_VALUE);
        _properties.put( DEPRECATED_PROPERTY_KEY, DEPRECATED_PROPERTY_VALUE);
    }

    private void resetLogCapture() {
        StringWriter writer = new StringWriter();
        _log = writer.getBuffer();
        LogManager.resetConfiguration();
        BasicConfigurator.configure( new WriterAppender( new SimpleLayout(),
                                                         writer));
    }

    @Test
    public void testNormalPropertyGet() {
        assertEquals( "testing normal property", NORMAL_PROPERTY_VALUE,
                _properties.get( NORMAL_PROPERTY_NAME));
    }

    @Test
    public void testDeprecatedPropertyGet() {
        assertEquals( "testing deprecated property", DEPRECATED_PROPERTY_VALUE,
                _properties.get( DEPRECATED_PROPERTY_NAME));
    }

    @Test
    public void testObsoletePropertyGet() {
        assertFalse( "testing obsolete property missing",
                _properties.containsKey( OBSOLETE_PROPERTY_NAME));
    }

    @Test
    public void testForbiddenWithErrorPropertyContainsKey() {
        assertFalse( "testing obsolete property missing",
                _properties.containsKey( FORBIDDEN_PROPERTY_W_ERROR_NAME));
    }

    @Test
    public void testForbiddenPropertyContainsKey() {
        assertFalse( "testing forbidden property missing",
                _properties.containsKey( FORBIDDEN_PROPERTY_NAME));
    }

    @Test
    public void testNormalPropertyPut() {
        _properties.put( NORMAL_PROPERTY_NAME, "some value");
        assertEquals( "", _log.toString());
    }

    @Test
    public void testDeprecatedPropertyPut() {
        _properties.put( DEPRECATED_PROPERTY_NAME, "some value");
        assertEquals( "WARN - The property " + DEPRECATED_PROPERTY_NAME +
                      " is deprecated and will be removed.\n", _log.toString());
    }

    @Test
    public void testObsoletePropertyPut() {
        _properties.put( OBSOLETE_PROPERTY_NAME, "some value");
        assertEquals( "WARN - The property " + OBSOLETE_PROPERTY_NAME +
                      " is no longer used.\n", _log.toString());
    }

    @Test
    public void testForbiddenPropertyPut() {
        try {
            _properties.put( FORBIDDEN_PROPERTY_NAME, "some value");
            fail( "no exception thrown");
        } catch (IllegalArgumentException e) {
            assertEquals( "Adjusting property " + FORBIDDEN_PROPERTY_NAME +
                          " is forbidden as different properties now control this aspect of dCache.",
                          e.getMessage());
        }
    }

    @Test
    public void testForbiddenWithErrorPropertyPut() {
        try {
            _properties.put( FORBIDDEN_PROPERTY_W_ERROR_NAME, "some value");
            fail( "no exception thrown");
        } catch (IllegalArgumentException e) {
            assertEquals( FORBIDDEN_PROPERTY_W_ERROR_VALUE, e.getMessage());
        }
    }
}

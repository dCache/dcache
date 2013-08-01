package org.dcache.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;

public class ConfigurationPropertiesTests {

    private static final String NORMAL_PROPERTY_NAME = "normalProperty";
    private static final String NORMAL_PROPERTY_VALUE = "some normal value";

    private static final String SIMPLE_PROPERTY_NAME = "simple-key";
    private static final String SIMPLE_PROPERTY_VALUE = "simple-value";

    private static final String OBSOLETE_PROPERTY_NAME = "obsoleteProperty";
    private static final String OBSOLETE_PROPERTY_KEY =
            "(obsolete)" + OBSOLETE_PROPERTY_NAME;

    private static final String OBSOLETE_PROPERTY_W_ERROR_NAME =
        "obsoletePropertyWithError";
    private static final String OBSOLETE_PROPERTY_W_ERROR_VALUE =
        "an error message";
    private static final String OBSOLETE_PROPERTY_W_ERROR_KEY =
            "(obsolete)" + OBSOLETE_PROPERTY_W_ERROR_NAME;

    private static final String FORBIDDEN_PROPERTY_NAME = "forbiddenProperty";
    private static final String FORBIDDEN_PROPERTY_KEY =
            "(forbidden)" + FORBIDDEN_PROPERTY_NAME;

    private static final String FORBIDDEN_PROPERTY_W_ERROR_NAME =
            "forbiddenPropertyWithError";
    private static final String FORBIDDEN_PROPERTY_W_ERROR_VALUE =
            "an error message";
    private static final String FORBIDDEN_PROPERTY_W_ERROR_KEY =
            "(forbidden)" + FORBIDDEN_PROPERTY_W_ERROR_NAME;

    private static final String DEPRECATED_PROPERTY_NAME = "deprecatedProperty";
    private static final String DEPRECATED_PROPERTY_VALUE =
            "some deprecated value";
    private static final String DEPRECATED_PROPERTY_KEY =
            "(deprecated)" + DEPRECATED_PROPERTY_NAME;

    private static final String IMMUTABLE_PROPERTY_NAME = "immutableProperty";
    private static final String IMMUTABLE_PROPERTY_VALUE =
            "some immutable value";
    private static final String IMMUTABLE_PROPERTY_KEY = "(immutable)"
            + IMMUTABLE_PROPERTY_NAME;

    private static final String DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_NAME = "deprecatedProperty.forward";
    private static final String DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_VALUE =
            "${" + SIMPLE_PROPERTY_NAME + "}";
    private static final String DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_KEY =
            "(deprecated)" + DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_NAME;

    private static final String DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME = "deprecatedProperty.backward";
    private static final String DEPRECATED_PROPERTY_W_BACK_SYNONYM_VALUE = "some value";
    private static final String DEPRECATED_PROPERTY_W_BACK_SYNONYM_KEY =
            "(deprecated)" + DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME;

    private static final String ONE_OF_PROPERTY_NAME = "one-ofProperty";
    private static final String ONE_OF_PROPERTY_VALUE = "value-A";
    private static final String ONE_OF_PROPERTY_KEY =
            "(one-of?value-A|value-B)" + ONE_OF_PROPERTY_NAME;

    private static final String SIMPLE_SYNONYM_OF_DEPRECATED_NAME = "synonym.of.deprecated";
    private static final String SIMPLE_SYNONYM_OF_DEPRECATED_VALUE = "${" + DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME + "}";
    private static final String SIMPLE_SYNONYM_OF_DEPRECATED_KEY = SIMPLE_SYNONYM_OF_DEPRECATED_NAME;


    private static final String PROPERTY_WITH_SUFFIX_NAME = SIMPLE_PROPERTY_NAME + "-foo";
    private static final String PROPERTY_WITH_SUFFIX_VALUE = "value-foo";

    private static final String PROPERTY_WITH_PREFIX_NAME = "foo-" + SIMPLE_PROPERTY_NAME;
    private static final String PROPERTY_WITH_PREFIX_VALUE = "foo-value";

    private static final String EXPANDING_PROPERTY_NAME = "expanding-key";

    private ConfigurationProperties _properties;
    private ConfigurationProperties _initiallyEmptyProperties;
    private ConfigurationProperties _standardProperties;
    private List<ILoggingEvent> _log;

    @Before
    public void setUp() {
        resetProperties();
        resetLogCapture();
    }

    private void resetProperties() {
        _properties = new ConfigurationProperties();
        _properties.put( NORMAL_PROPERTY_NAME, NORMAL_PROPERTY_VALUE);
        _properties.put( ONE_OF_PROPERTY_KEY, ONE_OF_PROPERTY_VALUE);
        _properties.put( IMMUTABLE_PROPERTY_KEY, IMMUTABLE_PROPERTY_VALUE);
        _properties.put( OBSOLETE_PROPERTY_KEY, "");
        _properties.put( OBSOLETE_PROPERTY_W_ERROR_KEY,
                OBSOLETE_PROPERTY_W_ERROR_VALUE);
        _properties.put( FORBIDDEN_PROPERTY_KEY, "");
        _properties.put( FORBIDDEN_PROPERTY_W_ERROR_KEY,
                FORBIDDEN_PROPERTY_W_ERROR_VALUE);
        _properties.put( DEPRECATED_PROPERTY_KEY, DEPRECATED_PROPERTY_VALUE);
        _properties.put( DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_KEY,
                DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_VALUE);
        _properties.put(DEPRECATED_PROPERTY_W_BACK_SYNONYM_KEY,
                DEPRECATED_PROPERTY_W_BACK_SYNONYM_VALUE);
        _properties.put(SIMPLE_SYNONYM_OF_DEPRECATED_KEY,
                SIMPLE_SYNONYM_OF_DEPRECATED_VALUE);


        _initiallyEmptyProperties = new ConfigurationProperties();

        _standardProperties = new ConfigurationProperties();
        _standardProperties.setProperty( SIMPLE_PROPERTY_NAME, SIMPLE_PROPERTY_VALUE);
        _standardProperties.setProperty( PROPERTY_WITH_SUFFIX_NAME, PROPERTY_WITH_SUFFIX_VALUE);
        _standardProperties.setProperty( PROPERTY_WITH_PREFIX_NAME, PROPERTY_WITH_PREFIX_VALUE);
    }

    private void resetLogCapture() {
        LoggerContext loggerContext =
            (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.reset();

        ListAppender<ILoggingEvent> appender =
            new ListAppender<>();
        appender.setContext(loggerContext);
        appender.setName("appender");
        appender.start();
        _log = appender.list;

        Logger logger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
    }

    @Test
    public void testNormalPropertyGet() {
        assertEquals( "testing normal property", NORMAL_PROPERTY_VALUE,
                _properties.get( NORMAL_PROPERTY_NAME));
    }

    @Test
    public void testImmutablePropertyGet() {
        assertEquals( "testing immutable property", IMMUTABLE_PROPERTY_VALUE,
                _properties.get(IMMUTABLE_PROPERTY_NAME));
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
    public void testObsoleteWithErrorPropertyContainsKey() {
        assertFalse( "testing obsolete property missing",
                _properties.containsKey( OBSOLETE_PROPERTY_W_ERROR_NAME));
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
        assertEquals(0, _log.size());
    }

    @Test
    public void testImmutablePropertyPut() {
        try {
            _properties.put(IMMUTABLE_PROPERTY_NAME, "some new value");
            fail( "no exception thrown");
        } catch (IllegalArgumentException e) {
            assertEquals( "Property " + IMMUTABLE_PROPERTY_NAME + ": " +
                          "may not be adjusted as it is marked 'immutable'",
                          e.getMessage());
        }
    }

    @Test
    public void testDeprecatedPropertyPut() {
        _properties.put( DEPRECATED_PROPERTY_NAME, "some value");
        assertEquals(1, _log.size());
        assertEquals(Level.WARN, _log.get(0).getLevel());
        assertEquals("Property " + DEPRECATED_PROPERTY_NAME +
                     ": please review configuration; support for " +
                     DEPRECATED_PROPERTY_NAME + " will be removed in the future",
                     _log.get(0).getFormattedMessage());
    }

    @Test
    public void testDeprecatedForwardSynonymPropertyPut() {
        _properties.put( DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_NAME, "some value");
        assertEquals(1, _log.size());
        assertEquals(Level.WARN, _log.get(0).getLevel());
        assertEquals("Property " + DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_NAME +
                     ": use \"" + SIMPLE_PROPERTY_NAME + "\" instead; support for " +
                     DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_NAME + " will be removed in the future",
                     _log.get(0).getFormattedMessage());
    }

    @Test
    public void testDeprecatedBackSynonymDefaultPropertyPut() {
        ConfigurationProperties properties = new ConfigurationProperties(_properties);
        properties.put( DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME, "some value");
        assertEquals(1, _log.size());
        assertEquals(Level.WARN, _log.get(0).getLevel());
        assertEquals("Property " + DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME +
                     ": use \"" + SIMPLE_SYNONYM_OF_DEPRECATED_NAME + "\" instead; support for " +
                     DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME + " will be removed in the future",
                     _log.get(0).getFormattedMessage());
    }

    @Test
    public void testDeprecatedBackSynonymPropertyPut() {
        _properties.put( DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME, "some value");
        assertEquals(1, _log.size());
        assertEquals(Level.WARN, _log.get(0).getLevel());
        assertEquals("Property " + DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME +
                     ": use \"" + SIMPLE_SYNONYM_OF_DEPRECATED_NAME + "\" instead; support for " +
                     DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME + " will be removed in the future",
                     _log.get(0).getFormattedMessage());
    }

    @Test
    public void testObsoletePropertyPut() {
        _properties.put( OBSOLETE_PROPERTY_NAME, "some value");
        assertEquals(1, _log.size());
        assertEquals(Level.WARN, _log.get(0).getLevel());
        assertEquals("Property " + OBSOLETE_PROPERTY_NAME + ": " +
                     "please remove this assignment; it has no effect", _log.get(0).getFormattedMessage());
    }

    @Test
    public void testObsoleteWithErrorPropertyPut() {
        _properties.put( OBSOLETE_PROPERTY_W_ERROR_NAME, "some value");
        assertEquals(1, _log.size());
        assertEquals(Level.WARN, _log.get(0).getLevel());
        assertEquals("Property " + OBSOLETE_PROPERTY_W_ERROR_NAME + ": " +
                     "please remove this assignment; " + OBSOLETE_PROPERTY_W_ERROR_VALUE, _log.get(0).getFormattedMessage());
    }

    @Test
    public void testForbiddenPropertyPut() {
        try {
            _properties.put( FORBIDDEN_PROPERTY_NAME, "some value");
            fail( "no exception thrown");
        } catch (IllegalArgumentException e) {
            assertEquals( "Property " + FORBIDDEN_PROPERTY_NAME + ": " +
                          "may not be adjusted; this property no longer affects dCache",
                          e.getMessage());
        }
    }

    @Test
    public void testForbiddenWithErrorPropertyPut() {
        try {
            _properties.put( FORBIDDEN_PROPERTY_W_ERROR_NAME, "some value");
            fail( "no exception thrown");
        } catch (IllegalArgumentException e) {
            assertEquals( "Property " + FORBIDDEN_PROPERTY_W_ERROR_NAME + ": " +
                          "may not be adjusted; " + FORBIDDEN_PROPERTY_W_ERROR_VALUE, e.getMessage());
        }
    }

    @Test
    public void testStringPropertyNames()
    {
        String[] expected =
            new String[] { NORMAL_PROPERTY_NAME, IMMUTABLE_PROPERTY_NAME,
                           DEPRECATED_PROPERTY_NAME,
                           DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_NAME,
                           DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME,
                           ONE_OF_PROPERTY_NAME,
                           SIMPLE_SYNONYM_OF_DEPRECATED_NAME};

        assertEquals(new HashSet<>(Arrays.asList(expected)),
                     _properties.stringPropertyNames());
    }

    @Test
    public void testPropertyNames()
    {
        String[] expected =
            new String[] { NORMAL_PROPERTY_NAME, IMMUTABLE_PROPERTY_NAME,
                           DEPRECATED_PROPERTY_NAME,
                           DEPRECATED_PROPERTY_W_FORWARD_SYNONYM_NAME,
                           DEPRECATED_PROPERTY_W_BACK_SYNONYM_NAME,
                           ONE_OF_PROPERTY_NAME,
                           SIMPLE_SYNONYM_OF_DEPRECATED_NAME};

        Collection<String> results = new HashSet<>();
        Enumeration<?> e = _properties.propertyNames();
        while( e.hasMoreElements()) {
            results.add( String.valueOf(e.nextElement()));
        }
        assertEquals(new HashSet<>(Arrays.asList(expected)),
                     results);
    }

    @Test
    public void testGetPropertySimple() {
        assertEquals( SIMPLE_PROPERTY_VALUE, _standardProperties.getProperty( SIMPLE_PROPERTY_NAME));
    }

    @Test
    public void testGetReplacementSimple() {
        assertEquals( SIMPLE_PROPERTY_VALUE, _standardProperties.getValue( SIMPLE_PROPERTY_NAME));
    }

    @Test
    public void testPropertyUpdatable() {
        String newValue = "new value";
        _standardProperties.setProperty( SIMPLE_PROPERTY_NAME, newValue);
        assertEquals( newValue, _standardProperties.getProperty( SIMPLE_PROPERTY_NAME));
    }

    @Test
    public void testGetPropertyExpanding() {
        String expandingValue = propertyReference( SIMPLE_PROPERTY_NAME);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expandingValue);
        assertEquals( expandingValue, _standardProperties.getProperty( EXPANDING_PROPERTY_NAME));
    }

    @Test
    public void testGetReplacementExpanding() {
        String expandingValue = propertyReference( SIMPLE_PROPERTY_NAME);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expandingValue);

        assertEquals( SIMPLE_PROPERTY_VALUE, _standardProperties.getValue( EXPANDING_PROPERTY_NAME));
    }

    @Test
    public void testGetReplacementExpandingWithPreamble() {
        String prefix = "FOO";

        String expandingValue = prefix + propertyReference( SIMPLE_PROPERTY_NAME);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expandingValue);

        assertEquals( prefix + SIMPLE_PROPERTY_VALUE, _standardProperties.getValue( EXPANDING_PROPERTY_NAME));
    }

    @Test
    public void testGetReplacementExpandingWithPostamble() {
        String postfix = "FOO";

        String expandingValue = propertyReference( SIMPLE_PROPERTY_NAME) + postfix;
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expandingValue);

        assertEquals( SIMPLE_PROPERTY_VALUE + postfix, _standardProperties.getValue( EXPANDING_PROPERTY_NAME));
    }

    @Test
    public void testGetReplacementExpandingWithSpace() {
        String valueWithSpace = "This is a test";
        _standardProperties.setProperty( SIMPLE_PROPERTY_NAME, valueWithSpace);

        String expandingValue = propertyReference( SIMPLE_PROPERTY_NAME);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expandingValue);

        assertEquals( valueWithSpace, _standardProperties.getValue( EXPANDING_PROPERTY_NAME));
    }

    @Test
    public void testTwoDeepExpansion() {
        String expanding1Value = propertyReference( SIMPLE_PROPERTY_NAME);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expanding1Value);

        String expanding2Name = EXPANDING_PROPERTY_NAME + "-2";
        String expanding2Value = propertyReference( EXPANDING_PROPERTY_NAME);
        _standardProperties.setProperty( expanding2Name, expanding2Value);

        assertEquals( SIMPLE_PROPERTY_VALUE, _standardProperties.getValue( expanding2Name));
    }

    @Test
    public void testTwoByTwoDeepExpansion() {
        String expanding1Value = propertyReference( SIMPLE_PROPERTY_NAME);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expanding1Value);

        String expanding2Name = EXPANDING_PROPERTY_NAME + "-2";
        String expanding2Value = propertyReference( EXPANDING_PROPERTY_NAME) +
                                 propertyReference( EXPANDING_PROPERTY_NAME);
        _standardProperties.setProperty( expanding2Name, expanding2Value);

        assertEquals( SIMPLE_PROPERTY_VALUE + SIMPLE_PROPERTY_VALUE,
                      _standardProperties.getValue( expanding2Name));
    }

    @Test
    public void testRecursiveExpansion() {
        String expanding2Name = EXPANDING_PROPERTY_NAME + "-2";

        String expanding1Value = propertyReference( expanding2Name);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expanding1Value);

        String expanding2Value = propertyReference( EXPANDING_PROPERTY_NAME);
        _standardProperties.setProperty( expanding2Name, expanding2Value);

        assertEquals( expanding2Value, _standardProperties.getValue( expanding2Name));
    }

    /*
     * Tests against load( Reader)
     */

    @Test
    public void testLoadEmptyContentsIsEmpty() throws IOException {
        Reader in = new StringReader("");

        _initiallyEmptyProperties.load( in);

        assertEquals( "check size is zero after loading empty data", 0, _initiallyEmptyProperties.size());
    }

    @Test
    public void testLoadSingleProperty() throws IOException {
        String assignment = SIMPLE_PROPERTY_NAME + "=" + SIMPLE_PROPERTY_VALUE + "\n";
        Reader in = new StringReader(assignment);

        _initiallyEmptyProperties.load( in);

        assertEquals( "check size is zero after loading empty data", 1, _initiallyEmptyProperties.size());
        assertEquals( SIMPLE_PROPERTY_VALUE, _initiallyEmptyProperties.getProperty( SIMPLE_PROPERTY_NAME));
    }

    @Test
    public void testLoadSinglePropertyWithSpace() throws IOException {
        String propertyValue = "This is a test";
        Reader in = new StringReader(SIMPLE_PROPERTY_NAME + "=" + propertyValue);

        _initiallyEmptyProperties.load( in);

        assertEquals( "check size is zero after loading empty data", 1, _initiallyEmptyProperties.size());
        assertEquals( propertyValue, _initiallyEmptyProperties.getProperty( SIMPLE_PROPERTY_NAME));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testLoadSinglePropertyTwiceFails() throws IOException {
        String propertyAssignment = SIMPLE_PROPERTY_NAME + "=" + "This is a test\n";
        Reader in = new StringReader(propertyAssignment + propertyAssignment);

        _initiallyEmptyProperties.load( in);
    }

    /*
     * Test against load( InputStream)
     */

    @Test
    public void testLoadInputStreamWithNoProperties() throws IOException {
        InputStream in = new ByteArrayInputStream( "".getBytes());

        _initiallyEmptyProperties.load( in);

        assertEquals( "checking number of entries", 0, _initiallyEmptyProperties.size());
    }

    @Test
    public void testLoadInputStreamWithSingleProperty() throws IOException {
        String assignment = SIMPLE_PROPERTY_NAME + "=" + SIMPLE_PROPERTY_VALUE + "\n";
        InputStream in = new ByteArrayInputStream( assignment.getBytes());

        _initiallyEmptyProperties.load( in);

        assertEquals( "check size is zero after loading empty data", 1, _initiallyEmptyProperties.size());
        assertEquals( SIMPLE_PROPERTY_VALUE, _initiallyEmptyProperties.getProperty( SIMPLE_PROPERTY_NAME));

    }

    @Test
    public void testLoadInputStreamWithSinglePropertyWithSpace() throws IOException {
        String propertyValue = "This is a test";
        String assignment = SIMPLE_PROPERTY_NAME + "=" + propertyValue + "\n";
        InputStream in = new ByteArrayInputStream( assignment.getBytes());

        _initiallyEmptyProperties.load( in);

        assertEquals( "check size is zero after loading empty data", 1, _initiallyEmptyProperties.size());
        assertEquals( propertyValue, _initiallyEmptyProperties.getProperty( SIMPLE_PROPERTY_NAME));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testLoadInputStreamWithSinglePropertyTwiceFails() throws IOException {
        String propertyAssignment = SIMPLE_PROPERTY_NAME + "=" + "This is a test\n";
        String twoAssignments = propertyAssignment + propertyAssignment;
        InputStream in = new ByteArrayInputStream( twoAssignments.getBytes());

        _initiallyEmptyProperties.load( in);
    }

    @Test
    public void testOneOfWithValidValueSucceeds() {
        _properties.put(ONE_OF_PROPERTY_NAME, "value-B");
    }

    @Test(expected=IllegalArgumentException.class)
    public void testOneOfWithInvalidValueFails() {
        _properties.put(ONE_OF_PROPERTY_NAME, "value-C");
    }

    /*
     * Support methods
     */

    private String propertyReference( String name) {
        return "${" + name + "}";
    }
}

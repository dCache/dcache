package org.dcache.util;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

public class ReplaceablePropertiesTests {

    private static final String SIMPLE_PROPERTY_NAME = "simple-key";
    private static final String SIMPLE_PROPERTY_VALUE = "simple-value";

    private static final String PROPERTY_WITH_SUFFIX_NAME = SIMPLE_PROPERTY_NAME + "-foo";
    private static final String PROPERTY_WITH_SUFFIX_VALUE = "value-foo";

    private static final String PROPERTY_WITH_PREFIX_NAME = "foo-" + SIMPLE_PROPERTY_NAME;
    private static final String PROPERTY_WITH_PREFIX_VALUE = "foo-value";

    private static final String EXPANDING_PROPERTY_NAME = "expanding-key";

    private static final Pattern MATCH_SIMPLE_EXACTLY = Pattern.compile( SIMPLE_PROPERTY_NAME);
    private static final Pattern MATCH_SIMPLE_WITH_ANY_PREFIX = Pattern.compile( ".*" + SIMPLE_PROPERTY_NAME);
    private static final Pattern MATCH_SIMPLE_WITH_ANY_SUFFIX = Pattern.compile( SIMPLE_PROPERTY_NAME + ".*");
    private static final Pattern MATCH_SIMPLE_WITH_ANY_PREFIX_OR_SUFFIX = Pattern.compile( ".*" + SIMPLE_PROPERTY_NAME + ".*");


    ReplaceableProperties _initiallyEmptyProperties;
    ReplaceableProperties _standardProperties;

    @Before
    public void setUp() {
        _initiallyEmptyProperties = new ReplaceableProperties( new Properties());

        _standardProperties = new ReplaceableProperties( new Properties());
        _standardProperties.setProperty( SIMPLE_PROPERTY_NAME, SIMPLE_PROPERTY_VALUE);
        _standardProperties.setProperty( PROPERTY_WITH_SUFFIX_NAME, PROPERTY_WITH_SUFFIX_VALUE);
        _standardProperties.setProperty( PROPERTY_WITH_PREFIX_NAME, PROPERTY_WITH_PREFIX_VALUE);
    }

    @Test
    public void testGetPropertySimple() {
        assertEquals( SIMPLE_PROPERTY_VALUE, _standardProperties.getProperty( SIMPLE_PROPERTY_NAME));
    }

    @Test
    public void testGetReplacementSimple() {
        assertEquals( SIMPLE_PROPERTY_VALUE, _standardProperties.getReplacement( SIMPLE_PROPERTY_NAME));
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

        assertEquals( SIMPLE_PROPERTY_VALUE, _standardProperties.getReplacement( EXPANDING_PROPERTY_NAME));
    }

    @Test
    public void testGetReplacementExpandingWithPreamble() {
        String prefix = "FOO";

        String expandingValue = prefix + propertyReference( SIMPLE_PROPERTY_NAME);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expandingValue);

        assertEquals( prefix + SIMPLE_PROPERTY_VALUE, _standardProperties.getReplacement( EXPANDING_PROPERTY_NAME));
    }

    @Test
    public void testGetReplacementExpandingWithPostamble() {
        String postfix = "FOO";

        String expandingValue = propertyReference( SIMPLE_PROPERTY_NAME) + postfix;
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expandingValue);

        assertEquals( SIMPLE_PROPERTY_VALUE + postfix, _standardProperties.getReplacement( EXPANDING_PROPERTY_NAME));
    }

    @Test
    public void testGetReplacementExpandingWithSpace() {
        String valueWithSpace = "This is a test";
        _standardProperties.setProperty( SIMPLE_PROPERTY_NAME, valueWithSpace);

        String expandingValue = propertyReference( SIMPLE_PROPERTY_NAME);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expandingValue);

        assertEquals( valueWithSpace, _standardProperties.getReplacement( EXPANDING_PROPERTY_NAME));
    }

    @Test
    public void testTwoDeepExpansion() {
        String expanding1Value = propertyReference( SIMPLE_PROPERTY_NAME);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expanding1Value);

        String expanding2Name = EXPANDING_PROPERTY_NAME + "-2";
        String expanding2Value = propertyReference( EXPANDING_PROPERTY_NAME);
        _standardProperties.setProperty( expanding2Name, expanding2Value);

        assertEquals( SIMPLE_PROPERTY_VALUE, _standardProperties.getReplacement( expanding2Name));
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
                      _standardProperties.getReplacement( expanding2Name));
    }

    @Test
    public void testRecursiveExpansion() {
        String expanding2Name = EXPANDING_PROPERTY_NAME + "-2";

        String expanding1Value = propertyReference( expanding2Name);
        _standardProperties.setProperty( EXPANDING_PROPERTY_NAME, expanding1Value);

        String expanding2Value = propertyReference( EXPANDING_PROPERTY_NAME);
        _standardProperties.setProperty( expanding2Name, expanding2Value);

        assertEquals( expanding2Value, _standardProperties.getReplacement( expanding2Name));
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


    /*
     * Test against matchingStringPropertyNames
     */

    @Test
    public void testSinglePatternExactMatch() {
        Collection<Pattern> selection = Collections.singletonList( MATCH_SIMPLE_EXACTLY);

        Set<String> results = _standardProperties.matchingStringPropertyNames( selection);

        Set<String> expected = new HashSet<String>(Arrays.asList( SIMPLE_PROPERTY_NAME));
        assertEquals( expected, results);
    }

    @Test
    public void testSinglePatternWithAnySuffix() {
        Collection<Pattern> selection = Collections.singletonList(MATCH_SIMPLE_WITH_ANY_SUFFIX);

        Set<String> results = _standardProperties.matchingStringPropertyNames(selection);

        Set<String> expected = new HashSet<String>(Arrays.asList(SIMPLE_PROPERTY_NAME,
                                                                 PROPERTY_WITH_SUFFIX_NAME));
        assertEquals( expected, results);
    }

    @Test
    public void testSinglePatternWithAnyPrefix() {
        Collection<Pattern> selection = Collections.singletonList(MATCH_SIMPLE_WITH_ANY_PREFIX);

        Set<String> results = _standardProperties.matchingStringPropertyNames( selection);

        Set<String> expected = new HashSet<String>(Arrays.asList( SIMPLE_PROPERTY_NAME,
                                                                  PROPERTY_WITH_PREFIX_NAME));
        assertEquals( expected, results);
    }

    @Test
    public void testSinglePatternWithAnyPrefixOrSuffix() {
        Collection<Pattern> selection = Collections.singletonList(MATCH_SIMPLE_WITH_ANY_PREFIX_OR_SUFFIX);

        Set<String> results = _standardProperties.matchingStringPropertyNames( selection);

        Set<String> expected = new HashSet<String>(Arrays.asList( SIMPLE_PROPERTY_NAME,
                                                                  PROPERTY_WITH_PREFIX_NAME,
                                                                  PROPERTY_WITH_SUFFIX_NAME));
        assertEquals( expected, results);
    }

    @Test
    public void testPatternsAnyPrefixAndAnySuffix() {
        Collection<Pattern> selection = Arrays.asList(MATCH_SIMPLE_WITH_ANY_PREFIX,
                                                      MATCH_SIMPLE_WITH_ANY_SUFFIX);

        Set<String> results = _standardProperties.matchingStringPropertyNames( selection);

        Set<String> expected = new HashSet<String>(Arrays.asList( SIMPLE_PROPERTY_NAME,
                                                                  PROPERTY_WITH_PREFIX_NAME,
                                                                  PROPERTY_WITH_SUFFIX_NAME));
        assertEquals( expected, results);
    }

    /*
     * Support methods
     */

    private String propertyReference( String name) {
        return "${" + name + "}";
    }
}

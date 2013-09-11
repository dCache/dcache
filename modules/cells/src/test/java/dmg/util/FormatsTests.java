package dmg.util;


import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class FormatsTests
{
    SimpleReplaceable _replacements;

    @Before
    public void setUp()
    {
        _replacements = new SimpleReplaceable(new Properties());
        _replacements.put("keyword", "replacement");
    }

    @Test
    public void testTrimWhiteSpacesInReplaceables(){
        _replacements.put("dcache.log.dir", "/var/log ");
        assertHasReplacement("${dcache.log.dir}/dcacheDomain.log", "/var/log/dcacheDomain.log");
    }

    @Test
    public void testReplaceKeywordsWithPlainWord()
    {
        assertHasReplacement("keyword", "keyword");
    }

    @Test
    public void testReplaceKeywordsWithInitialDollar()
    {
        assertHasReplacement("$keyword", "$keyword");
    }

    @Test
    public void testReplaceKeywordsWithInitialEscapedDollar()
    {
        assertHasReplacement("$$keyword", "$keyword");
    }

    @Test
    public void testReplaceKeywordsWithFinalDollar()
    {
        assertHasReplacement("keyword$", "keyword$");
    }

    @Test
    public void testReplaceKeywordsWithDollarBrace()
    {
        assertHasReplacement("${keyword", "${keyword");
    }

    @Test
    public void testReplaceKeywordsWithSimpleReference()
    {
        assertHasReplacement("${keyword}", "replacement");
    }

    @Test
    public void testReplaceKeywordWithDollar()
    {
        _replacements.put("$foo", "bar");
        assertHasReplacement("${$$foo}", "bar");
    }

    @Test
    public void testReplaceKeywordWithDollarBraceEscapedDollar()
    {
        _replacements.put("${foo", "bar");
        assertHasReplacement("${$${foo}", "bar");
    }

    @Test
    public void testEscapedDollarReplaceKeywordsWithSimpleReference()
    {
        assertHasReplacement("$$${keyword}", "$replacement");
    }

    @Test
    public void testDollarEscapedReplaceKeywordsWithSimpleReference()
    {
        assertHasReplacement("$$$${keyword}", "$${keyword}");
    }

    @Test
    public void testReplaceKeywordsWithReferenceFollowedBySomething()
    {
        assertHasReplacement("${keyword}-something", "replacement-something");
    }

    @Test
    public void testReplaceKeywordsWithSomethingFollowedByReference()
    {
        assertHasReplacement("something-${keyword}", "something-replacement");
    }

    @Test
    public void testReplaceKeywordsWithNonExistentReference()
    {
        assertHasReplacement("${does-not-exist}", "${does-not-exist}");
    }

    @Test
    public void testReplaceSimpleKeywordsWithinKeywordExists()
    {
        _replacements.put("other", "keyword");
        assertHasReplacement("${${other}}", "replacement");
    }

    @Test
    public void testReplaceSimpleKeywordsWithDollarWithinKeywordExistsNoEscape()
    {
        _replacements.put("$other", "keyword");
        assertHasReplacement("${${$other}}", "replacement");
    }

    @Test
    public void testReplaceSimpleKeywordsWithDollarWithinKeywordExistsWithSingleEscape()
    {
        _replacements.put("$other", "keyword");
        assertHasReplacement("${${$$other}}", "replacement");
    }

    @Test
    public void testReplaceSimpleKeywordsWithDollarWithinKeywordExistsWithDoubleEscape()
    {
        _replacements.put("$other", "keyword");
        assertHasReplacement("${${$$$$other}}", "${${$$other}}");
    }

    @Test
    public void testReplaceSimpleKeywordsWithDollarBraceWithinKeywordExistsWithDoubleEscape()
    {
        _replacements.put("${other", "keyword");
        assertHasReplacement("${${$${other}}", "replacement");
    }

    @Test
    public void testReplaceComplexKeywordsWithinKeywordExists()
    {
        _replacements.put("other", "wor");
        assertHasReplacement("${key${other}d}", "replacement");
    }

    @Test
    public void testEscapeDollarReplaceComplexKeywordsWithinKeywordExists()
    {
        _replacements.put("other", "wor");
        assertHasReplacement("${key$${other}d}", "${key${other}d}");
    }

    @Test
    public void testReplaceSimpleKeywordsWithinKeywordOuterDoesntExists()
    {
        _replacements.put("other", "foo");
        assertHasReplacement("${${other}}", "${foo}");
    }

    @Test
    public void testReplaceSimpleKeywordsWithinKeywordInnerDoesntExists()
    {
        assertHasReplacement("${${other}}", "${${other}}");
    }

    @Test
    public void testReplaceComplexKeywordsWithinKeywordPartial()
    {
        _replacements.put("other", "foo");
        assertHasReplacement("${${other}", "${foo");
    }

    private void assertHasReplacement(String in, String expected)
    {
        String actual = Formats.replaceKeywords(in, _replacements);
        assertEquals(expected, actual);
    }


    /**
     * A simple class to hold a set of replacements that is initially empty.
     * A replacement may be added using put.
     */
    public static class SimpleReplaceable extends PropertiesBackedReplaceable {

        private final Properties _properties;

        public SimpleReplaceable(Properties properties) {
            super(properties);
            _properties = properties;
        }

        public void put(String key, String replacement) {
            _properties.put(key, replacement);
        }
    }
}

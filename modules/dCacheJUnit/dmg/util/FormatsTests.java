package dmg.util;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FormatsTests
{
    SimpleReplaceable _replacements;

    @Before
    public void setUp()
    {
        _replacements = new SimpleReplaceable();
        _replacements.put("keyword", "replacement");
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
    public void testReplaceComplexKeywordsWithinKeywordExists()
    {
        _replacements.put("other", "wor");
        assertHasReplacement("${key${other}d}", "replacement");
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
        assertHasReplacement("${${other}", "${${other}");
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
    public static class SimpleReplaceable implements Replaceable
    {
        private Map<String,String> _storage = new HashMap<String,String>();

        public void put(String key, String replacement) {
            _storage.put(key,replacement);
        }

        @Override
        public String getReplacement( String name) {
            return _storage.get(name);
        }
    }
}

package org.dcache.util;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class StringMarkupTests
{

    String _src;
    String _markedUp;

    @Test
    public void quotedStringShouldJustAddQuotesForEmptyString()
    {
        givenSourceString("");

        whenMarkedUpWithQuotedString();

        assertResultIs("\"\"");
    }


    @Test
    public void quotedStringShouldJustAddQuotesForSimpleToken()
    {
        givenSourceString("foo");

        whenMarkedUpWithQuotedString();

        assertResultIs("\"foo\"");
    }


    @Test
    public void quotedStringShouldMarkupDoubleQuoteForTokenWithDoubleQuote()
    {
        givenSourceString("foo\"bar");

        whenMarkedUpWithQuotedString();

        assertResultIs("\"foo\\\"bar\"");
    }


    @Test
    public void quotedStringShouldMarkupBackslashForTokenWithBackslash()
    {
        givenSourceString("foo\\bar");

        whenMarkedUpWithQuotedString();

        assertResultIs("\"foo\\\\bar\"");
    }


    @Test
    public void quotedStringShouldCorrectlyMarkupTokenWithBackslashQuote()
    {
        givenSourceString("foo\\\"bar");

        whenMarkedUpWithQuotedString();

        assertResultIs("\"foo\\\\\\\"bar\"");
    }


    @Test
    public void percentEncodeShouldGivenEmptyStringForEmptySource()
    {
        givenSourceString("");

        whenMarkedUpWithPercentEncode();

        assertResultIs("");
    }


    @Test
    public void percentEncodeShouldGivenSameStringForAsciiSource()
    {
        givenSourceString("simple-string");

        whenMarkedUpWithPercentEncode();

        assertResultIs("simple-string");
    }


    @Test
    public void percentEncodeShouldNotThrowExceptionForSourceWithSpaceColon()
    {
        givenSourceString("foo :bar");

        whenMarkedUpWithPercentEncode();

        /*
         * This tests that Check that URISyntaxException isn't thrown, due
         * to bug:
         *     https://bugs.openjdk.java.net/show_bug.cgi?id=100223
         */
    }


    @Test
    public void percentEncodeShouldNotThrowExceptionForSourceWithIllegalSchemaNameThenColon()
    {
        givenSourceString("G\u00F6ttingen:bar");

        whenMarkedUpWithPercentEncode();

        /*
         * This tests that Check that URISyntaxException isn't thrown, due
         * to bug:
         *     https://bugs.openjdk.java.net/show_bug.cgi?id=100223
         */
    }

    @Test
    public void percentEncodeShouldCorrectlyMarkupSourceWithMiddleSlash()
    {
        givenSourceString("path/element");

        whenMarkedUpWithPercentEncode();

        assertResultIs("path/element");
    }

    @Test
    public void percentEncodeShouldCorrectlyMarkupSourceWithEndSlash()
    {
        givenSourceString("pathElement/");

        whenMarkedUpWithPercentEncode();

        assertResultIs("pathElement/");
    }

    @Test
    public void percentEncodeShouldCorrectlyMarkupSourceWithStartSlash()
    {
        givenSourceString("/pathElement");

        whenMarkedUpWithPercentEncode();

        assertResultIs("/pathElement");
    }

    @Test
    public void percentEncodeShouldCorrectlyMarkupSourceWithDoubleStartSlash()
    {
        givenSourceString("//pathElement");

        whenMarkedUpWithPercentEncode();

        assertResultIs("//pathElement");
    }


    @Test
    public void percentEncodeShouldCorrectlyMarkupSourceWithPercent()
    {
        givenSourceString("path%element");

        whenMarkedUpWithPercentEncode();

        assertResultIs("path%25element");
    }


    @Test
    public void percentEncodeShouldCorrectlyMarkupSourceWithQuestion()
    {
        givenSourceString("path?element");

        whenMarkedUpWithPercentEncode();

        assertResultIs("path%3Felement");
    }


    @Test
    public void percentEncodeShouldCorrectlyMarkupSourceWithSquareBrackets()
    {
        givenSourceString("path[element]");

        whenMarkedUpWithPercentEncode();

        assertResultIs("path%5Belement%5D");
    }


    @Test
    public void percentEncodeShouldCorrectlyMarkupSourceWithHash()
    {
        givenSourceString("path#element");

        whenMarkedUpWithPercentEncode();

        assertResultIs("path%23element");
    }

    @Test
    public void percentEncodeShouldMarkupSpacesForSourceWithSpaces()
    {
        givenSourceString("path element");

        whenMarkedUpWithPercentEncode();

        assertResultIs("path%20element");
    }

    @Test
    public void percentEncodeShouldCorrectlyEncodeNonASCIIWords()
    {
        givenSourceString("\u0561\u0580\u0574\u0578\u0582\u0576\u056F\u0020");

        whenMarkedUpWithPercentEncode();

        assertResultIs("%D5%A1%D6%80%D5%B4%D5%B8%D6%82%D5%B6%D5%AF%20");
    }



    public void givenSourceString(String src)
    {
        _src = src;
    }

    public void whenMarkedUpWithQuotedString()
    {
        _markedUp = StringMarkup.quotedString(_src);
    }

    public void whenMarkedUpWithPercentEncode()
    {
        _markedUp = StringMarkup.percentEncode(_src);
    }

    public void assertResultIs(String result)
    {
        assertThat(_markedUp, is(result));
    }
}

package org.dcache.webdav;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UrlPathWrapperTests
{
    private static final String UNENCODED = "\u0561\u0580\u0574\u0578\u0582\u0576\u056F\u0020";
    private static final String ENCODED = "%D5%A1%D6%80%D5%B4%D5%B8%D6%82%D5%B6%D5%AF%20";

    @Test
    public void testGetEmptyString()
    {
        UrlPathWrapper empty = UrlPathWrapper.forEmptyPath();

        assertEquals("", empty.toString());
        assertEquals("", empty.getEncoded());
        assertEquals("", empty.getUnencoded());
    }

    @Test
    public void testAsciiForPath()
    {
        String source = "pathElement";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(source, path.toString());
        assertEquals(source, path.getEncoded());
        assertEquals(source, path.getUnencoded());
    }

    @Test
    public void testForPathWithColon()
    {
        String source = "path:element";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(source, path.toString());
        assertEquals(source, path.getEncoded());
        assertEquals(source, path.getUnencoded());
    }

    @Test
    public void testForPathWithSpaceColon()
    {
        String decoded = "path :element";

        UrlPathWrapper.forPath(decoded);

        /*
         * Check that URISyntaxException isn't thrown, due to bug:
         *     https://bugs.openjdk.java.net/show_bug.cgi?id=100223
         */
    }

    @Test
    public void testGoettingen()
    {
        String decoded="Göttingen:information";
        UrlPathWrapper.forPath(decoded);

        /*
         * Check that URISyntaxException isn't thrown, due to bug:
         *     https://bugs.openjdk.java.net/show_bug.cgi?id=100223
         */
    }

    @Test
    public void testForPathWithMiddleSlash()
    {
        String source = "path/element";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(source, path.toString());
        assertEquals(source, path.getEncoded());
        assertEquals(source, path.getUnencoded());
    }

    @Test
    public void testForPathWithEndSlash()
    {
        String source = "pathElement/";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(source, path.toString());
        assertEquals(source, path.getEncoded());
        assertEquals(source, path.getUnencoded());
    }

    @Test
    public void testForPathWithStartSlash()
    {
        String source = "/pathElement";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(source, path.toString());
        assertEquals(source, path.getEncoded());
        assertEquals(source, path.getUnencoded());
    }

    @Test
    public void testForPathWithDoubleStartSlash()
    {
        String source = "//pathElement";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(source, path.toString());
        assertEquals(source, path.getEncoded());
        assertEquals(source, path.getUnencoded());
    }

    @Test
    public void testForPathWithPercent()
    {
        String source = "path%element";
        String encoded = "path%25element";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(encoded, path.getEncoded());
        assertEquals(source, path.getUnencoded());
        assertEquals(source, path.toString());
    }

    @Test
    public void testForPathWithQuestion()
    {
        String source = "path?element";
        String encoded = "path%3Felement";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(encoded, path.getEncoded());
        assertEquals(source, path.getUnencoded());
        assertEquals(source, path.toString());
    }

    @Test
    public void testForPathWithSpace()
    {
        String source = "path element";
        String encoded = "path%20element";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(encoded, path.getEncoded());
        assertEquals(source, path.getUnencoded());
        assertEquals(source, path.toString());
    }

    @Test
    public void testForPathWithSquareBrackets()
    {
        String source = "path[element]";
        String encoded = "path%5Belement%5D";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(encoded, path.getEncoded());
        assertEquals(source, path.getUnencoded());
        assertEquals(source, path.toString());
    }

    @Test
    public void testForPathWithHash()
    {
        String source = "path#element";
        String encoded = "path%23element";

        UrlPathWrapper path = UrlPathWrapper.forPath(source);

        assertEquals(encoded, path.getEncoded());
        assertEquals(source, path.getUnencoded());
        assertEquals(source, path.toString());
    }

    @Test
    public void testForPathWithNonAscii()
    {
        UrlPathWrapper path = UrlPathWrapper.forPath(UNENCODED);

        assertEquals(ENCODED, path.getEncoded());
        assertEquals(UNENCODED, path.getUnencoded());
        assertEquals(UNENCODED, path.toString());
    }
}

package org.dcache.tests.xrootd2;

import static org.junit.Assert.*;

import java.util.Map;

import org.dcache.xrootd2.util.OpaqueStringParser;
import org.dcache.xrootd2.util.ParseException;
import org.junit.Test;

public class OpaqueStringParserTest
{
    private final static String EDITOR_KEY = "editor";
    private final static String EDITOR_VALUE = "vim";
    private final static String OS_KEY = "os";
    private final static String OS_VALUE = "linux";
    private final static String DISTRIBUTION_KEY = "distribution";
    private final static String DISTRIBUTION_VALUE = "ubuntu";

    private final static String DCACHE_MOVER_KEY = "org.dcache.uuid";
    private final static String DCACHE_MOVER_VALUE = "787c932b-2dea-46b7-809d-d3c4b0f4707b";

    private final static String RT5946_EXAMPLE =
        "filetype=raw";
    private final static String RT5946_KEY = "filetype";
    private final static String RT5946_VALUE = "raw";

    /*
     * if we send back opaque information in the redirect response, xrootd
     * does not correctly combine it with opaque information in the request
     * to the door and will end up with strings like
     *
     * original_opaque=value?&our_opaque_key=our_opaque_value&...
     */
    private final static char OPAQUE_STRING_PREFIX = '?';
    private final static char OPAQUE_PREFIX =
        OpaqueStringParser.OPAQUE_PREFIX;
    private final static char OPAQUE_SEPARATOR =
        OpaqueStringParser.OPAQUE_SEPARATOR;

    @Test
    public void testEmpty() throws ParseException
    {
        String opaque = "";
        Map<String, String> resultMap =
            OpaqueStringParser.getOpaqueMap(opaque);

        assertNotNull("Parsing returned null result", resultMap);
        assertTrue("empty string does not parse to empty map",
                   resultMap.isEmpty());
    }

    @Test
    public void testRT5946() throws ParseException {
        String opaque = RT5946_EXAMPLE + OPAQUE_STRING_PREFIX + OPAQUE_PREFIX +
                        DCACHE_MOVER_KEY + OPAQUE_SEPARATOR +
                        DCACHE_MOVER_VALUE;

        Map<String, String> resultMap =
            OpaqueStringParser.getOpaqueMap(opaque);
        assertEquals("Parsing did not produce exactly two results",
                     2,
                     resultMap.size());
        assertEquals("Opaque string was not parsed correctly",
                     DCACHE_MOVER_VALUE,
                     resultMap.get(DCACHE_MOVER_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     RT5946_VALUE,
                     resultMap.get(RT5946_KEY));
    }

    @Test
    public void testRT5946MorePairs() throws ParseException {
        String opaque = RT5946_EXAMPLE + OPAQUE_STRING_PREFIX +
                        OPAQUE_PREFIX + DCACHE_MOVER_KEY +
                        OPAQUE_SEPARATOR + DCACHE_MOVER_VALUE + OPAQUE_PREFIX +
                        OS_KEY + OPAQUE_SEPARATOR + OS_VALUE + OPAQUE_PREFIX +
                        EDITOR_KEY + OPAQUE_SEPARATOR + EDITOR_VALUE;

        Map<String, String> resultMap =
            OpaqueStringParser.getOpaqueMap(opaque);

        assertEquals("Parsing did not produce exactly three results",
                     4,
                     resultMap.size());
        assertEquals("Opaque string was not parsed correctly",
                     DCACHE_MOVER_VALUE,
                     resultMap.get(DCACHE_MOVER_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     OS_VALUE,
                     resultMap.get(OS_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     EDITOR_VALUE,
                     resultMap.get(EDITOR_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     RT5946_VALUE,
                     resultMap.get(RT5946_KEY));
    }

    @Test
    public void testOnePair() throws ParseException
    {
        String opaque = OPAQUE_PREFIX + EDITOR_KEY + OPAQUE_SEPARATOR +
            EDITOR_VALUE;

        Map<String, String> resultMap =
            OpaqueStringParser.getOpaqueMap(opaque);

        assertNotNull("Parsing returned null result", resultMap);
        assertEquals("Parsing did not produce exactly one result",
                     1,
                     resultMap.size());
        assertEquals("Opaque string was not parsed correctly",
                     EDITOR_VALUE,
                     resultMap.get(EDITOR_KEY));
    }

    @Test
    public void testOnePairNoPrefix() throws ParseException
    {
        String opaque = EDITOR_KEY + OPAQUE_SEPARATOR + EDITOR_VALUE;

        Map<String, String> resultMap =
            OpaqueStringParser.getOpaqueMap(opaque);

        assertNotNull("Parsing returned null result", resultMap);
        assertEquals("Parsing did not produce exactly one result",
                     1,
                     resultMap.size());
        assertEquals("Opaque string was not parsed correctly",
                     EDITOR_VALUE,
                     resultMap.get(EDITOR_KEY));
    }

    @Test
    public void testMorePairs() throws ParseException
    {
        String opaque =  OPAQUE_PREFIX + EDITOR_KEY + OPAQUE_SEPARATOR +
            EDITOR_VALUE + OPAQUE_PREFIX + OS_KEY + OPAQUE_SEPARATOR +
            OS_VALUE + OPAQUE_PREFIX + DISTRIBUTION_KEY + OPAQUE_SEPARATOR +
            DISTRIBUTION_VALUE;
        Map<String, String> resultMap =
            OpaqueStringParser.getOpaqueMap(opaque);

        assertNotNull("Parsing returned null result", resultMap);

        assertEquals("Parsing did not produce exactly three results",
                     3,
                     resultMap.size());
        assertEquals("Opaque string was not parsed correctly",
                     EDITOR_VALUE,
                     resultMap.get(EDITOR_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     OS_VALUE,
                     resultMap.get(OS_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     DISTRIBUTION_VALUE,
                     resultMap.get(DISTRIBUTION_KEY));
    }

    @Test
    public void testMorePairsNoPrefix() throws ParseException
    {
        String opaque =  EDITOR_KEY + OPAQUE_SEPARATOR +
        EDITOR_VALUE + OPAQUE_PREFIX + OS_KEY + OPAQUE_SEPARATOR +
        OS_VALUE + OPAQUE_PREFIX + DISTRIBUTION_KEY + OPAQUE_SEPARATOR +
        DISTRIBUTION_VALUE;
        Map<String, String> resultMap =
        OpaqueStringParser.getOpaqueMap(opaque);

        assertNotNull("Parsing returned null result", resultMap);

        assertEquals("Parsing did not produce exactly three results",
                     3,
                     resultMap.size());
        assertEquals("Opaque string was not parsed correctly",
                     EDITOR_VALUE,
                     resultMap.get(EDITOR_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     OS_VALUE,
                     resultMap.get(OS_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     DISTRIBUTION_VALUE,
                     resultMap.get(DISTRIBUTION_KEY));
    }

    @Test
    public void testMorePairsMixedPrefixes() throws ParseException
    {
        String opaque =  EDITOR_KEY + OPAQUE_SEPARATOR +
        EDITOR_VALUE + OPAQUE_PREFIX + OS_KEY + OPAQUE_SEPARATOR +
        OS_VALUE + OPAQUE_STRING_PREFIX + DISTRIBUTION_KEY + OPAQUE_SEPARATOR +
        DISTRIBUTION_VALUE;

        Map<String, String> resultMap =
            OpaqueStringParser.getOpaqueMap(opaque);

        assertNotNull("Parsing returned null result", resultMap);

        assertEquals("Parsing did not produce exactly three results",
                     3,
                     resultMap.size());
        assertEquals("Opaque string was not parsed correctly",
                     EDITOR_VALUE,
                     resultMap.get(EDITOR_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     OS_VALUE,
                     resultMap.get(OS_KEY));
        assertEquals("Opaque string was not parsed correctly",
                     DISTRIBUTION_VALUE,
                     resultMap.get(DISTRIBUTION_KEY));
    }

    @Test(expected = ParseException.class)
    public void testMissingValue() throws ParseException
    {
        String opaque = OPAQUE_PREFIX + EDITOR_KEY + OPAQUE_SEPARATOR +
            EDITOR_VALUE + OPAQUE_PREFIX + OS_KEY + OPAQUE_PREFIX +
            DISTRIBUTION_KEY + OPAQUE_SEPARATOR + DISTRIBUTION_VALUE;
        OpaqueStringParser.getOpaqueMap(opaque);
    }

    @Test
    public void testEmptyValue() throws ParseException
    {
        String opaque = OPAQUE_PREFIX + EDITOR_KEY + OPAQUE_SEPARATOR +
            EDITOR_VALUE + OPAQUE_PREFIX + OS_KEY + OPAQUE_SEPARATOR +
            OPAQUE_PREFIX + DISTRIBUTION_KEY + OPAQUE_SEPARATOR +
            DISTRIBUTION_VALUE;
        Map<String, String> resultMap =
            OpaqueStringParser.getOpaqueMap(opaque);

        assertNotNull("Parsing produced null result", resultMap);
        assertEquals("Empty opaque string value does not result in empty map" +
                     " entry",
                     "",
                     resultMap.get(OS_KEY));
    }
}

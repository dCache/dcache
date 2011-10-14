package org.dcache.util;

import static org.junit.Assert.*;

import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class ScopedConfigurationPropertiesTests
{
    private Properties properties;

    @Before
    public void setup()
    {
        properties = new Properties();
        properties.put("P1", "value1");
        properties.put("S1/P2", "value2");
        properties.put("S2/P2", "value3");
        properties.put("P3", "value4");
        properties.put("S1/P3", "value5");
    }

    @Test
    public void testDefaultPropertiesScope1()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S1");
        assertEquals("value1", p.getProperty("P1"));
        assertEquals("value2", p.getProperty("P2"));
        assertEquals("value5", p.getProperty("P3"));
        assertNull(p.getProperty("undeclared"));
    }

    @Test
    public void testDefaultPropertiesScope2()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S2");
        assertEquals("value1", p.getProperty("P1"));
        assertEquals("value3", p.getProperty("P2"));
        assertEquals("value4", p.getProperty("P3"));
        assertNull(p.getProperty("undeclared"));
    }

    @Test
    public void testLocalProperties()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S1");
        p.put("P1", "local1");
        p.put("P2", "local2");
        p.put("P3", "local3");
        p.put("P4", "local4");
        assertEquals("local1", p.getProperty("P1"));
        assertEquals("local2", p.getProperty("P2"));
        assertEquals("local3", p.getProperty("P3"));
        assertEquals("local4", p.getProperty("P4"));
        assertNull(p.getProperty("undeclared"));
    }

    @Test
    public void testScopedLocalPropertiesSameScope()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S1");
        p.put("S1/P1", "local1");
        p.put("S1/P2", "local2");
        p.put("S1/P3", "local3");
        p.put("S1/P4", "local4");
        assertEquals("local1", p.getProperty("P1"));
        assertEquals("local2", p.getProperty("P2"));
        assertEquals("local3", p.getProperty("P3"));
        assertEquals("local4", p.getProperty("P4"));
        assertNull(p.getProperty("undeclared"));
    }

    @Test
    public void testScopedLocalPropertiesOtherScope()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S2");
        p.put("S1/P1", "local1");
        p.put("S1/P2", "local2");
        p.put("S1/P3", "local3");
        p.put("S1/P4", "local4");
        assertEquals("value1", p.getProperty("P1"));
        assertEquals("value3", p.getProperty("P2"));
        assertEquals("value4", p.getProperty("P3"));
        assertNull(p.getProperty("P4"));
        assertNull(p.getProperty("undeclared"));
    }

    @Test
    public void testPropertyNamesForDefaultsS1()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S1");
        assertEquals(asSet("P1", "P2", "P3"), p.stringPropertyNames());
    }

    @Test
    public void testPropertyNamesForDefaultsS2()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S2");
        assertEquals(asSet("P1", "P2", "P3"), p.stringPropertyNames());
    }

    @Test
    public void testPropertyNamesForDefaultsOtherScope()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "undeclared");
        assertEquals(asSet("P1", "P3"), p.stringPropertyNames());
    }

    @Test
    public void testPropertyNamesLocal()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S1");
        p.put("P4", "local4");
        assertEquals(asSet("P1", "P2", "P3", "P4"), p.stringPropertyNames());
    }

    @Test
    public void testPropertyNamesLocalScoped()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S1");
        p.put("S1/P4", "local4");
        assertEquals(asSet("P1", "P2", "P3", "P4"), p.stringPropertyNames());
    }

    @Test
    public void testPropertyNamesLocalScopedOtherScope()
    {
        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(properties, "S1");
        p.put("S2/P4", "local4");
        assertEquals(asSet("P1", "P2", "P3"), p.stringPropertyNames());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSameScopeScopedForbiddenProperty()
    {
        ConfigurationProperties a = new ConfigurationProperties(properties);

        a.put("(forbidden)S1/forbidden", "local4");

        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(a, "S1");

        p.put("forbidden", "other value"); // attempt to update a (scoped) forbidden property
    }

    @Test
    public void testOtherScopeScopedForbiddenProperty()
    {
        ConfigurationProperties a = new ConfigurationProperties(properties);

        a.put("(forbidden)S2/forbidden", "local4");

        ScopedConfigurationProperties p =
            new ScopedConfigurationProperties(a, "S1");

        p.put("forbidden", "other value"); // (forbidden) is on other scope
    }

    private Set<String> asSet(String... names)
    {
        return new HashSet(Arrays.asList(names));
    }
}

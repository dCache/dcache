package org.dcache.boot;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Properties;

import dmg.cells.nucleus.CellShell;
import dmg.cells.nucleus.SystemCell;
import dmg.util.CommandException;
import dmg.util.Formats;

import org.dcache.util.ConfigurationProperties;

import static org.junit.Assert.assertEquals;

public class DomainConfigurationTest
{
    private final static String DOMAIN_NAME = "domain";
    private final static String SERVICE1_NAME = "service1";
    private final static String SERVICE2_NAME = "service2";

    private final static String DEFAULTS =
        "dcache.domain.service.uri=classpath:/org/dcache/boot/empty.batch\n" +
        "a=1\n" +
        "b=${a}\n" +
        "c=2\n";

    private final static String CONFIGURATION =
        "a=2\n" +
        "c=1\n";

    private final static String SERVICE1_CONFIG =
        "a=3\n" +
        "c=4\n";

    private final static String SERVICE2_CONFIG =
        "a=3\n" +
        "b=2\n" +
        "c=5\n";

    private final static SystemCell system = new SystemCell(DOMAIN_NAME);
    private static final String SOURCE = "source";
    private static final LineNumberReader EMPTY_READER =
            new LineNumberReader(new StringReader(""));

    private ConfigurationProperties defaults;
    private ConfigurationProperties configuration;

    @Before
    public void setup()
        throws IOException
    {
        defaults = new ConfigurationProperties(new Properties());
        defaults.load(new StringReader(DEFAULTS));

        configuration = new ConfigurationProperties(defaults);
        configuration.load(new StringReader(CONFIGURATION));
    }

    public void assertPropertyEquals(String expected, String variable, CellShell shell)
    {
        String value = shell.getReplacement(variable);
        if (value != null) {
            value = Formats.replaceKeywords(value, shell);
        }
        assertEquals(expected, value);
    }

    @Test
    public void testWithDefaults()
            throws CommandException, IOException
    {
        Domain domain = new Domain(DOMAIN_NAME, defaults);
        ConfigurationProperties service = domain.createService(SOURCE, EMPTY_READER, SERVICE1_NAME);
        CellShell shell = domain.createShellForService(system, service);

        assertPropertyEquals("1", "a", shell);
        assertPropertyEquals("1", "b", shell);
        assertPropertyEquals("2", "c", shell);
        assertPropertyEquals(DOMAIN_NAME, "dcache.domain.name", shell);
        assertPropertyEquals(SERVICE1_NAME, "dcache.domain.service", shell);

        service = domain.createService(SOURCE, EMPTY_READER, SERVICE2_NAME);
        shell = domain.createShellForService(system, service);

        assertPropertyEquals("1", "a", shell);
        assertPropertyEquals("1", "b", shell);
        assertPropertyEquals("2", "c", shell);
        assertPropertyEquals(DOMAIN_NAME, "dcache.domain.name", shell);
        assertPropertyEquals(SERVICE2_NAME, "dcache.domain.service", shell);
    }

    @Test
    public void testWithConfiguration()
            throws CommandException, IOException
    {
        Domain domain = new Domain(DOMAIN_NAME, configuration);

        ConfigurationProperties service = domain.createService(SOURCE, EMPTY_READER, SERVICE1_NAME);
        CellShell shell = domain.createShellForService(system, service);

        assertPropertyEquals("2", "a", shell);
        assertPropertyEquals("2", "b", shell);
        assertPropertyEquals("1", "c", shell);
        assertPropertyEquals(DOMAIN_NAME, "dcache.domain.name", shell);
        assertPropertyEquals(SERVICE1_NAME, "dcache.domain.service", shell);

        service = domain.createService(SOURCE, EMPTY_READER, SERVICE2_NAME);
        shell = domain.createShellForService(system, service);

        assertPropertyEquals("2", "a", shell);
        assertPropertyEquals("2", "b", shell);
        assertPropertyEquals("1", "c", shell);
        assertPropertyEquals(DOMAIN_NAME, "dcache.domain.name", shell);
        assertPropertyEquals(SERVICE2_NAME, "dcache.domain.service", shell);
    }

    @Test
    public void testWithPerServiceConfiguration()
        throws CommandException, IOException
    {
        Domain domain = new Domain(DOMAIN_NAME, configuration);

        ConfigurationProperties service =
                domain.createService(SOURCE, new LineNumberReader(new StringReader(SERVICE1_CONFIG)), SERVICE1_NAME);
        CellShell shell = domain.createShellForService(system, service);

        assertPropertyEquals("3", "a", shell);
        assertPropertyEquals("3", "b", shell);
        assertPropertyEquals("4", "c", shell);
        assertPropertyEquals(DOMAIN_NAME, "dcache.domain.name", shell);
        assertPropertyEquals(SERVICE1_NAME, "dcache.domain.service", shell);

        service = domain.createService(SOURCE, new LineNumberReader(new StringReader(SERVICE2_CONFIG)), SERVICE2_NAME);
        shell = domain.createShellForService(system, service);

        assertPropertyEquals("3", "a", shell);
        assertPropertyEquals("2", "b", shell);
        assertPropertyEquals("5", "c", shell);
        assertPropertyEquals(DOMAIN_NAME, "dcache.domain.name", shell);
        assertPropertyEquals(SERVICE2_NAME, "dcache.domain.service", shell);
    }

    @Test
    public void testWithRuntimeOverrides()
        throws CommandException, IOException
    {
        Domain domain = new Domain(DOMAIN_NAME, configuration);

        ConfigurationProperties service =
                domain.createService(SOURCE, new LineNumberReader(new StringReader(SERVICE1_CONFIG)), SERVICE1_NAME);
        CellShell shell = domain.createShellForService(system, service);

        assertPropertyEquals("3", "a", shell);
        assertPropertyEquals("3", "b", shell);
        assertPropertyEquals("4", "c", shell);

        shell.environment().put("a", "${c}");

        assertPropertyEquals("4", "a", shell);
        assertPropertyEquals("4", "b", shell);
        assertPropertyEquals("4", "c", shell);
    }
}

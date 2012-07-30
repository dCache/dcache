package org.dcache.gplazma.configuration.parser;

import static org.dcache.gplazma.configuration.ConfigurationItemControl.OPTIONAL;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.REQUIRED;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.REQUISITE;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.SUFFICIENT;
import static org.dcache.gplazma.configuration.ConfigurationItemType.ACCOUNT;
import static org.dcache.gplazma.configuration.ConfigurationItemType.AUTHENTICATION;
import static org.dcache.gplazma.configuration.ConfigurationItemType.MAPPING;
import static org.dcache.gplazma.configuration.ConfigurationItemType.SESSION;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;

import org.dcache.gplazma.configuration.Configuration;
import org.dcache.gplazma.configuration.ConfigurationItem;
import org.dcache.gplazma.configuration.parser.FactoryConfigurationException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class ConfigurationParserTests
{
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationParserTests.class);

    private static final String PAMConfigParserFactory =
            "org.dcache.gplazma.configuration.parser.PAMStyleConfigurationParserFactory";
    // configurations to test with
    private static final String EMPTY_CONFIG_1 ="";

    private static final String EMPTY_CONFIG_2 =
            "# this is a comment\n"+
            "         \t \n" + //white spaces
            "    # this is another comment \n"+
            "\n"+ // empty line
            "";  // one more empty line
    // an invalide one
    private static final String INVALID_CONFIG =
            "this is not a valid configuration";

    private static final String INVALID_CONFIG_WRONG_TYPE =
            "auth1 require plugin";

    private static final String INVALID_CONFIG_WRONG_CONTROL =
            "auth require1 plugin";

    // configuration that has plugins of all possible conbination of all types
    // with all possible priorities with and without arguments
    private static final String TEST_CONFIG =
            "# this is our well formed test configuration\n" +
            "auth    required    plugin1\n"+
            "auth    sufficient plugin2 key1=value1\n"+
            "auth    requisite  plugin3 key1=value1 \"key2=Some other=xxx\"\n"+
            "auth    optional   plugin4 key1=value1 \"key2=Some other=xxx\" 'key3 = val3'\n"+

            "    # this is comment line \n"+
            "map     required    plugin1 '!@#=a crazy &$@#* argument'\n"+
            "map     sufficient plugin2\n"+
            "map     requisite  plugin3\n"+
            "map     optional   plugin4\n"+

            "account required    plugin5\n"+
            "account sufficient plugin6\n"+
            "account requisite  plugin7\n"+
            "account optional   plugin8\n"+

            "session required    plugin9\n"+
            "session sufficient plugin10\n"+
            "session requisite  plugin11\n"+
            "session optional   plugin12\n"+
            "# THE END, ENDE, KONETS, UNDANGEN, SLUTET, SLUTTEN, LA FIN, AL FINAL";

    private static final Properties EMPTY_PROPERTIES = new Properties();

    private static final Properties ONEARGPROPERTIES = new Properties();
    private static final Properties TWOARGPROPERTIES = new Properties();
    private static final Properties THREEARGPROPERTIES = new Properties();
    private static final Properties WEIRDARGPROPERTIES = new Properties();

    private static final ConfigurationItem[] TEST_CONFIG_ARRAY =
            new ConfigurationItem[] {
      new ConfigurationItem(  AUTHENTICATION, REQUIRED,   "plugin1", EMPTY_PROPERTIES),
      new ConfigurationItem(  AUTHENTICATION, SUFFICIENT, "plugin2", ONEARGPROPERTIES),
      new ConfigurationItem(  AUTHENTICATION, REQUISITE,  "plugin3", TWOARGPROPERTIES),
      new ConfigurationItem(  AUTHENTICATION, OPTIONAL,   "plugin4", THREEARGPROPERTIES),

      new ConfigurationItem(  MAPPING,        REQUIRED,   "plugin1", WEIRDARGPROPERTIES),
      new ConfigurationItem(  MAPPING,        SUFFICIENT, "plugin2", EMPTY_PROPERTIES),
      new ConfigurationItem(  MAPPING,        REQUISITE,  "plugin3", EMPTY_PROPERTIES),
      new ConfigurationItem(  MAPPING,        OPTIONAL,   "plugin4", EMPTY_PROPERTIES),

      new ConfigurationItem(  ACCOUNT,        REQUIRED,   "plugin5", EMPTY_PROPERTIES),
      new ConfigurationItem(  ACCOUNT,        SUFFICIENT, "plugin6", EMPTY_PROPERTIES),
      new ConfigurationItem(  ACCOUNT,        REQUISITE,  "plugin7", EMPTY_PROPERTIES),
      new ConfigurationItem(  ACCOUNT,        OPTIONAL,   "plugin8", EMPTY_PROPERTIES),

      new ConfigurationItem(  SESSION,        REQUIRED,   "plugin9", EMPTY_PROPERTIES),
      new ConfigurationItem(  SESSION,        SUFFICIENT, "plugin10",EMPTY_PROPERTIES),
      new ConfigurationItem(  SESSION,        REQUISITE,  "plugin11",EMPTY_PROPERTIES),
      new ConfigurationItem(  SESSION,        OPTIONAL,   "plugin12",EMPTY_PROPERTIES),
    };

    // TODO
    // Implement tests for classic configuration
    // Once classic configuration reader is implemented
    // private static final String TEST_CLASSIC_CONFIG = "";

    private ConfigurationParserFactory pamConfigParserFactory;

    @Before
    public void setup() throws FactoryConfigurationException
    {
        pamConfigParserFactory = ConfigurationParserFactory.getInstance(
                PAMConfigParserFactory);

        ONEARGPROPERTIES.put("key1", "value1");

        TWOARGPROPERTIES.put("key1", "value1");
        TWOARGPROPERTIES.put("key2", "Some other=xxx");

        THREEARGPROPERTIES.put("key1", "value1");
        THREEARGPROPERTIES.put("key2", "Some other=xxx");
        THREEARGPROPERTIES.put("key3", "val3");

        WEIRDARGPROPERTIES.put("!@#", "a crazy &$@#* argument");
    }

    @Test
    public void testDefaultFactoryGetInstanceReturnsAFactory()
            throws FactoryConfigurationException
    {
        ConfigurationParserFactory factory =
                ConfigurationParserFactory.getInstance();
        assertNotNull(factory);
        ConfigurationParser parser = factory.newConfigurationParser();
        assertNotNull(parser);
    }

    @Test
    public void testEmptyConfig() throws ParseException
    {
        ConfigurationParser parser = pamConfigParserFactory.newConfigurationParser();
        Configuration configuration =
                parser.parse(EMPTY_CONFIG_1);
        List<ConfigurationItem> configItemList =
                configuration.getConfigurationItemList();
        assertTrue(configItemList.isEmpty());
        configuration =
                parser.parse(EMPTY_CONFIG_2);
        configItemList =
                configuration.getConfigurationItemList();
        assertTrue(configItemList.isEmpty());
    }

    @Test(expected=ParseException.class)
    public void testInvalidConfig() throws ParseException
    {
        ConfigurationParser parser = pamConfigParserFactory.newConfigurationParser();
        Configuration configuration =
                parser.parse(INVALID_CONFIG);
        logger.error("Parsed INVALID_CONFIG is \n"+configuration);


    }

    @Test(expected=ParseException.class)
    public void testInvalidConfigWrongType() throws ParseException
    {
        ConfigurationParser parser = pamConfigParserFactory.newConfigurationParser();
        Configuration configuration =
                parser.parse(INVALID_CONFIG_WRONG_TYPE);
        logger.error("Parsed INVALID_CONFIG_WRONG_TYPE is \n"+configuration);

    }

    @Test(expected=ParseException.class)
    public void testInvalidConfigWrongControl() throws ParseException
    {
        ConfigurationParser parser = pamConfigParserFactory.newConfigurationParser();
        Configuration configuration =
                parser.parse(INVALID_CONFIG_WRONG_CONTROL);
        logger.error("Parsed INVALID_CONFIG_WRONG_CONTROL is \n"+configuration);

    }

    @Test
    public void testConfig() throws ParseException
    {
        ConfigurationParser parser = pamConfigParserFactory.newConfigurationParser();
        Configuration configuration =
                parser.parse(TEST_CONFIG);
        List<ConfigurationItem> configItemList =
                configuration.getConfigurationItemList();

        ConfigurationItem[] configItemArray =
                configItemList
                        .toArray(new ConfigurationItem[configItemList.size()]);
        logger.debug("Parsed TEST_CONFIG is \n"+configuration);

        assertArrayEquals(TEST_CONFIG_ARRAY, configItemArray);
    }
}

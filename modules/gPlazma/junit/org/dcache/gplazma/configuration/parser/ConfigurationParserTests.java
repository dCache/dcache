package org.dcache.gplazma.configuration.parser;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.dcache.gplazma.configuration.ConfigurationItemType.*;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;

import org.dcache.gplazma.configuration.Configuration;
import org.dcache.gplazma.configuration.ConfigurationItem;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class ConfigurationParserTests {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationParserTests.class);

    private static final String PAMConfigParserFactory =
            "org.dcache.gplazma.configuration.parser.DefaultConfigurationParserFactory";
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
            "auth    sufficient plugin2 arg1\n"+
            "auth    requisite  plugin3 arg1 arg2\n"+
            "auth    optional   plugin4 arg1 arg2 arg3\n"+

            "    # this is comment line \n"+
            "map     required    plugin1 a crasy &$@#* argument\n"+
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
    private static final ConfigurationItem[] TEST_CONFIG_ARRAY =
            new ConfigurationItem[] {
      new ConfigurationItem(  AUTHENTICATION, REQUIRED,    "plugin1",null),
      new ConfigurationItem(  AUTHENTICATION, SUFFICIENT, "plugin2","arg1"),
      new ConfigurationItem(  AUTHENTICATION, REQUISITE,  "plugin3","arg1 arg2"),
      new ConfigurationItem(  AUTHENTICATION, OPTIONAL,   "plugin4","arg1 arg2 arg3"),

      new ConfigurationItem(  MAPPING,        REQUIRED,    "plugin1","a crasy &$@#* argument"),
      new ConfigurationItem(  MAPPING,        SUFFICIENT, "plugin2",null),
      new ConfigurationItem(  MAPPING,        REQUISITE,  "plugin3",null),
      new ConfigurationItem(  MAPPING,        OPTIONAL,   "plugin4",null),

      new ConfigurationItem(  ACCOUNT,        REQUIRED,    "plugin5",null),
      new ConfigurationItem(  ACCOUNT,        SUFFICIENT, "plugin6",null),
      new ConfigurationItem(  ACCOUNT,        REQUISITE,  "plugin7",null),
      new ConfigurationItem(  ACCOUNT,        OPTIONAL,   "plugin8",null),

      new ConfigurationItem(  SESSION,        REQUIRED,    "plugin9",null),
      new ConfigurationItem(  SESSION,        SUFFICIENT, "plugin10",null),
      new ConfigurationItem(  SESSION,        REQUISITE,  "plugin11",null),
      new ConfigurationItem(  SESSION,        OPTIONAL,   "plugin12",null),
};

    // TODO
    // Implement tests for classic configuration
    // Once classic configuration reader is implemented
    private static final String TEST_CLASSIC_CONFIG =
            "";

    private ConfigurationParserFactory pamConfigParserFactory;

    @Before
    public void setUp() {
        pamConfigParserFactory = ConfigurationParserFactory.getInstance(
                PAMConfigParserFactory);
    }

    @Test
    public void testDefaultFactoryGetInstanceReturnsAFactory() {
        ConfigurationParserFactory factory =
                ConfigurationParserFactory.getInstanse();
        assertNotNull(factory);
        ConfigurationParser parser = factory.newConfigurationParser();
        assertNotNull(parser);
    }

    @Test
    public void testEmptyConfig() {
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
    public void testInvalidConfig() {
        ConfigurationParser parser = pamConfigParserFactory.newConfigurationParser();
        Configuration configuration =
                parser.parse(INVALID_CONFIG);
        logger.error("Parsed INVALID_CONFIG is \n"+configuration);


    }

    @Test(expected=ParseException.class)
    public void testInvalidConfigWrongType() {
        ConfigurationParser parser = pamConfigParserFactory.newConfigurationParser();
        Configuration configuration =
                parser.parse(INVALID_CONFIG_WRONG_TYPE);
        logger.error("Parsed INVALID_CONFIG_WRONG_TYPE is \n"+configuration);

    }

    @Test(expected=ParseException.class)
    public void testInvalidConfigWrongControl() {
        ConfigurationParser parser = pamConfigParserFactory.newConfigurationParser();
        Configuration configuration =
                parser.parse(INVALID_CONFIG_WRONG_CONTROL);
        logger.error("Parsed INVALID_CONFIG_WRONG_CONTROL is \n"+configuration);

    }

    @Test
    public void testConfig() {
        ConfigurationParser parser = pamConfigParserFactory.newConfigurationParser();
        Configuration configuration =
                parser.parse(TEST_CONFIG);
        List<ConfigurationItem> configItemList =
                configuration.getConfigurationItemList();

        ConfigurationItem[] configItemArray =
                configItemList.toArray(new ConfigurationItem[0]);
        logger.debug("Parsed TEST_CONFIG is \n"+configuration);
        assertArrayEquals(configItemArray,TEST_CONFIG_ARRAY);
    }


}

package org.dcache.gplazma.loader;

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import com.google.common.collect.ImmutableList;
import java.util.Properties;
import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.junit.Before;
import org.junit.Test;

public class StaticClassPluginLoaderTests {

    private static final String PLUGIN_NAME_VALID_PLUGIN = "ValidPlugin";
    private static final String PLUGIN_NAME_INVALID_PLUGIN = "InvalidPlugin";
    private static final String PLUGIN_NAME_EXCEPTION_PLUGIN = "ExceptionThrowingPlugin";

    /**
     * Name of some non-existent plugin
     */
    private static final String PLUGIN_NAME_MISSING_PLUGIN = "NoSuchPlugin";

    private static Properties _properties;

    PluginLoader _loaderNotInit;
    PluginLoader _loader;

    @Before
    public void setup() {
        _properties = new Properties();
        _properties.put("arg-1", "value1");
        _properties.put("arg-2", "value 2");

        _loaderNotInit = newLoader();
        _loader = newLoader();
        _loader.init();
    }

    public PluginLoader newLoader() {
        return StaticClassPluginLoader.newPluginLoader(
              ImmutableList.of(
                    ValidPlugin.class,
                    InvalidPlugin.class,
                    ExceptionThrowingPlugin.class));
    }

    @Test(expected = IllegalStateException.class)
    public void testNewPluginWithoutInit() throws PluginLoadingException {
        _loaderNotInit.newPluginByName(PLUGIN_NAME_VALID_PLUGIN);
    }

    @Test(expected = IllegalStateException.class)
    public void testDoubleInitFails() {
        _loaderNotInit.init();
        _loaderNotInit.init();
    }

    @Test(expected = PluginLoadingException.class)
    public void testNewPluginWithWrongName() throws PluginLoadingException {
        _loader.newPluginByName(PLUGIN_NAME_MISSING_PLUGIN);
    }

    @Test(expected = PluginLoadingException.class)
    public void testNewPluginWithInvalidPlugin() throws PluginLoadingException {
        _loader.newPluginByName(PLUGIN_NAME_INVALID_PLUGIN);
    }

    @Test
    public void testNewPluginByNameWithoutArgs() throws PluginLoadingException {
        GPlazmaPlugin plugin = _loader.newPluginByName(PLUGIN_NAME_VALID_PLUGIN);
        assertNotNull("plugin is real instance", plugin);
    }

    @Test
    public void testNewPluginByNameTwice() throws PluginLoadingException {
        GPlazmaPlugin plugin1 = _loader.newPluginByName(PLUGIN_NAME_VALID_PLUGIN);
        GPlazmaPlugin plugin2 = _loader.newPluginByName(PLUGIN_NAME_VALID_PLUGIN);
        assertNotNull("plugin1 is real instance", plugin1);
        assertNotNull("plugin2 is real instance", plugin2);
        assertNotSame("newPluginByName called twice is the same plugin", plugin1, plugin2);
    }

    @Test
    public void testNewPluginWithArgs() throws PluginLoadingException {
        GPlazmaPlugin genericPlugin = _loader.newPluginByName(PLUGIN_NAME_VALID_PLUGIN,
              _properties);
        ValidPlugin plugin = (ValidPlugin) genericPlugin;
        Properties expected = new Properties();
        expected.put("arg-1", "value1");
        expected.put("arg-2", "value 2");
        assertEquals("plugin has same array", expected, plugin.getProperties());
    }

    @Test(expected = PluginLoadingException.class)
    public void testNewPluginByNameExceptionNoArgs() throws PluginLoadingException {
        _loader.newPluginByName(PLUGIN_NAME_EXCEPTION_PLUGIN);
    }


    @Test(expected = PluginLoadingException.class)
    public void testNewPluginByNameExceptionWrongArgs() throws PluginLoadingException {
        Properties properties = new Properties();
        properties.put("key1", "value1");
        _loader.newPluginByName(PLUGIN_NAME_EXCEPTION_PLUGIN, properties);
    }

    @Test
    public void testNewPluginByNameExceptionCorrectArgs() throws PluginLoadingException {
        Properties properties = new Properties();
        properties.put("key1", "value1");
        properties.put(ExceptionThrowingPlugin.REQUIRED_KEY, "some value");
        _loader.newPluginByName(PLUGIN_NAME_EXCEPTION_PLUGIN, properties);
    }


    // An invalid plugin: it's missing the Properties constructor
    public static class InvalidPlugin implements GPlazmaPlugin {
        // Invalid plugin has no correct constructor
    }

    // An valid plugin that throws an exception is a required argument isn't specified
    public static class ExceptionThrowingPlugin implements GPlazmaPlugin {

        public static final String REQUIRED_KEY = "required_key";

        public ExceptionThrowingPlugin(Properties properties) {
            checkArgument(properties.getProperty(REQUIRED_KEY) != null,
                  "Required property not present.");
        }
    }

    // A simple plugin that allows inspection of supplied arguments
    public static class ValidPlugin implements GPlazmaPlugin {

        private final Properties _properties;

        public ValidPlugin(Properties properties) {
            _properties = properties;
        }

        public Properties getProperties() {
            return _properties;
        }
    }
}

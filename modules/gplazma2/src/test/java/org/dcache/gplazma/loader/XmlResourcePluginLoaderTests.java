package org.dcache.gplazma.loader;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

import static org.junit.Assert.assertNotNull;

public class XmlResourcePluginLoaderTests
{
    private static final String PLUGIN_NAME = "example";

    Utf8DataClassLoader _classLoader;
    PluginLoader _loader;
    PluginXmlGenerator _pluginXml;

    @Before
    public void setup()
    {
        _classLoader = new Utf8DataClassLoader(XmlResourcePluginRepositoryFactory.RESOURCE_PATH);
        Thread currentThread = Thread.currentThread();
        currentThread.setContextClassLoader( _classLoader);

        _loader = XmlResourcePluginLoader.newPluginLoader();

        _pluginXml = new PluginXmlGenerator();
    }

    @Test(expected=IllegalStateException.class)
    public void testGetNoArgsWithoutInitFails() throws PluginLoadingException
    {
        _loader.newPluginByName("foo");
    }

    @Test(expected=IllegalStateException.class)
    public void testGetWithArgsWithoutInitFails() throws PluginLoadingException
    {
        _loader.newPluginByName("foo", new Properties());
    }

    @Test(expected=PluginLoadingException.class)
    public void testNoSuchPluginFails() throws PluginLoadingException
    {
        _loader.init();

        _loader.newPluginByName("foo");
    }

    @Test
    public void testExamplePluginNoArgs() throws PluginLoadingException
    {
        addPluginXmlResource();

        _loader.init();

        GPlazmaPlugin plugin = _loader.newPluginByName(PLUGIN_NAME);
        assertNotNull("plugin isn't null", plugin);
        ArgsAccessiblePlugin example = (ArgsAccessiblePlugin) plugin;
        example.assertArgumentsEqual(new Properties());
    }

    @Test
    public void testExamplePluginWithArgs() throws PluginLoadingException
    {
        addPluginXmlResource();

        _loader.init();

        Properties properties = new Properties();
        properties.put("key1","value1");
        properties.put("key2","value2");

        GPlazmaPlugin plugin = _loader.newPluginByName("example", properties);

        assertNotNull("plugin isn't null", plugin);

        ArgsAccessiblePlugin example = (ArgsAccessiblePlugin) plugin;
        example.assertArgumentsEqual(properties);
    }

    private void addPluginXmlResource()
    {
        _pluginXml.addPlugin( Collections.singleton( PLUGIN_NAME), ArgsAccessiblePlugin.class);
        _classLoader.addResource( _pluginXml);
    }

    /**
     * A dummy GPlazmaPlugin that stores the supplied arguments for later
     * checking.
     */
    public static class ArgsAccessiblePlugin implements GPlazmaPlugin
    {
        private final Properties _properties;

        public ArgsAccessiblePlugin(Properties properties) {
            _properties = properties;
        }

        public void assertArgumentsEqual(Properties properties) {
            Assert.assertEquals(_properties, properties);
        }
    }
}

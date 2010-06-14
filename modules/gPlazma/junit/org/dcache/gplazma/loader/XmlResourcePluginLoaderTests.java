package org.dcache.gplazma.loader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;

import javax.xml.parsers.ParserConfigurationException;

import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.junit.Before;
import org.junit.Test;

public class XmlResourcePluginLoaderTests {
    private static final String PLUGIN_NAME = "example";

    Utf8DataClassLoader _classLoader;
    PluginLoader _loader;
    PluginXmlGenerator _pluginXml;

    @Before
    public void setUp() throws ParserConfigurationException {
        _classLoader = new Utf8DataClassLoader(XmlResourcePluginRepositoryFactory.RESOURCE_PATH);
        Thread currentThread = Thread.currentThread();
        currentThread.setContextClassLoader( _classLoader);

        _loader = XmlResourcePluginLoader.newPluginLoader();

        _pluginXml = new PluginXmlGenerator();
    }

    @Test(expected=IllegalStateException.class)
    public void testGetNoArgsWithoutInitFails() {
        _loader.newPluginByName( "foo");
    }

    @Test(expected=IllegalStateException.class)
    public void testGetWithArgsWithoutInitFails() {
        _loader.newPluginByName( "foo", new String[]{"an argument"});
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNoSuchPluginFails() {
        _loader.init();

        _loader.newPluginByName("foo");
    }

    @Test
    public void testExamplePluginNoArgs() {
        addPluginXmlResource();

        _loader.init();

        GPlazmaPlugin plugin = _loader.newPluginByName(PLUGIN_NAME);
        assertNotNull("plugin isn't null", plugin);
        ArgsAccessiblePlugin example = (ArgsAccessiblePlugin) plugin;
        example.assertArgumentsEqual(new String[0]);
    }

    @Test
    public void testExamplePluginWithArgs() {
        addPluginXmlResource();

        _loader.init();

        String[] args = {"argument 1", "argument 2"};

        GPlazmaPlugin plugin = _loader.newPluginByName("example", args);

        assertNotNull("plugin isn't null", plugin);

        ArgsAccessiblePlugin example = (ArgsAccessiblePlugin) plugin;
        example.assertArgumentsEqual(args);
    }

    private void addPluginXmlResource() {
        _pluginXml.addPlugin( Collections.singleton( PLUGIN_NAME), ArgsAccessiblePlugin.class);
        _classLoader.addResource( _pluginXml);
    }

    /**
     * A dummy GPlazmaPlugin that stores the supplied arguments for later
     * checking.
     */
    public static class ArgsAccessiblePlugin implements GPlazmaPlugin {
        private String[] _args;

        public ArgsAccessiblePlugin(String[] args) {
            _args = args;
        }

        public void assertArgumentsEqual(String[] arguments) {
            assertArrayEquals("checking plugin arguments", arguments, _args);
        }
    }
}

package org.dcache.gplazma.loader;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.dcache.gplazma.plugins.GPlazmaPlugin;
import org.junit.Before;
import org.junit.Test;

public class XmlResourcePluginRepositoryFactoryTests {

    Utf8DataClassLoader _classLoader;
    XmlResourcePluginRepositoryFactory _factory;
    PluginXmlGenerator _pluginXml;

    @Before
    public void setUp() {
        ResourceBlockingClassLoader blockingLoader = new ResourceBlockingClassLoader();
        blockingLoader.setIsBlocking( true);

        _classLoader = new Utf8DataClassLoader(XmlResourcePluginRepositoryFactory.RESOURCE_PATH, blockingLoader);
        Thread currentThread = Thread.currentThread();
        currentThread.setContextClassLoader( _classLoader);

        _factory = new XmlResourcePluginRepositoryFactory();

        _pluginXml = new PluginXmlGenerator();
    }

    @Test
    public void testNoXml() {
        PluginRepository repository = _factory.newRepository();
        assertEquals("Check number of discovered plugins",0,repository.size());
    }

    @Test
    public void testSingleXmlNoPlugins() {
        _classLoader.addResource( _pluginXml);
        PluginRepository repository = _factory.newRepository();
        assertEquals("Check number of discovered plugins",0,repository.size());
    }

    @Test
    public void testSingleResourceWithSinglePlugin() {
        _pluginXml.addPlugin( Collections.singleton("foo"), DummyPlugin.class);
        _classLoader.addResource( _pluginXml);

        PluginRepository repository = _factory.newRepository();
        assertEquals("Check number of discovered plugins",1,repository.size());
    }

    @Test
    public void testSingleResourceWithTwoPlugins() {
        _pluginXml.addPlugin( Collections.singleton("foo"), DummyPlugin.class);
        _pluginXml.addPlugin( Collections.singleton("bar"), AnotherDummyPlugin.class);
        _classLoader.addResource( _pluginXml);

        PluginRepository repository = _factory.newRepository();
        assertEquals("Check number of discovered plugins",2,repository.size());
    }

    @Test
    public void testTwoResourcesWithSinglePlugin() {
        _pluginXml.addPlugin( Collections.singleton("foo"), DummyPlugin.class);
        _classLoader.addResource( _pluginXml);

        _pluginXml.clear();

        _pluginXml.addPlugin( Collections.singleton("bar"), AnotherDummyPlugin.class);
        _classLoader.addResource( _pluginXml);

        PluginRepository repository = _factory.newRepository();
        assertEquals("Check number of discovered plugins",2,repository.size());
    }

    public static class DummyPlugin implements GPlazmaPlugin {
        // dummy, empty plugin
    }

    public static class AnotherDummyPlugin implements GPlazmaPlugin {
        // dummy, empty plugin
    }
}

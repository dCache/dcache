package org.dcache.gplazma.loader;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

import static org.junit.Assert.*;

public class XmlParserTests {
    private static final DocumentBuilderFactory BUILDER_FACTORY =
            DocumentBuilderFactory.newInstance();

    private Document _emptyDocument;
    private PluginXmlGenerator _pluginXml;

    private Set<PluginMetadata> _plugins;

    @Before
    public void setUp() throws ParserConfigurationException {
        DocumentBuilder builder = BUILDER_FACTORY.newDocumentBuilder();
        _emptyDocument = builder.newDocument();
        _pluginXml = new PluginXmlGenerator();
    }

    @Test
    public void testEmpty() {
        // Note: as the XML is not well-formed, the support library generates
        // output on stderr. It's unclear how to suppress this.
        parseAndGetPlugins( _emptyDocument);
        assertEquals( "check plugins empty", _plugins, Collections.emptySet());
    }

    @Test
    public void testBrokenXml() {
        // Note: as the XML is not well-formed, the support library generates
        // output on stderr. It's unclear how to suppress this.
        parseAndGetPlugins( "<plugins>");
        assertEquals( "check plugins empty", _plugins, Collections.emptySet());
    }

    @Test
    public void testXmlWithExtraField() {
        parseAndGetPlugins( "<plugins><plugin><class>org.dcache.gplazma.loader.XmlParserTests$DummyPlugin</class><name>test</name><fruit>banana</fruit></plugin></plugins>");

        assertEquals( "check plugin count", 1, _plugins.size());
        PluginMetadata plugin = _plugins.iterator().next();

        assertPluginMetadata( plugin, Collections.singleton( "test"),
                DummyPlugin.class, "test", null);
    }

    @Test
    public void testOnlyRoot() {
        parseAndGetPlugins();
        assertEquals( "check plugins empty", _plugins, Collections.emptySet());
    }

    @Test
    public void testPluginsAndEmptyPlugin() {
        _pluginXml.addEmptyPlugin();

        parseAndGetPlugins();

        assertEquals( "check plugins empty", _plugins, Collections.emptySet());
    }

    @Test
    public void testPluginsAndValidPluginWithSingleName() {
        String pluginName = "foo";
        Set<String> names = Collections.singleton( pluginName);
        _pluginXml.addPlugin( names, DummyPlugin.class);

        parseAndGetPlugins();

        assertEquals( "check plugin count", 1, _plugins.size());

        PluginMetadata plugin = _plugins.iterator().next();

        assertPluginMetadata( plugin, names, DummyPlugin.class, pluginName,
                null);
    }

    @Test
    public void testPluginsAndValidPluginWithMultipleNames() {
        String pluginShortestName = "foo";
        Set<String> names =
                new HashSet<>( Arrays.asList( pluginShortestName,
                        "foo-o-matic", "original-foo"));

        _pluginXml.addPlugin( names, DummyPlugin.class);

        parseAndGetPlugins();

        assertEquals( "check plugin count", 1, _plugins.size());

        PluginMetadata plugin = _plugins.iterator().next();

        assertPluginMetadata( plugin, names, DummyPlugin.class,
                pluginShortestName, null);
    }

    @Test
    public void testPluginsAndTwoValidPlugins() {
        String plugin1Name = "foo";
        Set<String> plugin1Names = Collections.singleton( plugin1Name);
        _pluginXml.addPlugin( plugin1Names, DummyPlugin.class);

        String plugin2Name = "bar";
        Set<String> plugin2Names = Collections.singleton( plugin2Name);
        _pluginXml.addPlugin( plugin2Names, AnotherDummyPlugin.class);

        parseAndGetPlugins();

        assertEquals( "check plugin count", 2, _plugins.size());

        Map<String, PluginMetadata> results =
                new HashMap<>();
        for( PluginMetadata plugin : _plugins) {
            results.put( plugin.getShortestName(), plugin);
        }

        PluginMetadata plugin1Result = results.get( plugin1Name);
        assertPluginMetadata( plugin1Result, plugin1Names, DummyPlugin.class,
                plugin1Name, null);

        PluginMetadata plugin2Result = results.get( plugin2Name);
        assertPluginMetadata( plugin2Result, plugin2Names,
                AnotherDummyPlugin.class, plugin2Name, null);
    }

    @Test
    public void testPluginWithDefaultControl() {
        String pluginName = "foo";
        Set<String> names = Collections.singleton( pluginName);
        String defaultControl = "required";
        _pluginXml.addPlugin( names, DummyPlugin.class.getName(), defaultControl);

        parseAndGetPlugins();

        assertEquals( "check plugin count", 1, _plugins.size());

        PluginMetadata plugin = _plugins.iterator().next();

        assertPluginMetadata( plugin, names, DummyPlugin.class, pluginName,
                defaultControl);
    }

    @Test
    public void testTwoPluginsWithSameClassAcceptsNeither() {
        String className = DummyPlugin.class.getName();

        String plugin1Name = "foo";
        Set<String> plugin1Names = Collections.singleton( plugin1Name);
        _pluginXml.addPlugin( plugin1Names, className);

        String plugin2Name = "bar";
        Set<String> plugin2Names = Collections.singleton( plugin2Name);
        _pluginXml.addPlugin( plugin2Names, className);

        parseAndGetPlugins();

        assertEquals( "check plugin count", 0, _plugins.size());
    }

    @Test
    public void testThreePluginsWithSameClassAcceptsNone() {
        String className = DummyPlugin.class.getName();

        String plugin1Name = "foo";
        Set<String> plugin1Names = Collections.singleton( plugin1Name);
        _pluginXml.addPlugin( plugin1Names, className);

        String plugin2Name = "bar";
        Set<String> plugin2Names = Collections.singleton( plugin2Name);
        _pluginXml.addPlugin( plugin2Names, className);

        String plugin3Name = "baz";
        Set<String> plugin3Names = Collections.singleton( plugin3Name);
        _pluginXml.addPlugin( plugin3Names, className);

        parseAndGetPlugins();

        assertEquals( "check plugin count", 0, _plugins.size());
    }

    @Test
    public void testPluginWithTwoClassesNotAccepted() {
        String class1Name = DummyPlugin.class.getName();
        String class2Name = AnotherDummyPlugin.class.getName();

        String xmlData = String.format(  "<plugins><plugin>"
                                       + "<name>test</name>"
                                       + "<class>%s</class>"
                                       + "<class>%s</class>"
                                       + "</plugin></plugins>",
                                       class1Name, class2Name);
        parseAndGetPlugins( xmlData);

        assertEquals( "check plugin count", 0, _plugins.size());
    }

    /*
     * COMPOUND ASSERTIONS
     */

    private void assertPluginMetadata( PluginMetadata plugin,
                                       Set<String> assignedNames,
                                       Class<?> expectedClass,
                                       String expectedShortestName,
                                       String expectedDefaultControl) {
        assertNotNull( plugin);
        assertTrue( plugin.isValid());

        // We always add the class name to the list of names
        Set<String> expectedNames = new HashSet<>( assignedNames);
        expectedNames.add( expectedClass.getName());

        assertEquals( expectedNames, plugin.getPluginNames());
        assertEquals( expectedClass, plugin.getPluginClass());
        assertEquals( expectedShortestName, plugin.getShortestName());
        assertEquals( expectedDefaultControl, plugin.getDefaultControl());
    }

    /*
     * SUPPORT METHODS
     */


    private void parseAndGetPlugins() {
        String xmlData = _pluginXml.toString();
        parseAndGetPlugins( xmlData);
    }

    private void parseAndGetPlugins( Document document) {
        String xmlData = PluginXmlGenerator.documentAsString( document);
        parseAndGetPlugins( xmlData);
    }

    private void parseAndGetPlugins( String xmlData) {
        Reader xmlSrc = new StringReader( xmlData);
        XmlParser parser = new XmlParser( xmlSrc);
        parser.parse();
        _plugins = parser.getPlugins();
    }

    /**
     * Dummy implementation of a GPlazmaPlugin
     */
    public static final class DummyPlugin implements GPlazmaPlugin {
        // no content as the class isn't meant to be used.
    }

    /**
     * Another dummy implementation of a GPlazmaPlugin
     */
    public static final class AnotherDummyPlugin implements GPlazmaPlugin {
        // no content as the class isn't meant to be used.
    }

}

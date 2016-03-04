package org.dcache.gplazma.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.dcache.gplazma.plugins.GPlazmaPlugin;

/**
 * Class that parses the XML configuration file and returns useful
 * information. Plugin XML metadata has a root <tt>plugins</tt> element and
 * zero or more <tt>plugin</tt> child elements. Each <tt>plugin</tt> element
 * has metadata for that plugin as child elements of that <tt>plugin</tt>
 * element.
 * <p>
 * The following is a list of possible child elements (their local-name
 * values) along with the cardinality and a brief description.
 * <table>
 * <tr>
 * <th>local-name</th>
 * <th>cardinality</th>
 * <th>description</th>
 * </tr>
 * <tr>
 * <td>name</td>
 * <td>1..*</td>
 * <td>a name that may be used for this plugin. Names must be unique</td>
 * </tr>
 * <tr>
 * <td>class</td>
 * <td>1</td>
 * <td>the class name for this plugin (the result of
 * <code>Class.getName</code>)</td>
 * </tr>
 * <tr>
 * <td>default-control</td>
 * <td>0..1</td>
 * <td>currently unused</td>
 * </tr>
 * </table>
 *
 * The following is an example XML describing two plugins. One has a single
 * name, the other has two names.
 *
 * <pre>
 *     &lt;plugins>
 *         &lt;plugin>
 *             &lt;name>foo&lt;/name>
 *             &lt;class>org.dcache.foo.plugins.plugin1&lt;/class>
 *             &lt;default-control>required&lt;/default-control>
 *         &lt;/plugin>
 *         &lt;plugin>
 *             &lt;name>bar&lt;/name>
 *             &lt;name>bar-o-matic&lt;/name>
 *             &lt;class>org.dcache.foo.plugins.plugin2&lt;/class>
 *             &lt;default-control>optional&lt;/default-control>
 *         &lt;/plugin>
 *     &lt;/plugins>
 * </pre>
 *
 * The parser will generate a set of PluginMetadata objects, one for each
 * valid plugin described in the XML.
 */
public class XmlParser {

    private static final Logger LOGGER =
            LoggerFactory.getLogger( XmlParser.class);

    private static final XPathFactory XPATH_FACTORY =
            XPathFactory.newInstance();
    private static final String XPATH_EXPRESSION_PLUGINS_PLUGIN =
            "/plugins/plugin";

    /**
     * An enumeration of allowed XML elements that are children of the
     * <tt>/plugins/plugin</tt> elements. The enumeration also holds the
     * local-name of the elements and allows easy access to that information.
     */
    private enum XML_CHILD_NODE {
        NAME("name"), CLASS("class"), DEFAULT_CONTROL("default-control");

        private static Map<String, XML_CHILD_NODE> LOCAL_NAME_STORE =
                new HashMap<>();

        private String _localName;

        XML_CHILD_NODE( String localName) {
            _localName = localName;
        }

        public String getLocalName() {
            return _localName;
        }

        static {
            for( XML_CHILD_NODE node : XML_CHILD_NODE.values()) {
                LOCAL_NAME_STORE.put( node.getLocalName(), node);
            }
        }

        static boolean hasLocalName( String localName) {
            return LOCAL_NAME_STORE.containsKey( localName);
        }

        static XML_CHILD_NODE byLocalName( String localName) {
            return LOCAL_NAME_STORE.get( localName);
        }
    }

    private final InputSource _is;
    private final Set<PluginMetadata> _plugins = new HashSet<>();

    /**
     * If the same class is used by multiple plugin descriptions then we
     * cannot know which plugin description is correct. Because of this, we
     * remove all plugin descriptions that use this class.
     */
    private final Set<Class<? extends GPlazmaPlugin>> _bannedClasses =
            new HashSet<>();

    public XmlParser( Reader source) {
        _is = new InputSource( source);
    }

    public void parse() {
        LOGGER.debug( "starting parse");
        NodeList nodes = buildPluginNodeList();
        LOGGER.debug( "NodeList has {} entries", nodes.getLength());
        addPluginsFromNodeList( nodes);
        LOGGER.debug( "Created {} plugin metadata entries", _plugins.size());
    }

    public Set<PluginMetadata> getPlugins() {
        return Collections.unmodifiableSet( _plugins);
    }

    private NodeList buildPluginNodeList() {
        XPath xpath = XPATH_FACTORY.newXPath();

        XPathExpression expression;
        try {
            expression = xpath.compile( XPATH_EXPRESSION_PLUGINS_PLUGIN);
        } catch (XPathExpressionException e) {
            throw new RuntimeException( "Unable to compile XPath expression" +
                                        XPATH_EXPRESSION_PLUGINS_PLUGIN, e);
        }

        Object result;
        try {
            result = expression.evaluate( _is, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            Throwable genericCause = e.getCause();

            if( genericCause instanceof SAXParseException) {
                SAXParseException cause = (SAXParseException) genericCause;
                LOGGER.error( "Unable to parse plugin metadata: [{},{}] {}", cause.getLineNumber(), cause.getColumnNumber(),
                              cause.getMessage());
            } else {
                LOGGER.error( "Unable to parse plugin metadata: {}",
                        genericCause.getMessage());
            }
            return new EmptyNodeList();
        }

        return (NodeList) result;
    }

    private void addPluginsFromNodeList( NodeList nodes) {
        for( int i = 0; i < nodes.getLength(); i++) {
            Node item = nodes.item( i);
            tryToAddPluginFromNode( item);
        }
    }

    private void tryToAddPluginFromNode( Node pluginRootNode) {
        try {
            PluginMetadata plugin = processPluginNode( pluginRootNode);
            LOGGER.debug( "Adding plugin {}", plugin.getPluginNames());
            _plugins.add( plugin);
        } catch (IllegalArgumentException e) {
            LOGGER.error( "Unable register new plugin: {}", e.getMessage());
        }
    }

    private PluginMetadata processPluginNode( Node pluginRootNode) {
        PluginMetadata metadata = new PluginMetadata();
        addMetadataFromPluginNodeChildren( metadata, pluginRootNode);
        validMetadataGuard( metadata);
        return metadata;
    }

    private void validMetadataGuard( PluginMetadata metadata) {
        if( !metadata.isValid()) {
            throw new IllegalArgumentException( pluginName( metadata) +
                                                " metadata is incomplete");
        }

        Class<? extends GPlazmaPlugin> thisPluginClass =
                metadata.getPluginClass();
        LOGGER.debug( "examining plugin with class {}", thisPluginClass);

        if( removeIfClassAlreadyRegistered( thisPluginClass)) {
            _bannedClasses.add( thisPluginClass);

            throw new IllegalArgumentException( "Plugin '" +
                                                metadata.getShortestName() +
                                                "' uses class " +
                                                thisPluginClass.getName() +
                                                " which is already registered");
        }

        if( _bannedClasses.contains( thisPluginClass)) {
            throw new IllegalArgumentException( "Plugin '" +
                                                metadata.getShortestName() +
                                                "' uses class " +
                                                thisPluginClass.getName() +
                                                " which was used by another plugin");
        }
    }

    private boolean removeIfClassAlreadyRegistered( Class<? extends GPlazmaPlugin> pluginClass) {
        Iterator<PluginMetadata> itr = _plugins.iterator();

        while (itr.hasNext()) {
            PluginMetadata registeredPlugin = itr.next();
            Class<? extends GPlazmaPlugin> registeredPluginClass =
                    registeredPlugin.getPluginClass();

            LOGGER.debug("comparing plugin class {} against registered plugin with class {}",
                         pluginClass, registeredPluginClass);

            if( pluginClass.equals( registeredPluginClass)) {
                itr.remove();
                return true;
            }
        }

        return false;
    }

    private void addMetadataFromPluginNodeChildren( PluginMetadata metadata,
                                                    Node pluginRootNode) {
        NodeList childNodes = pluginRootNode.getChildNodes();

        for( int i = 0; i < childNodes.getLength(); i++) {
            Node childNode = childNodes.item( i);
            if( childNode.getNodeType() == Node.ELEMENT_NODE) {
                addMetadataFromChildNode( metadata, childNode);
            }
        }
    }

    private void addMetadataFromChildNode( PluginMetadata metadata, Node node) {
        String nodeName = node.getLocalName();
        String value = node.getTextContent();

        if( !XML_CHILD_NODE.hasLocalName( nodeName)) {
            LOGGER.warn( pluginName( metadata) + ": ignoring unknown field " +
                         nodeName);
            return;
        }

        switch (XML_CHILD_NODE.byLocalName( nodeName)) {
        case NAME:
            metadata.addName( value);
            break;
        case CLASS:
            metadata.setPluginClass( value);
            break;
        case DEFAULT_CONTROL:
            metadata.setDefaultControl( value);
            break;
        }
    }

    private String pluginName( PluginMetadata metadata) {
        String name;

        if( metadata.hasPluginName()) {
            name = "Plugin " + metadata.getShortestName();
        } else {
            name = "Plugin";
        }

        return name;
    }

    /**
     * A trivial implementation of NodeList that is always empty.
     */
    private static class EmptyNodeList implements NodeList {
        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public Node item( int index) {
            return null;
        }
    }
}

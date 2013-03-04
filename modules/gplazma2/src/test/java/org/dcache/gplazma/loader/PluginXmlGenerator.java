package org.dcache.gplazma.loader;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.StringWriter;
import java.util.Collections;
import java.util.Set;

/**
 * This class provides a handy way of generating XML that describes zero or
 * more plugins. Objects have initially no plugins, additional plugins may be
 * registered by using one of the {@link addPlugin} methods and {@link clear}
 * resets the generator to the initial state.
 */
public class PluginXmlGenerator {
    private static final DocumentBuilderFactory BUILDER_FACTORY =
            DocumentBuilderFactory.newInstance();
    private static final TransformerFactory TRANSFORMER_FACTORY =
            TransformerFactory.newInstance();

    private Document _document;
    private Node _pluginsElement;

    public static String documentAsString( Document document) {
        Source source = new DOMSource( document);

        StringWriter writer = new StringWriter();
        Result result = new StreamResult( writer);

        Transformer identityTransformer;
        try {
            identityTransformer = TRANSFORMER_FACTORY.newTransformer();
        } catch (TransformerConfigurationException e) {
            throw new RuntimeException(
                                        "Unable to create identity transformation",
                                        e);
        }

        try {
            identityTransformer.transform( source, result);
        } catch (TransformerException e) {
            throw new RuntimeException( "Identity transformation failed", e);
        }

        return writer.toString();
    }

    public PluginXmlGenerator() {
        clear();
    }

    /**
     * Reset generate to its initial, empty state
     */
    public void clear() {
        DocumentBuilder builder;
        try {
            builder = BUILDER_FACTORY.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException( "Failed to create XML builder", e);
        }
        _document = builder.newDocument();
        _pluginsElement = _document.createElement( "plugins");
        _document.appendChild( _pluginsElement);
    }

    public void addEmptyPlugin() {
        Set<String> names = Collections.emptySet();
        addPlugin( names);
    }

    public void addPlugin( Set<String> names) {
        addPlugin( names, (String) null);
    }

    public void addPlugin( Set<String> names, Class<?> pluginClass) {
        addPlugin( names, pluginClass.getName(), null);
    }

    public void addPlugin( Set<String> names, String pluginClass) {
        addPlugin( names, pluginClass, null);
    }

    public void addPlugin( Set<String> names, String pluginClass,
                           String defaultControl) {
        Node pluginNode = _document.createElement( "plugin");
        _pluginsElement.appendChild( pluginNode);

        for( String name : names) {
            addTextElement( pluginNode, "name", name);
        }

        if( pluginClass != null) {
            addTextElement( pluginNode, "class", pluginClass);
        }

        if( defaultControl != null) {
            addTextElement( pluginNode, "default-control", defaultControl);
        }
    }

    /**
     * Add an XML element like <pre>&lt;localName>contents&lt;/localName></pre>
     */
    private void addTextElement( Node parentNode, String localName,
                                 String contents) {
        Node childNode = _document.createElement( localName);
        Node textNode = _document.createTextNode( contents);
        childNode.appendChild( textNode);
        parentNode.appendChild( childNode);
    }

    @Override
    public String toString() {
        return documentAsString( _document);
    }
}

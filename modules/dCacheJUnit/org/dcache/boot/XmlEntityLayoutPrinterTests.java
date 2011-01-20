package org.dcache.boot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.dcache.util.ConfigurationProperties;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * A series of tests to verify that the XmlEntityLayoutPrinter provides
 * XML entity definitions that work as expected.
 */
public class XmlEntityLayoutPrinterTests {

    private static final String XML_ENCODING = "UTF-8";
    private static final String XPATH_EXPRESSION_FOR_TEST_ELEMENT = "/" + ParserContext.XML_TEST_ELEMENT_NAME;

    private static final String DEFAULT_PROPERTY_KEY = "default";
    private static final String DEFAULT_PROPERTY_VALUE = "default value";

    ConfigurationProperties globalProperties;
    Layout layout;

    @Before
    public void setUp() throws IOException {
        ConfigurationProperties defaults = new ConfigurationProperties( new Properties());
        defaults.setProperty(DEFAULT_PROPERTY_KEY, DEFAULT_PROPERTY_VALUE);

        layout = new Layout(defaults);
        globalProperties = layout.properties();
    }

    @Test
    public void testDefaultProperty() throws IOException {
        assertEntityHasValue(DEFAULT_PROPERTY_KEY, DEFAULT_PROPERTY_VALUE);
    }



    @Test
    public void testOverwriteDefaultProperty() throws IOException {
        String newValue = "a new value";
        globalProperties.setProperty(DEFAULT_PROPERTY_KEY, newValue);

        assertEntityHasValue(DEFAULT_PROPERTY_KEY, newValue);
    }

    @Test
    public void testPropertyValueWithQuotes() throws IOException {
        String key = "key";
        String value = "He said \"what?\" before leaving.";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
    }

    @Test
    public void testPropertyValueWithAmpersand() throws IOException {
        String key = "key";
        String value = "this&that";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
    }

    @Test
    public void testPropertyValueWithApostrophe() throws IOException {
        String key = "key";
        String value = "She said 'what?' before leaving.";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
    }

    @Test
    public void testPropertyValueWithLessThan() throws IOException {
        String key = "key";
        String value = "1 < 2";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
    }

    @Test
    public void testPropertyValueWithGreaterThan() throws IOException {
        String key = "key";
        String value = "2 > 1";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
    }

    @Test
    public void testPropertyNameWithDot() throws IOException {
        String key = "key.value";
        String value = "some information";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
        assertEntityNotDefined("key_value");
    }

    @Test
    public void testPropertyNameWithDash() throws IOException {
        String key = "key-value";
        String value = "some information";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
        assertEntityNotDefined("key_value");
    }

    @Test
    public void testPropertyNameWithDigits() throws IOException {
        String key = "key25";
        String value = "some information";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
        assertEntityNotDefined("key__");
    }

    @Test
    public void testPropertyNameWithUnderscore() throws IOException {
        String key = "interesting_key";
        String value = "some information";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
    }

    @Test
    public void testPropertyNameStartsWithUnderscore() throws IOException {
        String key = "_key";
        String value = "some information";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, value);
    }

    @Test
    public void testIllegalPropertyNameStartsWithDot() throws IOException {
        String key = ".key";
        String mappedKey = "_key";

        String value = "some information";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(mappedKey, value);
    }

    @Test
    public void testIllegalPropertyNameStartsWithDigit() throws IOException {
        String key = "25key";
        String mappedKey = "_5key";

        String value = "some information";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(mappedKey, value);
    }

    @Test
    public void testIllegalPropertyNameStartsWithDash() throws IOException {
        String key = "-key";
        String mappedKey = "_key";

        String value = "some information";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(mappedKey, value);
    }

    @Test
    public void testExpandingReference() throws IOException {
        String key = "key";
        String value = "${" + DEFAULT_PROPERTY_KEY + "}";

        globalProperties.setProperty(key, value);

        assertEntityHasValue(key, DEFAULT_PROPERTY_VALUE);
    }


    /*
     * Support methods and classes
     */

    private void assertEntityHasValue(String entityName, String expectedValue) {
        ParserContext context = new ParserContext(entityName);

        XPathExpression expression = buildExpression();

        String observedValue;
        try {
            Document doc = context.parse();
            observedValue = expression.evaluate(doc);
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        } catch (SAXException e) {
            fail(e.getMessage());
            return;
        }

        assertFalse(context.hasEntityResolvingError());
        assertEquals(expectedValue, observedValue);
    }

    private void assertEntityNotDefined(String entityName) throws IOException {
        ParserContext context = new ParserContext(entityName);
        XPathExpression expression = buildExpression();

        try {
            Document doc = context.parse();
            expression.evaluate(doc);
        } catch (SAXException e) {
            assertTrue( context.hasEntityResolvingError());
            return;
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }

        fail("Entity " + entityName + " was expanded successfully.");
    }

    private XPathExpression buildExpression() {
        XPath xpath = XPathFactory.newInstance().newXPath();

        try {
            return xpath.compile(XPATH_EXPRESSION_FOR_TEST_ELEMENT);
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
    }



    /**
     * A context for building and parsing a predefined XML data.  The
     * predefined XML tests contains a single XML element that encompasses
     * a single entity.  The document also includes the file with public ID of
     * <tt>-//dCache//ENTITIES dCache Properties//EN</tt>.  This allows the inclusion
     * of dCache property values as entities using a custom EntityResolver.
     * A custom ErrorHandler is used to catch potential problems expanding an
     * entity reference.
     * <p>
     * The overall effect is that calling {@link #parse} will build a Document
     * object that represents the value of a dCache property, if that property
     * exist; if the entity isn't defined then a SAXException is thrown.  The
     * {@link #hasEntityResolvingError} method returns true if the test
     * entity could not be resolved or false otherwise.
     */
    private class ParserContext {
        private static final String XML_TEST_ELEMENT_NAME = "test";

        private final DocumentBuilder _db;
        private final InputSource _source;
        private final RecordingErrorHandler _errorHandler;

        ParserContext(String entityName) {
            String data = buildTestXml(entityName);
            _source = new InputSource( new StringReader(data));
            try {
                _db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                throw new RuntimeException(e);
            }

            EntityResolver resolver = new DcachePropertiesEntityResolver();
            _db.setEntityResolver(resolver);

            _errorHandler = new RecordingErrorHandler(entityName);
            _db.setErrorHandler(_errorHandler);
        }

        /**
         * Build Document corresponding to:
         * <p>
         * <code>
         * &lt;test>&amp;entityName;&lt;/test>
         * </code>
         * with access to dCache properties as XML entities.
         */
        public Document parse() throws SAXException {
            try {
                return _db.parse(_source);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean hasEntityResolvingError() {
            return _errorHandler.hasEntityResolvingError();
        }

        private String buildTestXml(String entityName) {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(stream);

            out.println("<?xml version=\"1.0\" encoding=\"" + XML_ENCODING + "\"?>");
            out.println("<!DOCTYPE " + XML_TEST_ELEMENT_NAME + " [");
            out.println("<!ENTITY % properties-data PUBLIC \"" + DcachePropertiesEntityResolver.PUBLIC_NAME + "\" \"/\">");
            out.println("%properties-data;");
            out.println("]>");
            out.println("<" + XML_TEST_ELEMENT_NAME + ">&" + entityName + ";</" + XML_TEST_ELEMENT_NAME + ">");

            out.flush();

            try {
                return stream.toString(XML_ENCODING);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }




    /**
     * SAX ErrorHandler that remembers whether a document is invalid due to
     * an entity that cannot be resolved.  No output is emitted to stdout
     * or stderr.
     */
    private class RecordingErrorHandler implements ErrorHandler {
        private boolean _entityResolvingError;
        private final String _entityResolvingMessage;

        RecordingErrorHandler(String entityName) {
            _entityResolvingMessage = "The entity \"" + entityName +
                "\" was referenced, but not declared.";
        }


        public boolean hasEntityResolvingError() {
            return _entityResolvingError;
        }


        @Override
        public void error(SAXParseException exception) throws SAXException {
        }


        @Override
        public void fatalError(SAXParseException exception) throws SAXException {
            // This is ugly, but seems the only way to detect an entity resolving error.
            if( exception.getMessage().equals(_entityResolvingMessage)) {
                _entityResolvingError = true;
            }
        }

        @Override
        public void warning(SAXParseException exception) throws SAXException {
        }
    }

    /**
     * This class provides an alternative {@link EntityResolver} that may be
     * used for any XML SAX-based operation.
     * <p>
     * If the requested entity has a public ID of:
     * <p>
     * <tt>-//dCache//ENTITIES dCache Properties//EN</tt>
     * <p>
     * then the system ID is ignored and a list of auto-generated entities,
     * based on the {@link XmlEntityLayoutPrinter} class, is supplied.  All
     * other requests are resolved by the default handler, which will load
     * the corresponding files normally.
     */
    private class DcachePropertiesEntityResolver implements EntityResolver {
        public static final String PUBLIC_NAME = "-//dCache//ENTITIES dCache Properties//EN";

        private final EntityResolver _inner = new DefaultHandler();

        @Override
        public InputSource resolveEntity(String publicId, String systemId)
                throws SAXException, IOException {
            InputSource result;

            if( PUBLIC_NAME.equals(publicId)) {
                String xml = buildPropertiesFile();
                result = new InputSource(new StringReader(xml));
            } else {
                result = _inner.resolveEntity(publicId, systemId);
            }

            return result;
        }

        private String buildPropertiesFile() throws IOException {
            StringBuilder sb = new StringBuilder();
            sb.append("<?xml version=\"1.0\" encoding=\"" + XML_ENCODING + "\"?>\n");

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(out);

            LayoutPrinter printer = new XmlEntityLayoutPrinter(layout);
            printer.print(ps);
            ps.flush();

            sb.append(out.toString(XML_ENCODING));

            return sb.toString();
        }
    }
}

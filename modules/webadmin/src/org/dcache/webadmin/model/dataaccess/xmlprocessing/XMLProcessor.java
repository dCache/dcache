package org.dcache.webadmin.model.dataaccess.xmlprocessing;

import java.io.IOException;
import java.io.StringReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.w3c.dom.NodeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Object gets the information out of the delivered XML-Files and put it in
 * the appropriate businessobjects
 * @author jan schaefer
 */
public abstract class XMLProcessor {

    public static final String EMPTY_DOCUMENT_CONTENT =
            "<dCache xmlns='http://www.dcache.org/2008/01/Info'/>";
    protected static final String XPATH_PREDICATE_CLOSING_FRAGMENT = "']";
//  attributes
    protected static final String ATTRIBUTE_NAME = "name";
    protected static final String ATTRIBUTE_TYPE = "type";
    private XPathFactory _factory = XPathFactory.newInstance();
    private XPath _xpath = _factory.newXPath();
    private DocumentBuilderFactory _dbFactory;
    private DocumentBuilder _documentBuilder;
    private static final Logger _log = LoggerFactory.getLogger(XMLProcessor.class);

    public XMLProcessor() {
        try {
            _dbFactory = DocumentBuilderFactory.newInstance();
            _documentBuilder = _dbFactory.newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            _log.error("Couldn't get a document builder -- can't parse XMLs ", ex);
        }
    }

    /**
     * for an empty String an empty Document is returned
     * @param xmlcontent the xmlcontent in String Form
     * @return returns a dom-readable, normalised Document
     */
    public Document createXMLDocument(String xmlContent)
            throws ParsingException {
        try {
            if (_log.isDebugEnabled()) {
                _log.debug("xml-String received: {}", xmlContent);
            }
            String contentCopy = xmlContent;
            if (contentCopy.isEmpty()) {
                contentCopy = EMPTY_DOCUMENT_CONTENT;
            }
            InputSource inSource = new InputSource();
            inSource.setCharacterStream(new StringReader(contentCopy));
            Document dom = _documentBuilder.parse(inSource);
            dom.normalize();
            _log.debug("document created {}", dom.getNodeType());
            return dom;
        } catch (SAXException ex) {
            throw new ParsingException(ex);
        } catch (IOException ex) {
            throw new ParsingException(ex);
        }
    }

    protected String getStringFromXpath(String xpathExpression, Document document) {
        String result = "";
        try {
            result = (String) _xpath.evaluate(xpathExpression, document,
                    XPathConstants.STRING);
        } catch (XPathExpressionException ex) {
            _log.error("parsing error for xpath: {} - info provider up?", xpathExpression);
        }
        return result;
    }

    protected Long getLongFromXpath(String xpathExpression, Document document) {
        Long result = 0L;
        try {
            String xpathResult = (String) _xpath.evaluate(xpathExpression, document,
                    XPathConstants.STRING);

            result = Long.parseLong(xpathResult);
        } catch (XPathExpressionException ex) {
            _log.error("parsing error for xpath: {} - info provider up?", xpathExpression);
        } catch (NumberFormatException ex) {
            // ignore null return of xpath for numbers, most likely infoprovider
            // is not completly up yet
        }
        return result;
    }

    protected Boolean getBooleanFromXpath(String xpathExpression, Document document) {
        Boolean result = new Boolean(false);
        try {
            result = (Boolean) _xpath.evaluate(xpathExpression, document,
                    XPathConstants.BOOLEAN);
        } catch (XPathExpressionException ex) {
            _log.error("parsing error for xpath: {} - info provider up?", xpathExpression);
        }
        return result;
    }

    protected NodeList getNodesFromXpath(String xpathExpression, Document document) {
        NodeList result = null;
        try {
            result = (NodeList) _xpath.evaluate(xpathExpression, document,
                    XPathConstants.NODESET);

        } catch (XPathExpressionException e) {
            _log.debug("couldn't retrieve nodelist for: {}", xpathExpression);
        }
        return result;
    }
}

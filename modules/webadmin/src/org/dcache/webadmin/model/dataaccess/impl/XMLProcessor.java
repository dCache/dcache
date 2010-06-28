package org.dcache.webadmin.model.dataaccess.impl;

import java.io.IOException;
import java.io.StringReader;
import java.util.Set;
import java.util.HashSet;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Object gets the information out of the delivered XML-Files and put it in
 * the appropriate businessobjects
 * @author jan schaefer
 */
public class XMLProcessor {

    public static final String EMPTY_DOCUMENT_CONTENT =
            "<dCache xmlns='http://www.dcache.org/2008/01/Info'/>";
    private static final String CLOSING_FRAGMENT = "']";
    private static final String SPECIALCELL_FRAGMENT = "/dCache/domains/domain" +
            "[@name='dCacheDomain']/routing/named-cells/cell[@name='";
    private static final String SPECIALPOOL_FRAGMENT = "/dCache/pools/pool[@name='";
    private static final String ALL_POOLNODES = "/dCache/pools/pool";
    private static final String ALL_CELLNODES = "/dCache/domains/domain" +
            "[@name='dCacheDomain']/routing/named-cells/cell";
//   the equivalent for each NamedCellmember in the InfoproviderXML
//   to the NamedCell class
    private static final String NAMEDCELLMEMBER_DOMAIN = "/domainref/@name";
//   the equivalent for each Poolmember in the InfoproviderXML
//   to the Pool class
    private static final String POOLMEMBER_ENABLED = "/metric[@name='enabled']";
    private static final String POOLMEMBER_FREE_SPACE = "/space/metric[@name='free']";
    private static final String POOLMEMBER_TOTAL_SPACE = "/space/metric[@name='total']";
    private static final String POOLMEMBER_PRECIOUS_SPACE = "/space/metric[@name='precious']";
    private static final String POOLMEMBER_USED_SPACE = "/space/metric[@name='used']";
//  attributes
    private static final String ATTRIBUTE_NAME = "name";
    private XPathFactory _factory = XPathFactory.newInstance();
    private XPath _xpath = _factory.newXPath();
    private DocumentBuilderFactory _dbFactory;
    private DocumentBuilder _documentBuilder;
    private Logger _log = LoggerFactory.getLogger(XMLProcessor.class);

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
                _log.debug("xml-String received: " + xmlContent);
            }
            String contentCopy = xmlContent;
            if (contentCopy.isEmpty()) {
                contentCopy = EMPTY_DOCUMENT_CONTENT;
            }
            InputSource inSource = new InputSource();
            inSource.setCharacterStream(new StringReader(contentCopy));
            Document dom = _documentBuilder.parse(inSource);
            dom.normalize();
            _log.debug("document created " + dom.getNodeType());
            return dom;
        } catch (SAXException ex) {
            throw new ParsingException(ex);
        } catch (IOException ex) {
            throw new ParsingException(ex);
        }
    }

    /**
     * @param document document to parse
     * @return Set of NamedCell objects parsed out of the document
     */
    public Set<NamedCell> parseNamedCellsDocument(Document document) {
        assert document != null;
        Set<NamedCell> namedCells = new HashSet<NamedCell>();
        // get a nodelist of all cell Elements
        NodeList cellNodes = getNodesFromXpath(ALL_CELLNODES, document);
        if (cellNodes != null) {
            for (int cellIndex = 0; cellIndex < cellNodes.getLength(); cellIndex++) {
                Element currentCellNode = (Element) cellNodes.item(cellIndex);
                NamedCell namedCellEntry = createNamedCell(document,
                        currentCellNode.getAttribute(ATTRIBUTE_NAME));
                _log.debug("Named Cell parsed: " + namedCellEntry.getCellName());
                namedCells.add(namedCellEntry);
            }
        }
        return namedCells;
    }

    private NamedCell createNamedCell(Document document, String cellName) {
        NamedCell namedCellEntry = new NamedCell();
        namedCellEntry.setCellName(cellName);
        try {
            String xpathExpression = SPECIALCELL_FRAGMENT + cellName + CLOSING_FRAGMENT +
                    NAMEDCELLMEMBER_DOMAIN;
            namedCellEntry.setDomainName((String) _xpath.evaluate(xpathExpression, document,
                    XPathConstants.STRING));
        } catch (XPathExpressionException ex) {
            _log.error("parsing error cellname {}", cellName);
        }
        return namedCellEntry;
    }

    /**
     * @param document document to parse
     * @return Set of pool objects parsed out of the document
     */
    public Set<Pool> parsePoolsDocument(Document document) {
        assert document != null;
        Set<Pool> pools = new HashSet<Pool>();
        // get a nodelist of all pool Elements
        NodeList poolNodes = getNodesFromXpath(ALL_POOLNODES, document);
        if (poolNodes != null) {
            for (int poolIndex = 0; poolIndex < poolNodes.getLength(); poolIndex++) {
                Element currentPoolNode = (Element) poolNodes.item(poolIndex);
                Pool poolEntry = createPool(document,
                        currentPoolNode.getAttribute(ATTRIBUTE_NAME));

                _log.debug("Pool parsed: " + poolEntry.getName());
                pools.add(poolEntry);
            }
        }
        return pools;
    }

    private Pool createPool(Document document, String poolName) {
        Pool pool = new Pool();
        pool.setName(poolName);
        pool.setEnabled(getBooleanFromXpath(buildPoolXpathExpression(
                POOLMEMBER_ENABLED, poolName), document));
        pool.setTotalSpace(getLongFromXpath(buildPoolXpathExpression(
                POOLMEMBER_TOTAL_SPACE, poolName), document));
        pool.setFreeSpace(getLongFromXpath(buildPoolXpathExpression(
                POOLMEMBER_FREE_SPACE, poolName), document));
        pool.setPreciousSpace(getLongFromXpath(buildPoolXpathExpression(
                POOLMEMBER_PRECIOUS_SPACE, poolName), document));
        pool.setUsedSpace(getLongFromXpath(buildPoolXpathExpression(
                POOLMEMBER_USED_SPACE, poolName), document));
        return pool;
    }

    private String buildPoolXpathExpression(String metric, String poolName) {
        return SPECIALPOOL_FRAGMENT + poolName + CLOSING_FRAGMENT + metric;
    }

    private long getLongFromXpath(String xpathExpression, Document document) {
        long result = 0L;
        try {
            String xpathResult = (String) _xpath.evaluate(xpathExpression, document,
                    XPathConstants.STRING);

            result = Long.parseLong(xpathResult);
        } catch (XPathExpressionException ex) {
            _log.error("parsing error for xpath: {}", xpathExpression);
        } catch (NumberFormatException ex) {
            // ignore null return of xpath for numbers, most likely infoprovider
            // is not completly up yet
        }
        return result;
    }

    private Boolean getBooleanFromXpath(String xpathExpression, Document document) {
        Boolean result = new Boolean(false);
        try {
            result = (Boolean) _xpath.evaluate(xpathExpression, document,
                    XPathConstants.BOOLEAN);
        } catch (XPathExpressionException ex) {
            _log.error("parsing error for xpath: {}", xpathExpression);
        }
        return result;
    }

    private NodeList getNodesFromXpath(String xpathExpression, Document document) {
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

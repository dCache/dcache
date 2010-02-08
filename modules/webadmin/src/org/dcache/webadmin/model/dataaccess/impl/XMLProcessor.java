package org.dcache.webadmin.model.dataaccess.impl;

import java.io.IOException;
import java.io.StringReader;
import java.util.Set;
import java.util.HashSet;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.log4j.Logger;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * This Object gets the information out of the delivered XML-Files and put it in
 * the appropriate businessobjects
 * @author jan schaefer
 */
public class XMLProcessor {

    public static final String EMPTY_DOCUMENT_CONTENT =
            "<dCache xmlns='http://www.dcache.org/2008/01/Info'/>";
//   the equivalent for each Poolmember in the InfoproviderXML
//   to the Pool class
    private final String POOLMEMBER_NAME = "name";
    private final String POOLMEMBER_ENABLED = "enabled";
    private final String POOLMEMBER_FREE_SPACE = "free";
    private final String POOLMEMBER_TOTAL_SPACE = "total";
    private final String POOLMEMBER_PRECIOUS_SPACE = "precious";
    private final String POOLMEMBER_USED_SPACE = "used";
//    heavily used attributes and tags
    private final String ATTRIBUTE_NAME = "name";
    private final String TAG_METRIC = "metric";
    private final String TAG_POOL = "pool";
    private final String TAG_CELL = "cell";
    private final String TAG_DOMAINREF = "domainref";
    private DocumentBuilderFactory _dbFactory;
    private DocumentBuilder _documentBuilder;
    private Logger _log = Logger.getLogger(XMLProcessor.class);

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
     * @return Set of pool objects parsed out of the document
     */
    public Set<Pool> parsePoolsDocument(Document document) {
// structure should be like that
//  <pools>
//    <pool name="myFirstPool">
//      <metric name="enabled" type="boolean">true</metric>
//      ...
//      <queues>
//        <queue type="store">
//          <metric name="max-active" type="integer">0</metric>
//          ...
//        </queue>
//          ...
//      </queues>
//      <space>
//        <metric name="total" type="integer">2147483648</metric>
//        ...
//      </space>
//      <poolgroups>
//        <poolgroupref name="default"/>
//         ...
//      </poolgroups>
//    </pool>
//  ...
//  </pools>
        assert document != null;
        Set<Pool> pools = new HashSet<Pool>();

        Element root = document.getDocumentElement();
        // get a nodelist of all pool Elements
        NodeList poolNodes = root.getElementsByTagName(TAG_POOL);

        for (int poolIndex = 0; poolIndex < poolNodes.getLength(); poolIndex++) {
            Element currentPoolNode = (Element) poolNodes.item(poolIndex);

            Pool poolEntry = createPool(currentPoolNode);
            _log.debug("Pool parsed: " + poolEntry.getName());
            pools.add(poolEntry);
        }

        return pools;
    }

    private Pool createPool(Element PoolNode) {
        Pool poolEntry = new Pool();

        poolEntry.setName(PoolNode.getAttribute(POOLMEMBER_NAME));
        NodeList metricNodes = PoolNode.getElementsByTagName(TAG_METRIC);
        _log.debug("Number of metrics: " + metricNodes.getLength());

        for (int metricIndex = 0; metricIndex < metricNodes.getLength(); metricIndex++) {
            Element currentMetric = (Element) metricNodes.item(metricIndex);
            updatePoolFromMetricValue(poolEntry, currentMetric);
        }

        return poolEntry;
    }

    private void updatePoolFromMetricValue(Pool pool, Element metricElement) {

        String nodeValue = metricElement.getFirstChild().getNodeValue();
        _log.debug(metricElement.getAttributeNode(ATTRIBUTE_NAME));
        _log.debug(metricElement.getFirstChild().getNodeValue());
        if (isMetricNameMatching(metricElement, POOLMEMBER_ENABLED)) {
            pool.setEnabled(Boolean.parseBoolean(nodeValue));
            return;
        }

        if (isMetricNameMatching(metricElement, POOLMEMBER_TOTAL_SPACE)) {
            pool.setTotalSpace(Long.parseLong(nodeValue));
            return;
        }

        if (isMetricNameMatching(metricElement, POOLMEMBER_FREE_SPACE)) {
            pool.setFreeSpace(Long.parseLong(nodeValue));
            return;
        }

        if (isMetricNameMatching(metricElement, POOLMEMBER_PRECIOUS_SPACE)) {
            pool.setPreciousSpace(Long.parseLong(nodeValue));
            return;
        }
        if (isMetricNameMatching(metricElement, POOLMEMBER_USED_SPACE)) {
            pool.setUsedSpace(Long.parseLong(nodeValue));
            return;
        }
    }

    private boolean isMetricNameMatching(Element actualMetric, String poolMember) {
        return actualMetric.getAttribute(ATTRIBUTE_NAME).equals(poolMember);
    }

    /**
     * @param document document to parse
     * @return Set of NamedCell objects parsed out of the document
     */
    public Set<NamedCell> parseNamedCellsDocument(Document document) {
//       known xml structure like that:
//        <cell name="PinManager">
//            <domainref name="utilityDomain"/>
//        </cell>
//        <cell name="broadcast">
//            <domainref name="dCacheDomain"/>
//        </cell>
        assert document != null;
        Set<NamedCell> namedCells = new HashSet<NamedCell>();

        // get the root element
        Element root = document.getDocumentElement();
        // get a nodelist of all cell Elements
        NodeList cellNodes = root.getElementsByTagName(TAG_CELL);

        for (int cellIndex = 0; cellIndex < cellNodes.getLength(); cellIndex++) {
            Element currentCellNode = (Element) cellNodes.item(cellIndex);

            NamedCell namedCellEntry = createNamedCell(currentCellNode);
            _log.debug("Named Cell parsed: " + namedCellEntry.getCellName());
            namedCells.add(namedCellEntry);
        }

        return namedCells;
    }

    private NamedCell createNamedCell(Element actualCellNode) {
        NamedCell namedCellEntry = new NamedCell();

        namedCellEntry.setCellName(actualCellNode.getAttribute(ATTRIBUTE_NAME));
        NodeList domainrefNode = actualCellNode.getElementsByTagName(TAG_DOMAINREF);
        Element domainrefElement = (Element) domainrefNode.item(0);
        namedCellEntry.setDomainName(domainrefElement.getAttribute(ATTRIBUTE_NAME));

        return namedCellEntry;
    }
}

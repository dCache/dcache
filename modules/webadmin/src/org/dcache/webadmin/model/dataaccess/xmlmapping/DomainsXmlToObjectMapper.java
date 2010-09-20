package org.dcache.webadmin.model.dataaccess.xmlmapping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 *
 * @author jans
 */
public class DomainsXmlToObjectMapper extends XmlToObjectMapper {

    private static final Logger _log = LoggerFactory.getLogger(
            DomainsXmlToObjectMapper.class);
    private static final String ALL_CELLNODES = "/dCache/domains/domain" +
            "[@name='dCacheDomain']/routing/named-cells/cell";
    private static final String ALL_DOMAINNODES = "/dCache/domains/domain";
    private static final String ALL_DOORNODES = "/dCache/doors/door";
    private static final String ALL_POOLNODES = "/dCache/pools/pool";
    private static final String SPECIAL_DOMAIN_FRAGMENT = "/dCache/domains/domain[@name='";
    private static final String ALL_CELLS_OF_DOMAIN = "/cells/cell";
    private static final String SPECIAL_CELL_FRAGMENT = "/dCache/domains/domain" +
            "[@name='dCacheDomain']/routing/named-cells/cell[@name='";
    private static final String SPECIAL_CELL_OF_DOMAIN_FRAGMENT = "/cells/cell[@name='";
//    cell members
    private static final String CELLMEMBER_THREADCOUNT = "/metric[@name='thread-count']";
    private static final String CELLMEMBER_EVENTQUEUE = "/metric[@name='event-queue-size']";
    private static final String CELLMEMBER_DATETIME = "/created/metric[@name='simple']";
    private static final String CELLMEMBER_REVISION = "/version/metric[@name='revision']";
    private static final String CELLMEMBER_RELEASE = "/version/metric[@name='release']";
    private static final String[] STANDARD_DESIRED_NAMES = new String[]{"PoolManager",
        "srm-LoginBroker", "gPlazma", "LoginBroker", "PnfsManager"};
//   the equivalent for each NamedCellmember in the InfoproviderXML
//   to the NamedCell class
    private static final String NAMEDCELLMEMBER_DOMAIN = "/domainref/@name";
    private Set<String> _cellServicesToParse;

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
                _log.debug("Named Cell parsed: {}", namedCellEntry.getCellName());
                namedCells.add(namedCellEntry);
            }
        }
        return namedCells;
    }

    private NamedCell createNamedCell(Document document, String cellName) {
        NamedCell namedCellEntry = new NamedCell();
        namedCellEntry.setCellName(cellName);
        String xpathExpression = SPECIAL_CELL_FRAGMENT + cellName + XPATH_PREDICATE_CLOSING_FRAGMENT +
                NAMEDCELLMEMBER_DOMAIN;
        namedCellEntry.setDomainName(getStringFromXpath(xpathExpression, document));
        return namedCellEntry;
    }

    public Set<String> parseDoorList(Document document) {
        assert document != null;
        Set<String> doors = new HashSet<String>();
        NodeList doorNodes = getNodesFromXpath(ALL_DOORNODES, document);
        if (doorNodes != null) {
            for (int doorIndex = 0; doorIndex < doorNodes.getLength(); doorIndex++) {
                Element currentDoor = (Element) doorNodes.item(doorIndex);
                doors.add(currentDoor.getAttribute(ATTRIBUTE_NAME));
            }
        }
        return doors;
    }

    public Set<String> parsePoolsList(Document document) {
        assert document != null;
        Set<String> pools = new HashSet<String>();
        NodeList poolNodes = getNodesFromXpath(ALL_POOLNODES, document);
        if (poolNodes != null) {
            for (int poolIndex = 0; poolIndex < poolNodes.getLength(); poolIndex++) {
                Element currentPool = (Element) poolNodes.item(poolIndex);
                pools.add(currentPool.getAttribute(ATTRIBUTE_NAME));
            }
        }
        return pools;
    }

    public Set<CellStatus> parseDomainsDocument(Set<String> doors,
            Set<String> pools, Document document) {
        assert document != null;
        Set<CellStatus> cellStates = new HashSet<CellStatus>();
        // get all domainnames
        NodeList domainNodes = getNodesFromXpath(ALL_DOMAINNODES, document);
        if (domainNodes != null) {
            createDoorStatuses(cellStates, doors, document);
            buildWellKnownCellsToParseFor(pools);
            for (int domainIndex = 0; domainIndex < domainNodes.getLength(); domainIndex++) {
                Element currentNode = (Element) domainNodes.item(domainIndex);
                String domainName = currentNode.getAttribute(ATTRIBUTE_NAME);
                checkAllCellNamesPerDomain(cellStates, domainName, document);
            }
        }
        return cellStates;
    }

    private void checkAllCellNamesPerDomain(Set<CellStatus> cellStates,
            String domain, Document document) {
        NodeList cellNodes = getAllCellNodesForDomain(domain, document);
        if (cellNodes != null) {
            for (int cellIndex = 0; cellIndex < cellNodes.getLength(); cellIndex++) {
                Element currentCell = (Element) cellNodes.item(cellIndex);
                String cellName = currentCell.getAttribute(ATTRIBUTE_NAME);
                if (isDesiredName(cellName)) {
                    cellStates.add(createCellStatus(cellName, domain, document));
                }
            }
        }
    }

    private NodeList getAllCellNodesForDomain(String domain, Document document) {
        String xpathExpression = buildXpathForParticularDomain(domain) +
                ALL_CELLS_OF_DOMAIN;
        return getNodesFromXpath(xpathExpression, document);
    }

    private boolean isDesiredName(String cellName) {
        return _cellServicesToParse.contains(cellName);
    }

    private CellStatus createCellStatus(String cellName, String domain,
            Document document) {
        CellStatus cell = new CellStatus();
        cell.setName(cellName);
        cell.setDomainName(domain);
        cell.setCreatedDateTime(getStringFromXpath(
                buildXpathForElementOfCell(CELLMEMBER_DATETIME,
                cellName, domain), document));
        cell.setEventQueueSize(getLongFromXpath(
                buildXpathForElementOfCell(CELLMEMBER_EVENTQUEUE,
                cellName, domain), document).intValue());
//        cell.setPing(); not provided by info yet FIXME
        cell.setThreadCount(getLongFromXpath(
                buildXpathForElementOfCell(CELLMEMBER_THREADCOUNT,
                cellName, domain), document).intValue());
        String release = getStringFromXpath(
                buildXpathForElementOfCell(CELLMEMBER_RELEASE,
                cellName, domain), document);
        String revision = getStringFromXpath(
                buildXpathForElementOfCell(CELLMEMBER_REVISION,
                cellName, domain), document);
        cell.setVersion(release + "(" + revision + ")");
        return cell;
    }

    private String buildXpathForParticularDomain(String domain) {
        return SPECIAL_DOMAIN_FRAGMENT + domain + XPATH_PREDICATE_CLOSING_FRAGMENT;
    }

    private String buildXpathForParticularCell(String cell, String domain) {
        return buildXpathForParticularDomain(domain) +
                SPECIAL_CELL_OF_DOMAIN_FRAGMENT + cell + XPATH_PREDICATE_CLOSING_FRAGMENT;
    }

    private String buildXpathForElementOfCell(String element,
            String cell, String domain) {
        return buildXpathForParticularCell(cell, domain) + element;
    }

    private void createDoorStatuses(Set<CellStatus> cellStates, Set<String> doors,
            Document document) {
        for (String door : doors) {
            String[] temp = door.split("@");
            if (temp.length == 2) {
                String doorName = temp[0];
                String doorDomain = temp[1];
                cellStates.add(createCellStatus(doorName, doorDomain, document));
            }
        }
    }

    /*
     * important to be initialised per request on parseDomainsDocument to provide
     * the correct list of cellnames
     */
    private void buildWellKnownCellsToParseFor(Set<String> pools) {
        _cellServicesToParse = new HashSet<String>();
        _cellServicesToParse.addAll(pools);
        _cellServicesToParse.addAll((Arrays.asList(STANDARD_DESIRED_NAMES)));
    }

    public Map<String, List<String>> parseDomainsMapDocument(Document document) {
        assert document != null;
        Map<String, List<String>> domainMap = new HashMap<String, List<String>>();
        NodeList domainNodes = getNodesFromXpath(ALL_DOMAINNODES, document);
        if (domainNodes != null) {
            for (int domainIndex = 0; domainIndex < domainNodes.getLength(); domainIndex++) {
                Element currentNode = (Element) domainNodes.item(domainIndex);
                String domainName = currentNode.getAttribute(ATTRIBUTE_NAME);
                List<String> cells = createCells(domainName, document);
                domainMap.put(domainName, cells);
            }
        }
        return domainMap;
    }

    private List<String> createCells(String domainName, Document document) {
        List<String> cells = new ArrayList<String>();
        NodeList cellNodes = getAllCellNodesForDomain(domainName, document);
        if (cellNodes != null) {
            for (int cellIndex = 0; cellIndex < cellNodes.getLength(); cellIndex++) {
                Element currentCellNode = (Element) cellNodes.item(cellIndex);
                cells.add(currentCellNode.getAttribute(ATTRIBUTE_NAME));
            }
        }
        return cells;
    }
}



package org.dcache.webadmin.model.dataaccess.xmlprocessing;

import java.util.Set;
import java.util.HashSet;
import org.dcache.webadmin.model.businessobjects.MoverQueue;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.w3c.dom.Document;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class PoolXMLProcessor extends XMLProcessor {

    private static final String CLOSING_FRAGMENT = "']";
    private static final String SPECIAL_CELL_FRAGMENT = "/dCache/domains/domain" +
            "[@name='dCacheDomain']/routing/named-cells/cell[@name='";
    private static final String SPECIAL_POOL_FRAGMENT = "/dCache/pools/pool[@name='";
    private static final String SPECIAL_QUEUE_FRAGMENT = "/queues/queue[@type='";
    private static final String SPECIAL_NAMEDQUEUE_FRAGMENT = "/queues/named-queues/queue[@name='";
    private static final String ALL_POOLNODES = "/dCache/pools/pool";
    private static final String ALL_CELLNODES = "/dCache/domains/domain" +
            "[@name='dCacheDomain']/routing/named-cells/cell";
    private static final String ALL_QUEUES_OF_POOL = "/queues/queue";
    private static final String ALL_NAMEDQUEUES_OF_POOL = "/queues/named-queues/queue";
    private static final String ALL_POOLGROUPS_OF_POOL = "/poolgroups/poolgroupref";
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
    private static final String QUEUE_ACTIVE_FRAGMENT = "/metric[@name='active']/text()";
    private static final String QUEUE_MAX_FRAGMENT = "/metric[@name='max-active']/text()";
    private static final String QUEUE_QUEUED_FRAGMENT = "/metric[@name='queued']/text()";
    private static final Logger _log = LoggerFactory.getLogger(PoolXMLProcessor.class);

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
        String xpathExpression = SPECIAL_CELL_FRAGMENT + cellName + CLOSING_FRAGMENT +
                NAMEDCELLMEMBER_DOMAIN;
        namedCellEntry.setDomainName(getStringFromXpath(xpathExpression, document));
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

                _log.debug("Pool parsed: {}", poolEntry.getName());
                pools.add(poolEntry);
            }
        }
        return pools;
    }

    private Pool createPool(Document document, String poolName) {
        Pool pool = new Pool();
        pool.setName(poolName);
        pool.setEnabled(getBooleanFromXpath(buildPoolXpathExpression(poolName,
                POOLMEMBER_ENABLED), document));
        pool.setTotalSpace(getLongFromXpath(buildPoolXpathExpression(poolName,
                POOLMEMBER_TOTAL_SPACE), document));
        pool.setFreeSpace(getLongFromXpath(buildPoolXpathExpression(poolName,
                POOLMEMBER_FREE_SPACE), document));
        pool.setPreciousSpace(getLongFromXpath(buildPoolXpathExpression(poolName,
                POOLMEMBER_PRECIOUS_SPACE), document));
        pool.setUsedSpace(getLongFromXpath(buildPoolXpathExpression(poolName,
                POOLMEMBER_USED_SPACE), document));
        getPoolMovers(document, pool);
        getPoolGroups(document, pool);
        return pool;
    }

    private void getMoversForQueueType(String nodeFragment, String attribute,
            String specialQueueFragment, Pool pool, Document document) {
        //get all queue nodes
        NodeList queueNodes = getNodesFromXpath(
                buildPoolXpathExpression(pool.getName(), nodeFragment),
                document);
        //get all values into a moverqueue with name=attribute.value
        if (queueNodes != null) {
            for (int i = 0; i < queueNodes.getLength(); i++) {
                Element currentQueueNode = (Element) queueNodes.item(i);
                String queue = currentQueueNode.getAttribute(attribute);
                String metric = specialQueueFragment + queue + CLOSING_FRAGMENT;
                MoverQueue queueEntry = getMoverQueue(buildPoolXpathExpression(
                        pool.getName(), metric), document);
                queueEntry.setName(queue);
                pool.addMoverQueue(queueEntry);
            }
        }

    }

    private void getPoolGroups(Document document, Pool pool) {
        //get all poolgroupref nodes
        NodeList poolGroupNodes = getNodesFromXpath(
                buildPoolXpathExpression(pool.getName(), ALL_POOLGROUPS_OF_POOL),
                document);
        //add a poolgroup per node with name=name.value
        if (poolGroupNodes != null) {
            for (int i = 0; i < poolGroupNodes.getLength(); i++) {
                Element currentGroupNode = (Element) poolGroupNodes.item(i);
                String queue = currentGroupNode.getAttribute(ATTRIBUTE_NAME);
                pool.addPoolGroup(queue);
            }
        }
    }

    private void getPoolMovers(Document document, Pool pool) {
//      get all queues
        getMoversForQueueType(ALL_QUEUES_OF_POOL, ATTRIBUTE_TYPE,
                SPECIAL_QUEUE_FRAGMENT, pool, document);
//      get all named-queues
        getMoversForQueueType(ALL_NAMEDQUEUES_OF_POOL, ATTRIBUTE_NAME,
                SPECIAL_NAMEDQUEUE_FRAGMENT, pool, document);
    }

    private String buildPoolXpathExpression(String poolName, String metric) {
        return SPECIAL_POOL_FRAGMENT + poolName + CLOSING_FRAGMENT + metric;
    }

    private MoverQueue getMoverQueue(String queue, Document document) {
        MoverQueue moverQueue = new MoverQueue();
        String maxMetric = queue + QUEUE_MAX_FRAGMENT;
        moverQueue.setMax(getLongFromXpath(maxMetric, document).intValue());
        String queuedMetric = queue + QUEUE_QUEUED_FRAGMENT;
        moverQueue.setQueued(getLongFromXpath(queuedMetric, document).intValue());
        String activeMetric = queue + QUEUE_ACTIVE_FRAGMENT;
        moverQueue.setActive(getLongFromXpath(activeMetric, document).intValue());
        return moverQueue;
    }
}

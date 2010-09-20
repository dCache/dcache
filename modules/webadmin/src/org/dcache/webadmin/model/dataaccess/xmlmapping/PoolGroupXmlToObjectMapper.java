package org.dcache.webadmin.model.dataaccess.xmlmapping;

import java.util.HashSet;
import java.util.Set;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class PoolGroupXmlToObjectMapper extends XmlToObjectMapper {

    private static final String ALL_POOLGROUPS = "/dCache/poolgroups/poolgroup";
    private static final Logger _log = LoggerFactory.getLogger(PoolGroupXmlToObjectMapper.class);

    /**
     * @param document document to parse
     * @return Set of NamedCell objects parsed out of the document
     */
    public Set<String> parsePoolGroupNamesDocument(Document document) {
        assert document != null;
        Set<String> poolGroupNames = new HashSet<String>();
        // get a nodelist of all poolgroup Elements
        NodeList poolGroupNodes = getNodesFromXpath(ALL_POOLGROUPS, document);
        if (poolGroupNodes != null) {
            for (int groupIndex = 0; groupIndex < poolGroupNodes.getLength(); groupIndex++) {
                Element currentCellNode = (Element) poolGroupNodes.item(groupIndex);
                String groupName = currentCellNode.getAttribute(ATTRIBUTE_NAME);
                _log.debug("PoolGroup-name parsed: {}", groupName);
                poolGroupNames.add(groupName);
            }
        }
        return poolGroupNames;
    }
}

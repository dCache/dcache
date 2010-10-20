package org.dcache.webadmin.model.dataaccess.xmlmapping;

import java.util.HashSet;
import java.util.Set;
import org.dcache.webadmin.model.util.AccessLatency;
import org.dcache.webadmin.model.util.RetentionPolicy;
import org.dcache.webadmin.model.businessobjects.LinkGroup;
import org.dcache.webadmin.model.businessobjects.SpaceReservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Maps xmls concerning linkgroups and spacereservations of the expected format
 * from Info into webadmins business objects using xpath.
 * @author jans
 */
public class LinkGroupXmlToObjectMapper extends XmlToObjectMapper {

    private static final String ALL_LINKGROUPS = "/dCache/linkgroups/linkgroup";
    private static final String ALL_RESERVATIONS = "/dCache/reservations/reservation";
    private static final String ATTRIBUTE_LGID = "lgid";
    private static final String ATTRIBUTE_RESERVATION_ID = "reservation-id";
    private static final String SPECIAL_RESERVATION_FRAGMENT =
            "/dCache/reservations/reservation[@reservation-id='";
    private static final String SPECIAL_LINKGROUP_FRAGMENT =
            "/dCache/linkgroups/linkgroup[@lgid='";
    private static final String LINKGROUP_AVAILABLE =
            "/space/metric[@name='available']";
    private static final String LINKGROUP_CUSTODIAL =
            "/retention-policy/metric[@name='custodialAllowed']";
    private static final String LINKGROUP_FREE =
            "/space/metric[@name='free']";
    private static final String LINKGROUP_NAME =
            "/metric[@name='name']";
    private static final String LINKGROUP_NEARLINE =
            "/access-latency/metric[@name='nearlineAllowed']";
    private static final String LINKGROUP_ONLINE =
            "/access-latency/metric[@name='onlineAllowed']";
    private static final String LINKGROUP_OUTPUT =
            "/retention-policy/metric[@name='outputAllowed']";
    private static final String LINKGROUP_REPLICA =
            "/retention-policy/metric[@name='replicaAllowed']";
    private static final String LINKGROUP_RESERVED =
            "/space/metric[@name='reserved']";
    private static final String LINKGROUP_TOTAL =
            "/space/metric[@name='total']";
    private static final String LINKGROUP_USED =
            "/space/metric[@name='used']";
    private static final String LINKGROUP_AUTHORISATION_FOR_VOS =
            "/authorisation/authorised";
    private static final String RESERVATION_ALLOCATED_SPACE =
            "/space/metric[@name='allocated']";
    private static final String RESERVATION_CREATED =
            "/created/metric[@name='simple']";
    private static final String RESERVATION_DESCRIPTION =
            "/metric[@name='description']";
    private static final String RESERVATION_LINKGROUP =
            "/metric[@name='linkgroupref']";
    private static final String RESERVATION_STATE =
            "/metric[@name='state']";
    private static final String RESERVATION_TOTAL_SPACE =
            "/space/metric[@name='total']";
    private static final String RESERVATION_USED_SPACE =
            "/space/metric[@name='used']";
    private static final String RESERVATION_FREE_SPACE =
            "/space/metric[@name='free']";
    private static final String RESERVATION_FQAN_AUTHORISED_VOS =
            "/authorisation/metric[@name='FQAN']";
    private static final String RESERVATION_ACCESS_LATENCY =
            "/metric[@name='access-latency']";
    private static final String RESERVATION_RETENTION_POLICY =
            "/metric[@name='retention-policy']";
    private static final Logger _log = LoggerFactory.getLogger(LinkGroupXmlToObjectMapper.class);

    public Set<LinkGroup> parseLinkGroupsDocument(Document document) {
        Set<LinkGroup> linkGroups = new HashSet<LinkGroup>();
        NodeList linkGroupNodes = getNodesFromXpath(ALL_LINKGROUPS, document);
        if (linkGroupNodes != null) {
            for (int groupIndex = 0; groupIndex < linkGroupNodes.getLength(); groupIndex++) {
                Element linkGroupBlock = (Element) linkGroupNodes.item(groupIndex);
                LinkGroup linkGroup = createLinkGroup(document,
                        linkGroupBlock.getAttribute(ATTRIBUTE_LGID));
                _log.debug("LinkGroup parsed: {}", linkGroup);
                linkGroups.add(linkGroup);
            }
        }
        return linkGroups;
    }

    private LinkGroup createLinkGroup(Document document, String id) {
        LinkGroup linkGroup = new LinkGroup();

        linkGroup.setAvailable(getLongFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_AVAILABLE), document));
        linkGroup.setCustodialAllowed(getBooleanFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_CUSTODIAL), document));
        linkGroup.setFree(getLongFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_FREE), document));
        linkGroup.setId(id);
        linkGroup.setName(getStringFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_NAME), document));
        linkGroup.setNearlineAllowed(getBooleanFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_NEARLINE), document));
        linkGroup.setOnlineAllowed(getBooleanFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_ONLINE), document));
        linkGroup.setOutputAllowed(getBooleanFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_OUTPUT), document));
        linkGroup.setReplicaAllowed(getBooleanFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_REPLICA), document));
        linkGroup.setReserved(getLongFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_RESERVED), document));
        linkGroup.setTotal(getLongFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_TOTAL), document));
        linkGroup.setUsed(getLongFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_USED), document));
        linkGroup.setVos(getVos(id, document));
        return linkGroup;
    }

    private String getVos(String id, Document document) {
        StringBuilder vos = new StringBuilder("");
        NodeList authorisationNodes = getNodesFromXpath(buildLinkGroupXpathExpression(id,
                LINKGROUP_AUTHORISATION_FOR_VOS), document);
        if (authorisationNodes != null) {
            for (int i = 0; i < authorisationNodes.getLength(); i++) {
                Element authorisationNode = (Element) authorisationNodes.item(i);
                vos.append(authorisationNode.getAttribute(ATTRIBUTE_NAME) + ";");
            }
        }
        return vos.toString();
    }

    private String buildLinkGroupXpathExpression(String groupId, String metric) {
        return SPECIAL_LINKGROUP_FRAGMENT + groupId +
                XPATH_PREDICATE_CLOSING_FRAGMENT + metric;
    }

    public Set<SpaceReservation> parseSpaceReservationsDocument(Document document) {
        Set<SpaceReservation> spaceReservations = new HashSet<SpaceReservation>();
        NodeList spaceReservationNodes = getNodesFromXpath(ALL_RESERVATIONS, document);

        if (spaceReservationNodes != null) {
            for (int i = 0; i < spaceReservationNodes.getLength(); i++) {
                Element reservationBlock = (Element) spaceReservationNodes.item(i);
                SpaceReservation reservation = createSpaceReservation(document,
                        reservationBlock.getAttribute(ATTRIBUTE_RESERVATION_ID));
                _log.debug("Space Reservation parsed: {}", reservation);
                spaceReservations.add(reservation);
            }
        }
        return spaceReservations;
    }

    private SpaceReservation createSpaceReservation(Document document, String id) {
        SpaceReservation reservation = new SpaceReservation();
        reservation.setAllocatedSpace(getLongFromXpath(
                buildSpaceReservationXpathExpression(id,
                RESERVATION_ALLOCATED_SPACE), document));
        reservation.setCreated(getStringFromXpath(
                buildSpaceReservationXpathExpression(id,
                RESERVATION_CREATED), document));
        reservation.setDescription(getStringFromXpath(
                buildSpaceReservationXpathExpression(id,
                RESERVATION_DESCRIPTION), document));
        reservation.setFreeSpace(getLongFromXpath(
                buildSpaceReservationXpathExpression(id,
                RESERVATION_FREE_SPACE), document));
        reservation.setId(id);
        reservation.setLinkGroupRef(getStringFromXpath(
                buildSpaceReservationXpathExpression(id,
                RESERVATION_LINKGROUP), document));
        reservation.setState(getStringFromXpath(
                buildSpaceReservationXpathExpression(id,
                RESERVATION_STATE), document));
        reservation.setTotalSpace(getLongFromXpath(
                buildSpaceReservationXpathExpression(id,
                RESERVATION_TOTAL_SPACE), document));
        reservation.setUsedSpace(getLongFromXpath(
                buildSpaceReservationXpathExpression(id,
                RESERVATION_USED_SPACE), document));
        reservation.setVogroup(getStringFromXpath(
                buildSpaceReservationXpathExpression(id,
                RESERVATION_FQAN_AUTHORISED_VOS), document));
        reservation.setAccessLatency(AccessLatency.parseStringValue(
                getStringFromXpath(buildSpaceReservationXpathExpression(id,
                RESERVATION_ACCESS_LATENCY), document)));
        reservation.setRetentionPolicy(RetentionPolicy.parseStringValue(
                getStringFromXpath(buildSpaceReservationXpathExpression(id,
                RESERVATION_RETENTION_POLICY), document)));
        return reservation;
    }

    private String buildSpaceReservationXpathExpression(String id, String metric) {
        return SPECIAL_RESERVATION_FRAGMENT + id +
                XPATH_PREDICATE_CLOSING_FRAGMENT + metric;
    }
}

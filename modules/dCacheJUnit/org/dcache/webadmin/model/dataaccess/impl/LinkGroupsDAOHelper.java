package org.dcache.webadmin.model.dataaccess.impl;

import java.util.Set;
import org.dcache.webadmin.model.businessobjects.LinkGroup;
import org.dcache.webadmin.model.businessobjects.SpaceReservation;
import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.dataaccess.xmlmapping.LinkGroupXmlToObjectMapper;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.w3c.dom.Document;

/**
 * HelperDAO class to simulate an info with an xml-response for unittests
 * @author jans
 */
public class LinkGroupsDAOHelper implements LinkGroupsDAO {

    private final LinkGroupXmlToObjectMapper _objectMapper =
            new LinkGroupXmlToObjectMapper();

    @Override
    public Set<LinkGroup> getLinkGroups() throws DAOException {
        try {
            Document linkGroupsDoc = _objectMapper.createXMLDocument(
                    XMLDataGathererHelper.LINKGROUPS_XML);
            Set<LinkGroup> linkGroups = _objectMapper.parseLinkGroupsDocument(linkGroupsDoc);
            Document reservations = _objectMapper.createXMLDocument(
                    XMLDataGathererHelper.RESERVATIONS_XML);
            mapReservationsToLinkGroups(linkGroups,
                    _objectMapper.parseSpaceReservationsDocument(reservations));
            return linkGroups;
        } catch (ParsingException ex) {
            throw new DAOException(ex);
        }
    }

    private void mapReservationsToLinkGroups(Set<LinkGroup> linkGroups,
            Set<SpaceReservation> reservations) {
        for (LinkGroup linkGroup : linkGroups) {
            for (SpaceReservation reservation : reservations) {
                if (reservation.belongsToLinkGroup(linkGroup.getName())) {
                    linkGroup.addSpaceReservation(reservation);
                }
            }
        }
    }
}

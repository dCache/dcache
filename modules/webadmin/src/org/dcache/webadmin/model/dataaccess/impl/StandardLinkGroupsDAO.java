package org.dcache.webadmin.model.dataaccess.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.LinkGroup;
import org.dcache.webadmin.model.businessobjects.SpaceReservation;
import org.dcache.webadmin.model.dataaccess.LinkGroupsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.impl.InfoGetSerialisedDataMessageGenerator;
import org.dcache.webadmin.model.dataaccess.xmlmapping.LinkGroupXmlToObjectMapper;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.exceptions.DataGatheringException;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

/**
 * Standard implementation  of a LinkGroupsDAO in dCache using messagegenerators
 * for cellmessages to retrieve xml from info and map them into the business
 * objects via a LinkGroupXmlToObjectMapper.
 * @author jans
 */
public class StandardLinkGroupsDAO implements LinkGroupsDAO {

    private static final List<String> LINKGROUPS_PATH =
            Collections.singletonList("linkgroups");
    private static final List<String> RESERVATIONS_PATH =
            Collections.singletonList("reservations");
    private final CommandSenderFactory _commandSenderFactory;
    private final LinkGroupXmlToObjectMapper _xmlToObjectMapper =
            new LinkGroupXmlToObjectMapper();
    private static final Logger _log = LoggerFactory.getLogger(StandardLinkGroupsDAO.class);

    public StandardLinkGroupsDAO(CommandSenderFactory commandSenderFactory) {
        _commandSenderFactory = commandSenderFactory;
    }

    @Override
    public Set<LinkGroup> getLinkGroups() throws DAOException {
        _log.debug("getLinkGroups called");
        try {
            Set<LinkGroup> linkGroups = tryToGetLinkGroups();
            Set<SpaceReservation> reservations = tryToGetSpaceReservations();
            mapReservationsToLinkGroups(linkGroups, reservations);
            return linkGroups;
        } catch (ParsingException ex) {
            throw new DAOException(ex);
        } catch (DataGatheringException ex) {
            throw new DAOException(ex);
        }
    }

    private void mapReservationsToLinkGroups(Set<LinkGroup> linkGroups,
            Set<SpaceReservation> reservations) {
        for (LinkGroup linkGroup : linkGroups) {
            for (SpaceReservation reservation : reservations) {
                if (reservation.belongsToLinkGroup(linkGroup.getId())) {
                    linkGroup.addSpaceReservation(reservation);
                }
            }
        }
    }

    private Set<LinkGroup> tryToGetLinkGroups() throws ParsingException,
            DataGatheringException {
        String serialisedXML = getXmlForStatePath(LINKGROUPS_PATH);
        Document xmlDocument = _xmlToObjectMapper.createXMLDocument(serialisedXML);
        return _xmlToObjectMapper.parseLinkGroupsDocument(xmlDocument);
    }

    private Set<SpaceReservation> tryToGetSpaceReservations() throws ParsingException,
            DataGatheringException {
        String serialisedXML = getXmlForStatePath(RESERVATIONS_PATH);
        Document xmlDocument = _xmlToObjectMapper.createXMLDocument(serialisedXML);
        return _xmlToObjectMapper.parseSpaceReservationsDocument(xmlDocument);
    }

    private String getXmlForStatePath(List<String> statePath) throws DataGatheringException {
        try {
            InfoGetSerialisedDataMessageGenerator messageGenerator =
                    new InfoGetSerialisedDataMessageGenerator(statePath);
            CommandSender commandSender =
                    _commandSenderFactory.createCommandSender(messageGenerator);
            commandSender.sendAndWait();
            if (commandSender.allSuccessful()) {
                return messageGenerator.getXML();
            } else {
                throw new DataGatheringException("couldn't get data from info provider");
            }
        } catch (InterruptedException e) {
            throw new DataGatheringException("Interrupted during data gathering", e);
        }
    }
}

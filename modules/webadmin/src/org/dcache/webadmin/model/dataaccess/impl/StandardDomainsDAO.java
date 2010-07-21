package org.dcache.webadmin.model.dataaccess.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.impl.InfoGetSerialisedDataMessageGenerator;
import org.dcache.webadmin.model.dataaccess.xmlprocessing.DomainsXMLProcessor;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.exceptions.DataGatheringException;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

/**
 *
 * @author jans
 */
public class StandardDomainsDAO implements DomainsDAO {

    public static final List<String> DOMAINS_PATH = Arrays.asList("domains");
    public static final List<String> DOORS_PATH = Arrays.asList("doors");
    public static final List<String> POOLS_PATH = Arrays.asList("pools");
    private static final Logger _log = LoggerFactory.getLogger(StandardDomainsDAO.class);
    private DomainsXMLProcessor _xmlProcessor = new DomainsXMLProcessor();
    private CommandSenderFactory _commandSenderFactory;

    public StandardDomainsDAO(CommandSenderFactory commandSenderFactory) {
        _commandSenderFactory = commandSenderFactory;
    }

    @Override
    public Set<CellStatus> getCellStatuses() throws DAOException {
        _log.debug("getCellStatuses called");
        try {
            Set<String> doors = tryToGetAllDoorNames();
            Set<String> pools = tryToGetAllPoolNames();
            return tryToGetCellStatuses(doors, pools);
        } catch (ParsingException ex) {
            throw new DAOException(ex);
        } catch (DataGatheringException ex) {
            throw new DAOException(ex);
        }
    }

    private Set<String> tryToGetAllDoorNames() throws DataGatheringException,
            ParsingException {
        String serialisedXML = getXmlForStatePath(DOORS_PATH);
        Document xmlDocument = _xmlProcessor.createXMLDocument(serialisedXML);
        return _xmlProcessor.parseDoorList(xmlDocument);
    }

    private Set<String> tryToGetAllPoolNames() throws DataGatheringException, ParsingException {
        String serialisedXML = getXmlForStatePath(POOLS_PATH);
        Document xmlDocument = _xmlProcessor.createXMLDocument(serialisedXML);
        return _xmlProcessor.parsePoolsList(xmlDocument);
    }

    private Set<CellStatus> tryToGetCellStatuses(Set<String> doors,
            Set<String> pools) throws DataGatheringException, ParsingException {

        String serialisedXML = getXmlForStatePath(DOMAINS_PATH);
        Document xmlDocument = _xmlProcessor.createXMLDocument(serialisedXML);
        return _xmlProcessor.parseDomainsDocument(doors, pools, xmlDocument);
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

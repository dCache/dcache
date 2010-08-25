package org.dcache.webadmin.model.dataaccess.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.dataaccess.communication.impl.StringCommandMessageGenerator;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator.CellMessageRequest;
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

    public static final String EMPTY_STRING = "";
    public static final List<String> NAMEDCELLS_PATH = Arrays.asList("domains",
            "dCacheDomain", "routing", "named-cells");
    public static final List<String> DOMAINS_PATH = Arrays.asList("domains");
    public static final List<String> DOORS_PATH = Arrays.asList("doors");
    public static final List<String> POOLS_PATH = Arrays.asList("pools");
    public static final String RESPONSE_FAILED = "failed";
    private static final Logger _log = LoggerFactory.getLogger(StandardDomainsDAO.class);
    private DomainsXMLProcessor _xmlToObjectMapper = new DomainsXMLProcessor();
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
        Document xmlDocument = _xmlToObjectMapper.createXMLDocument(serialisedXML);
        return _xmlToObjectMapper.parseDoorList(xmlDocument);
    }

    private Set<String> tryToGetAllPoolNames() throws DataGatheringException, ParsingException {
        String serialisedXML = getXmlForStatePath(POOLS_PATH);
        Document xmlDocument = _xmlToObjectMapper.createXMLDocument(serialisedXML);
        return _xmlToObjectMapper.parsePoolsList(xmlDocument);
    }

    private Set<CellStatus> tryToGetCellStatuses(Set<String> doors,
            Set<String> pools) throws DataGatheringException, ParsingException {

        String serialisedXML = getXmlForStatePath(DOMAINS_PATH);
        Document xmlDocument = _xmlToObjectMapper.createXMLDocument(serialisedXML);
        return _xmlToObjectMapper.parseDomainsDocument(doors, pools, xmlDocument);
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

    @Override
    public Set<NamedCell> getNamedCells() throws DAOException {
        _log.debug("getNamedCells called");
        try {
            return tryToGetNamedCells();
        } catch (ParsingException ex) {
            throw new DAOException(ex);
        } catch (DataGatheringException ex) {
            throw new DAOException(ex);
        }
    }

    private Set<NamedCell> tryToGetNamedCells() throws ParsingException, DataGatheringException {
        String serialisedXML = getXmlForStatePath(NAMEDCELLS_PATH);
        Document xmlDocument = _xmlToObjectMapper.createXMLDocument(serialisedXML);
        return _xmlToObjectMapper.parseNamedCellsDocument(xmlDocument);
    }

    @Override
    public Set<CellResponse> sendCommand(Set<String> destinations, String command)
            throws DAOException {
        try {
            Set<CellResponse> responses = new HashSet<CellResponse>();
            if (!destinations.isEmpty() && !EMPTY_STRING.equals(command)) {
                StringCommandMessageGenerator messageGenerator =
                        new StringCommandMessageGenerator(destinations, command);
                CommandSender commandSender =
                        _commandSenderFactory.createCommandSender(
                        messageGenerator);
                commandSender.sendAndWait();
                createResponses(responses, messageGenerator);
            }
            return responses;
        } catch (InterruptedException e) {
            throw new DAOException(e);
        }
    }

    private void createResponses(Set<CellResponse> responses,
            CellMessageGenerator<String> messageGenerator) {
        for (CellMessageRequest<String> request : messageGenerator) {
            CellResponse response = new CellResponse();
            response.setCellName(request.getDestination().getCellName());
            if (request.isSuccessful()) {
                response.setResponse(request.getAnswer());
            } else {
                response.setIsFailure(true);
                response.setResponse(RESPONSE_FAILED);
            }
            responses.add(response);
        }
    }

    @Override
    public Map<String, List<String>> getDomainsMap() throws DAOException {
        _log.debug("getDomainsMap called");
        try {
            return tryToGetDomainsMap();
        } catch (ParsingException ex) {
            throw new DAOException(ex);
        } catch (DataGatheringException ex) {
            throw new DAOException(ex);
        }
    }

    private Map<String, List<String>> tryToGetDomainsMap()
            throws ParsingException, DataGatheringException {
        String serialisedXML = getXmlForStatePath(DOMAINS_PATH);
        Document xmlDocument = _xmlToObjectMapper.createXMLDocument(serialisedXML);
        return _xmlToObjectMapper.parseDomainsMapDocument(xmlDocument);
    }
}

package org.dcache.webadmin.model.dataaccess.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dcache.admin.webadmin.datacollector.datatypes.CellStatus;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.dataaccess.communication.impl.StringCommandMessageGenerator;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator.CellMessageRequest;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.webadmin.model.dataaccess.communication.impl.InfoGetSerialisedDataMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.impl.PageInfoCache;
import org.dcache.webadmin.model.dataaccess.xmlmapping.DomainsXmlToObjectMapper;
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

    private static final String EMPTY_STRING = "";
    private static final List<String> DOMAINS_PATH = Arrays.asList("domains");
    private static final String RESPONSE_FAILED = "failed";
    private static final Logger _log = LoggerFactory.getLogger(StandardDomainsDAO.class);
    private DomainsXmlToObjectMapper _xmlToObjectMapper = new DomainsXmlToObjectMapper();
    private PageInfoCache _pageCache;
    private CommandSenderFactory _commandSenderFactory;

    public StandardDomainsDAO(PageInfoCache pageCache,
            CommandSenderFactory commandSenderFactory) {
        _commandSenderFactory = commandSenderFactory;
        _pageCache = pageCache;
    }

    @Override
    public Set<CellStatus> getCellStatuses() throws DAOException {
        _log.debug("getCellStatuses called");
        Set<CellStatus> states = (Set<CellStatus>) _pageCache.getCacheContent(
                ContextPaths.CELLINFO_LIST);
        if (states != null) {
            return states;
        } else {
            return Collections.EMPTY_SET;
        }
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

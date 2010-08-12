package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.xmlprocessing.PoolXMLProcessor;
import diskCacheV111.pools.PoolV2Mode;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator.CellMessageRequest;
import org.dcache.webadmin.model.exceptions.DataGatheringException;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.w3c.dom.Document;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.impl.InfoGetSerialisedDataMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.impl.PoolModifyModeMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.impl.StringCommandMessageGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an DataAccessObject for the pools offered by the model. It provides
 * the access to the needed data concerning pools.
 * It processes the collected data
 * in the XML-processor and
 * puts it into the matching businessobjects. This is returned to the caller.
 * It sends commands via a commandSender.
 * @author jan schaefer 29-10-2009
 */
public class StandardPoolsDAO implements PoolsDAO {

    public static final String EMPTY_STRING = "";
    public static final List<String> NAMEDCELLS_PATH = Arrays.asList("domains", "dCacheDomain",
            "routing", "named-cells");
    public static final List<String> POOLS_PATH = Arrays.asList("pools");
    public static final String RESPONSE_FAILED = "failed";
    private static final Logger _log = LoggerFactory.getLogger(StandardPoolsDAO.class);
    private PoolXMLProcessor _xmlProcessor = new PoolXMLProcessor();
    private CommandSenderFactory _commandSenderFactory;

    public StandardPoolsDAO(CommandSenderFactory commandSenderFactory) {
        _commandSenderFactory = commandSenderFactory;
    }

    @Override
    public Set<Pool> getPools() throws DAOException {
        _log.debug("getPools called");
        try {
            return tryToGetPools();
        } catch (ParsingException ex) {
            throw new DAOException(ex);
        } catch (DataGatheringException ex) {
            throw new DAOException(ex);
        }
    }

    private Set<Pool> tryToGetPools() throws ParsingException, DataGatheringException {
        String serialisedXML = getXmlForStatePath(POOLS_PATH);
        Document xmlDocument = _xmlProcessor.createXMLDocument(serialisedXML);
        return _xmlProcessor.parsePoolsDocument(xmlDocument);
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
        Document xmlDocument = _xmlProcessor.createXMLDocument(serialisedXML);
        return _xmlProcessor.parseNamedCellsDocument(xmlDocument);
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
    public void changePoolMode(Set<String> poolIds, PoolV2Mode poolMode, String userName)
            throws DAOException {
        try {
            _log.debug("PoolModifyModeMsg-Command: {}", poolMode);
            if (!poolIds.isEmpty()) {
                PoolModifyModeMessageGenerator messageGenerator =
                        new PoolModifyModeMessageGenerator(
                        poolIds, poolMode, userName);
                CommandSender commandSender =
                        _commandSenderFactory.createCommandSender(messageGenerator);
                commandSender.sendAndWait();
                if (!commandSender.allSuccessful()) {
                    Set<String> failedIds = extractFailedIds(messageGenerator);
                    throw new DAOException(failedIds.toString());
                }
            }
            _log.debug("PoolDAO: Modechange-Commands send successfully");
        } catch (InterruptedException e) {
            _log.error("blocking was interrupted, change of PoolModes not yet done completly");
        }
    }

    private Set<String> extractFailedIds(CellMessageGenerator<?> messageGenerator) {
        Set<String> failedIds = new HashSet<String>();
        for (CellMessageRequest request : messageGenerator) {
            if (!request.isSuccessful()) {
                String destination = request.getDestination().toString();
                failedIds.add(destination);
            }
        }
        return failedIds;
    }

    @Override
    public Set<CellResponse> sendCommand(Set<String> poolIds, String command)
            throws DAOException {
        try {
            Set<CellResponse> responses = new HashSet<CellResponse>();
            if (!poolIds.isEmpty() && !command.equals(EMPTY_STRING)) {
                StringCommandMessageGenerator messageGenerator =
                        new StringCommandMessageGenerator(poolIds, command);
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
}

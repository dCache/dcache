package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.xmlmapping.PoolXmlToObjectMapper;
import diskCacheV111.pools.PoolV2Mode;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an DataAccessObject for the pools offered by the model. It provides
 * the access to the needed data concerning pools.
 * It maps the collected data
 * in the XML-processor to Business Objects. These are returned to the caller.
 * It sends commands via a commandSender.
 * @author jan schaefer 29-10-2009
 */
public class StandardPoolsDAO implements PoolsDAO {

    public static final List<String> POOLS_PATH = Arrays.asList("pools");
    public static final String RESPONSE_FAILED = "failed";
    private static final Logger _log = LoggerFactory.getLogger(StandardPoolsDAO.class);
    private PoolXmlToObjectMapper _xmlToObjectMapper = new PoolXmlToObjectMapper();
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
        Document xmlDocument = _xmlToObjectMapper.createXMLDocument(serialisedXML);
        return _xmlToObjectMapper.parsePoolsDocument(xmlDocument);
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
}

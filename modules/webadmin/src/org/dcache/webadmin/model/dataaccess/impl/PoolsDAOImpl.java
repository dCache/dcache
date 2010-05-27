package org.dcache.webadmin.model.dataaccess.impl;

import diskCacheV111.pools.PoolV2Mode;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.dataaccess.PoolsDAO;
import org.dcache.webadmin.model.dataaccess.XMLDataGatherer;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator.CellMessageRequest;
import org.dcache.webadmin.model.exceptions.DataGatheringException;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.w3c.dom.Document;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.impl.PoolModifyModeMessageGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an DataAccessObject for the pools offered by the model. It provides
 * the access to the needed data concerning pools.
 * It gets the data from its XMLDataGatherer. This answer is processed
 * in the XML-processor and
 * put into the matching businessobjects. This is returned to the caller.
 * It sends commands to pools via a commandSender.
 * @author jan schaefer 29-10-2009
 */
public class PoolsDAOImpl implements PoolsDAO {

    public static final List<String> NAMEDCELLS_PATH = Arrays.asList("domains", "dCacheDomain",
            "routing", "named-cells");
    public static final List<String> POOLS_PATH = Arrays.asList("pools");
    private static Logger _log = LoggerFactory.getLogger(PoolsDAOImpl.class);
    private XMLProcessor _xmlProcessor = new XMLProcessor();
    private XMLDataGatherer _xmlDataGatherer;
    private CommandSenderFactory _commandSenderFactory;

    public PoolsDAOImpl(XMLDataGatherer xmlDataGatherer, CommandSenderFactory commandSenderFactory) {
        _xmlDataGatherer = xmlDataGatherer;
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
        String serialisedXML = this.getPoolListXML();
        Document xmlDocument = _xmlProcessor.createXMLDocument(serialisedXML);
        return _xmlProcessor.parsePoolsDocument(xmlDocument);
    }

    private String getPoolListXML() throws DataGatheringException {
        return _xmlDataGatherer.getXML(POOLS_PATH);
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
        String serialisedXML = this.getNamedCellsXML();
        Document xmlDocument = _xmlProcessor.createXMLDocument(serialisedXML);
        return _xmlProcessor.parseNamedCellsDocument(xmlDocument);
    }

    private String getNamedCellsXML() throws DataGatheringException {
        return _xmlDataGatherer.getXML(NAMEDCELLS_PATH);
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

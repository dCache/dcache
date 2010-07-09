package org.dcache.webadmin.model.dataaccess.impl;

import org.dcache.webadmin.model.dataaccess.xmlprocessing.PoolGroupXMLProcessor;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.dcache.webadmin.model.dataaccess.PoolGroupDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.impl.InfoGetSerialisedDataMessageGenerator;
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
public class StandardPoolGroupDAO implements PoolGroupDAO {

    public static final List<String> POOLGROUPS_PATH = Arrays.asList("poolgroups");
    private static final Logger _log = LoggerFactory.getLogger(StandardPoolGroupDAO.class);
    private PoolGroupXMLProcessor _xmlProcessor = new PoolGroupXMLProcessor();
    private CommandSenderFactory _commandSenderFactory;

    public StandardPoolGroupDAO(CommandSenderFactory commandSenderFactory) {
        _commandSenderFactory = commandSenderFactory;
    }

    @Override
    public Set<String> getPoolGroupNames() throws DAOException {
        _log.debug("getPoolGroupNames called");
        try {
            return tryToGetPoolGroupNames();
        } catch (ParsingException ex) {
            throw new DAOException(ex);
        } catch (DataGatheringException ex) {
            throw new DAOException(ex);
        }
    }

    private Set<String> tryToGetPoolGroupNames() throws ParsingException, DataGatheringException {

        String serialisedXML = getXmlForStatePath(POOLGROUPS_PATH);
        Document xmlDocument = _xmlProcessor.createXMLDocument(serialisedXML);
        return _xmlProcessor.parsePoolGroupNamesDocument(xmlDocument);
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

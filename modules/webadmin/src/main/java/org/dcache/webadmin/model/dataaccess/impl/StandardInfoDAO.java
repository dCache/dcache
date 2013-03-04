package org.dcache.webadmin.model.dataaccess.impl;

import java.util.Arrays;
import java.util.List;

import org.dcache.webadmin.model.dataaccess.InfoDAO;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.impl.InfoGetSerialisedDataMessageGenerator;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.exceptions.DataGatheringException;

/**
 *
 * @author jans
 */
public class StandardInfoDAO implements InfoDAO {

    private CommandSenderFactory _commandSenderFactory;

    public StandardInfoDAO(CommandSenderFactory commandSenderFactory) {
        _commandSenderFactory = commandSenderFactory;
    }

    @Override
    public String getXmlForStatepath(String statepath) throws DAOException {
        try {

            InfoGetSerialisedDataMessageGenerator messageGenerator =
                    new InfoGetSerialisedDataMessageGenerator(
                    buildStatePathList(statepath));
            CommandSender commandSender =
                    _commandSenderFactory.createCommandSender(messageGenerator);
            commandSender.sendAndWait();
            if (commandSender.allSuccessful()) {
                return messageGenerator.getXML();
            } else {
                throw new DAOException("couldn't get data from info provider");
            }
        } catch (DataGatheringException | InterruptedException e) {
            throw new DAOException("Interrupted during data gathering", e);
        }
    }

    private List<String> buildStatePathList(String statepath) {
        String[] items = statepath.split("/");
        return Arrays.asList(items);
    }
}

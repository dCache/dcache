package org.dcache.webadmin.model.dataaccess.impl;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import java.util.List;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.log4j.Logger;
import org.dcache.cells.AbstractCell;
import org.dcache.vehicles.InfoGetSerialisedDataMessage;
import org.dcache.webadmin.model.dataaccess.XMLDataGatherer;
import org.dcache.webadmin.model.exceptions.DataGatheringException;

/**
 * gets XML Data from the Info Service via Cell-communication framework
 * @author jans
 */
public class CellXMLDataGatherer implements XMLDataGatherer {

    //    timeout time for cellmessages in ms
    private static final int CELLMESSAGE_TIMEOUT = 4000;
    private static final String INFO_CELL_NAME = "info";
    private static final CellPath INFOCELL_PATH = new CellPath(INFO_CELL_NAME);
    private AbstractCell _infoproviderConnection;
    private static Logger _log = Logger.getLogger(CellXMLDataGatherer.class);

    public CellXMLDataGatherer() throws NamingException {
        InitialContext lookupContext = new InitialContext();
//        maybe its later neccessary to use concrete methods of the jettycell.
//        Change the cast then and use jettycells static constant for the context
//        instead of literal!
        _infoproviderConnection = (AbstractCell) lookupContext.lookup("jettycell");
        _log.debug("lookup returned:" + _infoproviderConnection.toString());
    }

    @Override
    public String getXML(List<String> pathElements) throws DataGatheringException {

        InfoGetSerialisedDataMessage sendMsg = new InfoGetSerialisedDataMessage(
                pathElements);
        CellMessage envelope = new CellMessage(INFOCELL_PATH, sendMsg);

        CellMessage replyMsg;
        try {
            replyMsg = _infoproviderConnection.sendAndWait(envelope, CELLMESSAGE_TIMEOUT);
        } catch (SerializationException ex) {
            throw new DataGatheringException(ex);
        } catch (NoRouteToCellException ex) {
            throw new DataGatheringException(ex);
        } catch (InterruptedException ex) {
            throw new DataGatheringException(ex);
        }

        if (replyMsg == null) {
            throw new DataGatheringException("no reply from info service");
        }
        Object replyObj = replyMsg.getMessageObject();
        if (replyObj == null) {
            throw new DataGatheringException("no payload in message from info service");
        }

        InfoGetSerialisedDataMessage reply = (InfoGetSerialisedDataMessage) replyObj;
        String messageData = reply.getSerialisedData();

        if (messageData == null) {
            throw new DataGatheringException("no payload in message from info service");
        }

        _log.debug("Requested URL: " + pathElements.toString());
        _log.debug("InfoMessage length: " + messageData.length());
        return messageData;
    }
}

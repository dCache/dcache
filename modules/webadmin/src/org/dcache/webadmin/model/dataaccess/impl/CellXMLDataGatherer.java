package org.dcache.webadmin.model.dataaccess.impl;

import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.SerializationException;
import java.util.List;
import org.dcache.cells.CellStub;
import org.dcache.vehicles.InfoGetSerialisedDataMessage;
import org.dcache.webadmin.model.dataaccess.XMLDataGatherer;
import org.dcache.webadmin.model.exceptions.DataGatheringException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gets XML Data from the Info Service via Cell-communication framework
 * @author jans
 */
public class CellXMLDataGatherer implements XMLDataGatherer {

    private static final Logger _log = LoggerFactory.getLogger(CellXMLDataGatherer.class);
    private CellStub _infoCellStub;

    @Override
    public String getXML(List<String> pathElements) throws DataGatheringException {

        InfoGetSerialisedDataMessage sendMsg = new InfoGetSerialisedDataMessage(
                pathElements);
        try {
            InfoGetSerialisedDataMessage reply = _infoCellStub.sendAndWait(sendMsg);
            String messageData = reply.getSerialisedData();

            if (messageData == null) {
                throw new DataGatheringException("no payload in message from info service");
            }

            _log.debug("Requested URL: {}", pathElements);
            _log.debug("InfoMessage length: {}", messageData.length());
            return messageData;

        } catch (SerializationException ex) {
            throw new DataGatheringException(ex);
        } catch (CacheException ex) {
            throw new DataGatheringException(ex);
        } catch (InterruptedException ex) {
            throw new DataGatheringException(ex);
        }
    }

    public void setInfoCellStub(CellStub infoCellStub) {
        this._infoCellStub = infoCellStub;
    }
}

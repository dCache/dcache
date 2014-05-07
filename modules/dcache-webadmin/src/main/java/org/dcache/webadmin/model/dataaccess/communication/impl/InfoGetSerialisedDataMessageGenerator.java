package org.dcache.webadmin.model.dataaccess.communication.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.SerializationException;

import org.dcache.services.info.serialisation.XmlSerialiser;
import org.dcache.vehicles.InfoGetSerialisedDataMessage;
import org.dcache.webadmin.model.dataaccess.XMLDataGatherer;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.dcache.webadmin.model.exceptions.DataGatheringException;

/**
 *
 * @author jans
 */
public class InfoGetSerialisedDataMessageGenerator
        implements CellMessageGenerator<InfoGetSerialisedDataMessage>,
        XMLDataGatherer {

    private Set<CellMessageRequest<InfoGetSerialisedDataMessage>> _messageRequests =
            new HashSet<>();
    private InfoGetSerialisedDataMessage _answer;
    private List<String> _pathElements;
    private static final Logger _log = LoggerFactory.getLogger(
            InfoGetSerialisedDataMessageGenerator.class);

    public InfoGetSerialisedDataMessageGenerator(List<String> pathElements) {
        _pathElements = pathElements;
        InfoGetSerialisedDataMessageRequest sendRequest =
                new InfoGetSerialisedDataMessageRequest(pathElements);
        _messageRequests.add(sendRequest);
    }

    /*
     * caller has to make sure to call this
     * only if the message was sent successfully
     */
    @Override
    public String getXML() throws DataGatheringException {
        try {
            assert (_answer != null);
            String messageData = _answer.getSerialisedData();

            if (messageData == null) {
                throw new DataGatheringException("no payload in message from info service");
            }
            _log.debug("Requested URL: {}", _pathElements);
            _log.debug("InfoMessage length: {}", messageData.length());
            return messageData;
        } catch (SerializationException ex) {
            throw new DataGatheringException(ex);
        }
    }

    @Override
    public int getNumberOfMessages() {
        return 1;
    }

    @Override
    public Iterator<CellMessageRequest<InfoGetSerialisedDataMessage>> iterator() {
        return _messageRequests.iterator();
    }

    private class InfoGetSerialisedDataMessageRequest implements
            CellMessageRequest<InfoGetSerialisedDataMessage> {

        private static final String INFO_CELL = "info";
        private InfoGetSerialisedDataMessage _payload;
        private String _destination;
        private boolean _sentSuccessfully;

        public InfoGetSerialisedDataMessageRequest(List<String> pathElements) {
            _payload = new InfoGetSerialisedDataMessage(pathElements, XmlSerialiser.NAME);
            _destination = INFO_CELL;
        }

        @Override
        public InfoGetSerialisedDataMessage getPayload() {
            return _payload;
        }

        @Override
        public Class<InfoGetSerialisedDataMessage> getPayloadType() {
            return InfoGetSerialisedDataMessage.class;
        }

        @Override
        public boolean isSuccessful() {
            return _sentSuccessfully;
        }

        @Override
        public void setSuccessful(boolean successful) {
            _sentSuccessfully = successful;
        }

        @Override
        public CellPath getDestination() {
            return new CellPath(_destination);
        }

        @Override
        public void setAnswer(Serializable answer) {
            _answer = (InfoGetSerialisedDataMessage) answer;
        }

        @Override
        public InfoGetSerialisedDataMessage getAnswer() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}

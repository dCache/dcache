package org.dcache.webadmin.model.dataaccess.communication.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import dmg.cells.nucleus.CellPath;

import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;

/**
 * Used to send String messages (commands!) to cells and make response accessible
 * @author jans
 */
public class StringCommandMessageGenerator implements CellMessageGenerator<String> {

    private String _command;
    private Set<CellMessageRequest<String>> _messageRequests =
            new HashSet<>();
    private static final Logger _log = LoggerFactory.getLogger(
            StringCommandMessageGenerator.class);

    public StringCommandMessageGenerator(Set<String> destinations, String command) {
        _log.debug("Generating messages for {}", destinations);
        _command = command;
        createMessages(destinations);
    }

    private void createMessages(Set<String> destinations) {
        for (String pool : destinations) {
            StringCommandMessageRequest sendRequest = new StringCommandMessageRequest(pool,
                    _command);
            _messageRequests.add(sendRequest);
        }
    }

    @Override
    public int getNumberOfMessages() {
        return _messageRequests.size();
    }

    @Override
    public Iterator<CellMessageRequest<String>> iterator() {
        return _messageRequests.iterator();
    }

    private class StringCommandMessageRequest implements CellMessageRequest<String> {

        private String _payload;
        private String _destination;
        private String _answer;
        private boolean _sentSuccessfully;

        public StringCommandMessageRequest(String destination, String command) {
            _payload = command;
            _destination = destination;
        }

        @Override
        public String getPayload() {
            _log.debug("getPayload for {}", _destination);
            return _payload;
        }

        @Override
        public Class<String> getPayloadType() {
            return String.class;
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
            _answer = (String) answer;
        }

        @Override
        public String getAnswer() {
            return _answer;
        }
    }
}

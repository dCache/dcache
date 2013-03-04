package org.dcache.webadmin.model.dataaccess.communication.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import diskCacheV111.vehicles.PoolMoverKillMessage;

import dmg.cells.nucleus.CellPath;

import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;

/**
 *
 * @author jans
 */
public class PoolMoverKillMessageGenerator
        implements CellMessageGenerator<PoolMoverKillMessage> {

    private Set<CellMessageRequest<PoolMoverKillMessage>> _messageRequests =
            new HashSet<>();
    private int _numberOfMessages;
    private static final Logger _log = LoggerFactory.getLogger(
            PoolMoverKillMessageGenerator.class);

    public PoolMoverKillMessageGenerator(String targetPool, Set<Integer> jobIds) {
        if (targetPool == null) {
            throw new IllegalArgumentException();
        }
        _log.debug("Generating killmessages for pool {} jobids {}",
                targetPool, jobIds);
        _numberOfMessages = jobIds.size();
        createMessages(targetPool, jobIds);
    }

    private void createMessages(String targetPool, Set<Integer> jobIds) {
        for (Integer jobid : jobIds) {
            PoolMoverKillMessageRequest sendRequest =
                    new PoolMoverKillMessageRequest(targetPool, jobid);
            _messageRequests.add(sendRequest);
        }
    }

    @Override
    public Iterator<CellMessageRequest<PoolMoverKillMessage>> iterator() {
        return _messageRequests.iterator();
    }

    @Override
    public int getNumberOfMessages() {
        return _numberOfMessages;
    }

    private class PoolMoverKillMessageRequest implements CellMessageRequest<PoolMoverKillMessage> {

        private PoolMoverKillMessage _payload;
        private boolean _sentSuccessfully;
        private String _destination;

        private PoolMoverKillMessageRequest(String destination, Integer jobid) {
            _destination = destination;
            _payload = new PoolMoverKillMessage(destination, jobid);
        }

        @Override
        public PoolMoverKillMessage getPayload() {
            return _payload;
        }

        @Override
        public Class<PoolMoverKillMessage> getPayloadType() {
            return PoolMoverKillMessage.class;
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
//            currently no interest in answer since it is not used
        }

        @Override
        public PoolMoverKillMessage getAnswer() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}

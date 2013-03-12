package org.dcache.webadmin.model.dataaccess.communication.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolModifyModeMessage;

import dmg.cells.nucleus.CellPath;

import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;

/**
 * generates Messages for a number of given Pools. It returns an iterator over them
 * for accessing and processing.
 * @author jans
 */
public class PoolModifyModeMessageGenerator implements CellMessageGenerator<PoolModifyModeMessage> {

//    made up errrorcode for the poolmodifymessage, because it needs one, that has no
//    consequence on the pool itself- see org.dcache.pool.classic.PoolV4
    private static final int MADE_UP_ERRORCODE = 298;
    private String _userName;
    private int _numberOfMessages;
    private Set<CellMessageRequest<PoolModifyModeMessage>> _messageRequests =
            new HashSet<>();
    private static final Logger _log = LoggerFactory.getLogger(PoolModifyModeMessageGenerator.class);

    public PoolModifyModeMessageGenerator(Set<String> pools, PoolV2Mode poolMode, String username) {
        _log.debug("Generating messages for {}", pools);
        _userName = username;
        _numberOfMessages = pools.size();
        createMessages(pools, poolMode);
    }

    private void createMessages(Set<String> pools, PoolV2Mode poolMode) {
        for (String pool : pools) {
            PoolModifyModeMessageRequest sendRequest = new PoolModifyModeMessageRequest(pool,
                    poolMode);
            _messageRequests.add(sendRequest);
        }

    }

    @Override
    public Iterator<CellMessageRequest<PoolModifyModeMessage>> iterator() {
        return _messageRequests.iterator();
    }

    @Override
    public int getNumberOfMessages() {
        return _numberOfMessages;
    }

    private class PoolModifyModeMessageRequest implements CellMessageRequest<PoolModifyModeMessage> {

        private PoolModifyModeMessage _payload;
        private String _destination;
        private boolean _sentSuccessfully;

        public PoolModifyModeMessageRequest(String pool, PoolV2Mode poolMode) {
            _payload = new PoolModifyModeMessage(pool, poolMode);
            _payload.setStatusInfo(MADE_UP_ERRORCODE,
                    "Changed by WebAdminInterface - " + _userName);
            _destination = pool;
        }

        @Override
        public PoolModifyModeMessage getPayload() {
            _log.debug("getPayload for {}", _destination);
            return _payload;
        }

        @Override
        public Class<PoolModifyModeMessage> getPayloadType() {
            return PoolModifyModeMessage.class;
        }

        @Override
        public CellPath getDestination() {
            return new CellPath(_destination);
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
        public void setAnswer(Serializable answer) {
//            currently no interest in answer since it is not used
        }

        @Override
        public PoolModifyModeMessage getAnswer() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}

package org.dcache.webadmin.model.dataaccess.communication.impl;

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellPath;
import java.util.concurrent.CountDownLatch;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator.CellMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command Sender to send commands to dCache-Cells via cell-communication.
 * It does this with an implementation
 * of a callback, to be able to send multiple commands asynchronously.
 * @author jans
 */
public class CellCommandSender implements CommandSender {

    private CellMessageGenerator<?> _messageGenerator;
    private CellStub _cellStub;
    private CountDownLatch _doneSignal;
    private boolean _allSuccessful = true;
    private static final Logger _log = LoggerFactory.getLogger(CellCommandSender.class);

    public CellCommandSender(CellMessageGenerator<?> messageGenerator) {
        _messageGenerator = messageGenerator;
        _doneSignal = new CountDownLatch(messageGenerator.getNumberOfMessages());
    }

    @Override
    public void sendAndWait() throws InterruptedException {
        sendMessages();
        _doneSignal.await();
    }

    private void sendMessages() {
        for (CellMessageRequest messageRequest : _messageGenerator) {
            MessageCallback callback = new CellMessageCallback(messageRequest);
            _log.debug("sending to: {}", messageRequest.getDestination());
            Message message = messageRequest.getPayload();
            CellPath destination = messageRequest.getDestination();
            Class payloadType = messageRequest.getPayloadType();
            _cellStub.send(destination, message, payloadType, callback);
        }
        _log.debug("messages send");
    }

    public void setCellStub(CellStub cellStub) {
        _cellStub = cellStub;
    }

    @Override
    public boolean allSuccessful() {
        return _allSuccessful;
    }

    private void processFailure() {
        _allSuccessful = false;
    }

    private void processAnswered() {
        _doneSignal.countDown();
    }

    /**
     * Callback to handle answer of a Pool to a PoolModifyModeMessage.
     * @author jans
     */
    private class CellMessageCallback implements MessageCallback<Message> {

        private CellMessageRequest _messageRequest;

        public CellMessageCallback(CellMessageRequest messageRequest) {
            _messageRequest = messageRequest;
//            considered sending as not successful until replied
            _messageRequest.setSuccessful(false);
        }

        private void evaluateReply(Message answer) {
            if ((answer != null) && (answer.getReturnCode() == 0)) {
                _messageRequest.setSuccessful(true);
            }
        }

        @Override
        public void success(Message message) {
            evaluateReply(message);
            setAnswered();
        }

        @Override
        public void failure(int rc, Object error) {
            setAnswered();
            processFailure();
        }

        @Override
        public void noroute() {
            setAnswered();
            processFailure();
        }

        @Override
        public void timeout() {
            setAnswered();
            processFailure();
        }

        private void setAnswered() {
            processAnswered();
            _log.debug("{} answered {}", _messageRequest.getDestination(),
                    System.currentTimeMillis());
        }
    }
}

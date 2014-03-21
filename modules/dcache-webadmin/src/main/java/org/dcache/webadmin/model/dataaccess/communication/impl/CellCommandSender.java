package org.dcache.webadmin.model.dataaccess.communication.impl;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellPath;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator.CellMessageRequest;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;

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
        for (CellMessageRequest<? extends Serializable> messageRequest : _messageGenerator) {
            MessageCallback callback = new CellMessageCallback(messageRequest);
            _log.debug("sending to: {}", messageRequest.getDestination());
            Serializable message = messageRequest.getPayload();
            CellPath destination = messageRequest.getDestination();
            Class payloadType = messageRequest.getPayloadType();
            CellStub.addCallback(_cellStub.send(destination, message, payloadType),
                                 callback, MoreExecutors.sameThreadExecutor());
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
        _log.debug("failure {}", System.currentTimeMillis());
        _allSuccessful = false;
    }

    private void processAnswered() {
        _doneSignal.countDown();
    }

    /**
     * Callback to handle answer of a Cell to a Message.
     * @author jans
     */
    private class CellMessageCallback extends AbstractMessageCallback<Serializable>
    {
        private CellMessageRequest<? extends Serializable> _messageRequest;

        public CellMessageCallback(
                CellMessageRequest<? extends Serializable> messageRequest) {
            _messageRequest = messageRequest;
//            considered sending as not successful until replied
            _messageRequest.setSuccessful(false);
        }

        private void evaluateReply(Message answer) {
            if ((answer != null) && (answer.getReturnCode() == 0)) {
                _messageRequest.setSuccessful(true);
            } else {
                processFailure();
            }
        }

        @Override
        public void success(Serializable message) {
            if (message instanceof Message) {
                evaluateReply((Message) message);
            } else {
                _messageRequest.setSuccessful(true);
            }
            _messageRequest.setAnswer(message);
            setAnswered();
        }

        @Override
        public void failure(int rc, Object error) {
            _log.debug("error object: {}", error.toString());
            processFailure();
            setAnswered();
        }

        @Override
        public void noroute(CellPath path) {
            processFailure();
            setAnswered();
        }

        @Override
        public void timeout(String error) {
            processFailure();
            setAnswered();
        }

        private void setAnswered() {
            _log.debug("{} answered {}", _messageRequest.getDestination(),
                    System.currentTimeMillis());
            processAnswered();
        }
    }
}

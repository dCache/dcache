package diskCacheV111.vehicles;

import dmg.cells.nucleus.CellAddressCore;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.security.auth.Subject;

public abstract class InfoMessage implements Serializable {

    private static final AtomicLong COUNTER = new AtomicLong(0);

    private final String _cellType;
    private final String _messageType;

    private final CellAddressCore _cellAddress;

    private long _timeQueued;
    private int _resultCode;
    private String _message = "";
    private final long _timestamp = System.currentTimeMillis();
    private String _transaction;
    private final long _transactionID = COUNTER.incrementAndGet();
    private Subject _subject;

    private static final long serialVersionUID = -8035876156296337291L;

    public InfoMessage(String messageType, String cellType, CellAddressCore address) {
        _cellAddress = address;
        _cellType = cellType;
        _messageType = messageType;
    }

    public abstract void accept(InfoMessageVisitor visitor);

    @Override
    public String toString() {
        return "InfoMessage{" +
              "cellType='" + _cellType + '\'' +
              ", messageType='" + _messageType + '\'' +
              ", cellAddress=" + _cellAddress +
              ", timeQueued=" + _timeQueued +
              ", resultCode=" + _resultCode +
              ", message='" + _message + '\'' +
              ", timestamp=" + _timestamp +
              ", transaction='" + _transaction + '\'' +
              ", transactionID=" + _transactionID +
              ", subject=" + _subject +
              '}';
    }

    public void setResult(int resultCode, String resultMessage) {
        _message = resultMessage;
        _resultCode = resultCode;
    }

    public void setTimeQueued(long timeQueued) {
        _timeQueued = timeQueued;
    }

    public long getTimeQueued() {
        return _timeQueued;
    }

    public String getCellType() {
        return _cellType;
    }

    public String getMessageType() {
        return _messageType;
    }

    @Nullable
    public CellAddressCore getCellAddress() {
        return _cellAddress;
    }

    public String getMessage() {
        return _message;
    }

    public int getResultCode() {
        return _resultCode;
    }

    public long getTimestamp() {
        return _timestamp;
    }

    public synchronized void setTransaction(String transaction) {
        _transaction = transaction;
    }

    public synchronized String getTransaction() {

        if (_transaction == null) {
            String sb = this.getCellType() + ':' + this.getCellAddress() + ':' +
                  this.getTimestamp() + '-' + _transactionID;
            _transaction = sb;
        }
        return _transaction;
    }

    public void setSubject(Subject subject) {
        _subject = subject;
    }

    public Subject getSubject() {
        return _subject;
    }
}

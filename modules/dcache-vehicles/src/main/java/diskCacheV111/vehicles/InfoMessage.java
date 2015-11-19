package diskCacheV111.vehicles;

import javax.security.auth.Subject;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.dcache.auth.Subjects;

public abstract class InfoMessage implements Serializable
{
    private static final SimpleDateFormat __dateFormat = new SimpleDateFormat("MM.dd HH:mm:ss");

    private static AtomicLong COUNTER = new  AtomicLong(0);

    private final String _cellType;
    private final String _messageType;
    private final String _cellName;
    private long _timeQueued;
    private int _resultCode;
    private String _message = "";
    private long _timestamp = System.currentTimeMillis();
    private String _transaction;
    private long _transactionID = COUNTER.incrementAndGet();
    private Subject _subject;

    private static final long serialVersionUID = -8035876156296337291L;

    public InfoMessage(String messageType,
                       String cellType,
                       String cellName)
    {
        _cellName = cellName;
        _cellType = cellType;
        _messageType = messageType;
    }

    public abstract void accept(InfoMessageVisitor visitor);

    protected static synchronized String formatTimestamp(Date timestamp)
    {
        return __dateFormat.format(timestamp);
    }

    public String getInfoHeader()
    {
        return formatTimestamp(new Date(_timestamp)) + " [" + _cellType + ":" + _cellName + ":" + _messageType + "]";
    }

    public String getResult()
    {
        return "{" + _resultCode + ":\"" + _message + "\"}";
    }

    public String toString()
    {
        return getInfoHeader() + " " + getResult();
    }

    public void setResult(int resultCode, String resultMessage)
    {
        _message = resultMessage;
        _resultCode = resultCode;
    }

    public void setTimeQueued(long timeQueued)
    {
        _timeQueued = timeQueued;
    }

    public long getTimeQueued()
    {
        return _timeQueued;
    }

    public String getCellType()
    {
        return _cellType;
    }

    public String getMessageType()
    {
        return _messageType;
    }

    public String getCellName()
    {
        return _cellName;
    }

    public String getMessage()
    {
        return _message;
    }

    public int getResultCode()
    {
        return _resultCode;
    }

    public long getTimestamp()
    {
        return _timestamp;
    }

    public synchronized void setTransaction(String transaction)
    {
        _transaction = transaction;
    }

    public synchronized String getTransaction()
    {

        if (_transaction == null) {
            StringBuilder sb = new StringBuilder();
            sb.append(this.getCellType()).append(":").append(this.getCellName()).append(":");
            sb.append(this.getTimestamp()).append("-").append(_transactionID);
            _transaction = sb.toString();
        }
        return _transaction;
    }

    public void setSubject(Subject subject)
    {
        _subject = subject;
    }

    public Subject getSubject()
    {
        /* The null check ensures compatibility with pools earlier
         * than version 2.1. Those pools do not include a subject
         * field.
         */
        return (_subject == null) ? Subjects.ROOT : _subject;
    }
}

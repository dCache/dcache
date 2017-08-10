// $Id: JobInfo.java,v 1.3 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.google.common.base.Preconditions.checkNotNull;

public class JobInfo implements Serializable {

    private final DateTimeFormatter __formatter = DateTimeFormatter.ofPattern("MM/dd-HH:mm:ss");

    private static final long serialVersionUID = 5209798222006083955L;

    private final String _client;
    private final long _clientId;
    private final long _submitTime;
    private final long _startTime;
    private String _status;
    private final long _jobId;


    public JobInfo(long submitTime, long startTime, String status, int id, String clientName, long clientId) {
        _submitTime = submitTime;
        _startTime = startTime;
        _status = checkNotNull(status);
        _jobId = id;
        _client = clientName;
        _clientId = clientId;
    }

    public String getClientName() {
        return _client;
    }

    public long getClientId() {
        return _clientId;
    }

    public long getStartTime() {
        return _startTime;
    }

    public long getSubmitTime() {
        return _submitTime;
    }

    public String getStatus() {
        return _status;
    }

    public long getJobId() {
        return _jobId;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(_jobId).append(';');
        sb.append(_client).append(':').append(_clientId);
        sb.append(';').append(LocalDateTime.ofInstant(Instant.ofEpochMilli(_submitTime), ZoneId.systemDefault()).format(__formatter));
        sb.append(';').append(LocalDateTime.ofInstant(Instant.ofEpochMilli(_startTime), ZoneId.systemDefault()).format(__formatter));
        sb.append(';').append(_status).append(';');
        return sb.toString();
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        _status = _status.intern();
    }


}
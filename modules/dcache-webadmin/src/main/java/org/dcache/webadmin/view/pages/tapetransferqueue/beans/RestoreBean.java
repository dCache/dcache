package org.dcache.webadmin.view.pages.tapetransferqueue.beans;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author jans
 */
public class RestoreBean implements Serializable{

    private static final long serialVersionUID = 6511001022217245840L;
    private final String _pnfsId;
    private final String _subnet;
    private String _pool = "";
    private String _startTime = "";
    private int _clients = -1;
    private int _retries = -1;
    private String _status = "";
    private boolean _erroneous;
    private final int _errorCode;
    private final String _errorMessage;

    public RestoreBean(String name, int errorCode, String errorMessage) {
        int pos = name.indexOf('@');
        _pnfsId = name.substring(0, pos);
        _subnet = name.substring(pos + 1);
        _errorCode = errorCode;
        _errorMessage = errorMessage;
        _erroneous = (_errorCode != 0) || ((_errorMessage != null) && (!_errorMessage.isEmpty()));
    }

    public int getClients() {
        return _clients;
    }

    public void setClients(int clients) {
        _clients = clients;
    }

    public boolean isErroneous() {
        return _erroneous;
    }

    public int getErrorCode() {
        return _errorCode;
    }

    public String getErrorMessage() {
        return _errorMessage;
    }

    public String getPnfsId() {
        return _pnfsId;
    }

    public String getPool() {
        return _pool;
    }

    public void setPool(String pool) {
        _pool = pool;
    }

    public int getRetries() {
        return _retries;
    }

    public void setRetries(int retries) {
        _retries = retries;
    }

    public String getStartTime() {
        return _startTime;
    }

    public void setStartTime(long startTime) {
        _startTime = new SimpleDateFormat("MM.dd HH:mm:ss").format(new Date(startTime));
    }

    public String getStatus() {
        return _status;
    }

    public void setStatus(String status) {
        _status = (status == null) || (status.isEmpty()) ? "&nbsp;" : status;
    }

    public String getSubnet() {
        return _subnet;
    }
}

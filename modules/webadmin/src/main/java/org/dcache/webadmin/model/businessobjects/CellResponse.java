package org.dcache.webadmin.model.businessobjects;

import java.io.Serializable;

/**
 * Stores a communication Response of a Cell
 * @author jans
 */
public class CellResponse implements Serializable {

    private static final long serialVersionUID = -9101559184040729738L;
    private String _cellName = "";
    private String _response = "";
    private boolean _isFailure;

    public String getCellName() {
        return _cellName;
    }

    public void setCellName(String name) {
        _cellName = name;
    }

    public String getResponse() {
        return _response;
    }

    public void setResponse(String response) {
        _response = response;
    }

    public boolean isFailure() {
        return _isFailure;
    }

    public void setIsFailure(boolean isFailure) {
        _isFailure = isFailure;
    }
}

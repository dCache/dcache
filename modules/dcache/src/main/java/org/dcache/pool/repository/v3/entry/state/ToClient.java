package org.dcache.pool.repository.v3.entry.state;

public class ToClient {

    private boolean _isSet;


    public ToClient(boolean isSet) {
        _isSet = isSet;
    }


    public void set(boolean newValue) {
        _isSet = newValue;
    }

    public boolean isSet() {
        return _isSet;
    }


    public String stringValue() {
        if(_isSet) {
            return "to_client";
        }else{
            return "";
        }
    }
}

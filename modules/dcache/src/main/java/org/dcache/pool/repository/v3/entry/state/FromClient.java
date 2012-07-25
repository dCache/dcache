package org.dcache.pool.repository.v3.entry.state;

public class FromClient {
    private boolean _isSet;


    public FromClient(boolean isSet) {
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
            return "from_client";
        }else{
            return "";
        }
    }
}

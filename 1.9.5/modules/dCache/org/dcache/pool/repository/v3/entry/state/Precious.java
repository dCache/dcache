package org.dcache.pool.repository.v3.entry.state;

public class Precious {

    private boolean _isSet = false;


    public Precious(boolean isSet) {
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
            return "precious";
        }else{
            return "";
        }
    }

}

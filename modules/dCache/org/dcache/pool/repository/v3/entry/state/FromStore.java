package org.dcache.pool.repository.v3.entry.state;

public class FromStore {
    private boolean _isSet = false;


    public FromStore(boolean isSet) {
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
            return "from_store";
        }else{
            return "";
        }
    }
}

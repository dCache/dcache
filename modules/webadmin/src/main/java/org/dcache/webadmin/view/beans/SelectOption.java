package org.dcache.webadmin.view.beans;

import java.io.Serializable;

/**
 * A simple Bean to store a key/value pair for the use of dropdown menus
 * @author jans
 */
public class SelectOption implements Serializable {

    private static final long serialVersionUID = 10275539422237495L;
    private String _value;
    private int _key;

    public SelectOption(int key, String value) {
        this._key = key;
        this._value = value;
    }

    public int getKey() {
        return _key;
    }

    public void setKey(int key) {
        this._key = key;
    }

    public String getValue() {
        return _value;
    }

    public void setValue(String value) {
        this._value = value;
    }
}

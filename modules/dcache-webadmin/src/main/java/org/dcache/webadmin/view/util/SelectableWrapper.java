package org.dcache.webadmin.view.util;

import java.io.Serializable;

/**
 * Wrapper to make a Bean selectable
 * @author jans
 */
public class SelectableWrapper<T extends Serializable> implements Serializable {

    private static final long serialVersionUID = 2540365244546137089L;
    private T _wrapped;
    private Boolean _selected = Boolean.FALSE;
    private boolean _pending = false;

    public boolean isPending() {
        return _pending;
    }

    public void setPending(boolean pending) {
        _pending = pending;
    }

    public SelectableWrapper(T wrapped) {
        _wrapped = wrapped;
    }

    public Boolean isSelected() {
        return _selected;
    }

    public void setSelected(Boolean selected) {
        _selected = selected;
    }

    public T getWrapped() {
        return _wrapped;
    }

    public void setWrapped(T wrapped) {
        _wrapped = wrapped;
    }
}

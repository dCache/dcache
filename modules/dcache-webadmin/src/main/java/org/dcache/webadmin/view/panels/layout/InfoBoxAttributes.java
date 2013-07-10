/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.webadmin.view.panels.layout;

import java.io.Serializable;

/**
 *
 * @author tanja
 */
public class InfoBoxAttributes implements Serializable {

    private static final long serialVersionUID = 1L;
    private String _id;
    private String _layoutClass;
    private String _attr;

    public InfoBoxAttributes(String id, String layoutClass, String attr) {
        _id = id;
        _layoutClass = layoutClass;
        _attr = attr;
    }

    public String getAttributelId() {
        return _id;
    }

    public String getLayoutClass() {
        return _layoutClass;
    }

    public String getAttributelString() {
        return _attr;
    }

}

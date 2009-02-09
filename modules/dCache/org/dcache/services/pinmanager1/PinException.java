/*
 * PinException.java
 *
 * Created on December 20, 2007, 2:44 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.services.pinmanager1;

/**
 *
 * @author timur
 */
public class PinException extends diskCacheV111.util.CacheException {

    /** Creates a new instance of PinException */
    public PinException(String msg) {
       super(msg);
    }

    public PinException(int rc , String msg ) {
        super(rc,msg);
    }

}

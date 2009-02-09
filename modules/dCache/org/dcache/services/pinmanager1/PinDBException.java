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
public class PinDBException extends PinException {

    /** Creates a new instance of PinDBException */
    public PinDBException(String msg) {
       super(msg);
    }

    public PinDBException(int rc , String msg ) {
        super(rc,msg);
    }

}

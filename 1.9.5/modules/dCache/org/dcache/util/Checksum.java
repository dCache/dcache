/*
 * Checksum.java
 *
 * Created on November 4, 2008, 5:01 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.util;
import java.io.Serializable;

/**
 *
 * @author timur
 */
public class Checksum  implements Serializable{
    
    static final long serialVersionUID = 7338775749513974986L;
    
    private ChecksumType type;
    private String value;
    
    /** Creates a new instance of Checksum */
    public Checksum(ChecksumType type, String value) {
        this.type = type;
        this.value = value;
    }

    public ChecksumType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }
    
}

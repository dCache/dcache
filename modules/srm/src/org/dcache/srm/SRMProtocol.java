/*
 * SRMProtocol.java
 *
 * Created on January 11, 2006, 4:52 PM
 */

package org.dcache.srm;

/**
 *
 * @author  timur
 */
public class SRMProtocol {
    public static final SRMProtocol V1_1 = new SRMProtocol("V1.1");
    public static final SRMProtocol V2_1 = new SRMProtocol("V2.1");
    
    private String protocol;
    /** Creates a new instance of SRMProtocol */
    private SRMProtocol(String protocol) {
        this.protocol=protocol;
    }
    
    public static SRMProtocol getSRMProtocol(String protocol) {
        if(V1_1.protocol.equals(protocol)) return V1_1;
        if(V2_1.protocol.equals(protocol)) return V2_1;
        return null;
    }
    
    public String toString() {
        return protocol;
    }
  
}

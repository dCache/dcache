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
public enum  SRMProtocol {
    V1_1 ("V1.1"),
        V2_1 ("V2.1");

    private final String _protocol;

    private SRMProtocol(String protocol) {
        _protocol=protocol;
    }

    public static SRMProtocol getSRMProtocol(String protocol) {
        for (SRMProtocol value : values()) {
            if (value._protocol.equals(protocol)) {
                return value;
            }
        }
        return null;
    }

    public String toString() {
        return _protocol;
    }

}

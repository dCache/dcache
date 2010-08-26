/**
 * SRMService.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public interface SRMService extends javax.xml.rpc.Service {
    public java.lang.String getsrmAddress();

    public org.dcache.srm.v2_2.ISRM getsrm() throws javax.xml.rpc.ServiceException;

    public org.dcache.srm.v2_2.ISRM getsrm(java.net.URL portAddress) throws javax.xml.rpc.ServiceException;
}

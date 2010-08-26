/**
 * SRMServerV1.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.client.axis;

public interface SRMServerV1 extends javax.xml.rpc.Service {

/**
 * diskCacheV111.srm.server.SRMServerV1 web service
 */
    public java.lang.String getISRMAddress();

    public org.dcache.srm.client.axis.ISRM_PortType getISRM() throws javax.xml.rpc.ServiceException;

    public org.dcache.srm.client.axis.ISRM_PortType getISRM(java.net.URL portAddress) throws javax.xml.rpc.ServiceException;
}

/**
 * ISRM.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.2RC2 Nov 16, 2004 (12:19:44 EST) WSDL2Java emitter.
 */

package org.dcache.srm.client.axis;

public interface ISRM extends java.rmi.Remote {
    public org.dcache.srm.client.axis.RequestStatus put(java.lang.String[] arg0, java.lang.String[] arg1, long[] arg2, boolean[] arg3, java.lang.String[] arg4) throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.RequestStatus get(java.lang.String[] arg0, java.lang.String[] arg1) throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.RequestStatus copy(java.lang.String[] arg0, java.lang.String[] arg1, boolean[] arg2) throws java.rmi.RemoteException;
    public boolean ping() throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.RequestStatus pin(java.lang.String[] arg0) throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.RequestStatus unPin(java.lang.String[] arg0, int arg1) throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.RequestStatus setFileStatus(int arg0, int arg1, java.lang.String arg2) throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.RequestStatus getRequestStatus(int arg0) throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.FileMetaData[] getFileMetaData(java.lang.String[] arg0) throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.RequestStatus mkPermanent(java.lang.String[] arg0) throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.RequestStatus getEstGetTime(java.lang.String[] arg0, java.lang.String[] arg1) throws java.rmi.RemoteException;
    public org.dcache.srm.client.axis.RequestStatus getEstPutTime(java.lang.String[] arg0, java.lang.String[] arg1, long[] arg2, boolean[] arg3, java.lang.String[] arg4) throws java.rmi.RemoteException;
    public void advisoryDelete(java.lang.String[] arg0) throws java.rmi.RemoteException;
    public java.lang.String[] getProtocols() throws java.rmi.RemoteException;
}

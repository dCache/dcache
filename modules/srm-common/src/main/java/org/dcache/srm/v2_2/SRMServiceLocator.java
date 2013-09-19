/**
 * SRMServiceLocator.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SRMServiceLocator extends org.apache.axis.client.Service implements org.dcache.srm.v2_2.SRMService {

    private static final long serialVersionUID = 8424789468984987966L;

    public SRMServiceLocator() {
    }


    public SRMServiceLocator(org.apache.axis.EngineConfiguration config) {
        super(config);
    }

    public SRMServiceLocator(java.lang.String wsdlLoc, javax.xml.namespace.QName sName) throws javax.xml.rpc.ServiceException {
        super(wsdlLoc, sName);
    }

    // Use to get a proxy class for srm

    /**
     * the following location of the service is specific to the 
     *                particular deployment and is not part of the specification
     */
    private java.lang.String srm_address = "https://localhost:8443/ogsa/services/srm";

    @Override
    public java.lang.String getsrmAddress() {
        return srm_address;
    }

    // The WSDD service name defaults to the port name.
    private java.lang.String srmWSDDServiceName = "srm";

    public java.lang.String getsrmWSDDServiceName() {
        return srmWSDDServiceName;
    }

    public void setsrmWSDDServiceName(java.lang.String name) {
        srmWSDDServiceName = name;
    }

    @Override
    public org.dcache.srm.v2_2.ISRM getsrm() throws javax.xml.rpc.ServiceException {
       java.net.URL endpoint;
        try {
            endpoint = new java.net.URL(srm_address);
        }
        catch (java.net.MalformedURLException e) {
            throw new javax.xml.rpc.ServiceException(e);
        }
        return getsrm(endpoint);
    }

    @Override
    public org.dcache.srm.v2_2.ISRM getsrm(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
        try {
            org.dcache.srm.v2_2.SrmSoapBindingStub _stub = new org.dcache.srm.v2_2.SrmSoapBindingStub(portAddress, this);
            _stub.setPortName(getsrmWSDDServiceName());
            return _stub;
        }
        catch (org.apache.axis.AxisFault e) {
            return null;
        }
    }

    public void setsrmEndpointAddress(java.lang.String address) {
        srm_address = address;
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    @Override
    public java.rmi.Remote getPort(Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
        try {
            if (org.dcache.srm.v2_2.ISRM.class.isAssignableFrom(serviceEndpointInterface)) {
                org.dcache.srm.v2_2.SrmSoapBindingStub _stub = new org.dcache.srm.v2_2.SrmSoapBindingStub(new java.net.URL(srm_address), this);
                _stub.setPortName(getsrmWSDDServiceName());
                return _stub;
            }
        }
        catch (java.lang.Throwable t) {
            throw new javax.xml.rpc.ServiceException(t);
        }
        throw new javax.xml.rpc.ServiceException("There is no stub implementation for the interface:  " + (serviceEndpointInterface == null ? "null" : serviceEndpointInterface.getName()));
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    @Override
    public java.rmi.Remote getPort(javax.xml.namespace.QName portName, Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
        if (portName == null) {
            return getPort(serviceEndpointInterface);
        }
        java.lang.String inputPortName = portName.getLocalPart();
        if ("srm".equals(inputPortName)) {
            return getsrm();
        }
        else  {
            java.rmi.Remote _stub = getPort(serviceEndpointInterface);
            ((org.apache.axis.client.Stub) _stub).setPortName(portName);
            return _stub;
        }
    }

    @Override
    public javax.xml.namespace.QName getServiceName() {
        return new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "SRMService");
    }

    private java.util.HashSet ports;

    @Override
    public java.util.Iterator getPorts() {
        if (ports == null) {
            ports = new java.util.HashSet();
            ports.add(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srm"));
        }
        return ports.iterator();
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(java.lang.String portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        
if ("srm".equals(portName)) {
            setsrmEndpointAddress(address);
        }
        else 
{ // Unknown Port Name
            throw new javax.xml.rpc.ServiceException(" Cannot set Endpoint Address for Unknown Port" + portName);
        }
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(javax.xml.namespace.QName portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        setEndpointAddress(portName.getLocalPart(), address);
    }

}

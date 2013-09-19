/**
 * SRMServerV1Locator.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.client.axis;

import java.util.Collection;

public class SRMServerV1Locator extends org.apache.axis.client.Service implements org.dcache.srm.client.axis.SRMServerV1 {

    private static final long serialVersionUID = -2612073659479050609L;

    /**
 * diskCacheV111.srm.server.SRMServerV1 web service
 */

    public SRMServerV1Locator() {
    }


    public SRMServerV1Locator(org.apache.axis.EngineConfiguration config) {
        super(config);
    }

    public SRMServerV1Locator(java.lang.String wsdlLoc, javax.xml.namespace.QName sName) throws javax.xml.rpc.ServiceException {
        super(wsdlLoc, sName);
    }

    // Use to get a proxy class for ISRM
    private java.lang.String ISRM_address = "https://fndca.fnal.gov:24129/srm/managerv1";

    @Override
    public java.lang.String getISRMAddress() {
        return ISRM_address;
    }

    // The WSDD service name defaults to the port name.
    private java.lang.String ISRMWSDDServiceName = "ISRM";

    public java.lang.String getISRMWSDDServiceName() {
        return ISRMWSDDServiceName;
    }

    public void setISRMWSDDServiceName(java.lang.String name) {
        ISRMWSDDServiceName = name;
    }

    @Override
    public org.dcache.srm.client.axis.ISRM_PortType getISRM() throws javax.xml.rpc.ServiceException {
       java.net.URL endpoint;
        try {
            endpoint = new java.net.URL(ISRM_address);
        }
        catch (java.net.MalformedURLException e) {
            throw new javax.xml.rpc.ServiceException(e);
        }
        return getISRM(endpoint);
    }

    @Override
    public org.dcache.srm.client.axis.ISRM_PortType getISRM(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
        try {
            org.dcache.srm.client.axis.ISRMStub _stub = new org.dcache.srm.client.axis.ISRMStub(portAddress, this);
            _stub.setPortName(getISRMWSDDServiceName());
            return _stub;
        }
        catch (org.apache.axis.AxisFault e) {
            return null;
        }
    }

    public void setISRMEndpointAddress(java.lang.String address) {
        ISRM_address = address;
    }

    /**
     * For the given interface, get the stub implementation.
     * If this service has no port for the given interface,
     * then ServiceException is thrown.
     */
    @Override
    public java.rmi.Remote getPort(Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
        try {
            if (org.dcache.srm.client.axis.ISRM_PortType.class.isAssignableFrom(serviceEndpointInterface)) {
                org.dcache.srm.client.axis.ISRMStub _stub = new org.dcache.srm.client.axis.ISRMStub(new java.net.URL(ISRM_address), this);
                _stub.setPortName(getISRMWSDDServiceName());
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
        if ("ISRM".equals(inputPortName)) {
            return getISRM();
        }
        else  {
            java.rmi.Remote _stub = getPort(serviceEndpointInterface);
            ((org.apache.axis.client.Stub) _stub).setPortName(portName);
            return _stub;
        }
    }

    @Override
    public javax.xml.namespace.QName getServiceName() {
        return new javax.xml.namespace.QName("http://srm.1.0.ns", "SRMServerV1");
    }

    private Collection ports;

    @Override
    public java.util.Iterator getPorts() {
        if (ports == null) {
            ports = new java.util.HashSet();
            ports.add(new javax.xml.namespace.QName("http://srm.1.0.ns", "ISRM"));
        }
        return ports.iterator();
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(java.lang.String portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        
if ("ISRM".equals(portName)) {
            setISRMEndpointAddress(address);
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

/**
 * SrmMvRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmMvRequest  implements java.io.Serializable {
    private static final long serialVersionUID = 4799863056406691330L;
    private java.lang.String authorizationID;

    private org.apache.axis.types.URI fromSURL;

    private org.apache.axis.types.URI toSURL;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo;

    public SrmMvRequest() {
    }

    public SrmMvRequest(
           java.lang.String authorizationID,
           org.apache.axis.types.URI fromSURL,
           org.apache.axis.types.URI toSURL,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
           this.authorizationID = authorizationID;
           this.fromSURL = fromSURL;
           this.toSURL = toSURL;
           this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the authorizationID value for this SrmMvRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmMvRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the fromSURL value for this SrmMvRequest.
     * 
     * @return fromSURL
     */
    public org.apache.axis.types.URI getFromSURL() {
        return fromSURL;
    }


    /**
     * Sets the fromSURL value for this SrmMvRequest.
     * 
     * @param fromSURL
     */
    public void setFromSURL(org.apache.axis.types.URI fromSURL) {
        this.fromSURL = fromSURL;
    }


    /**
     * Gets the toSURL value for this SrmMvRequest.
     * 
     * @return toSURL
     */
    public org.apache.axis.types.URI getToSURL() {
        return toSURL;
    }


    /**
     * Sets the toSURL value for this SrmMvRequest.
     * 
     * @param toSURL
     */
    public void setToSURL(org.apache.axis.types.URI toSURL) {
        this.toSURL = toSURL;
    }


    /**
     * Gets the storageSystemInfo value for this SrmMvRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmMvRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmMvRequest)) {
            return false;
        }
        SrmMvRequest other = (SrmMvRequest) obj;
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.authorizationID==null && other.getAuthorizationID()==null) || 
             (this.authorizationID!=null &&
              this.authorizationID.equals(other.getAuthorizationID()))) &&
            ((this.fromSURL==null && other.getFromSURL()==null) || 
             (this.fromSURL!=null &&
              this.fromSURL.equals(other.getFromSURL()))) &&
            ((this.toSURL==null && other.getToSURL()==null) || 
             (this.toSURL!=null &&
              this.toSURL.equals(other.getToSURL()))) &&
            ((this.storageSystemInfo==null && other.getStorageSystemInfo()==null) || 
             (this.storageSystemInfo!=null &&
              this.storageSystemInfo.equals(other.getStorageSystemInfo())));
        __equalsCalc = null;
        return _equals;
    }

    private boolean __hashCodeCalc;
    public synchronized int hashCode() {
        if (__hashCodeCalc) {
            return 0;
        }
        __hashCodeCalc = true;
        int _hashCode = 1;
        if (getAuthorizationID() != null) {
            _hashCode += getAuthorizationID().hashCode();
        }
        if (getFromSURL() != null) {
            _hashCode += getFromSURL().hashCode();
        }
        if (getToSURL() != null) {
            _hashCode += getToSURL().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmMvRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fromSURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fromSURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("toSURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "toSURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("storageSystemInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "storageSystemInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTExtraInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
    }

    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

    /**
     * Get Custom Serializer
     */
    public static org.apache.axis.encoding.Serializer getSerializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new  org.apache.axis.encoding.ser.BeanSerializer(
            _javaType, _xmlType, typeDesc);
    }

    /**
     * Get Custom Deserializer
     */
    public static org.apache.axis.encoding.Deserializer getDeserializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new  org.apache.axis.encoding.ser.BeanDeserializer(
            _javaType, _xmlType, typeDesc);
    }

}

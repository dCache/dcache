/**
 * SrmMkdirRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmMkdirRequest  implements java.io.Serializable {
    private static final long serialVersionUID = 1468930835796373883L;
    private java.lang.String authorizationID;

    private org.apache.axis.types.URI SURL;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo;

    public SrmMkdirRequest() {
    }

    public SrmMkdirRequest(
           java.lang.String authorizationID,
           org.apache.axis.types.URI SURL,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
           this.authorizationID = authorizationID;
           this.SURL = SURL;
           this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the authorizationID value for this SrmMkdirRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmMkdirRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the SURL value for this SrmMkdirRequest.
     * 
     * @return SURL
     */
    public org.apache.axis.types.URI getSURL() {
        return SURL;
    }


    /**
     * Sets the SURL value for this SrmMkdirRequest.
     * 
     * @param SURL
     */
    public void setSURL(org.apache.axis.types.URI SURL) {
        this.SURL = SURL;
    }


    /**
     * Gets the storageSystemInfo value for this SrmMkdirRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmMkdirRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmMkdirRequest)) {
            return false;
        }
        SrmMkdirRequest other = (SrmMkdirRequest) obj;
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
            ((this.SURL==null && other.getSURL()==null) || 
             (this.SURL!=null &&
              this.SURL.equals(other.getSURL()))) &&
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
        if (getSURL() != null) {
            _hashCode += getSURL().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmMkdirRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdirRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("SURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "SURL"));
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

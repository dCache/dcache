/**
 * SrmPurgeFromSpaceRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmPurgeFromSpaceRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -4368680252073483733L;
    private java.lang.String authorizationID;

    private org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs;

    private java.lang.String spaceToken;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo;

    public SrmPurgeFromSpaceRequest() {
    }

    public SrmPurgeFromSpaceRequest(
           java.lang.String authorizationID,
           org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs,
           java.lang.String spaceToken,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
           this.authorizationID = authorizationID;
           this.arrayOfSURLs = arrayOfSURLs;
           this.spaceToken = spaceToken;
           this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the authorizationID value for this SrmPurgeFromSpaceRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmPurgeFromSpaceRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the arrayOfSURLs value for this SrmPurgeFromSpaceRequest.
     * 
     * @return arrayOfSURLs
     */
    public org.dcache.srm.v2_2.ArrayOfAnyURI getArrayOfSURLs() {
        return arrayOfSURLs;
    }


    /**
     * Sets the arrayOfSURLs value for this SrmPurgeFromSpaceRequest.
     * 
     * @param arrayOfSURLs
     */
    public void setArrayOfSURLs(org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs) {
        this.arrayOfSURLs = arrayOfSURLs;
    }


    /**
     * Gets the spaceToken value for this SrmPurgeFromSpaceRequest.
     * 
     * @return spaceToken
     */
    public java.lang.String getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this SrmPurgeFromSpaceRequest.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(java.lang.String spaceToken) {
        this.spaceToken = spaceToken;
    }


    /**
     * Gets the storageSystemInfo value for this SrmPurgeFromSpaceRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmPurgeFromSpaceRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmPurgeFromSpaceRequest)) {
            return false;
        }
        SrmPurgeFromSpaceRequest other = (SrmPurgeFromSpaceRequest) obj;
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
            ((this.arrayOfSURLs==null && other.getArrayOfSURLs()==null) || 
             (this.arrayOfSURLs!=null &&
              this.arrayOfSURLs.equals(other.getArrayOfSURLs()))) &&
            ((this.spaceToken==null && other.getSpaceToken()==null) || 
             (this.spaceToken!=null &&
              this.spaceToken.equals(other.getSpaceToken()))) &&
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
        if (getArrayOfSURLs() != null) {
            _hashCode += getArrayOfSURLs().hashCode();
        }
        if (getSpaceToken() != null) {
            _hashCode += getSpaceToken().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmPurgeFromSpaceRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPurgeFromSpaceRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfSURLs");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfSURLs"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfAnyURI"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("spaceToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "spaceToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
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

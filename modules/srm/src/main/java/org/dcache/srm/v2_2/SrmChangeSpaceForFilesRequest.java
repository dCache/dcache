/**
 * SrmChangeSpaceForFilesRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmChangeSpaceForFilesRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -2003407402709482768L;
    private java.lang.String authorizationID;

    private org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs;

    private java.lang.String targetSpaceToken;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo;

    public SrmChangeSpaceForFilesRequest() {
    }

    public SrmChangeSpaceForFilesRequest(
           java.lang.String authorizationID,
           org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs,
           java.lang.String targetSpaceToken,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
           this.authorizationID = authorizationID;
           this.arrayOfSURLs = arrayOfSURLs;
           this.targetSpaceToken = targetSpaceToken;
           this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the authorizationID value for this SrmChangeSpaceForFilesRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmChangeSpaceForFilesRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the arrayOfSURLs value for this SrmChangeSpaceForFilesRequest.
     * 
     * @return arrayOfSURLs
     */
    public org.dcache.srm.v2_2.ArrayOfAnyURI getArrayOfSURLs() {
        return arrayOfSURLs;
    }


    /**
     * Sets the arrayOfSURLs value for this SrmChangeSpaceForFilesRequest.
     * 
     * @param arrayOfSURLs
     */
    public void setArrayOfSURLs(org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs) {
        this.arrayOfSURLs = arrayOfSURLs;
    }


    /**
     * Gets the targetSpaceToken value for this SrmChangeSpaceForFilesRequest.
     * 
     * @return targetSpaceToken
     */
    public java.lang.String getTargetSpaceToken() {
        return targetSpaceToken;
    }


    /**
     * Sets the targetSpaceToken value for this SrmChangeSpaceForFilesRequest.
     * 
     * @param targetSpaceToken
     */
    public void setTargetSpaceToken(java.lang.String targetSpaceToken) {
        this.targetSpaceToken = targetSpaceToken;
    }


    /**
     * Gets the storageSystemInfo value for this SrmChangeSpaceForFilesRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmChangeSpaceForFilesRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmChangeSpaceForFilesRequest)) {
            return false;
        }
        SrmChangeSpaceForFilesRequest other = (SrmChangeSpaceForFilesRequest) obj;
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
            ((this.targetSpaceToken==null && other.getTargetSpaceToken()==null) || 
             (this.targetSpaceToken!=null &&
              this.targetSpaceToken.equals(other.getTargetSpaceToken()))) &&
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
        if (getTargetSpaceToken() != null) {
            _hashCode += getTargetSpaceToken().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmChangeSpaceForFilesRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeSpaceForFilesRequest"));
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
        elemField.setFieldName("targetSpaceToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "targetSpaceToken"));
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

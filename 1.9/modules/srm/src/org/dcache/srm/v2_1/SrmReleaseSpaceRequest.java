/**
 * SrmReleaseSpaceRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmReleaseSpaceRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.TSpaceToken spaceToken;

    private org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo;

    private java.lang.Boolean forceFileRelease;

    public SrmReleaseSpaceRequest() {
    }

    public SrmReleaseSpaceRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.TSpaceToken spaceToken,
           org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo,
           java.lang.Boolean forceFileRelease) {
           this.userID = userID;
           this.spaceToken = spaceToken;
           this.storageSystemInfo = storageSystemInfo;
           this.forceFileRelease = forceFileRelease;
    }


    /**
     * Gets the userID value for this SrmReleaseSpaceRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmReleaseSpaceRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the spaceToken value for this SrmReleaseSpaceRequest.
     * 
     * @return spaceToken
     */
    public org.dcache.srm.v2_1.TSpaceToken getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this SrmReleaseSpaceRequest.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(org.dcache.srm.v2_1.TSpaceToken spaceToken) {
        this.spaceToken = spaceToken;
    }


    /**
     * Gets the storageSystemInfo value for this SrmReleaseSpaceRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_1.TStorageSystemInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmReleaseSpaceRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the forceFileRelease value for this SrmReleaseSpaceRequest.
     * 
     * @return forceFileRelease
     */
    public java.lang.Boolean getForceFileRelease() {
        return forceFileRelease;
    }


    /**
     * Sets the forceFileRelease value for this SrmReleaseSpaceRequest.
     * 
     * @param forceFileRelease
     */
    public void setForceFileRelease(java.lang.Boolean forceFileRelease) {
        this.forceFileRelease = forceFileRelease;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmReleaseSpaceRequest)) return false;
        SrmReleaseSpaceRequest other = (SrmReleaseSpaceRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.userID==null && other.getUserID()==null) || 
             (this.userID!=null &&
              this.userID.equals(other.getUserID()))) &&
            ((this.spaceToken==null && other.getSpaceToken()==null) || 
             (this.spaceToken!=null &&
              this.spaceToken.equals(other.getSpaceToken()))) &&
            ((this.storageSystemInfo==null && other.getStorageSystemInfo()==null) || 
             (this.storageSystemInfo!=null &&
              this.storageSystemInfo.equals(other.getStorageSystemInfo()))) &&
            ((this.forceFileRelease==null && other.getForceFileRelease()==null) || 
             (this.forceFileRelease!=null &&
              this.forceFileRelease.equals(other.getForceFileRelease())));
        __equalsCalc = null;
        return _equals;
    }

    private boolean __hashCodeCalc = false;
    public synchronized int hashCode() {
        if (__hashCodeCalc) {
            return 0;
        }
        __hashCodeCalc = true;
        int _hashCode = 1;
        if (getUserID() != null) {
            _hashCode += getUserID().hashCode();
        }
        if (getSpaceToken() != null) {
            _hashCode += getSpaceToken().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        if (getForceFileRelease() != null) {
            _hashCode += getForceFileRelease().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmReleaseSpaceRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpaceRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("spaceToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "spaceToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSpaceToken"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("storageSystemInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "storageSystemInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TStorageSystemInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("forceFileRelease");
        elemField.setXmlName(new javax.xml.namespace.QName("", "forceFileRelease"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
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

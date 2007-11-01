/**
 * SrmCompactSpaceRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmCompactSpaceRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.TSpaceToken spaceToken;

    private org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo;

    private java.lang.Boolean doDynamicCompactFromNowOn;

    public SrmCompactSpaceRequest() {
    }

    public SrmCompactSpaceRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.TSpaceToken spaceToken,
           org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo,
           java.lang.Boolean doDynamicCompactFromNowOn) {
           this.userID = userID;
           this.spaceToken = spaceToken;
           this.storageSystemInfo = storageSystemInfo;
           this.doDynamicCompactFromNowOn = doDynamicCompactFromNowOn;
    }


    /**
     * Gets the userID value for this SrmCompactSpaceRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmCompactSpaceRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the spaceToken value for this SrmCompactSpaceRequest.
     * 
     * @return spaceToken
     */
    public org.dcache.srm.v2_1.TSpaceToken getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this SrmCompactSpaceRequest.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(org.dcache.srm.v2_1.TSpaceToken spaceToken) {
        this.spaceToken = spaceToken;
    }


    /**
     * Gets the storageSystemInfo value for this SrmCompactSpaceRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_1.TStorageSystemInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmCompactSpaceRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the doDynamicCompactFromNowOn value for this SrmCompactSpaceRequest.
     * 
     * @return doDynamicCompactFromNowOn
     */
    public java.lang.Boolean getDoDynamicCompactFromNowOn() {
        return doDynamicCompactFromNowOn;
    }


    /**
     * Sets the doDynamicCompactFromNowOn value for this SrmCompactSpaceRequest.
     * 
     * @param doDynamicCompactFromNowOn
     */
    public void setDoDynamicCompactFromNowOn(java.lang.Boolean doDynamicCompactFromNowOn) {
        this.doDynamicCompactFromNowOn = doDynamicCompactFromNowOn;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmCompactSpaceRequest)) return false;
        SrmCompactSpaceRequest other = (SrmCompactSpaceRequest) obj;
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
            ((this.doDynamicCompactFromNowOn==null && other.getDoDynamicCompactFromNowOn()==null) || 
             (this.doDynamicCompactFromNowOn!=null &&
              this.doDynamicCompactFromNowOn.equals(other.getDoDynamicCompactFromNowOn())));
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
        if (getDoDynamicCompactFromNowOn() != null) {
            _hashCode += getDoDynamicCompactFromNowOn().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmCompactSpaceRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCompactSpaceRequest"));
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
        elemField.setFieldName("doDynamicCompactFromNowOn");
        elemField.setXmlName(new javax.xml.namespace.QName("", "doDynamicCompactFromNowOn"));
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

/**
 * SrmUpdateSpaceRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmUpdateSpaceRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.TSpaceToken spaceToken;

    private org.dcache.srm.v2_1.TSizeInBytes newSizeOfTotalSpaceDesired;

    private org.dcache.srm.v2_1.TSizeInBytes newSizeOfGuaranteedSpaceDesired;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds newLifeTimeFromCallingTime;

    private org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo;

    public SrmUpdateSpaceRequest() {
    }

    public SrmUpdateSpaceRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.TSpaceToken spaceToken,
           org.dcache.srm.v2_1.TSizeInBytes newSizeOfTotalSpaceDesired,
           org.dcache.srm.v2_1.TSizeInBytes newSizeOfGuaranteedSpaceDesired,
           org.dcache.srm.v2_1.TLifeTimeInSeconds newLifeTimeFromCallingTime,
           org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo) {
           this.userID = userID;
           this.spaceToken = spaceToken;
           this.newSizeOfTotalSpaceDesired = newSizeOfTotalSpaceDesired;
           this.newSizeOfGuaranteedSpaceDesired = newSizeOfGuaranteedSpaceDesired;
           this.newLifeTimeFromCallingTime = newLifeTimeFromCallingTime;
           this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the userID value for this SrmUpdateSpaceRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmUpdateSpaceRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the spaceToken value for this SrmUpdateSpaceRequest.
     * 
     * @return spaceToken
     */
    public org.dcache.srm.v2_1.TSpaceToken getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this SrmUpdateSpaceRequest.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(org.dcache.srm.v2_1.TSpaceToken spaceToken) {
        this.spaceToken = spaceToken;
    }


    /**
     * Gets the newSizeOfTotalSpaceDesired value for this SrmUpdateSpaceRequest.
     * 
     * @return newSizeOfTotalSpaceDesired
     */
    public org.dcache.srm.v2_1.TSizeInBytes getNewSizeOfTotalSpaceDesired() {
        return newSizeOfTotalSpaceDesired;
    }


    /**
     * Sets the newSizeOfTotalSpaceDesired value for this SrmUpdateSpaceRequest.
     * 
     * @param newSizeOfTotalSpaceDesired
     */
    public void setNewSizeOfTotalSpaceDesired(org.dcache.srm.v2_1.TSizeInBytes newSizeOfTotalSpaceDesired) {
        this.newSizeOfTotalSpaceDesired = newSizeOfTotalSpaceDesired;
    }


    /**
     * Gets the newSizeOfGuaranteedSpaceDesired value for this SrmUpdateSpaceRequest.
     * 
     * @return newSizeOfGuaranteedSpaceDesired
     */
    public org.dcache.srm.v2_1.TSizeInBytes getNewSizeOfGuaranteedSpaceDesired() {
        return newSizeOfGuaranteedSpaceDesired;
    }


    /**
     * Sets the newSizeOfGuaranteedSpaceDesired value for this SrmUpdateSpaceRequest.
     * 
     * @param newSizeOfGuaranteedSpaceDesired
     */
    public void setNewSizeOfGuaranteedSpaceDesired(org.dcache.srm.v2_1.TSizeInBytes newSizeOfGuaranteedSpaceDesired) {
        this.newSizeOfGuaranteedSpaceDesired = newSizeOfGuaranteedSpaceDesired;
    }


    /**
     * Gets the newLifeTimeFromCallingTime value for this SrmUpdateSpaceRequest.
     * 
     * @return newLifeTimeFromCallingTime
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getNewLifeTimeFromCallingTime() {
        return newLifeTimeFromCallingTime;
    }


    /**
     * Sets the newLifeTimeFromCallingTime value for this SrmUpdateSpaceRequest.
     * 
     * @param newLifeTimeFromCallingTime
     */
    public void setNewLifeTimeFromCallingTime(org.dcache.srm.v2_1.TLifeTimeInSeconds newLifeTimeFromCallingTime) {
        this.newLifeTimeFromCallingTime = newLifeTimeFromCallingTime;
    }


    /**
     * Gets the storageSystemInfo value for this SrmUpdateSpaceRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_1.TStorageSystemInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmUpdateSpaceRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmUpdateSpaceRequest)) return false;
        SrmUpdateSpaceRequest other = (SrmUpdateSpaceRequest) obj;
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
            ((this.newSizeOfTotalSpaceDesired==null && other.getNewSizeOfTotalSpaceDesired()==null) || 
             (this.newSizeOfTotalSpaceDesired!=null &&
              this.newSizeOfTotalSpaceDesired.equals(other.getNewSizeOfTotalSpaceDesired()))) &&
            ((this.newSizeOfGuaranteedSpaceDesired==null && other.getNewSizeOfGuaranteedSpaceDesired()==null) || 
             (this.newSizeOfGuaranteedSpaceDesired!=null &&
              this.newSizeOfGuaranteedSpaceDesired.equals(other.getNewSizeOfGuaranteedSpaceDesired()))) &&
            ((this.newLifeTimeFromCallingTime==null && other.getNewLifeTimeFromCallingTime()==null) || 
             (this.newLifeTimeFromCallingTime!=null &&
              this.newLifeTimeFromCallingTime.equals(other.getNewLifeTimeFromCallingTime()))) &&
            ((this.storageSystemInfo==null && other.getStorageSystemInfo()==null) || 
             (this.storageSystemInfo!=null &&
              this.storageSystemInfo.equals(other.getStorageSystemInfo())));
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
        if (getNewSizeOfTotalSpaceDesired() != null) {
            _hashCode += getNewSizeOfTotalSpaceDesired().hashCode();
        }
        if (getNewSizeOfGuaranteedSpaceDesired() != null) {
            _hashCode += getNewSizeOfGuaranteedSpaceDesired().hashCode();
        }
        if (getNewLifeTimeFromCallingTime() != null) {
            _hashCode += getNewLifeTimeFromCallingTime().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmUpdateSpaceRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceRequest"));
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
        elemField.setFieldName("newSizeOfTotalSpaceDesired");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newSizeOfTotalSpaceDesired"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("newSizeOfGuaranteedSpaceDesired");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newSizeOfGuaranteedSpaceDesired"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("newLifeTimeFromCallingTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newLifeTimeFromCallingTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("storageSystemInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "storageSystemInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TStorageSystemInfo"));
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

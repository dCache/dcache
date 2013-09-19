/**
 * SrmUpdateSpaceRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmUpdateSpaceRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -6440014060356939852L;
    private java.lang.String authorizationID;

    private java.lang.String spaceToken;

    private org.apache.axis.types.UnsignedLong newSizeOfTotalSpaceDesired;

    private org.apache.axis.types.UnsignedLong newSizeOfGuaranteedSpaceDesired;

    private java.lang.Integer newLifeTime;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo;

    public SrmUpdateSpaceRequest() {
    }

    public SrmUpdateSpaceRequest(
           java.lang.String authorizationID,
           java.lang.String spaceToken,
           org.apache.axis.types.UnsignedLong newSizeOfTotalSpaceDesired,
           org.apache.axis.types.UnsignedLong newSizeOfGuaranteedSpaceDesired,
           java.lang.Integer newLifeTime,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
           this.authorizationID = authorizationID;
           this.spaceToken = spaceToken;
           this.newSizeOfTotalSpaceDesired = newSizeOfTotalSpaceDesired;
           this.newSizeOfGuaranteedSpaceDesired = newSizeOfGuaranteedSpaceDesired;
           this.newLifeTime = newLifeTime;
           this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the authorizationID value for this SrmUpdateSpaceRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmUpdateSpaceRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the spaceToken value for this SrmUpdateSpaceRequest.
     * 
     * @return spaceToken
     */
    public java.lang.String getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this SrmUpdateSpaceRequest.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(java.lang.String spaceToken) {
        this.spaceToken = spaceToken;
    }


    /**
     * Gets the newSizeOfTotalSpaceDesired value for this SrmUpdateSpaceRequest.
     * 
     * @return newSizeOfTotalSpaceDesired
     */
    public org.apache.axis.types.UnsignedLong getNewSizeOfTotalSpaceDesired() {
        return newSizeOfTotalSpaceDesired;
    }


    /**
     * Sets the newSizeOfTotalSpaceDesired value for this SrmUpdateSpaceRequest.
     * 
     * @param newSizeOfTotalSpaceDesired
     */
    public void setNewSizeOfTotalSpaceDesired(org.apache.axis.types.UnsignedLong newSizeOfTotalSpaceDesired) {
        this.newSizeOfTotalSpaceDesired = newSizeOfTotalSpaceDesired;
    }


    /**
     * Gets the newSizeOfGuaranteedSpaceDesired value for this SrmUpdateSpaceRequest.
     * 
     * @return newSizeOfGuaranteedSpaceDesired
     */
    public org.apache.axis.types.UnsignedLong getNewSizeOfGuaranteedSpaceDesired() {
        return newSizeOfGuaranteedSpaceDesired;
    }


    /**
     * Sets the newSizeOfGuaranteedSpaceDesired value for this SrmUpdateSpaceRequest.
     * 
     * @param newSizeOfGuaranteedSpaceDesired
     */
    public void setNewSizeOfGuaranteedSpaceDesired(org.apache.axis.types.UnsignedLong newSizeOfGuaranteedSpaceDesired) {
        this.newSizeOfGuaranteedSpaceDesired = newSizeOfGuaranteedSpaceDesired;
    }


    /**
     * Gets the newLifeTime value for this SrmUpdateSpaceRequest.
     * 
     * @return newLifeTime
     */
    public java.lang.Integer getNewLifeTime() {
        return newLifeTime;
    }


    /**
     * Sets the newLifeTime value for this SrmUpdateSpaceRequest.
     * 
     * @param newLifeTime
     */
    public void setNewLifeTime(java.lang.Integer newLifeTime) {
        this.newLifeTime = newLifeTime;
    }


    /**
     * Gets the storageSystemInfo value for this SrmUpdateSpaceRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmUpdateSpaceRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmUpdateSpaceRequest)) {
            return false;
        }
        SrmUpdateSpaceRequest other = (SrmUpdateSpaceRequest) obj;
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
            ((this.spaceToken==null && other.getSpaceToken()==null) || 
             (this.spaceToken!=null &&
              this.spaceToken.equals(other.getSpaceToken()))) &&
            ((this.newSizeOfTotalSpaceDesired==null && other.getNewSizeOfTotalSpaceDesired()==null) || 
             (this.newSizeOfTotalSpaceDesired!=null &&
              this.newSizeOfTotalSpaceDesired.equals(other.getNewSizeOfTotalSpaceDesired()))) &&
            ((this.newSizeOfGuaranteedSpaceDesired==null && other.getNewSizeOfGuaranteedSpaceDesired()==null) || 
             (this.newSizeOfGuaranteedSpaceDesired!=null &&
              this.newSizeOfGuaranteedSpaceDesired.equals(other.getNewSizeOfGuaranteedSpaceDesired()))) &&
            ((this.newLifeTime==null && other.getNewLifeTime()==null) || 
             (this.newLifeTime!=null &&
              this.newLifeTime.equals(other.getNewLifeTime()))) &&
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
        if (getSpaceToken() != null) {
            _hashCode += getSpaceToken().hashCode();
        }
        if (getNewSizeOfTotalSpaceDesired() != null) {
            _hashCode += getNewSizeOfTotalSpaceDesired().hashCode();
        }
        if (getNewSizeOfGuaranteedSpaceDesired() != null) {
            _hashCode += getNewSizeOfGuaranteedSpaceDesired().hashCode();
        }
        if (getNewLifeTime() != null) {
            _hashCode += getNewLifeTime().hashCode();
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
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("spaceToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "spaceToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("newSizeOfTotalSpaceDesired");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newSizeOfTotalSpaceDesired"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("newSizeOfGuaranteedSpaceDesired");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newSizeOfGuaranteedSpaceDesired"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("newLifeTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newLifeTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
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

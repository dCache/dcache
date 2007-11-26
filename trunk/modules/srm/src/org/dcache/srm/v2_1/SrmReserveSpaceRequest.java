/**
 * SrmReserveSpaceRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmReserveSpaceRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.TSpaceType typeOfSpace;

    private java.lang.String userSpaceTokenDescription;

    private org.dcache.srm.v2_1.TSizeInBytes sizeOfTotalSpaceDesired;

    private org.dcache.srm.v2_1.TSizeInBytes sizeOfGuaranteedSpaceDesired;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeOfSpaceToReserve;

    private org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo;

    public SrmReserveSpaceRequest() {
    }

    public SrmReserveSpaceRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.TSpaceType typeOfSpace,
           java.lang.String userSpaceTokenDescription,
           org.dcache.srm.v2_1.TSizeInBytes sizeOfTotalSpaceDesired,
           org.dcache.srm.v2_1.TSizeInBytes sizeOfGuaranteedSpaceDesired,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeOfSpaceToReserve,
           org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo) {
           this.userID = userID;
           this.typeOfSpace = typeOfSpace;
           this.userSpaceTokenDescription = userSpaceTokenDescription;
           this.sizeOfTotalSpaceDesired = sizeOfTotalSpaceDesired;
           this.sizeOfGuaranteedSpaceDesired = sizeOfGuaranteedSpaceDesired;
           this.lifetimeOfSpaceToReserve = lifetimeOfSpaceToReserve;
           this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the userID value for this SrmReserveSpaceRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmReserveSpaceRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the typeOfSpace value for this SrmReserveSpaceRequest.
     * 
     * @return typeOfSpace
     */
    public org.dcache.srm.v2_1.TSpaceType getTypeOfSpace() {
        return typeOfSpace;
    }


    /**
     * Sets the typeOfSpace value for this SrmReserveSpaceRequest.
     * 
     * @param typeOfSpace
     */
    public void setTypeOfSpace(org.dcache.srm.v2_1.TSpaceType typeOfSpace) {
        this.typeOfSpace = typeOfSpace;
    }


    /**
     * Gets the userSpaceTokenDescription value for this SrmReserveSpaceRequest.
     * 
     * @return userSpaceTokenDescription
     */
    public java.lang.String getUserSpaceTokenDescription() {
        return userSpaceTokenDescription;
    }


    /**
     * Sets the userSpaceTokenDescription value for this SrmReserveSpaceRequest.
     * 
     * @param userSpaceTokenDescription
     */
    public void setUserSpaceTokenDescription(java.lang.String userSpaceTokenDescription) {
        this.userSpaceTokenDescription = userSpaceTokenDescription;
    }


    /**
     * Gets the sizeOfTotalSpaceDesired value for this SrmReserveSpaceRequest.
     * 
     * @return sizeOfTotalSpaceDesired
     */
    public org.dcache.srm.v2_1.TSizeInBytes getSizeOfTotalSpaceDesired() {
        return sizeOfTotalSpaceDesired;
    }


    /**
     * Sets the sizeOfTotalSpaceDesired value for this SrmReserveSpaceRequest.
     * 
     * @param sizeOfTotalSpaceDesired
     */
    public void setSizeOfTotalSpaceDesired(org.dcache.srm.v2_1.TSizeInBytes sizeOfTotalSpaceDesired) {
        this.sizeOfTotalSpaceDesired = sizeOfTotalSpaceDesired;
    }


    /**
     * Gets the sizeOfGuaranteedSpaceDesired value for this SrmReserveSpaceRequest.
     * 
     * @return sizeOfGuaranteedSpaceDesired
     */
    public org.dcache.srm.v2_1.TSizeInBytes getSizeOfGuaranteedSpaceDesired() {
        return sizeOfGuaranteedSpaceDesired;
    }


    /**
     * Sets the sizeOfGuaranteedSpaceDesired value for this SrmReserveSpaceRequest.
     * 
     * @param sizeOfGuaranteedSpaceDesired
     */
    public void setSizeOfGuaranteedSpaceDesired(org.dcache.srm.v2_1.TSizeInBytes sizeOfGuaranteedSpaceDesired) {
        this.sizeOfGuaranteedSpaceDesired = sizeOfGuaranteedSpaceDesired;
    }


    /**
     * Gets the lifetimeOfSpaceToReserve value for this SrmReserveSpaceRequest.
     * 
     * @return lifetimeOfSpaceToReserve
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifetimeOfSpaceToReserve() {
        return lifetimeOfSpaceToReserve;
    }


    /**
     * Sets the lifetimeOfSpaceToReserve value for this SrmReserveSpaceRequest.
     * 
     * @param lifetimeOfSpaceToReserve
     */
    public void setLifetimeOfSpaceToReserve(org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeOfSpaceToReserve) {
        this.lifetimeOfSpaceToReserve = lifetimeOfSpaceToReserve;
    }


    /**
     * Gets the storageSystemInfo value for this SrmReserveSpaceRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_1.TStorageSystemInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmReserveSpaceRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmReserveSpaceRequest)) return false;
        SrmReserveSpaceRequest other = (SrmReserveSpaceRequest) obj;
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
            ((this.typeOfSpace==null && other.getTypeOfSpace()==null) || 
             (this.typeOfSpace!=null &&
              this.typeOfSpace.equals(other.getTypeOfSpace()))) &&
            ((this.userSpaceTokenDescription==null && other.getUserSpaceTokenDescription()==null) || 
             (this.userSpaceTokenDescription!=null &&
              this.userSpaceTokenDescription.equals(other.getUserSpaceTokenDescription()))) &&
            ((this.sizeOfTotalSpaceDesired==null && other.getSizeOfTotalSpaceDesired()==null) || 
             (this.sizeOfTotalSpaceDesired!=null &&
              this.sizeOfTotalSpaceDesired.equals(other.getSizeOfTotalSpaceDesired()))) &&
            ((this.sizeOfGuaranteedSpaceDesired==null && other.getSizeOfGuaranteedSpaceDesired()==null) || 
             (this.sizeOfGuaranteedSpaceDesired!=null &&
              this.sizeOfGuaranteedSpaceDesired.equals(other.getSizeOfGuaranteedSpaceDesired()))) &&
            ((this.lifetimeOfSpaceToReserve==null && other.getLifetimeOfSpaceToReserve()==null) || 
             (this.lifetimeOfSpaceToReserve!=null &&
              this.lifetimeOfSpaceToReserve.equals(other.getLifetimeOfSpaceToReserve()))) &&
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
        if (getTypeOfSpace() != null) {
            _hashCode += getTypeOfSpace().hashCode();
        }
        if (getUserSpaceTokenDescription() != null) {
            _hashCode += getUserSpaceTokenDescription().hashCode();
        }
        if (getSizeOfTotalSpaceDesired() != null) {
            _hashCode += getSizeOfTotalSpaceDesired().hashCode();
        }
        if (getSizeOfGuaranteedSpaceDesired() != null) {
            _hashCode += getSizeOfGuaranteedSpaceDesired().hashCode();
        }
        if (getLifetimeOfSpaceToReserve() != null) {
            _hashCode += getLifetimeOfSpaceToReserve().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmReserveSpaceRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("typeOfSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "typeOfSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSpaceType"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userSpaceTokenDescription");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userSpaceTokenDescription"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sizeOfTotalSpaceDesired");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfTotalSpaceDesired"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sizeOfGuaranteedSpaceDesired");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfGuaranteedSpaceDesired"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetimeOfSpaceToReserve");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetimeOfSpaceToReserve"));
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

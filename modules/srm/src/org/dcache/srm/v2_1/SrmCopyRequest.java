/**
 * SrmCopyRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmCopyRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.ArrayOfTCopyFileRequest arrayOfFileRequests;

    private java.lang.String userRequestDescription;

    private org.dcache.srm.v2_1.TOverwriteMode overwriteOption;

    private java.lang.Boolean removeSourceFiles;

    private org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds totalRetryTime;

    public SrmCopyRequest() {
    }

    public SrmCopyRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.ArrayOfTCopyFileRequest arrayOfFileRequests,
           java.lang.String userRequestDescription,
           org.dcache.srm.v2_1.TOverwriteMode overwriteOption,
           java.lang.Boolean removeSourceFiles,
           org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo,
           org.dcache.srm.v2_1.TLifeTimeInSeconds totalRetryTime) {
           this.userID = userID;
           this.arrayOfFileRequests = arrayOfFileRequests;
           this.userRequestDescription = userRequestDescription;
           this.overwriteOption = overwriteOption;
           this.removeSourceFiles = removeSourceFiles;
           this.storageSystemInfo = storageSystemInfo;
           this.totalRetryTime = totalRetryTime;
    }


    /**
     * Gets the userID value for this SrmCopyRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmCopyRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the arrayOfFileRequests value for this SrmCopyRequest.
     * 
     * @return arrayOfFileRequests
     */
    public org.dcache.srm.v2_1.ArrayOfTCopyFileRequest getArrayOfFileRequests() {
        return arrayOfFileRequests;
    }


    /**
     * Sets the arrayOfFileRequests value for this SrmCopyRequest.
     * 
     * @param arrayOfFileRequests
     */
    public void setArrayOfFileRequests(org.dcache.srm.v2_1.ArrayOfTCopyFileRequest arrayOfFileRequests) {
        this.arrayOfFileRequests = arrayOfFileRequests;
    }


    /**
     * Gets the userRequestDescription value for this SrmCopyRequest.
     * 
     * @return userRequestDescription
     */
    public java.lang.String getUserRequestDescription() {
        return userRequestDescription;
    }


    /**
     * Sets the userRequestDescription value for this SrmCopyRequest.
     * 
     * @param userRequestDescription
     */
    public void setUserRequestDescription(java.lang.String userRequestDescription) {
        this.userRequestDescription = userRequestDescription;
    }


    /**
     * Gets the overwriteOption value for this SrmCopyRequest.
     * 
     * @return overwriteOption
     */
    public org.dcache.srm.v2_1.TOverwriteMode getOverwriteOption() {
        return overwriteOption;
    }


    /**
     * Sets the overwriteOption value for this SrmCopyRequest.
     * 
     * @param overwriteOption
     */
    public void setOverwriteOption(org.dcache.srm.v2_1.TOverwriteMode overwriteOption) {
        this.overwriteOption = overwriteOption;
    }


    /**
     * Gets the removeSourceFiles value for this SrmCopyRequest.
     * 
     * @return removeSourceFiles
     */
    public java.lang.Boolean getRemoveSourceFiles() {
        return removeSourceFiles;
    }


    /**
     * Sets the removeSourceFiles value for this SrmCopyRequest.
     * 
     * @param removeSourceFiles
     */
    public void setRemoveSourceFiles(java.lang.Boolean removeSourceFiles) {
        this.removeSourceFiles = removeSourceFiles;
    }


    /**
     * Gets the storageSystemInfo value for this SrmCopyRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_1.TStorageSystemInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmCopyRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the totalRetryTime value for this SrmCopyRequest.
     * 
     * @return totalRetryTime
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getTotalRetryTime() {
        return totalRetryTime;
    }


    /**
     * Sets the totalRetryTime value for this SrmCopyRequest.
     * 
     * @param totalRetryTime
     */
    public void setTotalRetryTime(org.dcache.srm.v2_1.TLifeTimeInSeconds totalRetryTime) {
        this.totalRetryTime = totalRetryTime;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmCopyRequest)) return false;
        SrmCopyRequest other = (SrmCopyRequest) obj;
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
            ((this.arrayOfFileRequests==null && other.getArrayOfFileRequests()==null) || 
             (this.arrayOfFileRequests!=null &&
              this.arrayOfFileRequests.equals(other.getArrayOfFileRequests()))) &&
            ((this.userRequestDescription==null && other.getUserRequestDescription()==null) || 
             (this.userRequestDescription!=null &&
              this.userRequestDescription.equals(other.getUserRequestDescription()))) &&
            ((this.overwriteOption==null && other.getOverwriteOption()==null) || 
             (this.overwriteOption!=null &&
              this.overwriteOption.equals(other.getOverwriteOption()))) &&
            ((this.removeSourceFiles==null && other.getRemoveSourceFiles()==null) || 
             (this.removeSourceFiles!=null &&
              this.removeSourceFiles.equals(other.getRemoveSourceFiles()))) &&
            ((this.storageSystemInfo==null && other.getStorageSystemInfo()==null) || 
             (this.storageSystemInfo!=null &&
              this.storageSystemInfo.equals(other.getStorageSystemInfo()))) &&
            ((this.totalRetryTime==null && other.getTotalRetryTime()==null) || 
             (this.totalRetryTime!=null &&
              this.totalRetryTime.equals(other.getTotalRetryTime())));
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
        if (getArrayOfFileRequests() != null) {
            _hashCode += getArrayOfFileRequests().hashCode();
        }
        if (getUserRequestDescription() != null) {
            _hashCode += getUserRequestDescription().hashCode();
        }
        if (getOverwriteOption() != null) {
            _hashCode += getOverwriteOption().hashCode();
        }
        if (getRemoveSourceFiles() != null) {
            _hashCode += getRemoveSourceFiles().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        if (getTotalRetryTime() != null) {
            _hashCode += getTotalRetryTime().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmCopyRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfFileRequests");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfFileRequests"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTCopyFileRequest"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userRequestDescription");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userRequestDescription"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("overwriteOption");
        elemField.setXmlName(new javax.xml.namespace.QName("", "overwriteOption"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOverwriteMode"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("removeSourceFiles");
        elemField.setXmlName(new javax.xml.namespace.QName("", "removeSourceFiles"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
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
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("totalRetryTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "totalRetryTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
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

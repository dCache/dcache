/**
 * SrmLsRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmLsRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -6789057647886856160L;
    private java.lang.String authorizationID;

    private org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo;

    private org.dcache.srm.v2_2.TFileStorageType fileStorageType;

    private java.lang.Boolean fullDetailedList;

    private java.lang.Boolean allLevelRecursive;

    private java.lang.Integer numOfLevels;

    private java.lang.Integer offset;

    private java.lang.Integer count;

    public SrmLsRequest() {
    }

    public SrmLsRequest(
           java.lang.String authorizationID,
           org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo,
           org.dcache.srm.v2_2.TFileStorageType fileStorageType,
           java.lang.Boolean fullDetailedList,
           java.lang.Boolean allLevelRecursive,
           java.lang.Integer numOfLevels,
           java.lang.Integer offset,
           java.lang.Integer count) {
           this.authorizationID = authorizationID;
           this.arrayOfSURLs = arrayOfSURLs;
           this.storageSystemInfo = storageSystemInfo;
           this.fileStorageType = fileStorageType;
           this.fullDetailedList = fullDetailedList;
           this.allLevelRecursive = allLevelRecursive;
           this.numOfLevels = numOfLevels;
           this.offset = offset;
           this.count = count;
    }


    /**
     * Gets the authorizationID value for this SrmLsRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmLsRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the arrayOfSURLs value for this SrmLsRequest.
     * 
     * @return arrayOfSURLs
     */
    public org.dcache.srm.v2_2.ArrayOfAnyURI getArrayOfSURLs() {
        return arrayOfSURLs;
    }


    /**
     * Sets the arrayOfSURLs value for this SrmLsRequest.
     * 
     * @param arrayOfSURLs
     */
    public void setArrayOfSURLs(org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs) {
        this.arrayOfSURLs = arrayOfSURLs;
    }


    /**
     * Gets the storageSystemInfo value for this SrmLsRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmLsRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the fileStorageType value for this SrmLsRequest.
     * 
     * @return fileStorageType
     */
    public org.dcache.srm.v2_2.TFileStorageType getFileStorageType() {
        return fileStorageType;
    }


    /**
     * Sets the fileStorageType value for this SrmLsRequest.
     * 
     * @param fileStorageType
     */
    public void setFileStorageType(org.dcache.srm.v2_2.TFileStorageType fileStorageType) {
        this.fileStorageType = fileStorageType;
    }


    /**
     * Gets the fullDetailedList value for this SrmLsRequest.
     * 
     * @return fullDetailedList
     */
    public java.lang.Boolean getFullDetailedList() {
        return fullDetailedList;
    }


    /**
     * Sets the fullDetailedList value for this SrmLsRequest.
     * 
     * @param fullDetailedList
     */
    public void setFullDetailedList(java.lang.Boolean fullDetailedList) {
        this.fullDetailedList = fullDetailedList;
    }


    /**
     * Gets the allLevelRecursive value for this SrmLsRequest.
     * 
     * @return allLevelRecursive
     */
    public java.lang.Boolean getAllLevelRecursive() {
        return allLevelRecursive;
    }


    /**
     * Sets the allLevelRecursive value for this SrmLsRequest.
     * 
     * @param allLevelRecursive
     */
    public void setAllLevelRecursive(java.lang.Boolean allLevelRecursive) {
        this.allLevelRecursive = allLevelRecursive;
    }


    /**
     * Gets the numOfLevels value for this SrmLsRequest.
     * 
     * @return numOfLevels
     */
    public java.lang.Integer getNumOfLevels() {
        return numOfLevels;
    }


    /**
     * Sets the numOfLevels value for this SrmLsRequest.
     * 
     * @param numOfLevels
     */
    public void setNumOfLevels(java.lang.Integer numOfLevels) {
        this.numOfLevels = numOfLevels;
    }


    /**
     * Gets the offset value for this SrmLsRequest.
     * 
     * @return offset
     */
    public java.lang.Integer getOffset() {
        return offset;
    }


    /**
     * Sets the offset value for this SrmLsRequest.
     * 
     * @param offset
     */
    public void setOffset(java.lang.Integer offset) {
        this.offset = offset;
    }


    /**
     * Gets the count value for this SrmLsRequest.
     * 
     * @return count
     */
    public java.lang.Integer getCount() {
        return count;
    }


    /**
     * Sets the count value for this SrmLsRequest.
     * 
     * @param count
     */
    public void setCount(java.lang.Integer count) {
        this.count = count;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmLsRequest)) {
            return false;
        }
        SrmLsRequest other = (SrmLsRequest) obj;
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
            ((this.storageSystemInfo==null && other.getStorageSystemInfo()==null) || 
             (this.storageSystemInfo!=null &&
              this.storageSystemInfo.equals(other.getStorageSystemInfo()))) &&
            ((this.fileStorageType==null && other.getFileStorageType()==null) || 
             (this.fileStorageType!=null &&
              this.fileStorageType.equals(other.getFileStorageType()))) &&
            ((this.fullDetailedList==null && other.getFullDetailedList()==null) || 
             (this.fullDetailedList!=null &&
              this.fullDetailedList.equals(other.getFullDetailedList()))) &&
            ((this.allLevelRecursive==null && other.getAllLevelRecursive()==null) || 
             (this.allLevelRecursive!=null &&
              this.allLevelRecursive.equals(other.getAllLevelRecursive()))) &&
            ((this.numOfLevels==null && other.getNumOfLevels()==null) || 
             (this.numOfLevels!=null &&
              this.numOfLevels.equals(other.getNumOfLevels()))) &&
            ((this.offset==null && other.getOffset()==null) || 
             (this.offset!=null &&
              this.offset.equals(other.getOffset()))) &&
            ((this.count==null && other.getCount()==null) || 
             (this.count!=null &&
              this.count.equals(other.getCount())));
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
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        if (getFileStorageType() != null) {
            _hashCode += getFileStorageType().hashCode();
        }
        if (getFullDetailedList() != null) {
            _hashCode += getFullDetailedList().hashCode();
        }
        if (getAllLevelRecursive() != null) {
            _hashCode += getAllLevelRecursive().hashCode();
        }
        if (getNumOfLevels() != null) {
            _hashCode += getNumOfLevels().hashCode();
        }
        if (getOffset() != null) {
            _hashCode += getOffset().hashCode();
        }
        if (getCount() != null) {
            _hashCode += getCount().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmLsRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLsRequest"));
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
        elemField.setFieldName("storageSystemInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "storageSystemInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTExtraInfo"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fileStorageType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fileStorageType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileStorageType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fullDetailedList");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fullDetailedList"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("allLevelRecursive");
        elemField.setXmlName(new javax.xml.namespace.QName("", "allLevelRecursive"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("numOfLevels");
        elemField.setXmlName(new javax.xml.namespace.QName("", "numOfLevels"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("offset");
        elemField.setXmlName(new javax.xml.namespace.QName("", "offset"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("count");
        elemField.setXmlName(new javax.xml.namespace.QName("", "count"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
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

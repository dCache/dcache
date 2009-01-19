/**
 * SrmLsRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmLsRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.ArrayOfTSURLInfo paths;

    private org.dcache.srm.v2_1.TFileStorageType fileStorageType;

    private java.lang.Boolean fullDetailedList;

    private java.lang.Boolean allLevelRecursive;

    private java.lang.Integer numOfLevels;

    private java.lang.Integer offset;

    private java.lang.Integer count;

    public SrmLsRequest() {
    }

    public SrmLsRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.ArrayOfTSURLInfo paths,
           org.dcache.srm.v2_1.TFileStorageType fileStorageType,
           java.lang.Boolean fullDetailedList,
           java.lang.Boolean allLevelRecursive,
           java.lang.Integer numOfLevels,
           java.lang.Integer offset,
           java.lang.Integer count) {
           this.userID = userID;
           this.paths = paths;
           this.fileStorageType = fileStorageType;
           this.fullDetailedList = fullDetailedList;
           this.allLevelRecursive = allLevelRecursive;
           this.numOfLevels = numOfLevels;
           this.offset = offset;
           this.count = count;
    }


    /**
     * Gets the userID value for this SrmLsRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmLsRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the paths value for this SrmLsRequest.
     * 
     * @return paths
     */
    public org.dcache.srm.v2_1.ArrayOfTSURLInfo getPaths() {
        return paths;
    }


    /**
     * Sets the paths value for this SrmLsRequest.
     * 
     * @param paths
     */
    public void setPaths(org.dcache.srm.v2_1.ArrayOfTSURLInfo paths) {
        this.paths = paths;
    }


    /**
     * Gets the fileStorageType value for this SrmLsRequest.
     * 
     * @return fileStorageType
     */
    public org.dcache.srm.v2_1.TFileStorageType getFileStorageType() {
        return fileStorageType;
    }


    /**
     * Sets the fileStorageType value for this SrmLsRequest.
     * 
     * @param fileStorageType
     */
    public void setFileStorageType(org.dcache.srm.v2_1.TFileStorageType fileStorageType) {
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

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmLsRequest)) return false;
        SrmLsRequest other = (SrmLsRequest) obj;
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
            ((this.paths==null && other.getPaths()==null) || 
             (this.paths!=null &&
              this.paths.equals(other.getPaths()))) &&
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
        if (getPaths() != null) {
            _hashCode += getPaths().hashCode();
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
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("paths");
        elemField.setXmlName(new javax.xml.namespace.QName("", "paths"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLInfo"));
        elemField.setNillable(false);
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

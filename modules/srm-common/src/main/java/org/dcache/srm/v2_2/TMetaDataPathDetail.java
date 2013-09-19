/**
 * TMetaDataPathDetail.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TMetaDataPathDetail  implements java.io.Serializable {
    private static final long serialVersionUID = -4485018334922060215L;
    private java.lang.String path;

    private org.dcache.srm.v2_2.TReturnStatus status;

    private org.apache.axis.types.UnsignedLong size;

    private java.util.Calendar createdAtTime;

    private java.util.Calendar lastModificationTime;

    private org.dcache.srm.v2_2.TFileStorageType fileStorageType;

    private org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo;

    private org.dcache.srm.v2_2.TFileLocality fileLocality;

    private org.dcache.srm.v2_2.ArrayOfString arrayOfSpaceTokens;

    private org.dcache.srm.v2_2.TFileType type;

    private java.lang.Integer lifetimeAssigned;

    private java.lang.Integer lifetimeLeft;

    private org.dcache.srm.v2_2.TUserPermission ownerPermission;

    private org.dcache.srm.v2_2.TGroupPermission groupPermission;

    private org.dcache.srm.v2_2.TPermissionMode otherPermission;

    private java.lang.String checkSumType;

    private java.lang.String checkSumValue;

    private org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail arrayOfSubPaths;

    public TMetaDataPathDetail() {
    }

    public TMetaDataPathDetail(
           java.lang.String path,
           org.dcache.srm.v2_2.TReturnStatus status,
           org.apache.axis.types.UnsignedLong size,
           java.util.Calendar createdAtTime,
           java.util.Calendar lastModificationTime,
           org.dcache.srm.v2_2.TFileStorageType fileStorageType,
           org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo,
           org.dcache.srm.v2_2.TFileLocality fileLocality,
           org.dcache.srm.v2_2.ArrayOfString arrayOfSpaceTokens,
           org.dcache.srm.v2_2.TFileType type,
           java.lang.Integer lifetimeAssigned,
           java.lang.Integer lifetimeLeft,
           org.dcache.srm.v2_2.TUserPermission ownerPermission,
           org.dcache.srm.v2_2.TGroupPermission groupPermission,
           org.dcache.srm.v2_2.TPermissionMode otherPermission,
           java.lang.String checkSumType,
           java.lang.String checkSumValue,
           org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail arrayOfSubPaths) {
           this.path = path;
           this.status = status;
           this.size = size;
           this.createdAtTime = createdAtTime;
           this.lastModificationTime = lastModificationTime;
           this.fileStorageType = fileStorageType;
           this.retentionPolicyInfo = retentionPolicyInfo;
           this.fileLocality = fileLocality;
           this.arrayOfSpaceTokens = arrayOfSpaceTokens;
           this.type = type;
           this.lifetimeAssigned = lifetimeAssigned;
           this.lifetimeLeft = lifetimeLeft;
           this.ownerPermission = ownerPermission;
           this.groupPermission = groupPermission;
           this.otherPermission = otherPermission;
           this.checkSumType = checkSumType;
           this.checkSumValue = checkSumValue;
           this.arrayOfSubPaths = arrayOfSubPaths;
    }


    /**
     * Gets the path value for this TMetaDataPathDetail.
     * 
     * @return path
     */
    public java.lang.String getPath() {
        return path;
    }


    /**
     * Sets the path value for this TMetaDataPathDetail.
     * 
     * @param path
     */
    public void setPath(java.lang.String path) {
        this.path = path;
    }


    /**
     * Gets the status value for this TMetaDataPathDetail.
     * 
     * @return status
     */
    public org.dcache.srm.v2_2.TReturnStatus getStatus() {
        return status;
    }


    /**
     * Sets the status value for this TMetaDataPathDetail.
     * 
     * @param status
     */
    public void setStatus(org.dcache.srm.v2_2.TReturnStatus status) {
        this.status = status;
    }


    /**
     * Gets the size value for this TMetaDataPathDetail.
     * 
     * @return size
     */
    public org.apache.axis.types.UnsignedLong getSize() {
        return size;
    }


    /**
     * Sets the size value for this TMetaDataPathDetail.
     * 
     * @param size
     */
    public void setSize(org.apache.axis.types.UnsignedLong size) {
        this.size = size;
    }


    /**
     * Gets the createdAtTime value for this TMetaDataPathDetail.
     * 
     * @return createdAtTime
     */
    public java.util.Calendar getCreatedAtTime() {
        return createdAtTime;
    }


    /**
     * Sets the createdAtTime value for this TMetaDataPathDetail.
     * 
     * @param createdAtTime
     */
    public void setCreatedAtTime(java.util.Calendar createdAtTime) {
        this.createdAtTime = createdAtTime;
    }


    /**
     * Gets the lastModificationTime value for this TMetaDataPathDetail.
     * 
     * @return lastModificationTime
     */
    public java.util.Calendar getLastModificationTime() {
        return lastModificationTime;
    }


    /**
     * Sets the lastModificationTime value for this TMetaDataPathDetail.
     * 
     * @param lastModificationTime
     */
    public void setLastModificationTime(java.util.Calendar lastModificationTime) {
        this.lastModificationTime = lastModificationTime;
    }


    /**
     * Gets the fileStorageType value for this TMetaDataPathDetail.
     * 
     * @return fileStorageType
     */
    public org.dcache.srm.v2_2.TFileStorageType getFileStorageType() {
        return fileStorageType;
    }


    /**
     * Sets the fileStorageType value for this TMetaDataPathDetail.
     * 
     * @param fileStorageType
     */
    public void setFileStorageType(org.dcache.srm.v2_2.TFileStorageType fileStorageType) {
        this.fileStorageType = fileStorageType;
    }


    /**
     * Gets the retentionPolicyInfo value for this TMetaDataPathDetail.
     * 
     * @return retentionPolicyInfo
     */
    public org.dcache.srm.v2_2.TRetentionPolicyInfo getRetentionPolicyInfo() {
        return retentionPolicyInfo;
    }


    /**
     * Sets the retentionPolicyInfo value for this TMetaDataPathDetail.
     * 
     * @param retentionPolicyInfo
     */
    public void setRetentionPolicyInfo(org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo) {
        this.retentionPolicyInfo = retentionPolicyInfo;
    }


    /**
     * Gets the fileLocality value for this TMetaDataPathDetail.
     * 
     * @return fileLocality
     */
    public org.dcache.srm.v2_2.TFileLocality getFileLocality() {
        return fileLocality;
    }


    /**
     * Sets the fileLocality value for this TMetaDataPathDetail.
     * 
     * @param fileLocality
     */
    public void setFileLocality(org.dcache.srm.v2_2.TFileLocality fileLocality) {
        this.fileLocality = fileLocality;
    }


    /**
     * Gets the arrayOfSpaceTokens value for this TMetaDataPathDetail.
     * 
     * @return arrayOfSpaceTokens
     */
    public org.dcache.srm.v2_2.ArrayOfString getArrayOfSpaceTokens() {
        return arrayOfSpaceTokens;
    }


    /**
     * Sets the arrayOfSpaceTokens value for this TMetaDataPathDetail.
     * 
     * @param arrayOfSpaceTokens
     */
    public void setArrayOfSpaceTokens(org.dcache.srm.v2_2.ArrayOfString arrayOfSpaceTokens) {
        this.arrayOfSpaceTokens = arrayOfSpaceTokens;
    }


    /**
     * Gets the type value for this TMetaDataPathDetail.
     * 
     * @return type
     */
    public org.dcache.srm.v2_2.TFileType getType() {
        return type;
    }


    /**
     * Sets the type value for this TMetaDataPathDetail.
     * 
     * @param type
     */
    public void setType(org.dcache.srm.v2_2.TFileType type) {
        this.type = type;
    }


    /**
     * Gets the lifetimeAssigned value for this TMetaDataPathDetail.
     * 
     * @return lifetimeAssigned
     */
    public java.lang.Integer getLifetimeAssigned() {
        return lifetimeAssigned;
    }


    /**
     * Sets the lifetimeAssigned value for this TMetaDataPathDetail.
     * 
     * @param lifetimeAssigned
     */
    public void setLifetimeAssigned(java.lang.Integer lifetimeAssigned) {
        this.lifetimeAssigned = lifetimeAssigned;
    }


    /**
     * Gets the lifetimeLeft value for this TMetaDataPathDetail.
     * 
     * @return lifetimeLeft
     */
    public java.lang.Integer getLifetimeLeft() {
        return lifetimeLeft;
    }


    /**
     * Sets the lifetimeLeft value for this TMetaDataPathDetail.
     * 
     * @param lifetimeLeft
     */
    public void setLifetimeLeft(java.lang.Integer lifetimeLeft) {
        this.lifetimeLeft = lifetimeLeft;
    }


    /**
     * Gets the ownerPermission value for this TMetaDataPathDetail.
     * 
     * @return ownerPermission
     */
    public org.dcache.srm.v2_2.TUserPermission getOwnerPermission() {
        return ownerPermission;
    }


    /**
     * Sets the ownerPermission value for this TMetaDataPathDetail.
     * 
     * @param ownerPermission
     */
    public void setOwnerPermission(org.dcache.srm.v2_2.TUserPermission ownerPermission) {
        this.ownerPermission = ownerPermission;
    }


    /**
     * Gets the groupPermission value for this TMetaDataPathDetail.
     * 
     * @return groupPermission
     */
    public org.dcache.srm.v2_2.TGroupPermission getGroupPermission() {
        return groupPermission;
    }


    /**
     * Sets the groupPermission value for this TMetaDataPathDetail.
     * 
     * @param groupPermission
     */
    public void setGroupPermission(org.dcache.srm.v2_2.TGroupPermission groupPermission) {
        this.groupPermission = groupPermission;
    }


    /**
     * Gets the otherPermission value for this TMetaDataPathDetail.
     * 
     * @return otherPermission
     */
    public org.dcache.srm.v2_2.TPermissionMode getOtherPermission() {
        return otherPermission;
    }


    /**
     * Sets the otherPermission value for this TMetaDataPathDetail.
     * 
     * @param otherPermission
     */
    public void setOtherPermission(org.dcache.srm.v2_2.TPermissionMode otherPermission) {
        this.otherPermission = otherPermission;
    }


    /**
     * Gets the checkSumType value for this TMetaDataPathDetail.
     * 
     * @return checkSumType
     */
    public java.lang.String getCheckSumType() {
        return checkSumType;
    }


    /**
     * Sets the checkSumType value for this TMetaDataPathDetail.
     * 
     * @param checkSumType
     */
    public void setCheckSumType(java.lang.String checkSumType) {
        this.checkSumType = checkSumType;
    }


    /**
     * Gets the checkSumValue value for this TMetaDataPathDetail.
     * 
     * @return checkSumValue
     */
    public java.lang.String getCheckSumValue() {
        return checkSumValue;
    }


    /**
     * Sets the checkSumValue value for this TMetaDataPathDetail.
     * 
     * @param checkSumValue
     */
    public void setCheckSumValue(java.lang.String checkSumValue) {
        this.checkSumValue = checkSumValue;
    }


    /**
     * Gets the arrayOfSubPaths value for this TMetaDataPathDetail.
     * 
     * @return arrayOfSubPaths
     */
    public org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail getArrayOfSubPaths() {
        return arrayOfSubPaths;
    }


    /**
     * Sets the arrayOfSubPaths value for this TMetaDataPathDetail.
     * 
     * @param arrayOfSubPaths
     */
    public void setArrayOfSubPaths(org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail arrayOfSubPaths) {
        this.arrayOfSubPaths = arrayOfSubPaths;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TMetaDataPathDetail)) {
            return false;
        }
        TMetaDataPathDetail other = (TMetaDataPathDetail) obj;
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
            ((this.path==null && other.getPath()==null) || 
             (this.path!=null &&
              this.path.equals(other.getPath()))) &&
            ((this.status==null && other.getStatus()==null) || 
             (this.status!=null &&
              this.status.equals(other.getStatus()))) &&
            ((this.size==null && other.getSize()==null) || 
             (this.size!=null &&
              this.size.equals(other.getSize()))) &&
            ((this.createdAtTime==null && other.getCreatedAtTime()==null) || 
             (this.createdAtTime!=null &&
              this.createdAtTime.equals(other.getCreatedAtTime()))) &&
            ((this.lastModificationTime==null && other.getLastModificationTime()==null) || 
             (this.lastModificationTime!=null &&
              this.lastModificationTime.equals(other.getLastModificationTime()))) &&
            ((this.fileStorageType==null && other.getFileStorageType()==null) || 
             (this.fileStorageType!=null &&
              this.fileStorageType.equals(other.getFileStorageType()))) &&
            ((this.retentionPolicyInfo==null && other.getRetentionPolicyInfo()==null) || 
             (this.retentionPolicyInfo!=null &&
              this.retentionPolicyInfo.equals(other.getRetentionPolicyInfo()))) &&
            ((this.fileLocality==null && other.getFileLocality()==null) || 
             (this.fileLocality!=null &&
              this.fileLocality.equals(other.getFileLocality()))) &&
            ((this.arrayOfSpaceTokens==null && other.getArrayOfSpaceTokens()==null) || 
             (this.arrayOfSpaceTokens!=null &&
              this.arrayOfSpaceTokens.equals(other.getArrayOfSpaceTokens()))) &&
            ((this.type==null && other.getType()==null) || 
             (this.type!=null &&
              this.type.equals(other.getType()))) &&
            ((this.lifetimeAssigned==null && other.getLifetimeAssigned()==null) || 
             (this.lifetimeAssigned!=null &&
              this.lifetimeAssigned.equals(other.getLifetimeAssigned()))) &&
            ((this.lifetimeLeft==null && other.getLifetimeLeft()==null) || 
             (this.lifetimeLeft!=null &&
              this.lifetimeLeft.equals(other.getLifetimeLeft()))) &&
            ((this.ownerPermission==null && other.getOwnerPermission()==null) || 
             (this.ownerPermission!=null &&
              this.ownerPermission.equals(other.getOwnerPermission()))) &&
            ((this.groupPermission==null && other.getGroupPermission()==null) || 
             (this.groupPermission!=null &&
              this.groupPermission.equals(other.getGroupPermission()))) &&
            ((this.otherPermission==null && other.getOtherPermission()==null) || 
             (this.otherPermission!=null &&
              this.otherPermission.equals(other.getOtherPermission()))) &&
            ((this.checkSumType==null && other.getCheckSumType()==null) || 
             (this.checkSumType!=null &&
              this.checkSumType.equals(other.getCheckSumType()))) &&
            ((this.checkSumValue==null && other.getCheckSumValue()==null) || 
             (this.checkSumValue!=null &&
              this.checkSumValue.equals(other.getCheckSumValue()))) &&
            ((this.arrayOfSubPaths==null && other.getArrayOfSubPaths()==null) || 
             (this.arrayOfSubPaths!=null &&
              this.arrayOfSubPaths.equals(other.getArrayOfSubPaths())));
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
        if (getPath() != null) {
            _hashCode += getPath().hashCode();
        }
        if (getStatus() != null) {
            _hashCode += getStatus().hashCode();
        }
        if (getSize() != null) {
            _hashCode += getSize().hashCode();
        }
        if (getCreatedAtTime() != null) {
            _hashCode += getCreatedAtTime().hashCode();
        }
        if (getLastModificationTime() != null) {
            _hashCode += getLastModificationTime().hashCode();
        }
        if (getFileStorageType() != null) {
            _hashCode += getFileStorageType().hashCode();
        }
        if (getRetentionPolicyInfo() != null) {
            _hashCode += getRetentionPolicyInfo().hashCode();
        }
        if (getFileLocality() != null) {
            _hashCode += getFileLocality().hashCode();
        }
        if (getArrayOfSpaceTokens() != null) {
            _hashCode += getArrayOfSpaceTokens().hashCode();
        }
        if (getType() != null) {
            _hashCode += getType().hashCode();
        }
        if (getLifetimeAssigned() != null) {
            _hashCode += getLifetimeAssigned().hashCode();
        }
        if (getLifetimeLeft() != null) {
            _hashCode += getLifetimeLeft().hashCode();
        }
        if (getOwnerPermission() != null) {
            _hashCode += getOwnerPermission().hashCode();
        }
        if (getGroupPermission() != null) {
            _hashCode += getGroupPermission().hashCode();
        }
        if (getOtherPermission() != null) {
            _hashCode += getOtherPermission().hashCode();
        }
        if (getCheckSumType() != null) {
            _hashCode += getCheckSumType().hashCode();
        }
        if (getCheckSumValue() != null) {
            _hashCode += getCheckSumValue().hashCode();
        }
        if (getArrayOfSubPaths() != null) {
            _hashCode += getArrayOfSubPaths().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TMetaDataPathDetail.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TMetaDataPathDetail"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("path");
        elemField.setXmlName(new javax.xml.namespace.QName("", "path"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("status");
        elemField.setXmlName(new javax.xml.namespace.QName("", "status"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("size");
        elemField.setXmlName(new javax.xml.namespace.QName("", "size"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("createdAtTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "createdAtTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "dateTime"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lastModificationTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lastModificationTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "dateTime"));
        elemField.setMinOccurs(0);
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
        elemField.setFieldName("retentionPolicyInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "retentionPolicyInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicyInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fileLocality");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fileLocality"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileLocality"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfSpaceTokens");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfSpaceTokens"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfString"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("type");
        elemField.setXmlName(new javax.xml.namespace.QName("", "type"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetimeAssigned");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetimeAssigned"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetimeLeft");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetimeLeft"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("ownerPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "ownerPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("groupPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "groupPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGroupPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("otherPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "otherPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionMode"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("checkSumType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "checkSumType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("checkSumValue");
        elemField.setXmlName(new javax.xml.namespace.QName("", "checkSumValue"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfSubPaths");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfSubPaths"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTMetaDataPathDetail"));
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

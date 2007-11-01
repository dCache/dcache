/**
 * TMetaDataPathDetail.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class TMetaDataPathDetail  implements java.io.Serializable {
    private java.lang.String path;

    private org.dcache.srm.v2_1.TReturnStatus status;

    private org.dcache.srm.v2_1.TSizeInBytes size;

    private org.dcache.srm.v2_1.TOwnerPermission ownerPermission;

    private org.dcache.srm.v2_1.ArrayOfTUserPermission userPermissions;

    private org.dcache.srm.v2_1.ArrayOfTGroupPermission groupPermissions;

    private org.dcache.srm.v2_1.TOtherPermission otherPermission;

    private org.dcache.srm.v2_1.TGMTTime createdAtTime;

    private org.dcache.srm.v2_1.TGMTTime lastModificationTime;

    private org.dcache.srm.v2_1.TUserID owner;

    private org.dcache.srm.v2_1.TFileStorageType fileStorageType;

    private org.dcache.srm.v2_1.TFileType type;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeAssigned;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeLeft;

    private org.dcache.srm.v2_1.TCheckSumType checkSumType;

    private org.dcache.srm.v2_1.TCheckSumValue checkSumValue;

    private org.dcache.srm.v2_1.TSURL originalSURL;

    private org.dcache.srm.v2_1.ArrayOfTMetaDataPathDetail subPaths;

    public TMetaDataPathDetail() {
    }

    public TMetaDataPathDetail(
           java.lang.String path,
           org.dcache.srm.v2_1.TReturnStatus status,
           org.dcache.srm.v2_1.TSizeInBytes size,
           org.dcache.srm.v2_1.TOwnerPermission ownerPermission,
           org.dcache.srm.v2_1.ArrayOfTUserPermission userPermissions,
           org.dcache.srm.v2_1.ArrayOfTGroupPermission groupPermissions,
           org.dcache.srm.v2_1.TOtherPermission otherPermission,
           org.dcache.srm.v2_1.TGMTTime createdAtTime,
           org.dcache.srm.v2_1.TGMTTime lastModificationTime,
           org.dcache.srm.v2_1.TUserID owner,
           org.dcache.srm.v2_1.TFileStorageType fileStorageType,
           org.dcache.srm.v2_1.TFileType type,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeAssigned,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeLeft,
           org.dcache.srm.v2_1.TCheckSumType checkSumType,
           org.dcache.srm.v2_1.TCheckSumValue checkSumValue,
           org.dcache.srm.v2_1.TSURL originalSURL,
           org.dcache.srm.v2_1.ArrayOfTMetaDataPathDetail subPaths) {
           this.path = path;
           this.status = status;
           this.size = size;
           this.ownerPermission = ownerPermission;
           this.userPermissions = userPermissions;
           this.groupPermissions = groupPermissions;
           this.otherPermission = otherPermission;
           this.createdAtTime = createdAtTime;
           this.lastModificationTime = lastModificationTime;
           this.owner = owner;
           this.fileStorageType = fileStorageType;
           this.type = type;
           this.lifetimeAssigned = lifetimeAssigned;
           this.lifetimeLeft = lifetimeLeft;
           this.checkSumType = checkSumType;
           this.checkSumValue = checkSumValue;
           this.originalSURL = originalSURL;
           this.subPaths = subPaths;
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
    public org.dcache.srm.v2_1.TReturnStatus getStatus() {
        return status;
    }


    /**
     * Sets the status value for this TMetaDataPathDetail.
     * 
     * @param status
     */
    public void setStatus(org.dcache.srm.v2_1.TReturnStatus status) {
        this.status = status;
    }


    /**
     * Gets the size value for this TMetaDataPathDetail.
     * 
     * @return size
     */
    public org.dcache.srm.v2_1.TSizeInBytes getSize() {
        return size;
    }


    /**
     * Sets the size value for this TMetaDataPathDetail.
     * 
     * @param size
     */
    public void setSize(org.dcache.srm.v2_1.TSizeInBytes size) {
        this.size = size;
    }


    /**
     * Gets the ownerPermission value for this TMetaDataPathDetail.
     * 
     * @return ownerPermission
     */
    public org.dcache.srm.v2_1.TOwnerPermission getOwnerPermission() {
        return ownerPermission;
    }


    /**
     * Sets the ownerPermission value for this TMetaDataPathDetail.
     * 
     * @param ownerPermission
     */
    public void setOwnerPermission(org.dcache.srm.v2_1.TOwnerPermission ownerPermission) {
        this.ownerPermission = ownerPermission;
    }


    /**
     * Gets the userPermissions value for this TMetaDataPathDetail.
     * 
     * @return userPermissions
     */
    public org.dcache.srm.v2_1.ArrayOfTUserPermission getUserPermissions() {
        return userPermissions;
    }


    /**
     * Sets the userPermissions value for this TMetaDataPathDetail.
     * 
     * @param userPermissions
     */
    public void setUserPermissions(org.dcache.srm.v2_1.ArrayOfTUserPermission userPermissions) {
        this.userPermissions = userPermissions;
    }


    /**
     * Gets the groupPermissions value for this TMetaDataPathDetail.
     * 
     * @return groupPermissions
     */
    public org.dcache.srm.v2_1.ArrayOfTGroupPermission getGroupPermissions() {
        return groupPermissions;
    }


    /**
     * Sets the groupPermissions value for this TMetaDataPathDetail.
     * 
     * @param groupPermissions
     */
    public void setGroupPermissions(org.dcache.srm.v2_1.ArrayOfTGroupPermission groupPermissions) {
        this.groupPermissions = groupPermissions;
    }


    /**
     * Gets the otherPermission value for this TMetaDataPathDetail.
     * 
     * @return otherPermission
     */
    public org.dcache.srm.v2_1.TOtherPermission getOtherPermission() {
        return otherPermission;
    }


    /**
     * Sets the otherPermission value for this TMetaDataPathDetail.
     * 
     * @param otherPermission
     */
    public void setOtherPermission(org.dcache.srm.v2_1.TOtherPermission otherPermission) {
        this.otherPermission = otherPermission;
    }


    /**
     * Gets the createdAtTime value for this TMetaDataPathDetail.
     * 
     * @return createdAtTime
     */
    public org.dcache.srm.v2_1.TGMTTime getCreatedAtTime() {
        return createdAtTime;
    }


    /**
     * Sets the createdAtTime value for this TMetaDataPathDetail.
     * 
     * @param createdAtTime
     */
    public void setCreatedAtTime(org.dcache.srm.v2_1.TGMTTime createdAtTime) {
        this.createdAtTime = createdAtTime;
    }


    /**
     * Gets the lastModificationTime value for this TMetaDataPathDetail.
     * 
     * @return lastModificationTime
     */
    public org.dcache.srm.v2_1.TGMTTime getLastModificationTime() {
        return lastModificationTime;
    }


    /**
     * Sets the lastModificationTime value for this TMetaDataPathDetail.
     * 
     * @param lastModificationTime
     */
    public void setLastModificationTime(org.dcache.srm.v2_1.TGMTTime lastModificationTime) {
        this.lastModificationTime = lastModificationTime;
    }


    /**
     * Gets the owner value for this TMetaDataPathDetail.
     * 
     * @return owner
     */
    public org.dcache.srm.v2_1.TUserID getOwner() {
        return owner;
    }


    /**
     * Sets the owner value for this TMetaDataPathDetail.
     * 
     * @param owner
     */
    public void setOwner(org.dcache.srm.v2_1.TUserID owner) {
        this.owner = owner;
    }


    /**
     * Gets the fileStorageType value for this TMetaDataPathDetail.
     * 
     * @return fileStorageType
     */
    public org.dcache.srm.v2_1.TFileStorageType getFileStorageType() {
        return fileStorageType;
    }


    /**
     * Sets the fileStorageType value for this TMetaDataPathDetail.
     * 
     * @param fileStorageType
     */
    public void setFileStorageType(org.dcache.srm.v2_1.TFileStorageType fileStorageType) {
        this.fileStorageType = fileStorageType;
    }


    /**
     * Gets the type value for this TMetaDataPathDetail.
     * 
     * @return type
     */
    public org.dcache.srm.v2_1.TFileType getType() {
        return type;
    }


    /**
     * Sets the type value for this TMetaDataPathDetail.
     * 
     * @param type
     */
    public void setType(org.dcache.srm.v2_1.TFileType type) {
        this.type = type;
    }


    /**
     * Gets the lifetimeAssigned value for this TMetaDataPathDetail.
     * 
     * @return lifetimeAssigned
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifetimeAssigned() {
        return lifetimeAssigned;
    }


    /**
     * Sets the lifetimeAssigned value for this TMetaDataPathDetail.
     * 
     * @param lifetimeAssigned
     */
    public void setLifetimeAssigned(org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeAssigned) {
        this.lifetimeAssigned = lifetimeAssigned;
    }


    /**
     * Gets the lifetimeLeft value for this TMetaDataPathDetail.
     * 
     * @return lifetimeLeft
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifetimeLeft() {
        return lifetimeLeft;
    }


    /**
     * Sets the lifetimeLeft value for this TMetaDataPathDetail.
     * 
     * @param lifetimeLeft
     */
    public void setLifetimeLeft(org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeLeft) {
        this.lifetimeLeft = lifetimeLeft;
    }


    /**
     * Gets the checkSumType value for this TMetaDataPathDetail.
     * 
     * @return checkSumType
     */
    public org.dcache.srm.v2_1.TCheckSumType getCheckSumType() {
        return checkSumType;
    }


    /**
     * Sets the checkSumType value for this TMetaDataPathDetail.
     * 
     * @param checkSumType
     */
    public void setCheckSumType(org.dcache.srm.v2_1.TCheckSumType checkSumType) {
        this.checkSumType = checkSumType;
    }


    /**
     * Gets the checkSumValue value for this TMetaDataPathDetail.
     * 
     * @return checkSumValue
     */
    public org.dcache.srm.v2_1.TCheckSumValue getCheckSumValue() {
        return checkSumValue;
    }


    /**
     * Sets the checkSumValue value for this TMetaDataPathDetail.
     * 
     * @param checkSumValue
     */
    public void setCheckSumValue(org.dcache.srm.v2_1.TCheckSumValue checkSumValue) {
        this.checkSumValue = checkSumValue;
    }


    /**
     * Gets the originalSURL value for this TMetaDataPathDetail.
     * 
     * @return originalSURL
     */
    public org.dcache.srm.v2_1.TSURL getOriginalSURL() {
        return originalSURL;
    }


    /**
     * Sets the originalSURL value for this TMetaDataPathDetail.
     * 
     * @param originalSURL
     */
    public void setOriginalSURL(org.dcache.srm.v2_1.TSURL originalSURL) {
        this.originalSURL = originalSURL;
    }


    /**
     * Gets the subPaths value for this TMetaDataPathDetail.
     * 
     * @return subPaths
     */
    public org.dcache.srm.v2_1.ArrayOfTMetaDataPathDetail getSubPaths() {
        return subPaths;
    }


    /**
     * Sets the subPaths value for this TMetaDataPathDetail.
     * 
     * @param subPaths
     */
    public void setSubPaths(org.dcache.srm.v2_1.ArrayOfTMetaDataPathDetail subPaths) {
        this.subPaths = subPaths;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TMetaDataPathDetail)) return false;
        TMetaDataPathDetail other = (TMetaDataPathDetail) obj;
        if (obj == null) return false;
        if (this == obj) return true;
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
            ((this.ownerPermission==null && other.getOwnerPermission()==null) || 
             (this.ownerPermission!=null &&
              this.ownerPermission.equals(other.getOwnerPermission()))) &&
            ((this.userPermissions==null && other.getUserPermissions()==null) || 
             (this.userPermissions!=null &&
              this.userPermissions.equals(other.getUserPermissions()))) &&
            ((this.groupPermissions==null && other.getGroupPermissions()==null) || 
             (this.groupPermissions!=null &&
              this.groupPermissions.equals(other.getGroupPermissions()))) &&
            ((this.otherPermission==null && other.getOtherPermission()==null) || 
             (this.otherPermission!=null &&
              this.otherPermission.equals(other.getOtherPermission()))) &&
            ((this.createdAtTime==null && other.getCreatedAtTime()==null) || 
             (this.createdAtTime!=null &&
              this.createdAtTime.equals(other.getCreatedAtTime()))) &&
            ((this.lastModificationTime==null && other.getLastModificationTime()==null) || 
             (this.lastModificationTime!=null &&
              this.lastModificationTime.equals(other.getLastModificationTime()))) &&
            ((this.owner==null && other.getOwner()==null) || 
             (this.owner!=null &&
              this.owner.equals(other.getOwner()))) &&
            ((this.fileStorageType==null && other.getFileStorageType()==null) || 
             (this.fileStorageType!=null &&
              this.fileStorageType.equals(other.getFileStorageType()))) &&
            ((this.type==null && other.getType()==null) || 
             (this.type!=null &&
              this.type.equals(other.getType()))) &&
            ((this.lifetimeAssigned==null && other.getLifetimeAssigned()==null) || 
             (this.lifetimeAssigned!=null &&
              this.lifetimeAssigned.equals(other.getLifetimeAssigned()))) &&
            ((this.lifetimeLeft==null && other.getLifetimeLeft()==null) || 
             (this.lifetimeLeft!=null &&
              this.lifetimeLeft.equals(other.getLifetimeLeft()))) &&
            ((this.checkSumType==null && other.getCheckSumType()==null) || 
             (this.checkSumType!=null &&
              this.checkSumType.equals(other.getCheckSumType()))) &&
            ((this.checkSumValue==null && other.getCheckSumValue()==null) || 
             (this.checkSumValue!=null &&
              this.checkSumValue.equals(other.getCheckSumValue()))) &&
            ((this.originalSURL==null && other.getOriginalSURL()==null) || 
             (this.originalSURL!=null &&
              this.originalSURL.equals(other.getOriginalSURL()))) &&
            ((this.subPaths==null && other.getSubPaths()==null) || 
             (this.subPaths!=null &&
              this.subPaths.equals(other.getSubPaths())));
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
        if (getPath() != null) {
            _hashCode += getPath().hashCode();
        }
        if (getStatus() != null) {
            _hashCode += getStatus().hashCode();
        }
        if (getSize() != null) {
            _hashCode += getSize().hashCode();
        }
        if (getOwnerPermission() != null) {
            _hashCode += getOwnerPermission().hashCode();
        }
        if (getUserPermissions() != null) {
            _hashCode += getUserPermissions().hashCode();
        }
        if (getGroupPermissions() != null) {
            _hashCode += getGroupPermissions().hashCode();
        }
        if (getOtherPermission() != null) {
            _hashCode += getOtherPermission().hashCode();
        }
        if (getCreatedAtTime() != null) {
            _hashCode += getCreatedAtTime().hashCode();
        }
        if (getLastModificationTime() != null) {
            _hashCode += getLastModificationTime().hashCode();
        }
        if (getOwner() != null) {
            _hashCode += getOwner().hashCode();
        }
        if (getFileStorageType() != null) {
            _hashCode += getFileStorageType().hashCode();
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
        if (getCheckSumType() != null) {
            _hashCode += getCheckSumType().hashCode();
        }
        if (getCheckSumValue() != null) {
            _hashCode += getCheckSumValue().hashCode();
        }
        if (getOriginalSURL() != null) {
            _hashCode += getOriginalSURL().hashCode();
        }
        if (getSubPaths() != null) {
            _hashCode += getSubPaths().hashCode();
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
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("ownerPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "ownerPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOwnerPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userPermissions");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userPermissions"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTUserPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("groupPermissions");
        elemField.setXmlName(new javax.xml.namespace.QName("", "groupPermissions"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGroupPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("otherPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "otherPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOtherPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("createdAtTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "createdAtTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGMTTime"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lastModificationTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lastModificationTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGMTTime"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("owner");
        elemField.setXmlName(new javax.xml.namespace.QName("", "owner"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
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
        elemField.setFieldName("type");
        elemField.setXmlName(new javax.xml.namespace.QName("", "type"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetimeAssigned");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetimeAssigned"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetimeLeft");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetimeLeft"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("checkSumType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "checkSumType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCheckSumType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("checkSumValue");
        elemField.setXmlName(new javax.xml.namespace.QName("", "checkSumValue"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCheckSumValue"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("originalSURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "originalSURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURL"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("subPaths");
        elemField.setXmlName(new javax.xml.namespace.QName("", "subPaths"));
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

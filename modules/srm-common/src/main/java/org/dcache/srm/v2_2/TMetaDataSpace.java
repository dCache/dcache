/**
 * TMetaDataSpace.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TMetaDataSpace  implements java.io.Serializable {
    private static final long serialVersionUID = 174056283791706022L;
    private java.lang.String spaceToken;

    private org.dcache.srm.v2_2.TReturnStatus status;

    private org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo;

    private java.lang.String owner;

    private org.apache.axis.types.UnsignedLong totalSize;

    private org.apache.axis.types.UnsignedLong guaranteedSize;

    private org.apache.axis.types.UnsignedLong unusedSize;

    private java.lang.Integer lifetimeAssigned;

    private java.lang.Integer lifetimeLeft;

    public TMetaDataSpace() {
    }

    public TMetaDataSpace(
           java.lang.String spaceToken,
           org.dcache.srm.v2_2.TReturnStatus status,
           org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo,
           java.lang.String owner,
           org.apache.axis.types.UnsignedLong totalSize,
           org.apache.axis.types.UnsignedLong guaranteedSize,
           org.apache.axis.types.UnsignedLong unusedSize,
           java.lang.Integer lifetimeAssigned,
           java.lang.Integer lifetimeLeft) {
           this.spaceToken = spaceToken;
           this.status = status;
           this.retentionPolicyInfo = retentionPolicyInfo;
           this.owner = owner;
           this.totalSize = totalSize;
           this.guaranteedSize = guaranteedSize;
           this.unusedSize = unusedSize;
           this.lifetimeAssigned = lifetimeAssigned;
           this.lifetimeLeft = lifetimeLeft;
    }


    /**
     * Gets the spaceToken value for this TMetaDataSpace.
     * 
     * @return spaceToken
     */
    public java.lang.String getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this TMetaDataSpace.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(java.lang.String spaceToken) {
        this.spaceToken = spaceToken;
    }


    /**
     * Gets the status value for this TMetaDataSpace.
     * 
     * @return status
     */
    public org.dcache.srm.v2_2.TReturnStatus getStatus() {
        return status;
    }


    /**
     * Sets the status value for this TMetaDataSpace.
     * 
     * @param status
     */
    public void setStatus(org.dcache.srm.v2_2.TReturnStatus status) {
        this.status = status;
    }


    /**
     * Gets the retentionPolicyInfo value for this TMetaDataSpace.
     * 
     * @return retentionPolicyInfo
     */
    public org.dcache.srm.v2_2.TRetentionPolicyInfo getRetentionPolicyInfo() {
        return retentionPolicyInfo;
    }


    /**
     * Sets the retentionPolicyInfo value for this TMetaDataSpace.
     * 
     * @param retentionPolicyInfo
     */
    public void setRetentionPolicyInfo(org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo) {
        this.retentionPolicyInfo = retentionPolicyInfo;
    }


    /**
     * Gets the owner value for this TMetaDataSpace.
     * 
     * @return owner
     */
    public java.lang.String getOwner() {
        return owner;
    }


    /**
     * Sets the owner value for this TMetaDataSpace.
     * 
     * @param owner
     */
    public void setOwner(java.lang.String owner) {
        this.owner = owner;
    }


    /**
     * Gets the totalSize value for this TMetaDataSpace.
     * 
     * @return totalSize
     */
    public org.apache.axis.types.UnsignedLong getTotalSize() {
        return totalSize;
    }


    /**
     * Sets the totalSize value for this TMetaDataSpace.
     * 
     * @param totalSize
     */
    public void setTotalSize(org.apache.axis.types.UnsignedLong totalSize) {
        this.totalSize = totalSize;
    }


    /**
     * Gets the guaranteedSize value for this TMetaDataSpace.
     * 
     * @return guaranteedSize
     */
    public org.apache.axis.types.UnsignedLong getGuaranteedSize() {
        return guaranteedSize;
    }


    /**
     * Sets the guaranteedSize value for this TMetaDataSpace.
     * 
     * @param guaranteedSize
     */
    public void setGuaranteedSize(org.apache.axis.types.UnsignedLong guaranteedSize) {
        this.guaranteedSize = guaranteedSize;
    }


    /**
     * Gets the unusedSize value for this TMetaDataSpace.
     * 
     * @return unusedSize
     */
    public org.apache.axis.types.UnsignedLong getUnusedSize() {
        return unusedSize;
    }


    /**
     * Sets the unusedSize value for this TMetaDataSpace.
     * 
     * @param unusedSize
     */
    public void setUnusedSize(org.apache.axis.types.UnsignedLong unusedSize) {
        this.unusedSize = unusedSize;
    }


    /**
     * Gets the lifetimeAssigned value for this TMetaDataSpace.
     * 
     * @return lifetimeAssigned
     */
    public java.lang.Integer getLifetimeAssigned() {
        return lifetimeAssigned;
    }


    /**
     * Sets the lifetimeAssigned value for this TMetaDataSpace.
     * 
     * @param lifetimeAssigned
     */
    public void setLifetimeAssigned(java.lang.Integer lifetimeAssigned) {
        this.lifetimeAssigned = lifetimeAssigned;
    }


    /**
     * Gets the lifetimeLeft value for this TMetaDataSpace.
     * 
     * @return lifetimeLeft
     */
    public java.lang.Integer getLifetimeLeft() {
        return lifetimeLeft;
    }


    /**
     * Sets the lifetimeLeft value for this TMetaDataSpace.
     * 
     * @param lifetimeLeft
     */
    public void setLifetimeLeft(java.lang.Integer lifetimeLeft) {
        this.lifetimeLeft = lifetimeLeft;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TMetaDataSpace)) {
            return false;
        }
        TMetaDataSpace other = (TMetaDataSpace) obj;
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
            ((this.spaceToken==null && other.getSpaceToken()==null) || 
             (this.spaceToken!=null &&
              this.spaceToken.equals(other.getSpaceToken()))) &&
            ((this.status==null && other.getStatus()==null) || 
             (this.status!=null &&
              this.status.equals(other.getStatus()))) &&
            ((this.retentionPolicyInfo==null && other.getRetentionPolicyInfo()==null) || 
             (this.retentionPolicyInfo!=null &&
              this.retentionPolicyInfo.equals(other.getRetentionPolicyInfo()))) &&
            ((this.owner==null && other.getOwner()==null) || 
             (this.owner!=null &&
              this.owner.equals(other.getOwner()))) &&
            ((this.totalSize==null && other.getTotalSize()==null) || 
             (this.totalSize!=null &&
              this.totalSize.equals(other.getTotalSize()))) &&
            ((this.guaranteedSize==null && other.getGuaranteedSize()==null) || 
             (this.guaranteedSize!=null &&
              this.guaranteedSize.equals(other.getGuaranteedSize()))) &&
            ((this.unusedSize==null && other.getUnusedSize()==null) || 
             (this.unusedSize!=null &&
              this.unusedSize.equals(other.getUnusedSize()))) &&
            ((this.lifetimeAssigned==null && other.getLifetimeAssigned()==null) || 
             (this.lifetimeAssigned!=null &&
              this.lifetimeAssigned.equals(other.getLifetimeAssigned()))) &&
            ((this.lifetimeLeft==null && other.getLifetimeLeft()==null) || 
             (this.lifetimeLeft!=null &&
              this.lifetimeLeft.equals(other.getLifetimeLeft())));
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
        if (getSpaceToken() != null) {
            _hashCode += getSpaceToken().hashCode();
        }
        if (getStatus() != null) {
            _hashCode += getStatus().hashCode();
        }
        if (getRetentionPolicyInfo() != null) {
            _hashCode += getRetentionPolicyInfo().hashCode();
        }
        if (getOwner() != null) {
            _hashCode += getOwner().hashCode();
        }
        if (getTotalSize() != null) {
            _hashCode += getTotalSize().hashCode();
        }
        if (getGuaranteedSize() != null) {
            _hashCode += getGuaranteedSize().hashCode();
        }
        if (getUnusedSize() != null) {
            _hashCode += getUnusedSize().hashCode();
        }
        if (getLifetimeAssigned() != null) {
            _hashCode += getLifetimeAssigned().hashCode();
        }
        if (getLifetimeLeft() != null) {
            _hashCode += getLifetimeLeft().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TMetaDataSpace.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TMetaDataSpace"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("spaceToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "spaceToken"));
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
        elemField.setFieldName("retentionPolicyInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "retentionPolicyInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicyInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("owner");
        elemField.setXmlName(new javax.xml.namespace.QName("", "owner"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("totalSize");
        elemField.setXmlName(new javax.xml.namespace.QName("", "totalSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("guaranteedSize");
        elemField.setXmlName(new javax.xml.namespace.QName("", "guaranteedSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("unusedSize");
        elemField.setXmlName(new javax.xml.namespace.QName("", "unusedSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
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

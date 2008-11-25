/**
 * TMetaDataSpace.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class TMetaDataSpace  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TSpaceType type;

    private org.dcache.srm.v2_1.TSpaceToken spaceToken;

    private boolean isValid;

    private org.dcache.srm.v2_1.TUserID owner;

    private org.dcache.srm.v2_1.TSizeInBytes totalSize;

    private org.dcache.srm.v2_1.TSizeInBytes guaranteedSize;

    private org.dcache.srm.v2_1.TSizeInBytes unusedSize;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeAssigned;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeLeft;

    public TMetaDataSpace() {
    }

    public TMetaDataSpace(
           org.dcache.srm.v2_1.TSpaceType type,
           org.dcache.srm.v2_1.TSpaceToken spaceToken,
           boolean isValid,
           org.dcache.srm.v2_1.TUserID owner,
           org.dcache.srm.v2_1.TSizeInBytes totalSize,
           org.dcache.srm.v2_1.TSizeInBytes guaranteedSize,
           org.dcache.srm.v2_1.TSizeInBytes unusedSize,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeAssigned,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeLeft) {
           this.type = type;
           this.spaceToken = spaceToken;
           this.isValid = isValid;
           this.owner = owner;
           this.totalSize = totalSize;
           this.guaranteedSize = guaranteedSize;
           this.unusedSize = unusedSize;
           this.lifetimeAssigned = lifetimeAssigned;
           this.lifetimeLeft = lifetimeLeft;
    }


    /**
     * Gets the type value for this TMetaDataSpace.
     * 
     * @return type
     */
    public org.dcache.srm.v2_1.TSpaceType getType() {
        return type;
    }


    /**
     * Sets the type value for this TMetaDataSpace.
     * 
     * @param type
     */
    public void setType(org.dcache.srm.v2_1.TSpaceType type) {
        this.type = type;
    }


    /**
     * Gets the spaceToken value for this TMetaDataSpace.
     * 
     * @return spaceToken
     */
    public org.dcache.srm.v2_1.TSpaceToken getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this TMetaDataSpace.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(org.dcache.srm.v2_1.TSpaceToken spaceToken) {
        this.spaceToken = spaceToken;
    }


    /**
     * Gets the isValid value for this TMetaDataSpace.
     * 
     * @return isValid
     */
    public boolean isIsValid() {
        return isValid;
    }


    /**
     * Sets the isValid value for this TMetaDataSpace.
     * 
     * @param isValid
     */
    public void setIsValid(boolean isValid) {
        this.isValid = isValid;
    }


    /**
     * Gets the owner value for this TMetaDataSpace.
     * 
     * @return owner
     */
    public org.dcache.srm.v2_1.TUserID getOwner() {
        return owner;
    }


    /**
     * Sets the owner value for this TMetaDataSpace.
     * 
     * @param owner
     */
    public void setOwner(org.dcache.srm.v2_1.TUserID owner) {
        this.owner = owner;
    }


    /**
     * Gets the totalSize value for this TMetaDataSpace.
     * 
     * @return totalSize
     */
    public org.dcache.srm.v2_1.TSizeInBytes getTotalSize() {
        return totalSize;
    }


    /**
     * Sets the totalSize value for this TMetaDataSpace.
     * 
     * @param totalSize
     */
    public void setTotalSize(org.dcache.srm.v2_1.TSizeInBytes totalSize) {
        this.totalSize = totalSize;
    }


    /**
     * Gets the guaranteedSize value for this TMetaDataSpace.
     * 
     * @return guaranteedSize
     */
    public org.dcache.srm.v2_1.TSizeInBytes getGuaranteedSize() {
        return guaranteedSize;
    }


    /**
     * Sets the guaranteedSize value for this TMetaDataSpace.
     * 
     * @param guaranteedSize
     */
    public void setGuaranteedSize(org.dcache.srm.v2_1.TSizeInBytes guaranteedSize) {
        this.guaranteedSize = guaranteedSize;
    }


    /**
     * Gets the unusedSize value for this TMetaDataSpace.
     * 
     * @return unusedSize
     */
    public org.dcache.srm.v2_1.TSizeInBytes getUnusedSize() {
        return unusedSize;
    }


    /**
     * Sets the unusedSize value for this TMetaDataSpace.
     * 
     * @param unusedSize
     */
    public void setUnusedSize(org.dcache.srm.v2_1.TSizeInBytes unusedSize) {
        this.unusedSize = unusedSize;
    }


    /**
     * Gets the lifetimeAssigned value for this TMetaDataSpace.
     * 
     * @return lifetimeAssigned
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifetimeAssigned() {
        return lifetimeAssigned;
    }


    /**
     * Sets the lifetimeAssigned value for this TMetaDataSpace.
     * 
     * @param lifetimeAssigned
     */
    public void setLifetimeAssigned(org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeAssigned) {
        this.lifetimeAssigned = lifetimeAssigned;
    }


    /**
     * Gets the lifetimeLeft value for this TMetaDataSpace.
     * 
     * @return lifetimeLeft
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifetimeLeft() {
        return lifetimeLeft;
    }


    /**
     * Sets the lifetimeLeft value for this TMetaDataSpace.
     * 
     * @param lifetimeLeft
     */
    public void setLifetimeLeft(org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeLeft) {
        this.lifetimeLeft = lifetimeLeft;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TMetaDataSpace)) return false;
        TMetaDataSpace other = (TMetaDataSpace) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.type==null && other.getType()==null) || 
             (this.type!=null &&
              this.type.equals(other.getType()))) &&
            ((this.spaceToken==null && other.getSpaceToken()==null) || 
             (this.spaceToken!=null &&
              this.spaceToken.equals(other.getSpaceToken()))) &&
            this.isValid == other.isIsValid() &&
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

    private boolean __hashCodeCalc = false;
    public synchronized int hashCode() {
        if (__hashCodeCalc) {
            return 0;
        }
        __hashCodeCalc = true;
        int _hashCode = 1;
        if (getType() != null) {
            _hashCode += getType().hashCode();
        }
        if (getSpaceToken() != null) {
            _hashCode += getSpaceToken().hashCode();
        }
        _hashCode += (isIsValid() ? Boolean.TRUE : Boolean.FALSE).hashCode();
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
        elemField.setFieldName("type");
        elemField.setXmlName(new javax.xml.namespace.QName("", "type"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSpaceType"));
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
        elemField.setFieldName("isValid");
        elemField.setXmlName(new javax.xml.namespace.QName("", "isValid"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("owner");
        elemField.setXmlName(new javax.xml.namespace.QName("", "owner"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("totalSize");
        elemField.setXmlName(new javax.xml.namespace.QName("", "totalSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("guaranteedSize");
        elemField.setXmlName(new javax.xml.namespace.QName("", "guaranteedSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("unusedSize");
        elemField.setXmlName(new javax.xml.namespace.QName("", "unusedSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
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

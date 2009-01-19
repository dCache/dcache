/**
 * TGetFileRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class TGetFileRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TDirOption dirOption;

    private org.dcache.srm.v2_1.TFileStorageType fileStorageType;

    private org.dcache.srm.v2_1.TSURLInfo fromSURLInfo;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifetime;

    private org.dcache.srm.v2_1.TSpaceToken spaceToken;

    public TGetFileRequest() {
    }

    public TGetFileRequest(
           org.dcache.srm.v2_1.TDirOption dirOption,
           org.dcache.srm.v2_1.TFileStorageType fileStorageType,
           org.dcache.srm.v2_1.TSURLInfo fromSURLInfo,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifetime,
           org.dcache.srm.v2_1.TSpaceToken spaceToken) {
           this.dirOption = dirOption;
           this.fileStorageType = fileStorageType;
           this.fromSURLInfo = fromSURLInfo;
           this.lifetime = lifetime;
           this.spaceToken = spaceToken;
    }


    /**
     * Gets the dirOption value for this TGetFileRequest.
     * 
     * @return dirOption
     */
    public org.dcache.srm.v2_1.TDirOption getDirOption() {
        return dirOption;
    }


    /**
     * Sets the dirOption value for this TGetFileRequest.
     * 
     * @param dirOption
     */
    public void setDirOption(org.dcache.srm.v2_1.TDirOption dirOption) {
        this.dirOption = dirOption;
    }


    /**
     * Gets the fileStorageType value for this TGetFileRequest.
     * 
     * @return fileStorageType
     */
    public org.dcache.srm.v2_1.TFileStorageType getFileStorageType() {
        return fileStorageType;
    }


    /**
     * Sets the fileStorageType value for this TGetFileRequest.
     * 
     * @param fileStorageType
     */
    public void setFileStorageType(org.dcache.srm.v2_1.TFileStorageType fileStorageType) {
        this.fileStorageType = fileStorageType;
    }


    /**
     * Gets the fromSURLInfo value for this TGetFileRequest.
     * 
     * @return fromSURLInfo
     */
    public org.dcache.srm.v2_1.TSURLInfo getFromSURLInfo() {
        return fromSURLInfo;
    }


    /**
     * Sets the fromSURLInfo value for this TGetFileRequest.
     * 
     * @param fromSURLInfo
     */
    public void setFromSURLInfo(org.dcache.srm.v2_1.TSURLInfo fromSURLInfo) {
        this.fromSURLInfo = fromSURLInfo;
    }


    /**
     * Gets the lifetime value for this TGetFileRequest.
     * 
     * @return lifetime
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifetime() {
        return lifetime;
    }


    /**
     * Sets the lifetime value for this TGetFileRequest.
     * 
     * @param lifetime
     */
    public void setLifetime(org.dcache.srm.v2_1.TLifeTimeInSeconds lifetime) {
        this.lifetime = lifetime;
    }


    /**
     * Gets the spaceToken value for this TGetFileRequest.
     * 
     * @return spaceToken
     */
    public org.dcache.srm.v2_1.TSpaceToken getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this TGetFileRequest.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(org.dcache.srm.v2_1.TSpaceToken spaceToken) {
        this.spaceToken = spaceToken;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TGetFileRequest)) return false;
        TGetFileRequest other = (TGetFileRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.dirOption==null && other.getDirOption()==null) || 
             (this.dirOption!=null &&
              this.dirOption.equals(other.getDirOption()))) &&
            ((this.fileStorageType==null && other.getFileStorageType()==null) || 
             (this.fileStorageType!=null &&
              this.fileStorageType.equals(other.getFileStorageType()))) &&
            ((this.fromSURLInfo==null && other.getFromSURLInfo()==null) || 
             (this.fromSURLInfo!=null &&
              this.fromSURLInfo.equals(other.getFromSURLInfo()))) &&
            ((this.lifetime==null && other.getLifetime()==null) || 
             (this.lifetime!=null &&
              this.lifetime.equals(other.getLifetime()))) &&
            ((this.spaceToken==null && other.getSpaceToken()==null) || 
             (this.spaceToken!=null &&
              this.spaceToken.equals(other.getSpaceToken())));
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
        if (getDirOption() != null) {
            _hashCode += getDirOption().hashCode();
        }
        if (getFileStorageType() != null) {
            _hashCode += getFileStorageType().hashCode();
        }
        if (getFromSURLInfo() != null) {
            _hashCode += getFromSURLInfo().hashCode();
        }
        if (getLifetime() != null) {
            _hashCode += getLifetime().hashCode();
        }
        if (getSpaceToken() != null) {
            _hashCode += getSpaceToken().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TGetFileRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGetFileRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("dirOption");
        elemField.setXmlName(new javax.xml.namespace.QName("", "dirOption"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TDirOption"));
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
        elemField.setFieldName("fromSURLInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fromSURLInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLInfo"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("spaceToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "spaceToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSpaceToken"));
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

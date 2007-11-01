/**
 * TPutFileRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class TPutFileRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TFileStorageType fileStorageType;

    private org.dcache.srm.v2_1.TSizeInBytes knownSizeOfThisFile;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifetime;

    private org.dcache.srm.v2_1.TSpaceToken spaceToken;

    private org.dcache.srm.v2_1.TSURLInfo toSURLInfo;

    public TPutFileRequest() {
    }

    public TPutFileRequest(
           org.dcache.srm.v2_1.TFileStorageType fileStorageType,
           org.dcache.srm.v2_1.TSizeInBytes knownSizeOfThisFile,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifetime,
           org.dcache.srm.v2_1.TSpaceToken spaceToken,
           org.dcache.srm.v2_1.TSURLInfo toSURLInfo) {
           this.fileStorageType = fileStorageType;
           this.knownSizeOfThisFile = knownSizeOfThisFile;
           this.lifetime = lifetime;
           this.spaceToken = spaceToken;
           this.toSURLInfo = toSURLInfo;
    }


    /**
     * Gets the fileStorageType value for this TPutFileRequest.
     * 
     * @return fileStorageType
     */
    public org.dcache.srm.v2_1.TFileStorageType getFileStorageType() {
        return fileStorageType;
    }


    /**
     * Sets the fileStorageType value for this TPutFileRequest.
     * 
     * @param fileStorageType
     */
    public void setFileStorageType(org.dcache.srm.v2_1.TFileStorageType fileStorageType) {
        this.fileStorageType = fileStorageType;
    }


    /**
     * Gets the knownSizeOfThisFile value for this TPutFileRequest.
     * 
     * @return knownSizeOfThisFile
     */
    public org.dcache.srm.v2_1.TSizeInBytes getKnownSizeOfThisFile() {
        return knownSizeOfThisFile;
    }


    /**
     * Sets the knownSizeOfThisFile value for this TPutFileRequest.
     * 
     * @param knownSizeOfThisFile
     */
    public void setKnownSizeOfThisFile(org.dcache.srm.v2_1.TSizeInBytes knownSizeOfThisFile) {
        this.knownSizeOfThisFile = knownSizeOfThisFile;
    }


    /**
     * Gets the lifetime value for this TPutFileRequest.
     * 
     * @return lifetime
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifetime() {
        return lifetime;
    }


    /**
     * Sets the lifetime value for this TPutFileRequest.
     * 
     * @param lifetime
     */
    public void setLifetime(org.dcache.srm.v2_1.TLifeTimeInSeconds lifetime) {
        this.lifetime = lifetime;
    }


    /**
     * Gets the spaceToken value for this TPutFileRequest.
     * 
     * @return spaceToken
     */
    public org.dcache.srm.v2_1.TSpaceToken getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this TPutFileRequest.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(org.dcache.srm.v2_1.TSpaceToken spaceToken) {
        this.spaceToken = spaceToken;
    }


    /**
     * Gets the toSURLInfo value for this TPutFileRequest.
     * 
     * @return toSURLInfo
     */
    public org.dcache.srm.v2_1.TSURLInfo getToSURLInfo() {
        return toSURLInfo;
    }


    /**
     * Sets the toSURLInfo value for this TPutFileRequest.
     * 
     * @param toSURLInfo
     */
    public void setToSURLInfo(org.dcache.srm.v2_1.TSURLInfo toSURLInfo) {
        this.toSURLInfo = toSURLInfo;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TPutFileRequest)) return false;
        TPutFileRequest other = (TPutFileRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.fileStorageType==null && other.getFileStorageType()==null) || 
             (this.fileStorageType!=null &&
              this.fileStorageType.equals(other.getFileStorageType()))) &&
            ((this.knownSizeOfThisFile==null && other.getKnownSizeOfThisFile()==null) || 
             (this.knownSizeOfThisFile!=null &&
              this.knownSizeOfThisFile.equals(other.getKnownSizeOfThisFile()))) &&
            ((this.lifetime==null && other.getLifetime()==null) || 
             (this.lifetime!=null &&
              this.lifetime.equals(other.getLifetime()))) &&
            ((this.spaceToken==null && other.getSpaceToken()==null) || 
             (this.spaceToken!=null &&
              this.spaceToken.equals(other.getSpaceToken()))) &&
            ((this.toSURLInfo==null && other.getToSURLInfo()==null) || 
             (this.toSURLInfo!=null &&
              this.toSURLInfo.equals(other.getToSURLInfo())));
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
        if (getFileStorageType() != null) {
            _hashCode += getFileStorageType().hashCode();
        }
        if (getKnownSizeOfThisFile() != null) {
            _hashCode += getKnownSizeOfThisFile().hashCode();
        }
        if (getLifetime() != null) {
            _hashCode += getLifetime().hashCode();
        }
        if (getSpaceToken() != null) {
            _hashCode += getSpaceToken().hashCode();
        }
        if (getToSURLInfo() != null) {
            _hashCode += getToSURLInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TPutFileRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPutFileRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fileStorageType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fileStorageType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileStorageType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("knownSizeOfThisFile");
        elemField.setXmlName(new javax.xml.namespace.QName("", "knownSizeOfThisFile"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
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
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("toSURLInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "toSURLInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLInfo"));
        elemField.setNillable(false);
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

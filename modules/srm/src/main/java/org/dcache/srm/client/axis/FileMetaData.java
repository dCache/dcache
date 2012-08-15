/**
 * FileMetaData.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.client.axis;

public class FileMetaData  implements java.io.Serializable {
    private static final long serialVersionUID = 441630648850327616L;
    private java.lang.String SURL;

    private long size;

    private java.lang.String owner;

    private java.lang.String group;

    private int permMode;

    private java.lang.String checksumType;

    private java.lang.String checksumValue;

    private boolean isPinned;

    private boolean isPermanent;

    private boolean isCached;

    public FileMetaData() {
    }

    public FileMetaData(
           java.lang.String SURL,
           long size,
           java.lang.String owner,
           java.lang.String group,
           int permMode,
           java.lang.String checksumType,
           java.lang.String checksumValue,
           boolean isPinned,
           boolean isPermanent,
           boolean isCached) {
           this.SURL = SURL;
           this.size = size;
           this.owner = owner;
           this.group = group;
           this.permMode = permMode;
           this.checksumType = checksumType;
           this.checksumValue = checksumValue;
           this.isPinned = isPinned;
           this.isPermanent = isPermanent;
           this.isCached = isCached;
    }


    /**
     * Gets the SURL value for this FileMetaData.
     * 
     * @return SURL
     */
    public java.lang.String getSURL() {
        return SURL;
    }


    /**
     * Sets the SURL value for this FileMetaData.
     * 
     * @param SURL
     */
    public void setSURL(java.lang.String SURL) {
        this.SURL = SURL;
    }


    /**
     * Gets the size value for this FileMetaData.
     * 
     * @return size
     */
    public long getSize() {
        return size;
    }


    /**
     * Sets the size value for this FileMetaData.
     * 
     * @param size
     */
    public void setSize(long size) {
        this.size = size;
    }


    /**
     * Gets the owner value for this FileMetaData.
     * 
     * @return owner
     */
    public java.lang.String getOwner() {
        return owner;
    }


    /**
     * Sets the owner value for this FileMetaData.
     * 
     * @param owner
     */
    public void setOwner(java.lang.String owner) {
        this.owner = owner;
    }


    /**
     * Gets the group value for this FileMetaData.
     * 
     * @return group
     */
    public java.lang.String getGroup() {
        return group;
    }


    /**
     * Sets the group value for this FileMetaData.
     * 
     * @param group
     */
    public void setGroup(java.lang.String group) {
        this.group = group;
    }


    /**
     * Gets the permMode value for this FileMetaData.
     * 
     * @return permMode
     */
    public int getPermMode() {
        return permMode;
    }


    /**
     * Sets the permMode value for this FileMetaData.
     * 
     * @param permMode
     */
    public void setPermMode(int permMode) {
        this.permMode = permMode;
    }


    /**
     * Gets the checksumType value for this FileMetaData.
     * 
     * @return checksumType
     */
    public java.lang.String getChecksumType() {
        return checksumType;
    }


    /**
     * Sets the checksumType value for this FileMetaData.
     * 
     * @param checksumType
     */
    public void setChecksumType(java.lang.String checksumType) {
        this.checksumType = checksumType;
    }


    /**
     * Gets the checksumValue value for this FileMetaData.
     * 
     * @return checksumValue
     */
    public java.lang.String getChecksumValue() {
        return checksumValue;
    }


    /**
     * Sets the checksumValue value for this FileMetaData.
     * 
     * @param checksumValue
     */
    public void setChecksumValue(java.lang.String checksumValue) {
        this.checksumValue = checksumValue;
    }


    /**
     * Gets the isPinned value for this FileMetaData.
     * 
     * @return isPinned
     */
    public boolean isIsPinned() {
        return isPinned;
    }


    /**
     * Sets the isPinned value for this FileMetaData.
     * 
     * @param isPinned
     */
    public void setIsPinned(boolean isPinned) {
        this.isPinned = isPinned;
    }


    /**
     * Gets the isPermanent value for this FileMetaData.
     * 
     * @return isPermanent
     */
    public boolean isIsPermanent() {
        return isPermanent;
    }


    /**
     * Sets the isPermanent value for this FileMetaData.
     * 
     * @param isPermanent
     */
    public void setIsPermanent(boolean isPermanent) {
        this.isPermanent = isPermanent;
    }


    /**
     * Gets the isCached value for this FileMetaData.
     * 
     * @return isCached
     */
    public boolean isIsCached() {
        return isCached;
    }


    /**
     * Sets the isCached value for this FileMetaData.
     * 
     * @param isCached
     */
    public void setIsCached(boolean isCached) {
        this.isCached = isCached;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof FileMetaData)) {
            return false;
        }
        FileMetaData other = (FileMetaData) obj;
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
            ((this.SURL==null && other.getSURL()==null) || 
             (this.SURL!=null &&
              this.SURL.equals(other.getSURL()))) &&
            this.size == other.getSize() &&
            ((this.owner==null && other.getOwner()==null) || 
             (this.owner!=null &&
              this.owner.equals(other.getOwner()))) &&
            ((this.group==null && other.getGroup()==null) || 
             (this.group!=null &&
              this.group.equals(other.getGroup()))) &&
            this.permMode == other.getPermMode() &&
            ((this.checksumType==null && other.getChecksumType()==null) || 
             (this.checksumType!=null &&
              this.checksumType.equals(other.getChecksumType()))) &&
            ((this.checksumValue==null && other.getChecksumValue()==null) || 
             (this.checksumValue!=null &&
              this.checksumValue.equals(other.getChecksumValue()))) &&
            this.isPinned == other.isIsPinned() &&
            this.isPermanent == other.isIsPermanent() &&
            this.isCached == other.isIsCached();
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
        if (getSURL() != null) {
            _hashCode += getSURL().hashCode();
        }
        _hashCode += new Long(getSize()).hashCode();
        if (getOwner() != null) {
            _hashCode += getOwner().hashCode();
        }
        if (getGroup() != null) {
            _hashCode += getGroup().hashCode();
        }
        _hashCode += getPermMode();
        if (getChecksumType() != null) {
            _hashCode += getChecksumType().hashCode();
        }
        if (getChecksumValue() != null) {
            _hashCode += getChecksumValue().hashCode();
        }
        _hashCode += (isIsPinned() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isIsPermanent() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += (isIsCached() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(FileMetaData.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://www.themindelectric.com/package/diskCacheV111.srm/", "FileMetaData"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("SURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "SURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("size");
        elemField.setXmlName(new javax.xml.namespace.QName("", "size"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "long"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("owner");
        elemField.setXmlName(new javax.xml.namespace.QName("", "owner"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("group");
        elemField.setXmlName(new javax.xml.namespace.QName("", "group"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("permMode");
        elemField.setXmlName(new javax.xml.namespace.QName("", "permMode"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("checksumType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "checksumType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("checksumValue");
        elemField.setXmlName(new javax.xml.namespace.QName("", "checksumValue"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("isPinned");
        elemField.setXmlName(new javax.xml.namespace.QName("", "isPinned"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("isPermanent");
        elemField.setXmlName(new javax.xml.namespace.QName("", "isPermanent"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("isCached");
        elemField.setXmlName(new javax.xml.namespace.QName("", "isCached"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
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

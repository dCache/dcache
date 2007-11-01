/**
 * StorageElementInfo.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.client.axis1;

public class StorageElementInfo  implements java.io.Serializable {
    private long totalSpace;

    private long usedSpace;

    private long availableSpace;

    public StorageElementInfo() {
    }

    public StorageElementInfo(
           long totalSpace,
           long usedSpace,
           long availableSpace) {
           this.totalSpace = totalSpace;
           this.usedSpace = usedSpace;
           this.availableSpace = availableSpace;
    }


    /**
     * Gets the totalSpace value for this StorageElementInfo.
     * 
     * @return totalSpace
     */
    public long getTotalSpace() {
        return totalSpace;
    }


    /**
     * Sets the totalSpace value for this StorageElementInfo.
     * 
     * @param totalSpace
     */
    public void setTotalSpace(long totalSpace) {
        this.totalSpace = totalSpace;
    }


    /**
     * Gets the usedSpace value for this StorageElementInfo.
     * 
     * @return usedSpace
     */
    public long getUsedSpace() {
        return usedSpace;
    }


    /**
     * Sets the usedSpace value for this StorageElementInfo.
     * 
     * @param usedSpace
     */
    public void setUsedSpace(long usedSpace) {
        this.usedSpace = usedSpace;
    }


    /**
     * Gets the availableSpace value for this StorageElementInfo.
     * 
     * @return availableSpace
     */
    public long getAvailableSpace() {
        return availableSpace;
    }


    /**
     * Sets the availableSpace value for this StorageElementInfo.
     * 
     * @param availableSpace
     */
    public void setAvailableSpace(long availableSpace) {
        this.availableSpace = availableSpace;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof StorageElementInfo)) return false;
        StorageElementInfo other = (StorageElementInfo) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.totalSpace == other.getTotalSpace() &&
            this.usedSpace == other.getUsedSpace() &&
            this.availableSpace == other.getAvailableSpace();
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
        _hashCode += new Long(getTotalSpace()).hashCode();
        _hashCode += new Long(getUsedSpace()).hashCode();
        _hashCode += new Long(getAvailableSpace()).hashCode();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(StorageElementInfo.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://www.themindelectric.com/package/diskCacheV111.srm/", "StorageElementInfo"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("totalSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "totalSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "long"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("usedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "usedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "long"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("availableSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "availableSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "long"));
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

/**
 * TSupportedTransferProtocol.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TSupportedTransferProtocol  implements java.io.Serializable {
    private static final long serialVersionUID = -1675225351663839972L;
    private java.lang.String transferProtocol;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo attributes;

    public TSupportedTransferProtocol() {
    }

    public TSupportedTransferProtocol(
           java.lang.String transferProtocol,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo attributes) {
           this.transferProtocol = transferProtocol;
           this.attributes = attributes;
    }


    /**
     * Gets the transferProtocol value for this TSupportedTransferProtocol.
     * 
     * @return transferProtocol
     */
    public java.lang.String getTransferProtocol() {
        return transferProtocol;
    }


    /**
     * Sets the transferProtocol value for this TSupportedTransferProtocol.
     * 
     * @param transferProtocol
     */
    public void setTransferProtocol(java.lang.String transferProtocol) {
        this.transferProtocol = transferProtocol;
    }


    /**
     * Gets the attributes value for this TSupportedTransferProtocol.
     * 
     * @return attributes
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getAttributes() {
        return attributes;
    }


    /**
     * Sets the attributes value for this TSupportedTransferProtocol.
     * 
     * @param attributes
     */
    public void setAttributes(org.dcache.srm.v2_2.ArrayOfTExtraInfo attributes) {
        this.attributes = attributes;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TSupportedTransferProtocol)) {
            return false;
        }
        TSupportedTransferProtocol other = (TSupportedTransferProtocol) obj;
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
            ((this.transferProtocol==null && other.getTransferProtocol()==null) || 
             (this.transferProtocol!=null &&
              this.transferProtocol.equals(other.getTransferProtocol()))) &&
            ((this.attributes==null && other.getAttributes()==null) || 
             (this.attributes!=null &&
              this.attributes.equals(other.getAttributes())));
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
        if (getTransferProtocol() != null) {
            _hashCode += getTransferProtocol().hashCode();
        }
        if (getAttributes() != null) {
            _hashCode += getAttributes().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TSupportedTransferProtocol.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSupportedTransferProtocol"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("transferProtocol");
        elemField.setXmlName(new javax.xml.namespace.QName("", "transferProtocol"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("attributes");
        elemField.setXmlName(new javax.xml.namespace.QName("", "attributes"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTExtraInfo"));
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

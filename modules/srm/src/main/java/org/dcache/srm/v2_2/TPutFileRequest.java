/**
 * TPutFileRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TPutFileRequest  implements java.io.Serializable {
    private static final long serialVersionUID = 4873792560517462935L;
    private org.apache.axis.types.URI targetSURL;

    private org.apache.axis.types.UnsignedLong expectedFileSize;

    public TPutFileRequest() {
    }

    public TPutFileRequest(
           org.apache.axis.types.URI targetSURL,
           org.apache.axis.types.UnsignedLong expectedFileSize) {
           this.targetSURL = targetSURL;
           this.expectedFileSize = expectedFileSize;
    }


    /**
     * Gets the targetSURL value for this TPutFileRequest.
     * 
     * @return targetSURL
     */
    public org.apache.axis.types.URI getTargetSURL() {
        return targetSURL;
    }


    /**
     * Sets the targetSURL value for this TPutFileRequest.
     * 
     * @param targetSURL
     */
    public void setTargetSURL(org.apache.axis.types.URI targetSURL) {
        this.targetSURL = targetSURL;
    }


    /**
     * Gets the expectedFileSize value for this TPutFileRequest.
     * 
     * @return expectedFileSize
     */
    public org.apache.axis.types.UnsignedLong getExpectedFileSize() {
        return expectedFileSize;
    }


    /**
     * Sets the expectedFileSize value for this TPutFileRequest.
     * 
     * @param expectedFileSize
     */
    public void setExpectedFileSize(org.apache.axis.types.UnsignedLong expectedFileSize) {
        this.expectedFileSize = expectedFileSize;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TPutFileRequest)) {
            return false;
        }
        TPutFileRequest other = (TPutFileRequest) obj;
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
            ((this.targetSURL==null && other.getTargetSURL()==null) || 
             (this.targetSURL!=null &&
              this.targetSURL.equals(other.getTargetSURL()))) &&
            ((this.expectedFileSize==null && other.getExpectedFileSize()==null) || 
             (this.expectedFileSize!=null &&
              this.expectedFileSize.equals(other.getExpectedFileSize())));
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
        if (getTargetSURL() != null) {
            _hashCode += getTargetSURL().hashCode();
        }
        if (getExpectedFileSize() != null) {
            _hashCode += getExpectedFileSize().hashCode();
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
        elemField.setFieldName("targetSURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "targetSURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("expectedFileSize");
        elemField.setXmlName(new javax.xml.namespace.QName("", "expectedFileSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
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

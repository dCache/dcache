/**
 * ArrayOfAnyURI.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class ArrayOfAnyURI  implements java.io.Serializable {
    private static final long serialVersionUID = -3364439917148319957L;
    private org.apache.axis.types.URI[] urlArray;

    public ArrayOfAnyURI() {
    }

    public ArrayOfAnyURI(
           org.apache.axis.types.URI[] urlArray) {
           this.urlArray = urlArray;
    }


    /**
     * Gets the urlArray value for this ArrayOfAnyURI.
     * 
     * @return urlArray
     */
    public org.apache.axis.types.URI[] getUrlArray() {
        return urlArray;
    }


    /**
     * Sets the urlArray value for this ArrayOfAnyURI.
     * 
     * @param urlArray
     */
    public void setUrlArray(org.apache.axis.types.URI[] urlArray) {
        this.urlArray = urlArray;
    }

    public org.apache.axis.types.URI getUrlArray(int i) {
        return this.urlArray[i];
    }

    public void setUrlArray(int i, org.apache.axis.types.URI _value) {
        this.urlArray[i] = _value;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfAnyURI)) {
            return false;
        }
        ArrayOfAnyURI other = (ArrayOfAnyURI) obj;
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
            ((this.urlArray==null && other.getUrlArray()==null) || 
             (this.urlArray!=null &&
              java.util.Arrays.equals(this.urlArray, other.getUrlArray())));
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
        if (getUrlArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getUrlArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getUrlArray(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(ArrayOfAnyURI.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfAnyURI"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("urlArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "urlArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setNillable(true);
        elemField.setMaxOccursUnbounded(true);
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

/**
 * ArrayOfTPutFileRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class ArrayOfTPutFileRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TPutFileRequest[] putRequestArray;

    public ArrayOfTPutFileRequest() {
    }

    public ArrayOfTPutFileRequest(
           org.dcache.srm.v2_1.TPutFileRequest[] putRequestArray) {
           this.putRequestArray = putRequestArray;
    }


    /**
     * Gets the putRequestArray value for this ArrayOfTPutFileRequest.
     * 
     * @return putRequestArray
     */
    public org.dcache.srm.v2_1.TPutFileRequest[] getPutRequestArray() {
        return putRequestArray;
    }


    /**
     * Sets the putRequestArray value for this ArrayOfTPutFileRequest.
     * 
     * @param putRequestArray
     */
    public void setPutRequestArray(org.dcache.srm.v2_1.TPutFileRequest[] putRequestArray) {
        this.putRequestArray = putRequestArray;
    }

    public org.dcache.srm.v2_1.TPutFileRequest getPutRequestArray(int i) {
        return this.putRequestArray[i];
    }

    public void setPutRequestArray(int i, org.dcache.srm.v2_1.TPutFileRequest _value) {
        this.putRequestArray[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTPutFileRequest)) return false;
        ArrayOfTPutFileRequest other = (ArrayOfTPutFileRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.putRequestArray==null && other.getPutRequestArray()==null) || 
             (this.putRequestArray!=null &&
              java.util.Arrays.equals(this.putRequestArray, other.getPutRequestArray())));
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
        if (getPutRequestArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getPutRequestArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getPutRequestArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTPutFileRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTPutFileRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("putRequestArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "putRequestArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPutFileRequest"));
        elemField.setNillable(false);
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

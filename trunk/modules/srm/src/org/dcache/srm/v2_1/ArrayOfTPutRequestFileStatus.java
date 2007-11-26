/**
 * ArrayOfTPutRequestFileStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class ArrayOfTPutRequestFileStatus  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TPutRequestFileStatus[] putStatusArray;

    public ArrayOfTPutRequestFileStatus() {
    }

    public ArrayOfTPutRequestFileStatus(
           org.dcache.srm.v2_1.TPutRequestFileStatus[] putStatusArray) {
           this.putStatusArray = putStatusArray;
    }


    /**
     * Gets the putStatusArray value for this ArrayOfTPutRequestFileStatus.
     * 
     * @return putStatusArray
     */
    public org.dcache.srm.v2_1.TPutRequestFileStatus[] getPutStatusArray() {
        return putStatusArray;
    }


    /**
     * Sets the putStatusArray value for this ArrayOfTPutRequestFileStatus.
     * 
     * @param putStatusArray
     */
    public void setPutStatusArray(org.dcache.srm.v2_1.TPutRequestFileStatus[] putStatusArray) {
        this.putStatusArray = putStatusArray;
    }

    public org.dcache.srm.v2_1.TPutRequestFileStatus getPutStatusArray(int i) {
        return this.putStatusArray[i];
    }

    public void setPutStatusArray(int i, org.dcache.srm.v2_1.TPutRequestFileStatus _value) {
        this.putStatusArray[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTPutRequestFileStatus)) return false;
        ArrayOfTPutRequestFileStatus other = (ArrayOfTPutRequestFileStatus) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.putStatusArray==null && other.getPutStatusArray()==null) || 
             (this.putStatusArray!=null &&
              java.util.Arrays.equals(this.putStatusArray, other.getPutStatusArray())));
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
        if (getPutStatusArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getPutStatusArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getPutStatusArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTPutRequestFileStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTPutRequestFileStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("putStatusArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "putStatusArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPutRequestFileStatus"));
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

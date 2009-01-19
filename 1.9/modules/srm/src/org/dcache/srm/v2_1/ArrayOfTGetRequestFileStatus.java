/**
 * ArrayOfTGetRequestFileStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class ArrayOfTGetRequestFileStatus  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TGetRequestFileStatus[] getStatusArray;

    public ArrayOfTGetRequestFileStatus() {
    }

    public ArrayOfTGetRequestFileStatus(
           org.dcache.srm.v2_1.TGetRequestFileStatus[] getStatusArray) {
           this.getStatusArray = getStatusArray;
    }


    /**
     * Gets the getStatusArray value for this ArrayOfTGetRequestFileStatus.
     * 
     * @return getStatusArray
     */
    public org.dcache.srm.v2_1.TGetRequestFileStatus[] getGetStatusArray() {
        return getStatusArray;
    }


    /**
     * Sets the getStatusArray value for this ArrayOfTGetRequestFileStatus.
     * 
     * @param getStatusArray
     */
    public void setGetStatusArray(org.dcache.srm.v2_1.TGetRequestFileStatus[] getStatusArray) {
        this.getStatusArray = getStatusArray;
    }

    public org.dcache.srm.v2_1.TGetRequestFileStatus getGetStatusArray(int i) {
        return this.getStatusArray[i];
    }

    public void setGetStatusArray(int i, org.dcache.srm.v2_1.TGetRequestFileStatus _value) {
        this.getStatusArray[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTGetRequestFileStatus)) return false;
        ArrayOfTGetRequestFileStatus other = (ArrayOfTGetRequestFileStatus) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.getStatusArray==null && other.getGetStatusArray()==null) || 
             (this.getStatusArray!=null &&
              java.util.Arrays.equals(this.getStatusArray, other.getGetStatusArray())));
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
        if (getGetStatusArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getGetStatusArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getGetStatusArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTGetRequestFileStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGetRequestFileStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("getStatusArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "getStatusArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGetRequestFileStatus"));
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

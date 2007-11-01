/**
 * ArrayOfTRequestToken.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class ArrayOfTRequestToken  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TRequestToken[] requestTokenArray;

    public ArrayOfTRequestToken() {
    }

    public ArrayOfTRequestToken(
           org.dcache.srm.v2_1.TRequestToken[] requestTokenArray) {
           this.requestTokenArray = requestTokenArray;
    }


    /**
     * Gets the requestTokenArray value for this ArrayOfTRequestToken.
     * 
     * @return requestTokenArray
     */
    public org.dcache.srm.v2_1.TRequestToken[] getRequestTokenArray() {
        return requestTokenArray;
    }


    /**
     * Sets the requestTokenArray value for this ArrayOfTRequestToken.
     * 
     * @param requestTokenArray
     */
    public void setRequestTokenArray(org.dcache.srm.v2_1.TRequestToken[] requestTokenArray) {
        this.requestTokenArray = requestTokenArray;
    }

    public org.dcache.srm.v2_1.TRequestToken getRequestTokenArray(int i) {
        return this.requestTokenArray[i];
    }

    public void setRequestTokenArray(int i, org.dcache.srm.v2_1.TRequestToken _value) {
        this.requestTokenArray[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTRequestToken)) return false;
        ArrayOfTRequestToken other = (ArrayOfTRequestToken) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.requestTokenArray==null && other.getRequestTokenArray()==null) || 
             (this.requestTokenArray!=null &&
              java.util.Arrays.equals(this.requestTokenArray, other.getRequestTokenArray())));
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
        if (getRequestTokenArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getRequestTokenArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getRequestTokenArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTRequestToken.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTRequestToken"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestTokenArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestTokenArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestToken"));
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

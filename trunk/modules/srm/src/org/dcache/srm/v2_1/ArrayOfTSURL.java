/**
 * ArrayOfTSURL.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class ArrayOfTSURL  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TSURL[] surlArray;

    public ArrayOfTSURL() {
    }

    public ArrayOfTSURL(
           org.dcache.srm.v2_1.TSURL[] surlArray) {
           this.surlArray = surlArray;
    }


    /**
     * Gets the surlArray value for this ArrayOfTSURL.
     * 
     * @return surlArray
     */
    public org.dcache.srm.v2_1.TSURL[] getSurlArray() {
        return surlArray;
    }


    /**
     * Sets the surlArray value for this ArrayOfTSURL.
     * 
     * @param surlArray
     */
    public void setSurlArray(org.dcache.srm.v2_1.TSURL[] surlArray) {
        this.surlArray = surlArray;
    }

    public org.dcache.srm.v2_1.TSURL getSurlArray(int i) {
        return this.surlArray[i];
    }

    public void setSurlArray(int i, org.dcache.srm.v2_1.TSURL _value) {
        this.surlArray[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTSURL)) return false;
        ArrayOfTSURL other = (ArrayOfTSURL) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.surlArray==null && other.getSurlArray()==null) || 
             (this.surlArray!=null &&
              java.util.Arrays.equals(this.surlArray, other.getSurlArray())));
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
        if (getSurlArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getSurlArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getSurlArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTSURL.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURL"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("surlArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "surlArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURL"));
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

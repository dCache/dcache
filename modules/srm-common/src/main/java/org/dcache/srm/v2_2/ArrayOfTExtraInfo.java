/**
 * ArrayOfTExtraInfo.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class ArrayOfTExtraInfo  implements java.io.Serializable {
    private static final long serialVersionUID = -2035245743237069240L;
    private org.dcache.srm.v2_2.TExtraInfo[] extraInfoArray;

    public ArrayOfTExtraInfo() {
    }

    public ArrayOfTExtraInfo(
           org.dcache.srm.v2_2.TExtraInfo[] extraInfoArray) {
           this.extraInfoArray = extraInfoArray;
    }


    /**
     * Gets the extraInfoArray value for this ArrayOfTExtraInfo.
     * 
     * @return extraInfoArray
     */
    public org.dcache.srm.v2_2.TExtraInfo[] getExtraInfoArray() {
        return extraInfoArray;
    }


    /**
     * Sets the extraInfoArray value for this ArrayOfTExtraInfo.
     * 
     * @param extraInfoArray
     */
    public void setExtraInfoArray(org.dcache.srm.v2_2.TExtraInfo[] extraInfoArray) {
        this.extraInfoArray = extraInfoArray;
    }

    public org.dcache.srm.v2_2.TExtraInfo getExtraInfoArray(int i) {
        return this.extraInfoArray[i];
    }

    public void setExtraInfoArray(int i, org.dcache.srm.v2_2.TExtraInfo _value) {
        this.extraInfoArray[i] = _value;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTExtraInfo)) {
            return false;
        }
        ArrayOfTExtraInfo other = (ArrayOfTExtraInfo) obj;
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
            ((this.extraInfoArray==null && other.getExtraInfoArray()==null) || 
             (this.extraInfoArray!=null &&
              java.util.Arrays.equals(this.extraInfoArray, other.getExtraInfoArray())));
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
        if (getExtraInfoArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getExtraInfoArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getExtraInfoArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTExtraInfo.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTExtraInfo"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("extraInfoArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "extraInfoArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TExtraInfo"));
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

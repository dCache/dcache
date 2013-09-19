/**
 * ArrayOfTBringOnlineRequestFileStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class ArrayOfTBringOnlineRequestFileStatus  implements java.io.Serializable {
    private static final long serialVersionUID = 5505578165074941443L;
    private org.dcache.srm.v2_2.TBringOnlineRequestFileStatus[] statusArray;

    public ArrayOfTBringOnlineRequestFileStatus() {
    }

    public ArrayOfTBringOnlineRequestFileStatus(
           org.dcache.srm.v2_2.TBringOnlineRequestFileStatus[] statusArray) {
           this.statusArray = statusArray;
    }


    /**
     * Gets the statusArray value for this ArrayOfTBringOnlineRequestFileStatus.
     * 
     * @return statusArray
     */
    public org.dcache.srm.v2_2.TBringOnlineRequestFileStatus[] getStatusArray() {
        return statusArray;
    }


    /**
     * Sets the statusArray value for this ArrayOfTBringOnlineRequestFileStatus.
     * 
     * @param statusArray
     */
    public void setStatusArray(org.dcache.srm.v2_2.TBringOnlineRequestFileStatus[] statusArray) {
        this.statusArray = statusArray;
    }

    public org.dcache.srm.v2_2.TBringOnlineRequestFileStatus getStatusArray(int i) {
        return this.statusArray[i];
    }

    public void setStatusArray(int i, org.dcache.srm.v2_2.TBringOnlineRequestFileStatus _value) {
        this.statusArray[i] = _value;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTBringOnlineRequestFileStatus)) {
            return false;
        }
        ArrayOfTBringOnlineRequestFileStatus other = (ArrayOfTBringOnlineRequestFileStatus) obj;
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
            ((this.statusArray==null && other.getStatusArray()==null) || 
             (this.statusArray!=null &&
              java.util.Arrays.equals(this.statusArray, other.getStatusArray())));
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
        if (getStatusArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getStatusArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getStatusArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTBringOnlineRequestFileStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTBringOnlineRequestFileStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("statusArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "statusArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TBringOnlineRequestFileStatus"));
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

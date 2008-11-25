/**
 * ArrayOfTCopyRequestFileStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class ArrayOfTCopyRequestFileStatus  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TCopyRequestFileStatus[] copyStatusArray;

    public ArrayOfTCopyRequestFileStatus() {
    }

    public ArrayOfTCopyRequestFileStatus(
           org.dcache.srm.v2_1.TCopyRequestFileStatus[] copyStatusArray) {
           this.copyStatusArray = copyStatusArray;
    }


    /**
     * Gets the copyStatusArray value for this ArrayOfTCopyRequestFileStatus.
     * 
     * @return copyStatusArray
     */
    public org.dcache.srm.v2_1.TCopyRequestFileStatus[] getCopyStatusArray() {
        return copyStatusArray;
    }


    /**
     * Sets the copyStatusArray value for this ArrayOfTCopyRequestFileStatus.
     * 
     * @param copyStatusArray
     */
    public void setCopyStatusArray(org.dcache.srm.v2_1.TCopyRequestFileStatus[] copyStatusArray) {
        this.copyStatusArray = copyStatusArray;
    }

    public org.dcache.srm.v2_1.TCopyRequestFileStatus getCopyStatusArray(int i) {
        return this.copyStatusArray[i];
    }

    public void setCopyStatusArray(int i, org.dcache.srm.v2_1.TCopyRequestFileStatus _value) {
        this.copyStatusArray[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTCopyRequestFileStatus)) return false;
        ArrayOfTCopyRequestFileStatus other = (ArrayOfTCopyRequestFileStatus) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.copyStatusArray==null && other.getCopyStatusArray()==null) || 
             (this.copyStatusArray!=null &&
              java.util.Arrays.equals(this.copyStatusArray, other.getCopyStatusArray())));
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
        if (getCopyStatusArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getCopyStatusArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getCopyStatusArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTCopyRequestFileStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTCopyRequestFileStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("copyStatusArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "copyStatusArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCopyRequestFileStatus"));
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

/**
 * ArrayOfTCopyFileRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class ArrayOfTCopyFileRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TCopyFileRequest[] copyRequestArray;

    public ArrayOfTCopyFileRequest() {
    }

    public ArrayOfTCopyFileRequest(
           org.dcache.srm.v2_1.TCopyFileRequest[] copyRequestArray) {
           this.copyRequestArray = copyRequestArray;
    }


    /**
     * Gets the copyRequestArray value for this ArrayOfTCopyFileRequest.
     * 
     * @return copyRequestArray
     */
    public org.dcache.srm.v2_1.TCopyFileRequest[] getCopyRequestArray() {
        return copyRequestArray;
    }


    /**
     * Sets the copyRequestArray value for this ArrayOfTCopyFileRequest.
     * 
     * @param copyRequestArray
     */
    public void setCopyRequestArray(org.dcache.srm.v2_1.TCopyFileRequest[] copyRequestArray) {
        this.copyRequestArray = copyRequestArray;
    }

    public org.dcache.srm.v2_1.TCopyFileRequest getCopyRequestArray(int i) {
        return this.copyRequestArray[i];
    }

    public void setCopyRequestArray(int i, org.dcache.srm.v2_1.TCopyFileRequest _value) {
        this.copyRequestArray[i] = _value;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTCopyFileRequest)) return false;
        ArrayOfTCopyFileRequest other = (ArrayOfTCopyFileRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.copyRequestArray==null && other.getCopyRequestArray()==null) || 
             (this.copyRequestArray!=null &&
              java.util.Arrays.equals(this.copyRequestArray, other.getCopyRequestArray())));
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
        if (getCopyRequestArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getCopyRequestArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getCopyRequestArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTCopyFileRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTCopyFileRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("copyRequestArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "copyRequestArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCopyFileRequest"));
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

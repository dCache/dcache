/**
 * ArrayOfTPermissionReturn.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class ArrayOfTPermissionReturn  implements java.io.Serializable {
    private static final long serialVersionUID = 755939317760817962L;
    private org.dcache.srm.v2_2.TPermissionReturn[] permissionArray;

    public ArrayOfTPermissionReturn() {
    }

    public ArrayOfTPermissionReturn(
           org.dcache.srm.v2_2.TPermissionReturn[] permissionArray) {
           this.permissionArray = permissionArray;
    }


    /**
     * Gets the permissionArray value for this ArrayOfTPermissionReturn.
     * 
     * @return permissionArray
     */
    public org.dcache.srm.v2_2.TPermissionReturn[] getPermissionArray() {
        return permissionArray;
    }


    /**
     * Sets the permissionArray value for this ArrayOfTPermissionReturn.
     * 
     * @param permissionArray
     */
    public void setPermissionArray(org.dcache.srm.v2_2.TPermissionReturn[] permissionArray) {
        this.permissionArray = permissionArray;
    }

    public org.dcache.srm.v2_2.TPermissionReturn getPermissionArray(int i) {
        return this.permissionArray[i];
    }

    public void setPermissionArray(int i, org.dcache.srm.v2_2.TPermissionReturn _value) {
        this.permissionArray[i] = _value;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTPermissionReturn)) {
            return false;
        }
        ArrayOfTPermissionReturn other = (ArrayOfTPermissionReturn) obj;
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
            ((this.permissionArray==null && other.getPermissionArray()==null) || 
             (this.permissionArray!=null &&
              java.util.Arrays.equals(this.permissionArray, other.getPermissionArray())));
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
        if (getPermissionArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getPermissionArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getPermissionArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTPermissionReturn.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTPermissionReturn"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("permissionArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "permissionArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionReturn"));
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

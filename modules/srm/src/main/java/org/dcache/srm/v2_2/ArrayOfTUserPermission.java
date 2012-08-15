/**
 * ArrayOfTUserPermission.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class ArrayOfTUserPermission  implements java.io.Serializable {
    private static final long serialVersionUID = 3845382454272132191L;
    private org.dcache.srm.v2_2.TUserPermission[] userPermissionArray;

    public ArrayOfTUserPermission() {
    }

    public ArrayOfTUserPermission(
           org.dcache.srm.v2_2.TUserPermission[] userPermissionArray) {
           this.userPermissionArray = userPermissionArray;
    }


    /**
     * Gets the userPermissionArray value for this ArrayOfTUserPermission.
     * 
     * @return userPermissionArray
     */
    public org.dcache.srm.v2_2.TUserPermission[] getUserPermissionArray() {
        return userPermissionArray;
    }


    /**
     * Sets the userPermissionArray value for this ArrayOfTUserPermission.
     * 
     * @param userPermissionArray
     */
    public void setUserPermissionArray(org.dcache.srm.v2_2.TUserPermission[] userPermissionArray) {
        this.userPermissionArray = userPermissionArray;
    }

    public org.dcache.srm.v2_2.TUserPermission getUserPermissionArray(int i) {
        return this.userPermissionArray[i];
    }

    public void setUserPermissionArray(int i, org.dcache.srm.v2_2.TUserPermission _value) {
        this.userPermissionArray[i] = _value;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof ArrayOfTUserPermission)) {
            return false;
        }
        ArrayOfTUserPermission other = (ArrayOfTUserPermission) obj;
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
            ((this.userPermissionArray==null && other.getUserPermissionArray()==null) || 
             (this.userPermissionArray!=null &&
              java.util.Arrays.equals(this.userPermissionArray, other.getUserPermissionArray())));
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
        if (getUserPermissionArray() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getUserPermissionArray());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getUserPermissionArray(), i);
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
        new org.apache.axis.description.TypeDesc(ArrayOfTUserPermission.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTUserPermission"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userPermissionArray");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userPermissionArray"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserPermission"));
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

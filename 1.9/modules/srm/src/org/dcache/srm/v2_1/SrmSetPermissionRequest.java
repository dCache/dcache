/**
 * SrmSetPermissionRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmSetPermissionRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.TSURLInfo path;

    private org.dcache.srm.v2_1.TPermissionType permissionType;

    private org.dcache.srm.v2_1.TOwnerPermission ownerPermission;

    private org.dcache.srm.v2_1.ArrayOfTUserPermission userPermission;

    private org.dcache.srm.v2_1.ArrayOfTGroupPermission groupPermission;

    private org.dcache.srm.v2_1.TOtherPermission otherPermission;

    public SrmSetPermissionRequest() {
    }

    public SrmSetPermissionRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.TSURLInfo path,
           org.dcache.srm.v2_1.TPermissionType permissionType,
           org.dcache.srm.v2_1.TOwnerPermission ownerPermission,
           org.dcache.srm.v2_1.ArrayOfTUserPermission userPermission,
           org.dcache.srm.v2_1.ArrayOfTGroupPermission groupPermission,
           org.dcache.srm.v2_1.TOtherPermission otherPermission) {
           this.userID = userID;
           this.path = path;
           this.permissionType = permissionType;
           this.ownerPermission = ownerPermission;
           this.userPermission = userPermission;
           this.groupPermission = groupPermission;
           this.otherPermission = otherPermission;
    }


    /**
     * Gets the userID value for this SrmSetPermissionRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmSetPermissionRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the path value for this SrmSetPermissionRequest.
     * 
     * @return path
     */
    public org.dcache.srm.v2_1.TSURLInfo getPath() {
        return path;
    }


    /**
     * Sets the path value for this SrmSetPermissionRequest.
     * 
     * @param path
     */
    public void setPath(org.dcache.srm.v2_1.TSURLInfo path) {
        this.path = path;
    }


    /**
     * Gets the permissionType value for this SrmSetPermissionRequest.
     * 
     * @return permissionType
     */
    public org.dcache.srm.v2_1.TPermissionType getPermissionType() {
        return permissionType;
    }


    /**
     * Sets the permissionType value for this SrmSetPermissionRequest.
     * 
     * @param permissionType
     */
    public void setPermissionType(org.dcache.srm.v2_1.TPermissionType permissionType) {
        this.permissionType = permissionType;
    }


    /**
     * Gets the ownerPermission value for this SrmSetPermissionRequest.
     * 
     * @return ownerPermission
     */
    public org.dcache.srm.v2_1.TOwnerPermission getOwnerPermission() {
        return ownerPermission;
    }


    /**
     * Sets the ownerPermission value for this SrmSetPermissionRequest.
     * 
     * @param ownerPermission
     */
    public void setOwnerPermission(org.dcache.srm.v2_1.TOwnerPermission ownerPermission) {
        this.ownerPermission = ownerPermission;
    }


    /**
     * Gets the userPermission value for this SrmSetPermissionRequest.
     * 
     * @return userPermission
     */
    public org.dcache.srm.v2_1.ArrayOfTUserPermission getUserPermission() {
        return userPermission;
    }


    /**
     * Sets the userPermission value for this SrmSetPermissionRequest.
     * 
     * @param userPermission
     */
    public void setUserPermission(org.dcache.srm.v2_1.ArrayOfTUserPermission userPermission) {
        this.userPermission = userPermission;
    }


    /**
     * Gets the groupPermission value for this SrmSetPermissionRequest.
     * 
     * @return groupPermission
     */
    public org.dcache.srm.v2_1.ArrayOfTGroupPermission getGroupPermission() {
        return groupPermission;
    }


    /**
     * Sets the groupPermission value for this SrmSetPermissionRequest.
     * 
     * @param groupPermission
     */
    public void setGroupPermission(org.dcache.srm.v2_1.ArrayOfTGroupPermission groupPermission) {
        this.groupPermission = groupPermission;
    }


    /**
     * Gets the otherPermission value for this SrmSetPermissionRequest.
     * 
     * @return otherPermission
     */
    public org.dcache.srm.v2_1.TOtherPermission getOtherPermission() {
        return otherPermission;
    }


    /**
     * Sets the otherPermission value for this SrmSetPermissionRequest.
     * 
     * @param otherPermission
     */
    public void setOtherPermission(org.dcache.srm.v2_1.TOtherPermission otherPermission) {
        this.otherPermission = otherPermission;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmSetPermissionRequest)) return false;
        SrmSetPermissionRequest other = (SrmSetPermissionRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.userID==null && other.getUserID()==null) || 
             (this.userID!=null &&
              this.userID.equals(other.getUserID()))) &&
            ((this.path==null && other.getPath()==null) || 
             (this.path!=null &&
              this.path.equals(other.getPath()))) &&
            ((this.permissionType==null && other.getPermissionType()==null) || 
             (this.permissionType!=null &&
              this.permissionType.equals(other.getPermissionType()))) &&
            ((this.ownerPermission==null && other.getOwnerPermission()==null) || 
             (this.ownerPermission!=null &&
              this.ownerPermission.equals(other.getOwnerPermission()))) &&
            ((this.userPermission==null && other.getUserPermission()==null) || 
             (this.userPermission!=null &&
              this.userPermission.equals(other.getUserPermission()))) &&
            ((this.groupPermission==null && other.getGroupPermission()==null) || 
             (this.groupPermission!=null &&
              this.groupPermission.equals(other.getGroupPermission()))) &&
            ((this.otherPermission==null && other.getOtherPermission()==null) || 
             (this.otherPermission!=null &&
              this.otherPermission.equals(other.getOtherPermission())));
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
        if (getUserID() != null) {
            _hashCode += getUserID().hashCode();
        }
        if (getPath() != null) {
            _hashCode += getPath().hashCode();
        }
        if (getPermissionType() != null) {
            _hashCode += getPermissionType().hashCode();
        }
        if (getOwnerPermission() != null) {
            _hashCode += getOwnerPermission().hashCode();
        }
        if (getUserPermission() != null) {
            _hashCode += getUserPermission().hashCode();
        }
        if (getGroupPermission() != null) {
            _hashCode += getGroupPermission().hashCode();
        }
        if (getOtherPermission() != null) {
            _hashCode += getOtherPermission().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmSetPermissionRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermissionRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("path");
        elemField.setXmlName(new javax.xml.namespace.QName("", "path"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLInfo"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("permissionType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "permissionType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionType"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("ownerPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "ownerPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOwnerPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTUserPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("groupPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "groupPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGroupPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("otherPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "otherPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOtherPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
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

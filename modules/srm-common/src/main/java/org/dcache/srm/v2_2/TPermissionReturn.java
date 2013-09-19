/**
 * TPermissionReturn.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TPermissionReturn  implements java.io.Serializable {
    private static final long serialVersionUID = -8086338669150091106L;
    private org.apache.axis.types.URI surl;

    private org.dcache.srm.v2_2.TReturnStatus status;

    private java.lang.String owner;

    private org.dcache.srm.v2_2.TPermissionMode ownerPermission;

    private org.dcache.srm.v2_2.ArrayOfTUserPermission arrayOfUserPermissions;

    private org.dcache.srm.v2_2.ArrayOfTGroupPermission arrayOfGroupPermissions;

    private org.dcache.srm.v2_2.TPermissionMode otherPermission;

    public TPermissionReturn() {
    }

    public TPermissionReturn(
           org.apache.axis.types.URI surl,
           org.dcache.srm.v2_2.TReturnStatus status,
           java.lang.String owner,
           org.dcache.srm.v2_2.TPermissionMode ownerPermission,
           org.dcache.srm.v2_2.ArrayOfTUserPermission arrayOfUserPermissions,
           org.dcache.srm.v2_2.ArrayOfTGroupPermission arrayOfGroupPermissions,
           org.dcache.srm.v2_2.TPermissionMode otherPermission) {
           this.surl = surl;
           this.status = status;
           this.owner = owner;
           this.ownerPermission = ownerPermission;
           this.arrayOfUserPermissions = arrayOfUserPermissions;
           this.arrayOfGroupPermissions = arrayOfGroupPermissions;
           this.otherPermission = otherPermission;
    }


    /**
     * Gets the surl value for this TPermissionReturn.
     * 
     * @return surl
     */
    public org.apache.axis.types.URI getSurl() {
        return surl;
    }


    /**
     * Sets the surl value for this TPermissionReturn.
     * 
     * @param surl
     */
    public void setSurl(org.apache.axis.types.URI surl) {
        this.surl = surl;
    }


    /**
     * Gets the status value for this TPermissionReturn.
     * 
     * @return status
     */
    public org.dcache.srm.v2_2.TReturnStatus getStatus() {
        return status;
    }


    /**
     * Sets the status value for this TPermissionReturn.
     * 
     * @param status
     */
    public void setStatus(org.dcache.srm.v2_2.TReturnStatus status) {
        this.status = status;
    }


    /**
     * Gets the owner value for this TPermissionReturn.
     * 
     * @return owner
     */
    public java.lang.String getOwner() {
        return owner;
    }


    /**
     * Sets the owner value for this TPermissionReturn.
     * 
     * @param owner
     */
    public void setOwner(java.lang.String owner) {
        this.owner = owner;
    }


    /**
     * Gets the ownerPermission value for this TPermissionReturn.
     * 
     * @return ownerPermission
     */
    public org.dcache.srm.v2_2.TPermissionMode getOwnerPermission() {
        return ownerPermission;
    }


    /**
     * Sets the ownerPermission value for this TPermissionReturn.
     * 
     * @param ownerPermission
     */
    public void setOwnerPermission(org.dcache.srm.v2_2.TPermissionMode ownerPermission) {
        this.ownerPermission = ownerPermission;
    }


    /**
     * Gets the arrayOfUserPermissions value for this TPermissionReturn.
     * 
     * @return arrayOfUserPermissions
     */
    public org.dcache.srm.v2_2.ArrayOfTUserPermission getArrayOfUserPermissions() {
        return arrayOfUserPermissions;
    }


    /**
     * Sets the arrayOfUserPermissions value for this TPermissionReturn.
     * 
     * @param arrayOfUserPermissions
     */
    public void setArrayOfUserPermissions(org.dcache.srm.v2_2.ArrayOfTUserPermission arrayOfUserPermissions) {
        this.arrayOfUserPermissions = arrayOfUserPermissions;
    }


    /**
     * Gets the arrayOfGroupPermissions value for this TPermissionReturn.
     * 
     * @return arrayOfGroupPermissions
     */
    public org.dcache.srm.v2_2.ArrayOfTGroupPermission getArrayOfGroupPermissions() {
        return arrayOfGroupPermissions;
    }


    /**
     * Sets the arrayOfGroupPermissions value for this TPermissionReturn.
     * 
     * @param arrayOfGroupPermissions
     */
    public void setArrayOfGroupPermissions(org.dcache.srm.v2_2.ArrayOfTGroupPermission arrayOfGroupPermissions) {
        this.arrayOfGroupPermissions = arrayOfGroupPermissions;
    }


    /**
     * Gets the otherPermission value for this TPermissionReturn.
     * 
     * @return otherPermission
     */
    public org.dcache.srm.v2_2.TPermissionMode getOtherPermission() {
        return otherPermission;
    }


    /**
     * Sets the otherPermission value for this TPermissionReturn.
     * 
     * @param otherPermission
     */
    public void setOtherPermission(org.dcache.srm.v2_2.TPermissionMode otherPermission) {
        this.otherPermission = otherPermission;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TPermissionReturn)) {
            return false;
        }
        TPermissionReturn other = (TPermissionReturn) obj;
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
            ((this.surl==null && other.getSurl()==null) || 
             (this.surl!=null &&
              this.surl.equals(other.getSurl()))) &&
            ((this.status==null && other.getStatus()==null) || 
             (this.status!=null &&
              this.status.equals(other.getStatus()))) &&
            ((this.owner==null && other.getOwner()==null) || 
             (this.owner!=null &&
              this.owner.equals(other.getOwner()))) &&
            ((this.ownerPermission==null && other.getOwnerPermission()==null) || 
             (this.ownerPermission!=null &&
              this.ownerPermission.equals(other.getOwnerPermission()))) &&
            ((this.arrayOfUserPermissions==null && other.getArrayOfUserPermissions()==null) || 
             (this.arrayOfUserPermissions!=null &&
              this.arrayOfUserPermissions.equals(other.getArrayOfUserPermissions()))) &&
            ((this.arrayOfGroupPermissions==null && other.getArrayOfGroupPermissions()==null) || 
             (this.arrayOfGroupPermissions!=null &&
              this.arrayOfGroupPermissions.equals(other.getArrayOfGroupPermissions()))) &&
            ((this.otherPermission==null && other.getOtherPermission()==null) || 
             (this.otherPermission!=null &&
              this.otherPermission.equals(other.getOtherPermission())));
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
        if (getSurl() != null) {
            _hashCode += getSurl().hashCode();
        }
        if (getStatus() != null) {
            _hashCode += getStatus().hashCode();
        }
        if (getOwner() != null) {
            _hashCode += getOwner().hashCode();
        }
        if (getOwnerPermission() != null) {
            _hashCode += getOwnerPermission().hashCode();
        }
        if (getArrayOfUserPermissions() != null) {
            _hashCode += getArrayOfUserPermissions().hashCode();
        }
        if (getArrayOfGroupPermissions() != null) {
            _hashCode += getArrayOfGroupPermissions().hashCode();
        }
        if (getOtherPermission() != null) {
            _hashCode += getOtherPermission().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TPermissionReturn.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionReturn"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("surl");
        elemField.setXmlName(new javax.xml.namespace.QName("", "surl"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("status");
        elemField.setXmlName(new javax.xml.namespace.QName("", "status"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("owner");
        elemField.setXmlName(new javax.xml.namespace.QName("", "owner"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("ownerPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "ownerPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionMode"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfUserPermissions");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfUserPermissions"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTUserPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfGroupPermissions");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfGroupPermissions"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGroupPermission"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("otherPermission");
        elemField.setXmlName(new javax.xml.namespace.QName("", "otherPermission"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionMode"));
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

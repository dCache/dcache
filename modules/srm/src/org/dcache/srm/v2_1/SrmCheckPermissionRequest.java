/**
 * SrmCheckPermissionRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmCheckPermissionRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.ArrayOfTSURLInfo arrayOfSiteURLs;

    private org.dcache.srm.v2_1.TUserID userID;

    private java.lang.Boolean checkInLocalCacheOnly;

    public SrmCheckPermissionRequest() {
    }

    public SrmCheckPermissionRequest(
           org.dcache.srm.v2_1.ArrayOfTSURLInfo arrayOfSiteURLs,
           org.dcache.srm.v2_1.TUserID userID,
           java.lang.Boolean checkInLocalCacheOnly) {
           this.arrayOfSiteURLs = arrayOfSiteURLs;
           this.userID = userID;
           this.checkInLocalCacheOnly = checkInLocalCacheOnly;
    }


    /**
     * Gets the arrayOfSiteURLs value for this SrmCheckPermissionRequest.
     * 
     * @return arrayOfSiteURLs
     */
    public org.dcache.srm.v2_1.ArrayOfTSURLInfo getArrayOfSiteURLs() {
        return arrayOfSiteURLs;
    }


    /**
     * Sets the arrayOfSiteURLs value for this SrmCheckPermissionRequest.
     * 
     * @param arrayOfSiteURLs
     */
    public void setArrayOfSiteURLs(org.dcache.srm.v2_1.ArrayOfTSURLInfo arrayOfSiteURLs) {
        this.arrayOfSiteURLs = arrayOfSiteURLs;
    }


    /**
     * Gets the userID value for this SrmCheckPermissionRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmCheckPermissionRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the checkInLocalCacheOnly value for this SrmCheckPermissionRequest.
     * 
     * @return checkInLocalCacheOnly
     */
    public java.lang.Boolean getCheckInLocalCacheOnly() {
        return checkInLocalCacheOnly;
    }


    /**
     * Sets the checkInLocalCacheOnly value for this SrmCheckPermissionRequest.
     * 
     * @param checkInLocalCacheOnly
     */
    public void setCheckInLocalCacheOnly(java.lang.Boolean checkInLocalCacheOnly) {
        this.checkInLocalCacheOnly = checkInLocalCacheOnly;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmCheckPermissionRequest)) return false;
        SrmCheckPermissionRequest other = (SrmCheckPermissionRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.arrayOfSiteURLs==null && other.getArrayOfSiteURLs()==null) || 
             (this.arrayOfSiteURLs!=null &&
              this.arrayOfSiteURLs.equals(other.getArrayOfSiteURLs()))) &&
            ((this.userID==null && other.getUserID()==null) || 
             (this.userID!=null &&
              this.userID.equals(other.getUserID()))) &&
            ((this.checkInLocalCacheOnly==null && other.getCheckInLocalCacheOnly()==null) || 
             (this.checkInLocalCacheOnly!=null &&
              this.checkInLocalCacheOnly.equals(other.getCheckInLocalCacheOnly())));
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
        if (getArrayOfSiteURLs() != null) {
            _hashCode += getArrayOfSiteURLs().hashCode();
        }
        if (getUserID() != null) {
            _hashCode += getUserID().hashCode();
        }
        if (getCheckInLocalCacheOnly() != null) {
            _hashCode += getCheckInLocalCacheOnly().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmCheckPermissionRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermissionRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfSiteURLs");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfSiteURLs"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLInfo"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("checkInLocalCacheOnly");
        elemField.setXmlName(new javax.xml.namespace.QName("", "checkInLocalCacheOnly"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
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

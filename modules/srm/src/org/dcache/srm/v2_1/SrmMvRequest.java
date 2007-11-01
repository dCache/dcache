/**
 * SrmMvRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmMvRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.TSURLInfo fromPath;

    private org.dcache.srm.v2_1.TSURLInfo toPath;

    public SrmMvRequest() {
    }

    public SrmMvRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.TSURLInfo fromPath,
           org.dcache.srm.v2_1.TSURLInfo toPath) {
           this.userID = userID;
           this.fromPath = fromPath;
           this.toPath = toPath;
    }


    /**
     * Gets the userID value for this SrmMvRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmMvRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the fromPath value for this SrmMvRequest.
     * 
     * @return fromPath
     */
    public org.dcache.srm.v2_1.TSURLInfo getFromPath() {
        return fromPath;
    }


    /**
     * Sets the fromPath value for this SrmMvRequest.
     * 
     * @param fromPath
     */
    public void setFromPath(org.dcache.srm.v2_1.TSURLInfo fromPath) {
        this.fromPath = fromPath;
    }


    /**
     * Gets the toPath value for this SrmMvRequest.
     * 
     * @return toPath
     */
    public org.dcache.srm.v2_1.TSURLInfo getToPath() {
        return toPath;
    }


    /**
     * Sets the toPath value for this SrmMvRequest.
     * 
     * @param toPath
     */
    public void setToPath(org.dcache.srm.v2_1.TSURLInfo toPath) {
        this.toPath = toPath;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmMvRequest)) return false;
        SrmMvRequest other = (SrmMvRequest) obj;
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
            ((this.fromPath==null && other.getFromPath()==null) || 
             (this.fromPath!=null &&
              this.fromPath.equals(other.getFromPath()))) &&
            ((this.toPath==null && other.getToPath()==null) || 
             (this.toPath!=null &&
              this.toPath.equals(other.getToPath())));
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
        if (getFromPath() != null) {
            _hashCode += getFromPath().hashCode();
        }
        if (getToPath() != null) {
            _hashCode += getToPath().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmMvRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fromPath");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fromPath"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLInfo"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("toPath");
        elemField.setXmlName(new javax.xml.namespace.QName("", "toPath"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLInfo"));
        elemField.setNillable(false);
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

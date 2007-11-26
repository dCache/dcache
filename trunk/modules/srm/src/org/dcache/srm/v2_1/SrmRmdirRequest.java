/**
 * SrmRmdirRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmRmdirRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.TSURLInfo directoryPath;

    private java.lang.Boolean recursive;

    public SrmRmdirRequest() {
    }

    public SrmRmdirRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.TSURLInfo directoryPath,
           java.lang.Boolean recursive) {
           this.userID = userID;
           this.directoryPath = directoryPath;
           this.recursive = recursive;
    }


    /**
     * Gets the userID value for this SrmRmdirRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmRmdirRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the directoryPath value for this SrmRmdirRequest.
     * 
     * @return directoryPath
     */
    public org.dcache.srm.v2_1.TSURLInfo getDirectoryPath() {
        return directoryPath;
    }


    /**
     * Sets the directoryPath value for this SrmRmdirRequest.
     * 
     * @param directoryPath
     */
    public void setDirectoryPath(org.dcache.srm.v2_1.TSURLInfo directoryPath) {
        this.directoryPath = directoryPath;
    }


    /**
     * Gets the recursive value for this SrmRmdirRequest.
     * 
     * @return recursive
     */
    public java.lang.Boolean getRecursive() {
        return recursive;
    }


    /**
     * Sets the recursive value for this SrmRmdirRequest.
     * 
     * @param recursive
     */
    public void setRecursive(java.lang.Boolean recursive) {
        this.recursive = recursive;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmRmdirRequest)) return false;
        SrmRmdirRequest other = (SrmRmdirRequest) obj;
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
            ((this.directoryPath==null && other.getDirectoryPath()==null) || 
             (this.directoryPath!=null &&
              this.directoryPath.equals(other.getDirectoryPath()))) &&
            ((this.recursive==null && other.getRecursive()==null) || 
             (this.recursive!=null &&
              this.recursive.equals(other.getRecursive())));
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
        if (getDirectoryPath() != null) {
            _hashCode += getDirectoryPath().hashCode();
        }
        if (getRecursive() != null) {
            _hashCode += getRecursive().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmRmdirRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdirRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("directoryPath");
        elemField.setXmlName(new javax.xml.namespace.QName("", "directoryPath"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLInfo"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("recursive");
        elemField.setXmlName(new javax.xml.namespace.QName("", "recursive"));
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

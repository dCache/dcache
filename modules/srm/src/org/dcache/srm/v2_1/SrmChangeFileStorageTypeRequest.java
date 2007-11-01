/**
 * SrmChangeFileStorageTypeRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmChangeFileStorageTypeRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.ArrayOfTSURLInfo arrayOfPaths;

    private org.dcache.srm.v2_1.TFileStorageType desiredStorageType;

    public SrmChangeFileStorageTypeRequest() {
    }

    public SrmChangeFileStorageTypeRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.ArrayOfTSURLInfo arrayOfPaths,
           org.dcache.srm.v2_1.TFileStorageType desiredStorageType) {
           this.userID = userID;
           this.arrayOfPaths = arrayOfPaths;
           this.desiredStorageType = desiredStorageType;
    }


    /**
     * Gets the userID value for this SrmChangeFileStorageTypeRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmChangeFileStorageTypeRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the arrayOfPaths value for this SrmChangeFileStorageTypeRequest.
     * 
     * @return arrayOfPaths
     */
    public org.dcache.srm.v2_1.ArrayOfTSURLInfo getArrayOfPaths() {
        return arrayOfPaths;
    }


    /**
     * Sets the arrayOfPaths value for this SrmChangeFileStorageTypeRequest.
     * 
     * @param arrayOfPaths
     */
    public void setArrayOfPaths(org.dcache.srm.v2_1.ArrayOfTSURLInfo arrayOfPaths) {
        this.arrayOfPaths = arrayOfPaths;
    }


    /**
     * Gets the desiredStorageType value for this SrmChangeFileStorageTypeRequest.
     * 
     * @return desiredStorageType
     */
    public org.dcache.srm.v2_1.TFileStorageType getDesiredStorageType() {
        return desiredStorageType;
    }


    /**
     * Sets the desiredStorageType value for this SrmChangeFileStorageTypeRequest.
     * 
     * @param desiredStorageType
     */
    public void setDesiredStorageType(org.dcache.srm.v2_1.TFileStorageType desiredStorageType) {
        this.desiredStorageType = desiredStorageType;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmChangeFileStorageTypeRequest)) return false;
        SrmChangeFileStorageTypeRequest other = (SrmChangeFileStorageTypeRequest) obj;
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
            ((this.arrayOfPaths==null && other.getArrayOfPaths()==null) || 
             (this.arrayOfPaths!=null &&
              this.arrayOfPaths.equals(other.getArrayOfPaths()))) &&
            ((this.desiredStorageType==null && other.getDesiredStorageType()==null) || 
             (this.desiredStorageType!=null &&
              this.desiredStorageType.equals(other.getDesiredStorageType())));
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
        if (getArrayOfPaths() != null) {
            _hashCode += getArrayOfPaths().hashCode();
        }
        if (getDesiredStorageType() != null) {
            _hashCode += getDesiredStorageType().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmChangeFileStorageTypeRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeFileStorageTypeRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfPaths");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfPaths"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("desiredStorageType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredStorageType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileStorageType"));
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

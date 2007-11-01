/**
 * SrmGetSpaceTokenRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmGetSpaceTokenRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private java.lang.String userSpaceTokenDescription;

    public SrmGetSpaceTokenRequest() {
    }

    public SrmGetSpaceTokenRequest(
           org.dcache.srm.v2_1.TUserID userID,
           java.lang.String userSpaceTokenDescription) {
           this.userID = userID;
           this.userSpaceTokenDescription = userSpaceTokenDescription;
    }


    /**
     * Gets the userID value for this SrmGetSpaceTokenRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmGetSpaceTokenRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the userSpaceTokenDescription value for this SrmGetSpaceTokenRequest.
     * 
     * @return userSpaceTokenDescription
     */
    public java.lang.String getUserSpaceTokenDescription() {
        return userSpaceTokenDescription;
    }


    /**
     * Sets the userSpaceTokenDescription value for this SrmGetSpaceTokenRequest.
     * 
     * @param userSpaceTokenDescription
     */
    public void setUserSpaceTokenDescription(java.lang.String userSpaceTokenDescription) {
        this.userSpaceTokenDescription = userSpaceTokenDescription;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmGetSpaceTokenRequest)) return false;
        SrmGetSpaceTokenRequest other = (SrmGetSpaceTokenRequest) obj;
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
            ((this.userSpaceTokenDescription==null && other.getUserSpaceTokenDescription()==null) || 
             (this.userSpaceTokenDescription!=null &&
              this.userSpaceTokenDescription.equals(other.getUserSpaceTokenDescription())));
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
        if (getUserSpaceTokenDescription() != null) {
            _hashCode += getUserSpaceTokenDescription().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmGetSpaceTokenRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokenRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userSpaceTokenDescription");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userSpaceTokenDescription"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
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

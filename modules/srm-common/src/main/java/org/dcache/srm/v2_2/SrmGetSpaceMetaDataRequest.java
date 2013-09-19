/**
 * SrmGetSpaceMetaDataRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmGetSpaceMetaDataRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -7823828887181035380L;
    private java.lang.String authorizationID;

    private org.dcache.srm.v2_2.ArrayOfString arrayOfSpaceTokens;

    public SrmGetSpaceMetaDataRequest() {
    }

    public SrmGetSpaceMetaDataRequest(
           java.lang.String authorizationID,
           org.dcache.srm.v2_2.ArrayOfString arrayOfSpaceTokens) {
           this.authorizationID = authorizationID;
           this.arrayOfSpaceTokens = arrayOfSpaceTokens;
    }


    /**
     * Gets the authorizationID value for this SrmGetSpaceMetaDataRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmGetSpaceMetaDataRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the arrayOfSpaceTokens value for this SrmGetSpaceMetaDataRequest.
     * 
     * @return arrayOfSpaceTokens
     */
    public org.dcache.srm.v2_2.ArrayOfString getArrayOfSpaceTokens() {
        return arrayOfSpaceTokens;
    }


    /**
     * Sets the arrayOfSpaceTokens value for this SrmGetSpaceMetaDataRequest.
     * 
     * @param arrayOfSpaceTokens
     */
    public void setArrayOfSpaceTokens(org.dcache.srm.v2_2.ArrayOfString arrayOfSpaceTokens) {
        this.arrayOfSpaceTokens = arrayOfSpaceTokens;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmGetSpaceMetaDataRequest)) {
            return false;
        }
        SrmGetSpaceMetaDataRequest other = (SrmGetSpaceMetaDataRequest) obj;
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
            ((this.authorizationID==null && other.getAuthorizationID()==null) || 
             (this.authorizationID!=null &&
              this.authorizationID.equals(other.getAuthorizationID()))) &&
            ((this.arrayOfSpaceTokens==null && other.getArrayOfSpaceTokens()==null) || 
             (this.arrayOfSpaceTokens!=null &&
              this.arrayOfSpaceTokens.equals(other.getArrayOfSpaceTokens())));
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
        if (getAuthorizationID() != null) {
            _hashCode += getAuthorizationID().hashCode();
        }
        if (getArrayOfSpaceTokens() != null) {
            _hashCode += getArrayOfSpaceTokens().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmGetSpaceMetaDataRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfSpaceTokens");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfSpaceTokens"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfString"));
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

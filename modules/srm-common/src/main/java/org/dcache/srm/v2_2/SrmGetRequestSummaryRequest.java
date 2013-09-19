/**
 * SrmGetRequestSummaryRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmGetRequestSummaryRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -3435055308604239440L;
    private org.dcache.srm.v2_2.ArrayOfString arrayOfRequestTokens;

    private java.lang.String authorizationID;

    public SrmGetRequestSummaryRequest() {
    }

    public SrmGetRequestSummaryRequest(
           org.dcache.srm.v2_2.ArrayOfString arrayOfRequestTokens,
           java.lang.String authorizationID) {
           this.arrayOfRequestTokens = arrayOfRequestTokens;
           this.authorizationID = authorizationID;
    }


    /**
     * Gets the arrayOfRequestTokens value for this SrmGetRequestSummaryRequest.
     * 
     * @return arrayOfRequestTokens
     */
    public org.dcache.srm.v2_2.ArrayOfString getArrayOfRequestTokens() {
        return arrayOfRequestTokens;
    }


    /**
     * Sets the arrayOfRequestTokens value for this SrmGetRequestSummaryRequest.
     * 
     * @param arrayOfRequestTokens
     */
    public void setArrayOfRequestTokens(org.dcache.srm.v2_2.ArrayOfString arrayOfRequestTokens) {
        this.arrayOfRequestTokens = arrayOfRequestTokens;
    }


    /**
     * Gets the authorizationID value for this SrmGetRequestSummaryRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmGetRequestSummaryRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmGetRequestSummaryRequest)) {
            return false;
        }
        SrmGetRequestSummaryRequest other = (SrmGetRequestSummaryRequest) obj;
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
            ((this.arrayOfRequestTokens==null && other.getArrayOfRequestTokens()==null) || 
             (this.arrayOfRequestTokens!=null &&
              this.arrayOfRequestTokens.equals(other.getArrayOfRequestTokens()))) &&
            ((this.authorizationID==null && other.getAuthorizationID()==null) || 
             (this.authorizationID!=null &&
              this.authorizationID.equals(other.getAuthorizationID())));
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
        if (getArrayOfRequestTokens() != null) {
            _hashCode += getArrayOfRequestTokens().hashCode();
        }
        if (getAuthorizationID() != null) {
            _hashCode += getAuthorizationID().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmGetRequestSummaryRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummaryRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfRequestTokens");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfRequestTokens"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfString"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
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

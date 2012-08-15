/**
 * SrmStatusOfPutRequestRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmStatusOfPutRequestRequest  implements java.io.Serializable {
    private static final long serialVersionUID = 4337589887663762358L;
    private java.lang.String requestToken;

    private java.lang.String authorizationID;

    private org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfTargetSURLs;

    public SrmStatusOfPutRequestRequest() {
    }

    public SrmStatusOfPutRequestRequest(
           java.lang.String requestToken,
           java.lang.String authorizationID,
           org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfTargetSURLs) {
           this.requestToken = requestToken;
           this.authorizationID = authorizationID;
           this.arrayOfTargetSURLs = arrayOfTargetSURLs;
    }


    /**
     * Gets the requestToken value for this SrmStatusOfPutRequestRequest.
     * 
     * @return requestToken
     */
    public java.lang.String getRequestToken() {
        return requestToken;
    }


    /**
     * Sets the requestToken value for this SrmStatusOfPutRequestRequest.
     * 
     * @param requestToken
     */
    public void setRequestToken(java.lang.String requestToken) {
        this.requestToken = requestToken;
    }


    /**
     * Gets the authorizationID value for this SrmStatusOfPutRequestRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmStatusOfPutRequestRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the arrayOfTargetSURLs value for this SrmStatusOfPutRequestRequest.
     * 
     * @return arrayOfTargetSURLs
     */
    public org.dcache.srm.v2_2.ArrayOfAnyURI getArrayOfTargetSURLs() {
        return arrayOfTargetSURLs;
    }


    /**
     * Sets the arrayOfTargetSURLs value for this SrmStatusOfPutRequestRequest.
     * 
     * @param arrayOfTargetSURLs
     */
    public void setArrayOfTargetSURLs(org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfTargetSURLs) {
        this.arrayOfTargetSURLs = arrayOfTargetSURLs;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmStatusOfPutRequestRequest)) {
            return false;
        }
        SrmStatusOfPutRequestRequest other = (SrmStatusOfPutRequestRequest) obj;
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
            ((this.requestToken==null && other.getRequestToken()==null) || 
             (this.requestToken!=null &&
              this.requestToken.equals(other.getRequestToken()))) &&
            ((this.authorizationID==null && other.getAuthorizationID()==null) || 
             (this.authorizationID!=null &&
              this.authorizationID.equals(other.getAuthorizationID()))) &&
            ((this.arrayOfTargetSURLs==null && other.getArrayOfTargetSURLs()==null) || 
             (this.arrayOfTargetSURLs!=null &&
              this.arrayOfTargetSURLs.equals(other.getArrayOfTargetSURLs())));
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
        if (getRequestToken() != null) {
            _hashCode += getRequestToken().hashCode();
        }
        if (getAuthorizationID() != null) {
            _hashCode += getAuthorizationID().hashCode();
        }
        if (getArrayOfTargetSURLs() != null) {
            _hashCode += getArrayOfTargetSURLs().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmStatusOfPutRequestRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequestRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfTargetSURLs");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfTargetSURLs"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfAnyURI"));
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

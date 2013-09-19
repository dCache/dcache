/**
 * SrmExtendFileLifeTimeInSpaceRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmExtendFileLifeTimeInSpaceRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -9060891733993980817L;
    private java.lang.String authorizationID;

    private java.lang.String spaceToken;

    private org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs;

    private java.lang.Integer newLifeTime;

    public SrmExtendFileLifeTimeInSpaceRequest() {
    }

    public SrmExtendFileLifeTimeInSpaceRequest(
           java.lang.String authorizationID,
           java.lang.String spaceToken,
           org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs,
           java.lang.Integer newLifeTime) {
           this.authorizationID = authorizationID;
           this.spaceToken = spaceToken;
           this.arrayOfSURLs = arrayOfSURLs;
           this.newLifeTime = newLifeTime;
    }


    /**
     * Gets the authorizationID value for this SrmExtendFileLifeTimeInSpaceRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmExtendFileLifeTimeInSpaceRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the spaceToken value for this SrmExtendFileLifeTimeInSpaceRequest.
     * 
     * @return spaceToken
     */
    public java.lang.String getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this SrmExtendFileLifeTimeInSpaceRequest.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(java.lang.String spaceToken) {
        this.spaceToken = spaceToken;
    }


    /**
     * Gets the arrayOfSURLs value for this SrmExtendFileLifeTimeInSpaceRequest.
     * 
     * @return arrayOfSURLs
     */
    public org.dcache.srm.v2_2.ArrayOfAnyURI getArrayOfSURLs() {
        return arrayOfSURLs;
    }


    /**
     * Sets the arrayOfSURLs value for this SrmExtendFileLifeTimeInSpaceRequest.
     * 
     * @param arrayOfSURLs
     */
    public void setArrayOfSURLs(org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs) {
        this.arrayOfSURLs = arrayOfSURLs;
    }


    /**
     * Gets the newLifeTime value for this SrmExtendFileLifeTimeInSpaceRequest.
     * 
     * @return newLifeTime
     */
    public java.lang.Integer getNewLifeTime() {
        return newLifeTime;
    }


    /**
     * Sets the newLifeTime value for this SrmExtendFileLifeTimeInSpaceRequest.
     * 
     * @param newLifeTime
     */
    public void setNewLifeTime(java.lang.Integer newLifeTime) {
        this.newLifeTime = newLifeTime;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmExtendFileLifeTimeInSpaceRequest)) {
            return false;
        }
        SrmExtendFileLifeTimeInSpaceRequest other = (SrmExtendFileLifeTimeInSpaceRequest) obj;
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
            ((this.spaceToken==null && other.getSpaceToken()==null) || 
             (this.spaceToken!=null &&
              this.spaceToken.equals(other.getSpaceToken()))) &&
            ((this.arrayOfSURLs==null && other.getArrayOfSURLs()==null) || 
             (this.arrayOfSURLs!=null &&
              this.arrayOfSURLs.equals(other.getArrayOfSURLs()))) &&
            ((this.newLifeTime==null && other.getNewLifeTime()==null) || 
             (this.newLifeTime!=null &&
              this.newLifeTime.equals(other.getNewLifeTime())));
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
        if (getSpaceToken() != null) {
            _hashCode += getSpaceToken().hashCode();
        }
        if (getArrayOfSURLs() != null) {
            _hashCode += getArrayOfSURLs().hashCode();
        }
        if (getNewLifeTime() != null) {
            _hashCode += getNewLifeTime().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmExtendFileLifeTimeInSpaceRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeInSpaceRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("spaceToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "spaceToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfSURLs");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfSURLs"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfAnyURI"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("newLifeTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newLifeTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
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

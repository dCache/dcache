/**
 * SrmExtendFileLifeTimeRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmExtendFileLifeTimeRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -6834351361075069412L;
    private java.lang.String authorizationID;

    private java.lang.String requestToken;

    private org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs;

    private java.lang.Integer newFileLifeTime;

    private java.lang.Integer newPinLifeTime;

    public SrmExtendFileLifeTimeRequest() {
    }

    public SrmExtendFileLifeTimeRequest(
           java.lang.String authorizationID,
           java.lang.String requestToken,
           org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs,
           java.lang.Integer newFileLifeTime,
           java.lang.Integer newPinLifeTime) {
           this.authorizationID = authorizationID;
           this.requestToken = requestToken;
           this.arrayOfSURLs = arrayOfSURLs;
           this.newFileLifeTime = newFileLifeTime;
           this.newPinLifeTime = newPinLifeTime;
    }


    /**
     * Gets the authorizationID value for this SrmExtendFileLifeTimeRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmExtendFileLifeTimeRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the requestToken value for this SrmExtendFileLifeTimeRequest.
     * 
     * @return requestToken
     */
    public java.lang.String getRequestToken() {
        return requestToken;
    }


    /**
     * Sets the requestToken value for this SrmExtendFileLifeTimeRequest.
     * 
     * @param requestToken
     */
    public void setRequestToken(java.lang.String requestToken) {
        this.requestToken = requestToken;
    }


    /**
     * Gets the arrayOfSURLs value for this SrmExtendFileLifeTimeRequest.
     * 
     * @return arrayOfSURLs
     */
    public org.dcache.srm.v2_2.ArrayOfAnyURI getArrayOfSURLs() {
        return arrayOfSURLs;
    }


    /**
     * Sets the arrayOfSURLs value for this SrmExtendFileLifeTimeRequest.
     * 
     * @param arrayOfSURLs
     */
    public void setArrayOfSURLs(org.dcache.srm.v2_2.ArrayOfAnyURI arrayOfSURLs) {
        this.arrayOfSURLs = arrayOfSURLs;
    }


    /**
     * Gets the newFileLifeTime value for this SrmExtendFileLifeTimeRequest.
     * 
     * @return newFileLifeTime
     */
    public java.lang.Integer getNewFileLifeTime() {
        return newFileLifeTime;
    }


    /**
     * Sets the newFileLifeTime value for this SrmExtendFileLifeTimeRequest.
     * 
     * @param newFileLifeTime
     */
    public void setNewFileLifeTime(java.lang.Integer newFileLifeTime) {
        this.newFileLifeTime = newFileLifeTime;
    }


    /**
     * Gets the newPinLifeTime value for this SrmExtendFileLifeTimeRequest.
     * 
     * @return newPinLifeTime
     */
    public java.lang.Integer getNewPinLifeTime() {
        return newPinLifeTime;
    }


    /**
     * Sets the newPinLifeTime value for this SrmExtendFileLifeTimeRequest.
     * 
     * @param newPinLifeTime
     */
    public void setNewPinLifeTime(java.lang.Integer newPinLifeTime) {
        this.newPinLifeTime = newPinLifeTime;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmExtendFileLifeTimeRequest)) {
            return false;
        }
        SrmExtendFileLifeTimeRequest other = (SrmExtendFileLifeTimeRequest) obj;
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
            ((this.requestToken==null && other.getRequestToken()==null) || 
             (this.requestToken!=null &&
              this.requestToken.equals(other.getRequestToken()))) &&
            ((this.arrayOfSURLs==null && other.getArrayOfSURLs()==null) || 
             (this.arrayOfSURLs!=null &&
              this.arrayOfSURLs.equals(other.getArrayOfSURLs()))) &&
            ((this.newFileLifeTime==null && other.getNewFileLifeTime()==null) || 
             (this.newFileLifeTime!=null &&
              this.newFileLifeTime.equals(other.getNewFileLifeTime()))) &&
            ((this.newPinLifeTime==null && other.getNewPinLifeTime()==null) || 
             (this.newPinLifeTime!=null &&
              this.newPinLifeTime.equals(other.getNewPinLifeTime())));
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
        if (getRequestToken() != null) {
            _hashCode += getRequestToken().hashCode();
        }
        if (getArrayOfSURLs() != null) {
            _hashCode += getArrayOfSURLs().hashCode();
        }
        if (getNewFileLifeTime() != null) {
            _hashCode += getNewFileLifeTime().hashCode();
        }
        if (getNewPinLifeTime() != null) {
            _hashCode += getNewPinLifeTime().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmExtendFileLifeTimeRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfSURLs");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfSURLs"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfAnyURI"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("newFileLifeTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newFileLifeTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("newPinLifeTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newPinLifeTime"));
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

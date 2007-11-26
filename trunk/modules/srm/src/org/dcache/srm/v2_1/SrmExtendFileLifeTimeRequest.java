/**
 * SrmExtendFileLifeTimeRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmExtendFileLifeTimeRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TRequestToken requestToken;

    private org.dcache.srm.v2_1.TSURL siteURL;

    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds newLifeTime;

    public SrmExtendFileLifeTimeRequest() {
    }

    public SrmExtendFileLifeTimeRequest(
           org.dcache.srm.v2_1.TRequestToken requestToken,
           org.dcache.srm.v2_1.TSURL siteURL,
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.TLifeTimeInSeconds newLifeTime) {
           this.requestToken = requestToken;
           this.siteURL = siteURL;
           this.userID = userID;
           this.newLifeTime = newLifeTime;
    }


    /**
     * Gets the requestToken value for this SrmExtendFileLifeTimeRequest.
     * 
     * @return requestToken
     */
    public org.dcache.srm.v2_1.TRequestToken getRequestToken() {
        return requestToken;
    }


    /**
     * Sets the requestToken value for this SrmExtendFileLifeTimeRequest.
     * 
     * @param requestToken
     */
    public void setRequestToken(org.dcache.srm.v2_1.TRequestToken requestToken) {
        this.requestToken = requestToken;
    }


    /**
     * Gets the siteURL value for this SrmExtendFileLifeTimeRequest.
     * 
     * @return siteURL
     */
    public org.dcache.srm.v2_1.TSURL getSiteURL() {
        return siteURL;
    }


    /**
     * Sets the siteURL value for this SrmExtendFileLifeTimeRequest.
     * 
     * @param siteURL
     */
    public void setSiteURL(org.dcache.srm.v2_1.TSURL siteURL) {
        this.siteURL = siteURL;
    }


    /**
     * Gets the userID value for this SrmExtendFileLifeTimeRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmExtendFileLifeTimeRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the newLifeTime value for this SrmExtendFileLifeTimeRequest.
     * 
     * @return newLifeTime
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getNewLifeTime() {
        return newLifeTime;
    }


    /**
     * Sets the newLifeTime value for this SrmExtendFileLifeTimeRequest.
     * 
     * @param newLifeTime
     */
    public void setNewLifeTime(org.dcache.srm.v2_1.TLifeTimeInSeconds newLifeTime) {
        this.newLifeTime = newLifeTime;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmExtendFileLifeTimeRequest)) return false;
        SrmExtendFileLifeTimeRequest other = (SrmExtendFileLifeTimeRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.requestToken==null && other.getRequestToken()==null) || 
             (this.requestToken!=null &&
              this.requestToken.equals(other.getRequestToken()))) &&
            ((this.siteURL==null && other.getSiteURL()==null) || 
             (this.siteURL!=null &&
              this.siteURL.equals(other.getSiteURL()))) &&
            ((this.userID==null && other.getUserID()==null) || 
             (this.userID!=null &&
              this.userID.equals(other.getUserID()))) &&
            ((this.newLifeTime==null && other.getNewLifeTime()==null) || 
             (this.newLifeTime!=null &&
              this.newLifeTime.equals(other.getNewLifeTime())));
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
        if (getRequestToken() != null) {
            _hashCode += getRequestToken().hashCode();
        }
        if (getSiteURL() != null) {
            _hashCode += getSiteURL().hashCode();
        }
        if (getUserID() != null) {
            _hashCode += getUserID().hashCode();
        }
        if (getNewLifeTime() != null) {
            _hashCode += getNewLifeTime().hashCode();
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
        elemField.setFieldName("requestToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestToken"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("siteURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "siteURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURL"));
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
        elemField.setFieldName("newLifeTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "newLifeTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
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

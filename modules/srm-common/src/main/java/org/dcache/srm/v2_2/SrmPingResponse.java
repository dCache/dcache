/**
 * SrmPingResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmPingResponse  implements java.io.Serializable {
    private static final long serialVersionUID = 3541295487608923707L;
    private java.lang.String versionInfo;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo otherInfo;

    public SrmPingResponse() {
    }

    public SrmPingResponse(
           java.lang.String versionInfo,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo otherInfo) {
           this.versionInfo = versionInfo;
           this.otherInfo = otherInfo;
    }


    /**
     * Gets the versionInfo value for this SrmPingResponse.
     * 
     * @return versionInfo
     */
    public java.lang.String getVersionInfo() {
        return versionInfo;
    }


    /**
     * Sets the versionInfo value for this SrmPingResponse.
     * 
     * @param versionInfo
     */
    public void setVersionInfo(java.lang.String versionInfo) {
        this.versionInfo = versionInfo;
    }


    /**
     * Gets the otherInfo value for this SrmPingResponse.
     * 
     * @return otherInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getOtherInfo() {
        return otherInfo;
    }


    /**
     * Sets the otherInfo value for this SrmPingResponse.
     * 
     * @param otherInfo
     */
    public void setOtherInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo otherInfo) {
        this.otherInfo = otherInfo;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmPingResponse)) {
            return false;
        }
        SrmPingResponse other = (SrmPingResponse) obj;
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
            ((this.versionInfo==null && other.getVersionInfo()==null) || 
             (this.versionInfo!=null &&
              this.versionInfo.equals(other.getVersionInfo()))) &&
            ((this.otherInfo==null && other.getOtherInfo()==null) || 
             (this.otherInfo!=null &&
              this.otherInfo.equals(other.getOtherInfo())));
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
        if (getVersionInfo() != null) {
            _hashCode += getVersionInfo().hashCode();
        }
        if (getOtherInfo() != null) {
            _hashCode += getOtherInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmPingResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPingResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("versionInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "versionInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("otherInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "otherInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTExtraInfo"));
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

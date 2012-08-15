/**
 * TSURLReturnStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TSURLReturnStatus  implements java.io.Serializable {
    private static final long serialVersionUID = -160825511817998486L;
    private org.apache.axis.types.URI surl;

    private org.dcache.srm.v2_2.TReturnStatus status;

    public TSURLReturnStatus() {
    }

    public TSURLReturnStatus(
           org.apache.axis.types.URI surl,
           org.dcache.srm.v2_2.TReturnStatus status) {
           this.surl = surl;
           this.status = status;
    }


    /**
     * Gets the surl value for this TSURLReturnStatus.
     * 
     * @return surl
     */
    public org.apache.axis.types.URI getSurl() {
        return surl;
    }


    /**
     * Sets the surl value for this TSURLReturnStatus.
     * 
     * @param surl
     */
    public void setSurl(org.apache.axis.types.URI surl) {
        this.surl = surl;
    }


    /**
     * Gets the status value for this TSURLReturnStatus.
     * 
     * @return status
     */
    public org.dcache.srm.v2_2.TReturnStatus getStatus() {
        return status;
    }


    /**
     * Sets the status value for this TSURLReturnStatus.
     * 
     * @param status
     */
    public void setStatus(org.dcache.srm.v2_2.TReturnStatus status) {
        this.status = status;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TSURLReturnStatus)) {
            return false;
        }
        TSURLReturnStatus other = (TSURLReturnStatus) obj;
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
            ((this.surl==null && other.getSurl()==null) || 
             (this.surl!=null &&
              this.surl.equals(other.getSurl()))) &&
            ((this.status==null && other.getStatus()==null) || 
             (this.status!=null &&
              this.status.equals(other.getStatus())));
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
        if (getSurl() != null) {
            _hashCode += getSurl().hashCode();
        }
        if (getStatus() != null) {
            _hashCode += getStatus().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TSURLReturnStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLReturnStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("surl");
        elemField.setXmlName(new javax.xml.namespace.QName("", "surl"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("status");
        elemField.setXmlName(new javax.xml.namespace.QName("", "status"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
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

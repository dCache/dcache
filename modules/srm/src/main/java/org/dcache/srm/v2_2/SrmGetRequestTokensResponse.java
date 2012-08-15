/**
 * SrmGetRequestTokensResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmGetRequestTokensResponse  implements java.io.Serializable {
    private static final long serialVersionUID = -46014695829241901L;
    private org.dcache.srm.v2_2.TReturnStatus returnStatus;

    private org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn arrayOfRequestTokens;

    public SrmGetRequestTokensResponse() {
    }

    public SrmGetRequestTokensResponse(
           org.dcache.srm.v2_2.TReturnStatus returnStatus,
           org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn arrayOfRequestTokens) {
           this.returnStatus = returnStatus;
           this.arrayOfRequestTokens = arrayOfRequestTokens;
    }


    /**
     * Gets the returnStatus value for this SrmGetRequestTokensResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_2.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmGetRequestTokensResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_2.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }


    /**
     * Gets the arrayOfRequestTokens value for this SrmGetRequestTokensResponse.
     * 
     * @return arrayOfRequestTokens
     */
    public org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn getArrayOfRequestTokens() {
        return arrayOfRequestTokens;
    }


    /**
     * Sets the arrayOfRequestTokens value for this SrmGetRequestTokensResponse.
     * 
     * @param arrayOfRequestTokens
     */
    public void setArrayOfRequestTokens(org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn arrayOfRequestTokens) {
        this.arrayOfRequestTokens = arrayOfRequestTokens;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmGetRequestTokensResponse)) {
            return false;
        }
        SrmGetRequestTokensResponse other = (SrmGetRequestTokensResponse) obj;
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
            ((this.returnStatus==null && other.getReturnStatus()==null) || 
             (this.returnStatus!=null &&
              this.returnStatus.equals(other.getReturnStatus()))) &&
            ((this.arrayOfRequestTokens==null && other.getArrayOfRequestTokens()==null) || 
             (this.arrayOfRequestTokens!=null &&
              this.arrayOfRequestTokens.equals(other.getArrayOfRequestTokens())));
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
        if (getReturnStatus() != null) {
            _hashCode += getReturnStatus().hashCode();
        }
        if (getArrayOfRequestTokens() != null) {
            _hashCode += getArrayOfRequestTokens().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmGetRequestTokensResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestTokensResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("returnStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("", "returnStatus"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfRequestTokens");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfRequestTokens"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTRequestTokenReturn"));
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

/**
 * SrmGetRequestIDResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmGetRequestIDResponse  implements java.io.Serializable {
    private org.dcache.srm.v2_1.ArrayOfTRequestToken arrayOfRequestTokens;

    private org.dcache.srm.v2_1.TReturnStatus returnStatus;

    public SrmGetRequestIDResponse() {
    }

    public SrmGetRequestIDResponse(
           org.dcache.srm.v2_1.ArrayOfTRequestToken arrayOfRequestTokens,
           org.dcache.srm.v2_1.TReturnStatus returnStatus) {
           this.arrayOfRequestTokens = arrayOfRequestTokens;
           this.returnStatus = returnStatus;
    }


    /**
     * Gets the arrayOfRequestTokens value for this SrmGetRequestIDResponse.
     * 
     * @return arrayOfRequestTokens
     */
    public org.dcache.srm.v2_1.ArrayOfTRequestToken getArrayOfRequestTokens() {
        return arrayOfRequestTokens;
    }


    /**
     * Sets the arrayOfRequestTokens value for this SrmGetRequestIDResponse.
     * 
     * @param arrayOfRequestTokens
     */
    public void setArrayOfRequestTokens(org.dcache.srm.v2_1.ArrayOfTRequestToken arrayOfRequestTokens) {
        this.arrayOfRequestTokens = arrayOfRequestTokens;
    }


    /**
     * Gets the returnStatus value for this SrmGetRequestIDResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_1.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmGetRequestIDResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_1.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmGetRequestIDResponse)) return false;
        SrmGetRequestIDResponse other = (SrmGetRequestIDResponse) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.arrayOfRequestTokens==null && other.getArrayOfRequestTokens()==null) || 
             (this.arrayOfRequestTokens!=null &&
              this.arrayOfRequestTokens.equals(other.getArrayOfRequestTokens()))) &&
            ((this.returnStatus==null && other.getReturnStatus()==null) || 
             (this.returnStatus!=null &&
              this.returnStatus.equals(other.getReturnStatus())));
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
        if (getArrayOfRequestTokens() != null) {
            _hashCode += getArrayOfRequestTokens().hashCode();
        }
        if (getReturnStatus() != null) {
            _hashCode += getReturnStatus().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmGetRequestIDResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestIDResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfRequestTokens");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfRequestTokens"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTRequestToken"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("returnStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("", "returnStatus"));
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

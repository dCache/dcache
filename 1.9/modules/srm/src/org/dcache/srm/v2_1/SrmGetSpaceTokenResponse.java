/**
 * SrmGetSpaceTokenResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmGetSpaceTokenResponse  implements java.io.Serializable {
    private org.dcache.srm.v2_1.ArrayOfTSpaceToken arrayOfPossibleSpaceTokens;

    private org.dcache.srm.v2_1.TReturnStatus returnStatus;

    public SrmGetSpaceTokenResponse() {
    }

    public SrmGetSpaceTokenResponse(
           org.dcache.srm.v2_1.ArrayOfTSpaceToken arrayOfPossibleSpaceTokens,
           org.dcache.srm.v2_1.TReturnStatus returnStatus) {
           this.arrayOfPossibleSpaceTokens = arrayOfPossibleSpaceTokens;
           this.returnStatus = returnStatus;
    }


    /**
     * Gets the arrayOfPossibleSpaceTokens value for this SrmGetSpaceTokenResponse.
     * 
     * @return arrayOfPossibleSpaceTokens
     */
    public org.dcache.srm.v2_1.ArrayOfTSpaceToken getArrayOfPossibleSpaceTokens() {
        return arrayOfPossibleSpaceTokens;
    }


    /**
     * Sets the arrayOfPossibleSpaceTokens value for this SrmGetSpaceTokenResponse.
     * 
     * @param arrayOfPossibleSpaceTokens
     */
    public void setArrayOfPossibleSpaceTokens(org.dcache.srm.v2_1.ArrayOfTSpaceToken arrayOfPossibleSpaceTokens) {
        this.arrayOfPossibleSpaceTokens = arrayOfPossibleSpaceTokens;
    }


    /**
     * Gets the returnStatus value for this SrmGetSpaceTokenResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_1.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmGetSpaceTokenResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_1.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmGetSpaceTokenResponse)) return false;
        SrmGetSpaceTokenResponse other = (SrmGetSpaceTokenResponse) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.arrayOfPossibleSpaceTokens==null && other.getArrayOfPossibleSpaceTokens()==null) || 
             (this.arrayOfPossibleSpaceTokens!=null &&
              this.arrayOfPossibleSpaceTokens.equals(other.getArrayOfPossibleSpaceTokens()))) &&
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
        if (getArrayOfPossibleSpaceTokens() != null) {
            _hashCode += getArrayOfPossibleSpaceTokens().hashCode();
        }
        if (getReturnStatus() != null) {
            _hashCode += getReturnStatus().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmGetSpaceTokenResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokenResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfPossibleSpaceTokens");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfPossibleSpaceTokens"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSpaceToken"));
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

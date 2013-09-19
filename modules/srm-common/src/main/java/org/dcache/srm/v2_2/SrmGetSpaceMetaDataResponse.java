/**
 * SrmGetSpaceMetaDataResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmGetSpaceMetaDataResponse  implements java.io.Serializable {
    private static final long serialVersionUID = -1790311533420016285L;
    private org.dcache.srm.v2_2.TReturnStatus returnStatus;

    private org.dcache.srm.v2_2.ArrayOfTMetaDataSpace arrayOfSpaceDetails;

    public SrmGetSpaceMetaDataResponse() {
    }

    public SrmGetSpaceMetaDataResponse(
           org.dcache.srm.v2_2.TReturnStatus returnStatus,
           org.dcache.srm.v2_2.ArrayOfTMetaDataSpace arrayOfSpaceDetails) {
           this.returnStatus = returnStatus;
           this.arrayOfSpaceDetails = arrayOfSpaceDetails;
    }


    /**
     * Gets the returnStatus value for this SrmGetSpaceMetaDataResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_2.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmGetSpaceMetaDataResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_2.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }


    /**
     * Gets the arrayOfSpaceDetails value for this SrmGetSpaceMetaDataResponse.
     * 
     * @return arrayOfSpaceDetails
     */
    public org.dcache.srm.v2_2.ArrayOfTMetaDataSpace getArrayOfSpaceDetails() {
        return arrayOfSpaceDetails;
    }


    /**
     * Sets the arrayOfSpaceDetails value for this SrmGetSpaceMetaDataResponse.
     * 
     * @param arrayOfSpaceDetails
     */
    public void setArrayOfSpaceDetails(org.dcache.srm.v2_2.ArrayOfTMetaDataSpace arrayOfSpaceDetails) {
        this.arrayOfSpaceDetails = arrayOfSpaceDetails;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmGetSpaceMetaDataResponse)) {
            return false;
        }
        SrmGetSpaceMetaDataResponse other = (SrmGetSpaceMetaDataResponse) obj;
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
            ((this.arrayOfSpaceDetails==null && other.getArrayOfSpaceDetails()==null) || 
             (this.arrayOfSpaceDetails!=null &&
              this.arrayOfSpaceDetails.equals(other.getArrayOfSpaceDetails())));
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
        if (getArrayOfSpaceDetails() != null) {
            _hashCode += getArrayOfSpaceDetails().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmGetSpaceMetaDataResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("returnStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("", "returnStatus"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfSpaceDetails");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfSpaceDetails"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTMetaDataSpace"));
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

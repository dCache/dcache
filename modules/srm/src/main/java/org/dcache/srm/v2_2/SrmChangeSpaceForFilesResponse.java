/**
 * SrmChangeSpaceForFilesResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmChangeSpaceForFilesResponse  implements java.io.Serializable {
    private static final long serialVersionUID = 1010916474316905267L;
    private org.dcache.srm.v2_2.TReturnStatus returnStatus;

    private java.lang.String requestToken;

    private java.lang.Integer estimatedProcessingTime;

    private org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus arrayOfFileStatuses;

    public SrmChangeSpaceForFilesResponse() {
    }

    public SrmChangeSpaceForFilesResponse(
           org.dcache.srm.v2_2.TReturnStatus returnStatus,
           java.lang.String requestToken,
           java.lang.Integer estimatedProcessingTime,
           org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus arrayOfFileStatuses) {
           this.returnStatus = returnStatus;
           this.requestToken = requestToken;
           this.estimatedProcessingTime = estimatedProcessingTime;
           this.arrayOfFileStatuses = arrayOfFileStatuses;
    }


    /**
     * Gets the returnStatus value for this SrmChangeSpaceForFilesResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_2.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmChangeSpaceForFilesResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_2.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }


    /**
     * Gets the requestToken value for this SrmChangeSpaceForFilesResponse.
     * 
     * @return requestToken
     */
    public java.lang.String getRequestToken() {
        return requestToken;
    }


    /**
     * Sets the requestToken value for this SrmChangeSpaceForFilesResponse.
     * 
     * @param requestToken
     */
    public void setRequestToken(java.lang.String requestToken) {
        this.requestToken = requestToken;
    }


    /**
     * Gets the estimatedProcessingTime value for this SrmChangeSpaceForFilesResponse.
     * 
     * @return estimatedProcessingTime
     */
    public java.lang.Integer getEstimatedProcessingTime() {
        return estimatedProcessingTime;
    }


    /**
     * Sets the estimatedProcessingTime value for this SrmChangeSpaceForFilesResponse.
     * 
     * @param estimatedProcessingTime
     */
    public void setEstimatedProcessingTime(java.lang.Integer estimatedProcessingTime) {
        this.estimatedProcessingTime = estimatedProcessingTime;
    }


    /**
     * Gets the arrayOfFileStatuses value for this SrmChangeSpaceForFilesResponse.
     * 
     * @return arrayOfFileStatuses
     */
    public org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus getArrayOfFileStatuses() {
        return arrayOfFileStatuses;
    }


    /**
     * Sets the arrayOfFileStatuses value for this SrmChangeSpaceForFilesResponse.
     * 
     * @param arrayOfFileStatuses
     */
    public void setArrayOfFileStatuses(org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus arrayOfFileStatuses) {
        this.arrayOfFileStatuses = arrayOfFileStatuses;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmChangeSpaceForFilesResponse)) {
            return false;
        }
        SrmChangeSpaceForFilesResponse other = (SrmChangeSpaceForFilesResponse) obj;
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
            ((this.requestToken==null && other.getRequestToken()==null) || 
             (this.requestToken!=null &&
              this.requestToken.equals(other.getRequestToken()))) &&
            ((this.estimatedProcessingTime==null && other.getEstimatedProcessingTime()==null) || 
             (this.estimatedProcessingTime!=null &&
              this.estimatedProcessingTime.equals(other.getEstimatedProcessingTime()))) &&
            ((this.arrayOfFileStatuses==null && other.getArrayOfFileStatuses()==null) || 
             (this.arrayOfFileStatuses!=null &&
              this.arrayOfFileStatuses.equals(other.getArrayOfFileStatuses())));
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
        if (getRequestToken() != null) {
            _hashCode += getRequestToken().hashCode();
        }
        if (getEstimatedProcessingTime() != null) {
            _hashCode += getEstimatedProcessingTime().hashCode();
        }
        if (getArrayOfFileStatuses() != null) {
            _hashCode += getArrayOfFileStatuses().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmChangeSpaceForFilesResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeSpaceForFilesResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("returnStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("", "returnStatus"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("estimatedProcessingTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "estimatedProcessingTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfFileStatuses");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfFileStatuses"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLReturnStatus"));
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

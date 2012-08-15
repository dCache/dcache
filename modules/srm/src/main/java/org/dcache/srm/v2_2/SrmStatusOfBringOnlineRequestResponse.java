/**
 * SrmStatusOfBringOnlineRequestResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmStatusOfBringOnlineRequestResponse  implements java.io.Serializable {
    private static final long serialVersionUID = -6624496540270878293L;
    private org.dcache.srm.v2_2.TReturnStatus returnStatus;

    private org.dcache.srm.v2_2.ArrayOfTBringOnlineRequestFileStatus arrayOfFileStatuses;

    private java.lang.Integer remainingTotalRequestTime;

    private java.lang.Integer remainingDeferredStartTime;

    public SrmStatusOfBringOnlineRequestResponse() {
    }

    public SrmStatusOfBringOnlineRequestResponse(
           org.dcache.srm.v2_2.TReturnStatus returnStatus,
           org.dcache.srm.v2_2.ArrayOfTBringOnlineRequestFileStatus arrayOfFileStatuses,
           java.lang.Integer remainingTotalRequestTime,
           java.lang.Integer remainingDeferredStartTime) {
           this.returnStatus = returnStatus;
           this.arrayOfFileStatuses = arrayOfFileStatuses;
           this.remainingTotalRequestTime = remainingTotalRequestTime;
           this.remainingDeferredStartTime = remainingDeferredStartTime;
    }


    /**
     * Gets the returnStatus value for this SrmStatusOfBringOnlineRequestResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_2.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmStatusOfBringOnlineRequestResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_2.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }


    /**
     * Gets the arrayOfFileStatuses value for this SrmStatusOfBringOnlineRequestResponse.
     * 
     * @return arrayOfFileStatuses
     */
    public org.dcache.srm.v2_2.ArrayOfTBringOnlineRequestFileStatus getArrayOfFileStatuses() {
        return arrayOfFileStatuses;
    }


    /**
     * Sets the arrayOfFileStatuses value for this SrmStatusOfBringOnlineRequestResponse.
     * 
     * @param arrayOfFileStatuses
     */
    public void setArrayOfFileStatuses(org.dcache.srm.v2_2.ArrayOfTBringOnlineRequestFileStatus arrayOfFileStatuses) {
        this.arrayOfFileStatuses = arrayOfFileStatuses;
    }


    /**
     * Gets the remainingTotalRequestTime value for this SrmStatusOfBringOnlineRequestResponse.
     * 
     * @return remainingTotalRequestTime
     */
    public java.lang.Integer getRemainingTotalRequestTime() {
        return remainingTotalRequestTime;
    }


    /**
     * Sets the remainingTotalRequestTime value for this SrmStatusOfBringOnlineRequestResponse.
     * 
     * @param remainingTotalRequestTime
     */
    public void setRemainingTotalRequestTime(java.lang.Integer remainingTotalRequestTime) {
        this.remainingTotalRequestTime = remainingTotalRequestTime;
    }


    /**
     * Gets the remainingDeferredStartTime value for this SrmStatusOfBringOnlineRequestResponse.
     * 
     * @return remainingDeferredStartTime
     */
    public java.lang.Integer getRemainingDeferredStartTime() {
        return remainingDeferredStartTime;
    }


    /**
     * Sets the remainingDeferredStartTime value for this SrmStatusOfBringOnlineRequestResponse.
     * 
     * @param remainingDeferredStartTime
     */
    public void setRemainingDeferredStartTime(java.lang.Integer remainingDeferredStartTime) {
        this.remainingDeferredStartTime = remainingDeferredStartTime;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmStatusOfBringOnlineRequestResponse)) {
            return false;
        }
        SrmStatusOfBringOnlineRequestResponse other = (SrmStatusOfBringOnlineRequestResponse) obj;
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
            ((this.arrayOfFileStatuses==null && other.getArrayOfFileStatuses()==null) || 
             (this.arrayOfFileStatuses!=null &&
              this.arrayOfFileStatuses.equals(other.getArrayOfFileStatuses()))) &&
            ((this.remainingTotalRequestTime==null && other.getRemainingTotalRequestTime()==null) || 
             (this.remainingTotalRequestTime!=null &&
              this.remainingTotalRequestTime.equals(other.getRemainingTotalRequestTime()))) &&
            ((this.remainingDeferredStartTime==null && other.getRemainingDeferredStartTime()==null) || 
             (this.remainingDeferredStartTime!=null &&
              this.remainingDeferredStartTime.equals(other.getRemainingDeferredStartTime())));
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
        if (getArrayOfFileStatuses() != null) {
            _hashCode += getArrayOfFileStatuses().hashCode();
        }
        if (getRemainingTotalRequestTime() != null) {
            _hashCode += getRemainingTotalRequestTime().hashCode();
        }
        if (getRemainingDeferredStartTime() != null) {
            _hashCode += getRemainingDeferredStartTime().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmStatusOfBringOnlineRequestResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfBringOnlineRequestResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("returnStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("", "returnStatus"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfFileStatuses");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfFileStatuses"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTBringOnlineRequestFileStatus"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("remainingTotalRequestTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "remainingTotalRequestTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("remainingDeferredStartTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "remainingDeferredStartTime"));
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

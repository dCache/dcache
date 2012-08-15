/**
 * SrmStatusOfCopyRequestResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmStatusOfCopyRequestResponse  implements java.io.Serializable {
    private static final long serialVersionUID = -4077622181928322138L;
    private org.dcache.srm.v2_2.TReturnStatus returnStatus;

    private org.dcache.srm.v2_2.ArrayOfTCopyRequestFileStatus arrayOfFileStatuses;

    private java.lang.Integer remainingTotalRequestTime;

    public SrmStatusOfCopyRequestResponse() {
    }

    public SrmStatusOfCopyRequestResponse(
           org.dcache.srm.v2_2.TReturnStatus returnStatus,
           org.dcache.srm.v2_2.ArrayOfTCopyRequestFileStatus arrayOfFileStatuses,
           java.lang.Integer remainingTotalRequestTime) {
           this.returnStatus = returnStatus;
           this.arrayOfFileStatuses = arrayOfFileStatuses;
           this.remainingTotalRequestTime = remainingTotalRequestTime;
    }


    /**
     * Gets the returnStatus value for this SrmStatusOfCopyRequestResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_2.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmStatusOfCopyRequestResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_2.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }


    /**
     * Gets the arrayOfFileStatuses value for this SrmStatusOfCopyRequestResponse.
     * 
     * @return arrayOfFileStatuses
     */
    public org.dcache.srm.v2_2.ArrayOfTCopyRequestFileStatus getArrayOfFileStatuses() {
        return arrayOfFileStatuses;
    }


    /**
     * Sets the arrayOfFileStatuses value for this SrmStatusOfCopyRequestResponse.
     * 
     * @param arrayOfFileStatuses
     */
    public void setArrayOfFileStatuses(org.dcache.srm.v2_2.ArrayOfTCopyRequestFileStatus arrayOfFileStatuses) {
        this.arrayOfFileStatuses = arrayOfFileStatuses;
    }


    /**
     * Gets the remainingTotalRequestTime value for this SrmStatusOfCopyRequestResponse.
     * 
     * @return remainingTotalRequestTime
     */
    public java.lang.Integer getRemainingTotalRequestTime() {
        return remainingTotalRequestTime;
    }


    /**
     * Sets the remainingTotalRequestTime value for this SrmStatusOfCopyRequestResponse.
     * 
     * @param remainingTotalRequestTime
     */
    public void setRemainingTotalRequestTime(java.lang.Integer remainingTotalRequestTime) {
        this.remainingTotalRequestTime = remainingTotalRequestTime;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmStatusOfCopyRequestResponse)) {
            return false;
        }
        SrmStatusOfCopyRequestResponse other = (SrmStatusOfCopyRequestResponse) obj;
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
              this.remainingTotalRequestTime.equals(other.getRemainingTotalRequestTime())));
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
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmStatusOfCopyRequestResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequestResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("returnStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("", "returnStatus"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfFileStatuses");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfFileStatuses"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTCopyRequestFileStatus"));
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

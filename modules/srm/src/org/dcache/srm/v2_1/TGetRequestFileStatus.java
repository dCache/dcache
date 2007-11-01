/**
 * TGetRequestFileStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class TGetRequestFileStatus  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TLifeTimeInSeconds estimatedProcessingTime;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds estimatedWaitTimeOnQueue;

    private org.dcache.srm.v2_1.TSizeInBytes fileSize;

    private org.dcache.srm.v2_1.TSURL fromSURLInfo;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds remainingPinTime;

    private org.dcache.srm.v2_1.TReturnStatus status;

    private org.dcache.srm.v2_1.TTURL transferURL;

    public TGetRequestFileStatus() {
    }

    public TGetRequestFileStatus(
           org.dcache.srm.v2_1.TLifeTimeInSeconds estimatedProcessingTime,
           org.dcache.srm.v2_1.TLifeTimeInSeconds estimatedWaitTimeOnQueue,
           org.dcache.srm.v2_1.TSizeInBytes fileSize,
           org.dcache.srm.v2_1.TSURL fromSURLInfo,
           org.dcache.srm.v2_1.TLifeTimeInSeconds remainingPinTime,
           org.dcache.srm.v2_1.TReturnStatus status,
           org.dcache.srm.v2_1.TTURL transferURL) {
           this.estimatedProcessingTime = estimatedProcessingTime;
           this.estimatedWaitTimeOnQueue = estimatedWaitTimeOnQueue;
           this.fileSize = fileSize;
           this.fromSURLInfo = fromSURLInfo;
           this.remainingPinTime = remainingPinTime;
           this.status = status;
           this.transferURL = transferURL;
    }


    /**
     * Gets the estimatedProcessingTime value for this TGetRequestFileStatus.
     * 
     * @return estimatedProcessingTime
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getEstimatedProcessingTime() {
        return estimatedProcessingTime;
    }


    /**
     * Sets the estimatedProcessingTime value for this TGetRequestFileStatus.
     * 
     * @param estimatedProcessingTime
     */
    public void setEstimatedProcessingTime(org.dcache.srm.v2_1.TLifeTimeInSeconds estimatedProcessingTime) {
        this.estimatedProcessingTime = estimatedProcessingTime;
    }


    /**
     * Gets the estimatedWaitTimeOnQueue value for this TGetRequestFileStatus.
     * 
     * @return estimatedWaitTimeOnQueue
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getEstimatedWaitTimeOnQueue() {
        return estimatedWaitTimeOnQueue;
    }


    /**
     * Sets the estimatedWaitTimeOnQueue value for this TGetRequestFileStatus.
     * 
     * @param estimatedWaitTimeOnQueue
     */
    public void setEstimatedWaitTimeOnQueue(org.dcache.srm.v2_1.TLifeTimeInSeconds estimatedWaitTimeOnQueue) {
        this.estimatedWaitTimeOnQueue = estimatedWaitTimeOnQueue;
    }


    /**
     * Gets the fileSize value for this TGetRequestFileStatus.
     * 
     * @return fileSize
     */
    public org.dcache.srm.v2_1.TSizeInBytes getFileSize() {
        return fileSize;
    }


    /**
     * Sets the fileSize value for this TGetRequestFileStatus.
     * 
     * @param fileSize
     */
    public void setFileSize(org.dcache.srm.v2_1.TSizeInBytes fileSize) {
        this.fileSize = fileSize;
    }


    /**
     * Gets the fromSURLInfo value for this TGetRequestFileStatus.
     * 
     * @return fromSURLInfo
     */
    public org.dcache.srm.v2_1.TSURL getFromSURLInfo() {
        return fromSURLInfo;
    }


    /**
     * Sets the fromSURLInfo value for this TGetRequestFileStatus.
     * 
     * @param fromSURLInfo
     */
    public void setFromSURLInfo(org.dcache.srm.v2_1.TSURL fromSURLInfo) {
        this.fromSURLInfo = fromSURLInfo;
    }


    /**
     * Gets the remainingPinTime value for this TGetRequestFileStatus.
     * 
     * @return remainingPinTime
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getRemainingPinTime() {
        return remainingPinTime;
    }


    /**
     * Sets the remainingPinTime value for this TGetRequestFileStatus.
     * 
     * @param remainingPinTime
     */
    public void setRemainingPinTime(org.dcache.srm.v2_1.TLifeTimeInSeconds remainingPinTime) {
        this.remainingPinTime = remainingPinTime;
    }


    /**
     * Gets the status value for this TGetRequestFileStatus.
     * 
     * @return status
     */
    public org.dcache.srm.v2_1.TReturnStatus getStatus() {
        return status;
    }


    /**
     * Sets the status value for this TGetRequestFileStatus.
     * 
     * @param status
     */
    public void setStatus(org.dcache.srm.v2_1.TReturnStatus status) {
        this.status = status;
    }


    /**
     * Gets the transferURL value for this TGetRequestFileStatus.
     * 
     * @return transferURL
     */
    public org.dcache.srm.v2_1.TTURL getTransferURL() {
        return transferURL;
    }


    /**
     * Sets the transferURL value for this TGetRequestFileStatus.
     * 
     * @param transferURL
     */
    public void setTransferURL(org.dcache.srm.v2_1.TTURL transferURL) {
        this.transferURL = transferURL;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TGetRequestFileStatus)) return false;
        TGetRequestFileStatus other = (TGetRequestFileStatus) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.estimatedProcessingTime==null && other.getEstimatedProcessingTime()==null) || 
             (this.estimatedProcessingTime!=null &&
              this.estimatedProcessingTime.equals(other.getEstimatedProcessingTime()))) &&
            ((this.estimatedWaitTimeOnQueue==null && other.getEstimatedWaitTimeOnQueue()==null) || 
             (this.estimatedWaitTimeOnQueue!=null &&
              this.estimatedWaitTimeOnQueue.equals(other.getEstimatedWaitTimeOnQueue()))) &&
            ((this.fileSize==null && other.getFileSize()==null) || 
             (this.fileSize!=null &&
              this.fileSize.equals(other.getFileSize()))) &&
            ((this.fromSURLInfo==null && other.getFromSURLInfo()==null) || 
             (this.fromSURLInfo!=null &&
              this.fromSURLInfo.equals(other.getFromSURLInfo()))) &&
            ((this.remainingPinTime==null && other.getRemainingPinTime()==null) || 
             (this.remainingPinTime!=null &&
              this.remainingPinTime.equals(other.getRemainingPinTime()))) &&
            ((this.status==null && other.getStatus()==null) || 
             (this.status!=null &&
              this.status.equals(other.getStatus()))) &&
            ((this.transferURL==null && other.getTransferURL()==null) || 
             (this.transferURL!=null &&
              this.transferURL.equals(other.getTransferURL())));
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
        if (getEstimatedProcessingTime() != null) {
            _hashCode += getEstimatedProcessingTime().hashCode();
        }
        if (getEstimatedWaitTimeOnQueue() != null) {
            _hashCode += getEstimatedWaitTimeOnQueue().hashCode();
        }
        if (getFileSize() != null) {
            _hashCode += getFileSize().hashCode();
        }
        if (getFromSURLInfo() != null) {
            _hashCode += getFromSURLInfo().hashCode();
        }
        if (getRemainingPinTime() != null) {
            _hashCode += getRemainingPinTime().hashCode();
        }
        if (getStatus() != null) {
            _hashCode += getStatus().hashCode();
        }
        if (getTransferURL() != null) {
            _hashCode += getTransferURL().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TGetRequestFileStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGetRequestFileStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("estimatedProcessingTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "estimatedProcessingTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("estimatedWaitTimeOnQueue");
        elemField.setXmlName(new javax.xml.namespace.QName("", "estimatedWaitTimeOnQueue"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fileSize");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fileSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fromSURLInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fromSURLInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURL"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("remainingPinTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "remainingPinTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("status");
        elemField.setXmlName(new javax.xml.namespace.QName("", "status"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("transferURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "transferURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TTURL"));
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

/**
 * TCopyRequestFileStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TCopyRequestFileStatus  implements java.io.Serializable {
    private static final long serialVersionUID = -6189594353297574940L;
    private org.apache.axis.types.URI sourceSURL;

    private org.apache.axis.types.URI targetSURL;

    private org.dcache.srm.v2_2.TReturnStatus status;

    private org.apache.axis.types.UnsignedLong fileSize;

    private java.lang.Integer estimatedWaitTime;

    private java.lang.Integer remainingFileLifetime;

    public TCopyRequestFileStatus() {
    }

    public TCopyRequestFileStatus(
           org.apache.axis.types.URI sourceSURL,
           org.apache.axis.types.URI targetSURL,
           org.dcache.srm.v2_2.TReturnStatus status,
           org.apache.axis.types.UnsignedLong fileSize,
           java.lang.Integer estimatedWaitTime,
           java.lang.Integer remainingFileLifetime) {
           this.sourceSURL = sourceSURL;
           this.targetSURL = targetSURL;
           this.status = status;
           this.fileSize = fileSize;
           this.estimatedWaitTime = estimatedWaitTime;
           this.remainingFileLifetime = remainingFileLifetime;
    }


    /**
     * Gets the sourceSURL value for this TCopyRequestFileStatus.
     * 
     * @return sourceSURL
     */
    public org.apache.axis.types.URI getSourceSURL() {
        return sourceSURL;
    }


    /**
     * Sets the sourceSURL value for this TCopyRequestFileStatus.
     * 
     * @param sourceSURL
     */
    public void setSourceSURL(org.apache.axis.types.URI sourceSURL) {
        this.sourceSURL = sourceSURL;
    }


    /**
     * Gets the targetSURL value for this TCopyRequestFileStatus.
     * 
     * @return targetSURL
     */
    public org.apache.axis.types.URI getTargetSURL() {
        return targetSURL;
    }


    /**
     * Sets the targetSURL value for this TCopyRequestFileStatus.
     * 
     * @param targetSURL
     */
    public void setTargetSURL(org.apache.axis.types.URI targetSURL) {
        this.targetSURL = targetSURL;
    }


    /**
     * Gets the status value for this TCopyRequestFileStatus.
     * 
     * @return status
     */
    public org.dcache.srm.v2_2.TReturnStatus getStatus() {
        return status;
    }


    /**
     * Sets the status value for this TCopyRequestFileStatus.
     * 
     * @param status
     */
    public void setStatus(org.dcache.srm.v2_2.TReturnStatus status) {
        this.status = status;
    }


    /**
     * Gets the fileSize value for this TCopyRequestFileStatus.
     * 
     * @return fileSize
     */
    public org.apache.axis.types.UnsignedLong getFileSize() {
        return fileSize;
    }


    /**
     * Sets the fileSize value for this TCopyRequestFileStatus.
     * 
     * @param fileSize
     */
    public void setFileSize(org.apache.axis.types.UnsignedLong fileSize) {
        this.fileSize = fileSize;
    }


    /**
     * Gets the estimatedWaitTime value for this TCopyRequestFileStatus.
     * 
     * @return estimatedWaitTime
     */
    public java.lang.Integer getEstimatedWaitTime() {
        return estimatedWaitTime;
    }


    /**
     * Sets the estimatedWaitTime value for this TCopyRequestFileStatus.
     * 
     * @param estimatedWaitTime
     */
    public void setEstimatedWaitTime(java.lang.Integer estimatedWaitTime) {
        this.estimatedWaitTime = estimatedWaitTime;
    }


    /**
     * Gets the remainingFileLifetime value for this TCopyRequestFileStatus.
     * 
     * @return remainingFileLifetime
     */
    public java.lang.Integer getRemainingFileLifetime() {
        return remainingFileLifetime;
    }


    /**
     * Sets the remainingFileLifetime value for this TCopyRequestFileStatus.
     * 
     * @param remainingFileLifetime
     */
    public void setRemainingFileLifetime(java.lang.Integer remainingFileLifetime) {
        this.remainingFileLifetime = remainingFileLifetime;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TCopyRequestFileStatus)) {
            return false;
        }
        TCopyRequestFileStatus other = (TCopyRequestFileStatus) obj;
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
            ((this.sourceSURL==null && other.getSourceSURL()==null) || 
             (this.sourceSURL!=null &&
              this.sourceSURL.equals(other.getSourceSURL()))) &&
            ((this.targetSURL==null && other.getTargetSURL()==null) || 
             (this.targetSURL!=null &&
              this.targetSURL.equals(other.getTargetSURL()))) &&
            ((this.status==null && other.getStatus()==null) || 
             (this.status!=null &&
              this.status.equals(other.getStatus()))) &&
            ((this.fileSize==null && other.getFileSize()==null) || 
             (this.fileSize!=null &&
              this.fileSize.equals(other.getFileSize()))) &&
            ((this.estimatedWaitTime==null && other.getEstimatedWaitTime()==null) || 
             (this.estimatedWaitTime!=null &&
              this.estimatedWaitTime.equals(other.getEstimatedWaitTime()))) &&
            ((this.remainingFileLifetime==null && other.getRemainingFileLifetime()==null) || 
             (this.remainingFileLifetime!=null &&
              this.remainingFileLifetime.equals(other.getRemainingFileLifetime())));
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
        if (getSourceSURL() != null) {
            _hashCode += getSourceSURL().hashCode();
        }
        if (getTargetSURL() != null) {
            _hashCode += getTargetSURL().hashCode();
        }
        if (getStatus() != null) {
            _hashCode += getStatus().hashCode();
        }
        if (getFileSize() != null) {
            _hashCode += getFileSize().hashCode();
        }
        if (getEstimatedWaitTime() != null) {
            _hashCode += getEstimatedWaitTime().hashCode();
        }
        if (getRemainingFileLifetime() != null) {
            _hashCode += getRemainingFileLifetime().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TCopyRequestFileStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCopyRequestFileStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sourceSURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sourceSURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("targetSURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "targetSURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("status");
        elemField.setXmlName(new javax.xml.namespace.QName("", "status"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fileSize");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fileSize"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("estimatedWaitTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "estimatedWaitTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("remainingFileLifetime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "remainingFileLifetime"));
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

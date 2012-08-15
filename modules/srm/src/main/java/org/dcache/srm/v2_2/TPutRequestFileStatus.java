/**
 * TPutRequestFileStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TPutRequestFileStatus  implements java.io.Serializable {
    private static final long serialVersionUID = -5541703327759455446L;
    private org.apache.axis.types.URI SURL;

    private org.dcache.srm.v2_2.TReturnStatus status;

    private org.apache.axis.types.UnsignedLong fileSize;

    private java.lang.Integer estimatedWaitTime;

    private java.lang.Integer remainingPinLifetime;

    private java.lang.Integer remainingFileLifetime;

    private org.apache.axis.types.URI transferURL;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo transferProtocolInfo;

    public TPutRequestFileStatus() {
    }

    public TPutRequestFileStatus(
           org.apache.axis.types.URI SURL,
           org.dcache.srm.v2_2.TReturnStatus status,
           org.apache.axis.types.UnsignedLong fileSize,
           java.lang.Integer estimatedWaitTime,
           java.lang.Integer remainingPinLifetime,
           java.lang.Integer remainingFileLifetime,
           org.apache.axis.types.URI transferURL,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo transferProtocolInfo) {
           this.SURL = SURL;
           this.status = status;
           this.fileSize = fileSize;
           this.estimatedWaitTime = estimatedWaitTime;
           this.remainingPinLifetime = remainingPinLifetime;
           this.remainingFileLifetime = remainingFileLifetime;
           this.transferURL = transferURL;
           this.transferProtocolInfo = transferProtocolInfo;
    }


    /**
     * Gets the SURL value for this TPutRequestFileStatus.
     * 
     * @return SURL
     */
    public org.apache.axis.types.URI getSURL() {
        return SURL;
    }


    /**
     * Sets the SURL value for this TPutRequestFileStatus.
     * 
     * @param SURL
     */
    public void setSURL(org.apache.axis.types.URI SURL) {
        this.SURL = SURL;
    }


    /**
     * Gets the status value for this TPutRequestFileStatus.
     * 
     * @return status
     */
    public org.dcache.srm.v2_2.TReturnStatus getStatus() {
        return status;
    }


    /**
     * Sets the status value for this TPutRequestFileStatus.
     * 
     * @param status
     */
    public void setStatus(org.dcache.srm.v2_2.TReturnStatus status) {
        this.status = status;
    }


    /**
     * Gets the fileSize value for this TPutRequestFileStatus.
     * 
     * @return fileSize
     */
    public org.apache.axis.types.UnsignedLong getFileSize() {
        return fileSize;
    }


    /**
     * Sets the fileSize value for this TPutRequestFileStatus.
     * 
     * @param fileSize
     */
    public void setFileSize(org.apache.axis.types.UnsignedLong fileSize) {
        this.fileSize = fileSize;
    }


    /**
     * Gets the estimatedWaitTime value for this TPutRequestFileStatus.
     * 
     * @return estimatedWaitTime
     */
    public java.lang.Integer getEstimatedWaitTime() {
        return estimatedWaitTime;
    }


    /**
     * Sets the estimatedWaitTime value for this TPutRequestFileStatus.
     * 
     * @param estimatedWaitTime
     */
    public void setEstimatedWaitTime(java.lang.Integer estimatedWaitTime) {
        this.estimatedWaitTime = estimatedWaitTime;
    }


    /**
     * Gets the remainingPinLifetime value for this TPutRequestFileStatus.
     * 
     * @return remainingPinLifetime
     */
    public java.lang.Integer getRemainingPinLifetime() {
        return remainingPinLifetime;
    }


    /**
     * Sets the remainingPinLifetime value for this TPutRequestFileStatus.
     * 
     * @param remainingPinLifetime
     */
    public void setRemainingPinLifetime(java.lang.Integer remainingPinLifetime) {
        this.remainingPinLifetime = remainingPinLifetime;
    }


    /**
     * Gets the remainingFileLifetime value for this TPutRequestFileStatus.
     * 
     * @return remainingFileLifetime
     */
    public java.lang.Integer getRemainingFileLifetime() {
        return remainingFileLifetime;
    }


    /**
     * Sets the remainingFileLifetime value for this TPutRequestFileStatus.
     * 
     * @param remainingFileLifetime
     */
    public void setRemainingFileLifetime(java.lang.Integer remainingFileLifetime) {
        this.remainingFileLifetime = remainingFileLifetime;
    }


    /**
     * Gets the transferURL value for this TPutRequestFileStatus.
     * 
     * @return transferURL
     */
    public org.apache.axis.types.URI getTransferURL() {
        return transferURL;
    }


    /**
     * Sets the transferURL value for this TPutRequestFileStatus.
     * 
     * @param transferURL
     */
    public void setTransferURL(org.apache.axis.types.URI transferURL) {
        this.transferURL = transferURL;
    }


    /**
     * Gets the transferProtocolInfo value for this TPutRequestFileStatus.
     * 
     * @return transferProtocolInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getTransferProtocolInfo() {
        return transferProtocolInfo;
    }


    /**
     * Sets the transferProtocolInfo value for this TPutRequestFileStatus.
     * 
     * @param transferProtocolInfo
     */
    public void setTransferProtocolInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo transferProtocolInfo) {
        this.transferProtocolInfo = transferProtocolInfo;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TPutRequestFileStatus)) {
            return false;
        }
        TPutRequestFileStatus other = (TPutRequestFileStatus) obj;
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
            ((this.SURL==null && other.getSURL()==null) || 
             (this.SURL!=null &&
              this.SURL.equals(other.getSURL()))) &&
            ((this.status==null && other.getStatus()==null) || 
             (this.status!=null &&
              this.status.equals(other.getStatus()))) &&
            ((this.fileSize==null && other.getFileSize()==null) || 
             (this.fileSize!=null &&
              this.fileSize.equals(other.getFileSize()))) &&
            ((this.estimatedWaitTime==null && other.getEstimatedWaitTime()==null) || 
             (this.estimatedWaitTime!=null &&
              this.estimatedWaitTime.equals(other.getEstimatedWaitTime()))) &&
            ((this.remainingPinLifetime==null && other.getRemainingPinLifetime()==null) || 
             (this.remainingPinLifetime!=null &&
              this.remainingPinLifetime.equals(other.getRemainingPinLifetime()))) &&
            ((this.remainingFileLifetime==null && other.getRemainingFileLifetime()==null) || 
             (this.remainingFileLifetime!=null &&
              this.remainingFileLifetime.equals(other.getRemainingFileLifetime()))) &&
            ((this.transferURL==null && other.getTransferURL()==null) || 
             (this.transferURL!=null &&
              this.transferURL.equals(other.getTransferURL()))) &&
            ((this.transferProtocolInfo==null && other.getTransferProtocolInfo()==null) || 
             (this.transferProtocolInfo!=null &&
              this.transferProtocolInfo.equals(other.getTransferProtocolInfo())));
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
        if (getSURL() != null) {
            _hashCode += getSURL().hashCode();
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
        if (getRemainingPinLifetime() != null) {
            _hashCode += getRemainingPinLifetime().hashCode();
        }
        if (getRemainingFileLifetime() != null) {
            _hashCode += getRemainingFileLifetime().hashCode();
        }
        if (getTransferURL() != null) {
            _hashCode += getTransferURL().hashCode();
        }
        if (getTransferProtocolInfo() != null) {
            _hashCode += getTransferProtocolInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TPutRequestFileStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPutRequestFileStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("SURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "SURL"));
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
        elemField.setFieldName("remainingPinLifetime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "remainingPinLifetime"));
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
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("transferURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "transferURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("transferProtocolInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "transferProtocolInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTExtraInfo"));
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

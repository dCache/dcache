/**
 * TRequestSummary.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TRequestSummary  implements java.io.Serializable {
    private static final long serialVersionUID = 7000145578008790221L;
    private java.lang.String requestToken;

    private org.dcache.srm.v2_2.TReturnStatus status;

    private org.dcache.srm.v2_2.TRequestType requestType;

    private java.lang.Integer totalNumFilesInRequest;

    private java.lang.Integer numOfCompletedFiles;

    private java.lang.Integer numOfWaitingFiles;

    private java.lang.Integer numOfFailedFiles;

    public TRequestSummary() {
    }

    public TRequestSummary(
           java.lang.String requestToken,
           org.dcache.srm.v2_2.TReturnStatus status,
           org.dcache.srm.v2_2.TRequestType requestType,
           java.lang.Integer totalNumFilesInRequest,
           java.lang.Integer numOfCompletedFiles,
           java.lang.Integer numOfWaitingFiles,
           java.lang.Integer numOfFailedFiles) {
           this.requestToken = requestToken;
           this.status = status;
           this.requestType = requestType;
           this.totalNumFilesInRequest = totalNumFilesInRequest;
           this.numOfCompletedFiles = numOfCompletedFiles;
           this.numOfWaitingFiles = numOfWaitingFiles;
           this.numOfFailedFiles = numOfFailedFiles;
    }


    /**
     * Gets the requestToken value for this TRequestSummary.
     * 
     * @return requestToken
     */
    public java.lang.String getRequestToken() {
        return requestToken;
    }


    /**
     * Sets the requestToken value for this TRequestSummary.
     * 
     * @param requestToken
     */
    public void setRequestToken(java.lang.String requestToken) {
        this.requestToken = requestToken;
    }


    /**
     * Gets the status value for this TRequestSummary.
     * 
     * @return status
     */
    public org.dcache.srm.v2_2.TReturnStatus getStatus() {
        return status;
    }


    /**
     * Sets the status value for this TRequestSummary.
     * 
     * @param status
     */
    public void setStatus(org.dcache.srm.v2_2.TReturnStatus status) {
        this.status = status;
    }


    /**
     * Gets the requestType value for this TRequestSummary.
     * 
     * @return requestType
     */
    public org.dcache.srm.v2_2.TRequestType getRequestType() {
        return requestType;
    }


    /**
     * Sets the requestType value for this TRequestSummary.
     * 
     * @param requestType
     */
    public void setRequestType(org.dcache.srm.v2_2.TRequestType requestType) {
        this.requestType = requestType;
    }


    /**
     * Gets the totalNumFilesInRequest value for this TRequestSummary.
     * 
     * @return totalNumFilesInRequest
     */
    public java.lang.Integer getTotalNumFilesInRequest() {
        return totalNumFilesInRequest;
    }


    /**
     * Sets the totalNumFilesInRequest value for this TRequestSummary.
     * 
     * @param totalNumFilesInRequest
     */
    public void setTotalNumFilesInRequest(java.lang.Integer totalNumFilesInRequest) {
        this.totalNumFilesInRequest = totalNumFilesInRequest;
    }


    /**
     * Gets the numOfCompletedFiles value for this TRequestSummary.
     * 
     * @return numOfCompletedFiles
     */
    public java.lang.Integer getNumOfCompletedFiles() {
        return numOfCompletedFiles;
    }


    /**
     * Sets the numOfCompletedFiles value for this TRequestSummary.
     * 
     * @param numOfCompletedFiles
     */
    public void setNumOfCompletedFiles(java.lang.Integer numOfCompletedFiles) {
        this.numOfCompletedFiles = numOfCompletedFiles;
    }


    /**
     * Gets the numOfWaitingFiles value for this TRequestSummary.
     * 
     * @return numOfWaitingFiles
     */
    public java.lang.Integer getNumOfWaitingFiles() {
        return numOfWaitingFiles;
    }


    /**
     * Sets the numOfWaitingFiles value for this TRequestSummary.
     * 
     * @param numOfWaitingFiles
     */
    public void setNumOfWaitingFiles(java.lang.Integer numOfWaitingFiles) {
        this.numOfWaitingFiles = numOfWaitingFiles;
    }


    /**
     * Gets the numOfFailedFiles value for this TRequestSummary.
     * 
     * @return numOfFailedFiles
     */
    public java.lang.Integer getNumOfFailedFiles() {
        return numOfFailedFiles;
    }


    /**
     * Sets the numOfFailedFiles value for this TRequestSummary.
     * 
     * @param numOfFailedFiles
     */
    public void setNumOfFailedFiles(java.lang.Integer numOfFailedFiles) {
        this.numOfFailedFiles = numOfFailedFiles;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TRequestSummary)) {
            return false;
        }
        TRequestSummary other = (TRequestSummary) obj;
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
            ((this.requestToken==null && other.getRequestToken()==null) || 
             (this.requestToken!=null &&
              this.requestToken.equals(other.getRequestToken()))) &&
            ((this.status==null && other.getStatus()==null) || 
             (this.status!=null &&
              this.status.equals(other.getStatus()))) &&
            ((this.requestType==null && other.getRequestType()==null) || 
             (this.requestType!=null &&
              this.requestType.equals(other.getRequestType()))) &&
            ((this.totalNumFilesInRequest==null && other.getTotalNumFilesInRequest()==null) || 
             (this.totalNumFilesInRequest!=null &&
              this.totalNumFilesInRequest.equals(other.getTotalNumFilesInRequest()))) &&
            ((this.numOfCompletedFiles==null && other.getNumOfCompletedFiles()==null) || 
             (this.numOfCompletedFiles!=null &&
              this.numOfCompletedFiles.equals(other.getNumOfCompletedFiles()))) &&
            ((this.numOfWaitingFiles==null && other.getNumOfWaitingFiles()==null) || 
             (this.numOfWaitingFiles!=null &&
              this.numOfWaitingFiles.equals(other.getNumOfWaitingFiles()))) &&
            ((this.numOfFailedFiles==null && other.getNumOfFailedFiles()==null) || 
             (this.numOfFailedFiles!=null &&
              this.numOfFailedFiles.equals(other.getNumOfFailedFiles())));
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
        if (getRequestToken() != null) {
            _hashCode += getRequestToken().hashCode();
        }
        if (getStatus() != null) {
            _hashCode += getStatus().hashCode();
        }
        if (getRequestType() != null) {
            _hashCode += getRequestType().hashCode();
        }
        if (getTotalNumFilesInRequest() != null) {
            _hashCode += getTotalNumFilesInRequest().hashCode();
        }
        if (getNumOfCompletedFiles() != null) {
            _hashCode += getNumOfCompletedFiles().hashCode();
        }
        if (getNumOfWaitingFiles() != null) {
            _hashCode += getNumOfWaitingFiles().hashCode();
        }
        if (getNumOfFailedFiles() != null) {
            _hashCode += getNumOfFailedFiles().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TRequestSummary.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestSummary"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("status");
        elemField.setXmlName(new javax.xml.namespace.QName("", "status"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("totalNumFilesInRequest");
        elemField.setXmlName(new javax.xml.namespace.QName("", "totalNumFilesInRequest"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("numOfCompletedFiles");
        elemField.setXmlName(new javax.xml.namespace.QName("", "numOfCompletedFiles"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("numOfWaitingFiles");
        elemField.setXmlName(new javax.xml.namespace.QName("", "numOfWaitingFiles"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("numOfFailedFiles");
        elemField.setXmlName(new javax.xml.namespace.QName("", "numOfFailedFiles"));
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

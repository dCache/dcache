/**
 * TRequestSummary.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class TRequestSummary  implements java.io.Serializable {
    private boolean isSuspended;

    private int numOfFinishedRequests;

    private int numOfProgressingRequests;

    private int numOfQueuedRequests;

    private org.dcache.srm.v2_1.TRequestToken requestToken;

    private org.dcache.srm.v2_1.TRequestType requestType;

    private int totalFilesInThisRequest;

    public TRequestSummary() {
    }

    public TRequestSummary(
           boolean isSuspended,
           int numOfFinishedRequests,
           int numOfProgressingRequests,
           int numOfQueuedRequests,
           org.dcache.srm.v2_1.TRequestToken requestToken,
           org.dcache.srm.v2_1.TRequestType requestType,
           int totalFilesInThisRequest) {
           this.isSuspended = isSuspended;
           this.numOfFinishedRequests = numOfFinishedRequests;
           this.numOfProgressingRequests = numOfProgressingRequests;
           this.numOfQueuedRequests = numOfQueuedRequests;
           this.requestToken = requestToken;
           this.requestType = requestType;
           this.totalFilesInThisRequest = totalFilesInThisRequest;
    }


    /**
     * Gets the isSuspended value for this TRequestSummary.
     * 
     * @return isSuspended
     */
    public boolean isIsSuspended() {
        return isSuspended;
    }


    /**
     * Sets the isSuspended value for this TRequestSummary.
     * 
     * @param isSuspended
     */
    public void setIsSuspended(boolean isSuspended) {
        this.isSuspended = isSuspended;
    }


    /**
     * Gets the numOfFinishedRequests value for this TRequestSummary.
     * 
     * @return numOfFinishedRequests
     */
    public int getNumOfFinishedRequests() {
        return numOfFinishedRequests;
    }


    /**
     * Sets the numOfFinishedRequests value for this TRequestSummary.
     * 
     * @param numOfFinishedRequests
     */
    public void setNumOfFinishedRequests(int numOfFinishedRequests) {
        this.numOfFinishedRequests = numOfFinishedRequests;
    }


    /**
     * Gets the numOfProgressingRequests value for this TRequestSummary.
     * 
     * @return numOfProgressingRequests
     */
    public int getNumOfProgressingRequests() {
        return numOfProgressingRequests;
    }


    /**
     * Sets the numOfProgressingRequests value for this TRequestSummary.
     * 
     * @param numOfProgressingRequests
     */
    public void setNumOfProgressingRequests(int numOfProgressingRequests) {
        this.numOfProgressingRequests = numOfProgressingRequests;
    }


    /**
     * Gets the numOfQueuedRequests value for this TRequestSummary.
     * 
     * @return numOfQueuedRequests
     */
    public int getNumOfQueuedRequests() {
        return numOfQueuedRequests;
    }


    /**
     * Sets the numOfQueuedRequests value for this TRequestSummary.
     * 
     * @param numOfQueuedRequests
     */
    public void setNumOfQueuedRequests(int numOfQueuedRequests) {
        this.numOfQueuedRequests = numOfQueuedRequests;
    }


    /**
     * Gets the requestToken value for this TRequestSummary.
     * 
     * @return requestToken
     */
    public org.dcache.srm.v2_1.TRequestToken getRequestToken() {
        return requestToken;
    }


    /**
     * Sets the requestToken value for this TRequestSummary.
     * 
     * @param requestToken
     */
    public void setRequestToken(org.dcache.srm.v2_1.TRequestToken requestToken) {
        this.requestToken = requestToken;
    }


    /**
     * Gets the requestType value for this TRequestSummary.
     * 
     * @return requestType
     */
    public org.dcache.srm.v2_1.TRequestType getRequestType() {
        return requestType;
    }


    /**
     * Sets the requestType value for this TRequestSummary.
     * 
     * @param requestType
     */
    public void setRequestType(org.dcache.srm.v2_1.TRequestType requestType) {
        this.requestType = requestType;
    }


    /**
     * Gets the totalFilesInThisRequest value for this TRequestSummary.
     * 
     * @return totalFilesInThisRequest
     */
    public int getTotalFilesInThisRequest() {
        return totalFilesInThisRequest;
    }


    /**
     * Sets the totalFilesInThisRequest value for this TRequestSummary.
     * 
     * @param totalFilesInThisRequest
     */
    public void setTotalFilesInThisRequest(int totalFilesInThisRequest) {
        this.totalFilesInThisRequest = totalFilesInThisRequest;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TRequestSummary)) return false;
        TRequestSummary other = (TRequestSummary) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            this.isSuspended == other.isIsSuspended() &&
            this.numOfFinishedRequests == other.getNumOfFinishedRequests() &&
            this.numOfProgressingRequests == other.getNumOfProgressingRequests() &&
            this.numOfQueuedRequests == other.getNumOfQueuedRequests() &&
            ((this.requestToken==null && other.getRequestToken()==null) || 
             (this.requestToken!=null &&
              this.requestToken.equals(other.getRequestToken()))) &&
            ((this.requestType==null && other.getRequestType()==null) || 
             (this.requestType!=null &&
              this.requestType.equals(other.getRequestType()))) &&
            this.totalFilesInThisRequest == other.getTotalFilesInThisRequest();
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
        _hashCode += (isIsSuspended() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        _hashCode += getNumOfFinishedRequests();
        _hashCode += getNumOfProgressingRequests();
        _hashCode += getNumOfQueuedRequests();
        if (getRequestToken() != null) {
            _hashCode += getRequestToken().hashCode();
        }
        if (getRequestType() != null) {
            _hashCode += getRequestType().hashCode();
        }
        _hashCode += getTotalFilesInThisRequest();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TRequestSummary.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestSummary"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("isSuspended");
        elemField.setXmlName(new javax.xml.namespace.QName("", "isSuspended"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("numOfFinishedRequests");
        elemField.setXmlName(new javax.xml.namespace.QName("", "numOfFinishedRequests"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("numOfProgressingRequests");
        elemField.setXmlName(new javax.xml.namespace.QName("", "numOfProgressingRequests"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("numOfQueuedRequests");
        elemField.setXmlName(new javax.xml.namespace.QName("", "numOfQueuedRequests"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestToken"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("totalFilesInThisRequest");
        elemField.setXmlName(new javax.xml.namespace.QName("", "totalFilesInThisRequest"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
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

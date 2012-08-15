/**
 * RequestStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.client.axis;

public class RequestStatus  implements java.io.Serializable {
    private static final long serialVersionUID = 6807473468454397410L;
    private int requestId;

    private java.lang.String type;

    private java.lang.String state;

    private java.util.Calendar submitTime;

    private java.util.Calendar startTime;

    private java.util.Calendar finishTime;

    private int estTimeToStart;

    private org.dcache.srm.client.axis.RequestFileStatus[] fileStatuses;

    private java.lang.String errorMessage;

    private int retryDeltaTime;

    public RequestStatus() {
    }

    public RequestStatus(
           int requestId,
           java.lang.String type,
           java.lang.String state,
           java.util.Calendar submitTime,
           java.util.Calendar startTime,
           java.util.Calendar finishTime,
           int estTimeToStart,
           org.dcache.srm.client.axis.RequestFileStatus[] fileStatuses,
           java.lang.String errorMessage,
           int retryDeltaTime) {
           this.requestId = requestId;
           this.type = type;
           this.state = state;
           this.submitTime = submitTime;
           this.startTime = startTime;
           this.finishTime = finishTime;
           this.estTimeToStart = estTimeToStart;
           this.fileStatuses = fileStatuses;
           this.errorMessage = errorMessage;
           this.retryDeltaTime = retryDeltaTime;
    }


    /**
     * Gets the requestId value for this RequestStatus.
     * 
     * @return requestId
     */
    public int getRequestId() {
        return requestId;
    }


    /**
     * Sets the requestId value for this RequestStatus.
     * 
     * @param requestId
     */
    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }


    /**
     * Gets the type value for this RequestStatus.
     * 
     * @return type
     */
    public java.lang.String getType() {
        return type;
    }


    /**
     * Sets the type value for this RequestStatus.
     * 
     * @param type
     */
    public void setType(java.lang.String type) {
        this.type = type;
    }


    /**
     * Gets the state value for this RequestStatus.
     * 
     * @return state
     */
    public java.lang.String getState() {
        return state;
    }


    /**
     * Sets the state value for this RequestStatus.
     * 
     * @param state
     */
    public void setState(java.lang.String state) {
        this.state = state;
    }


    /**
     * Gets the submitTime value for this RequestStatus.
     * 
     * @return submitTime
     */
    public java.util.Calendar getSubmitTime() {
        return submitTime;
    }


    /**
     * Sets the submitTime value for this RequestStatus.
     * 
     * @param submitTime
     */
    public void setSubmitTime(java.util.Calendar submitTime) {
        this.submitTime = submitTime;
    }


    /**
     * Gets the startTime value for this RequestStatus.
     * 
     * @return startTime
     */
    public java.util.Calendar getStartTime() {
        return startTime;
    }


    /**
     * Sets the startTime value for this RequestStatus.
     * 
     * @param startTime
     */
    public void setStartTime(java.util.Calendar startTime) {
        this.startTime = startTime;
    }


    /**
     * Gets the finishTime value for this RequestStatus.
     * 
     * @return finishTime
     */
    public java.util.Calendar getFinishTime() {
        return finishTime;
    }


    /**
     * Sets the finishTime value for this RequestStatus.
     * 
     * @param finishTime
     */
    public void setFinishTime(java.util.Calendar finishTime) {
        this.finishTime = finishTime;
    }


    /**
     * Gets the estTimeToStart value for this RequestStatus.
     * 
     * @return estTimeToStart
     */
    public int getEstTimeToStart() {
        return estTimeToStart;
    }


    /**
     * Sets the estTimeToStart value for this RequestStatus.
     * 
     * @param estTimeToStart
     */
    public void setEstTimeToStart(int estTimeToStart) {
        this.estTimeToStart = estTimeToStart;
    }


    /**
     * Gets the fileStatuses value for this RequestStatus.
     * 
     * @return fileStatuses
     */
    public org.dcache.srm.client.axis.RequestFileStatus[] getFileStatuses() {
        return fileStatuses;
    }


    /**
     * Sets the fileStatuses value for this RequestStatus.
     * 
     * @param fileStatuses
     */
    public void setFileStatuses(org.dcache.srm.client.axis.RequestFileStatus[] fileStatuses) {
        this.fileStatuses = fileStatuses;
    }


    /**
     * Gets the errorMessage value for this RequestStatus.
     * 
     * @return errorMessage
     */
    public java.lang.String getErrorMessage() {
        return errorMessage;
    }


    /**
     * Sets the errorMessage value for this RequestStatus.
     * 
     * @param errorMessage
     */
    public void setErrorMessage(java.lang.String errorMessage) {
        this.errorMessage = errorMessage;
    }


    /**
     * Gets the retryDeltaTime value for this RequestStatus.
     * 
     * @return retryDeltaTime
     */
    public int getRetryDeltaTime() {
        return retryDeltaTime;
    }


    /**
     * Sets the retryDeltaTime value for this RequestStatus.
     * 
     * @param retryDeltaTime
     */
    public void setRetryDeltaTime(int retryDeltaTime) {
        this.retryDeltaTime = retryDeltaTime;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof RequestStatus)) {
            return false;
        }
        RequestStatus other = (RequestStatus) obj;
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
            this.requestId == other.getRequestId() &&
            ((this.type==null && other.getType()==null) || 
             (this.type!=null &&
              this.type.equals(other.getType()))) &&
            ((this.state==null && other.getState()==null) || 
             (this.state!=null &&
              this.state.equals(other.getState()))) &&
            ((this.submitTime==null && other.getSubmitTime()==null) || 
             (this.submitTime!=null &&
              this.submitTime.equals(other.getSubmitTime()))) &&
            ((this.startTime==null && other.getStartTime()==null) || 
             (this.startTime!=null &&
              this.startTime.equals(other.getStartTime()))) &&
            ((this.finishTime==null && other.getFinishTime()==null) || 
             (this.finishTime!=null &&
              this.finishTime.equals(other.getFinishTime()))) &&
            this.estTimeToStart == other.getEstTimeToStart() &&
            ((this.fileStatuses==null && other.getFileStatuses()==null) || 
             (this.fileStatuses!=null &&
              java.util.Arrays.equals(this.fileStatuses, other.getFileStatuses()))) &&
            ((this.errorMessage==null && other.getErrorMessage()==null) || 
             (this.errorMessage!=null &&
              this.errorMessage.equals(other.getErrorMessage()))) &&
            this.retryDeltaTime == other.getRetryDeltaTime();
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
        _hashCode += getRequestId();
        if (getType() != null) {
            _hashCode += getType().hashCode();
        }
        if (getState() != null) {
            _hashCode += getState().hashCode();
        }
        if (getSubmitTime() != null) {
            _hashCode += getSubmitTime().hashCode();
        }
        if (getStartTime() != null) {
            _hashCode += getStartTime().hashCode();
        }
        if (getFinishTime() != null) {
            _hashCode += getFinishTime().hashCode();
        }
        _hashCode += getEstTimeToStart();
        if (getFileStatuses() != null) {
            for (int i=0;
                 i<java.lang.reflect.Array.getLength(getFileStatuses());
                 i++) {
                java.lang.Object obj = java.lang.reflect.Array.get(getFileStatuses(), i);
                if (obj != null &&
                    !obj.getClass().isArray()) {
                    _hashCode += obj.hashCode();
                }
            }
        }
        if (getErrorMessage() != null) {
            _hashCode += getErrorMessage().hashCode();
        }
        _hashCode += getRetryDeltaTime();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(RequestStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://www.themindelectric.com/package/diskCacheV111.srm/", "RequestStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestId");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("type");
        elemField.setXmlName(new javax.xml.namespace.QName("", "type"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("state");
        elemField.setXmlName(new javax.xml.namespace.QName("", "state"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("submitTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "submitTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "dateTime"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("startTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "startTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "dateTime"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("finishTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "finishTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "dateTime"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("estTimeToStart");
        elemField.setXmlName(new javax.xml.namespace.QName("", "estTimeToStart"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fileStatuses");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fileStatuses"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.themindelectric.com/package/diskCacheV111.srm/", "RequestFileStatus"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("errorMessage");
        elemField.setXmlName(new javax.xml.namespace.QName("", "errorMessage"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("retryDeltaTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "retryDeltaTime"));
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

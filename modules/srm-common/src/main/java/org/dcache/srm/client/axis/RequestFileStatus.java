/**
 * RequestFileStatus.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.client.axis;

public class RequestFileStatus  extends org.dcache.srm.client.axis.FileMetaData  implements java.io.Serializable {
    private static final long serialVersionUID = 557616496634925886L;
    private java.lang.String state;

    private int fileId;

    private java.lang.String TURL;

    private int estSecondsToStart;

    private java.lang.String sourceFilename;

    private java.lang.String destFilename;

    private int queueOrder;

    public RequestFileStatus() {
    }

    public RequestFileStatus(
           java.lang.String SURL,
           long size,
           java.lang.String owner,
           java.lang.String group,
           int permMode,
           java.lang.String checksumType,
           java.lang.String checksumValue,
           boolean isPinned,
           boolean isPermanent,
           boolean isCached,
           java.lang.String state,
           int fileId,
           java.lang.String TURL,
           int estSecondsToStart,
           java.lang.String sourceFilename,
           java.lang.String destFilename,
           int queueOrder) {
        super(
            SURL,
            size,
            owner,
            group,
            permMode,
            checksumType,
            checksumValue,
            isPinned,
            isPermanent,
            isCached);
        this.state = state;
        this.fileId = fileId;
        this.TURL = TURL;
        this.estSecondsToStart = estSecondsToStart;
        this.sourceFilename = sourceFilename;
        this.destFilename = destFilename;
        this.queueOrder = queueOrder;
    }


    /**
     * Gets the state value for this RequestFileStatus.
     * 
     * @return state
     */
    public java.lang.String getState() {
        return state;
    }


    /**
     * Sets the state value for this RequestFileStatus.
     * 
     * @param state
     */
    public void setState(java.lang.String state) {
        this.state = state;
    }


    /**
     * Gets the fileId value for this RequestFileStatus.
     * 
     * @return fileId
     */
    public int getFileId() {
        return fileId;
    }


    /**
     * Sets the fileId value for this RequestFileStatus.
     * 
     * @param fileId
     */
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }


    /**
     * Gets the TURL value for this RequestFileStatus.
     * 
     * @return TURL
     */
    public java.lang.String getTURL() {
        return TURL;
    }


    /**
     * Sets the TURL value for this RequestFileStatus.
     * 
     * @param TURL
     */
    public void setTURL(java.lang.String TURL) {
        this.TURL = TURL;
    }


    /**
     * Gets the estSecondsToStart value for this RequestFileStatus.
     * 
     * @return estSecondsToStart
     */
    public int getEstSecondsToStart() {
        return estSecondsToStart;
    }


    /**
     * Sets the estSecondsToStart value for this RequestFileStatus.
     * 
     * @param estSecondsToStart
     */
    public void setEstSecondsToStart(int estSecondsToStart) {
        this.estSecondsToStart = estSecondsToStart;
    }


    /**
     * Gets the sourceFilename value for this RequestFileStatus.
     * 
     * @return sourceFilename
     */
    public java.lang.String getSourceFilename() {
        return sourceFilename;
    }


    /**
     * Sets the sourceFilename value for this RequestFileStatus.
     * 
     * @param sourceFilename
     */
    public void setSourceFilename(java.lang.String sourceFilename) {
        this.sourceFilename = sourceFilename;
    }


    /**
     * Gets the destFilename value for this RequestFileStatus.
     * 
     * @return destFilename
     */
    public java.lang.String getDestFilename() {
        return destFilename;
    }


    /**
     * Sets the destFilename value for this RequestFileStatus.
     * 
     * @param destFilename
     */
    public void setDestFilename(java.lang.String destFilename) {
        this.destFilename = destFilename;
    }


    /**
     * Gets the queueOrder value for this RequestFileStatus.
     * 
     * @return queueOrder
     */
    public int getQueueOrder() {
        return queueOrder;
    }


    /**
     * Sets the queueOrder value for this RequestFileStatus.
     * 
     * @param queueOrder
     */
    public void setQueueOrder(int queueOrder) {
        this.queueOrder = queueOrder;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof RequestFileStatus)) {
            return false;
        }
        RequestFileStatus other = (RequestFileStatus) obj;
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
        _equals = super.equals(obj) && 
            ((this.state==null && other.getState()==null) || 
             (this.state!=null &&
              this.state.equals(other.getState()))) &&
            this.fileId == other.getFileId() &&
            ((this.TURL==null && other.getTURL()==null) || 
             (this.TURL!=null &&
              this.TURL.equals(other.getTURL()))) &&
            this.estSecondsToStart == other.getEstSecondsToStart() &&
            ((this.sourceFilename==null && other.getSourceFilename()==null) || 
             (this.sourceFilename!=null &&
              this.sourceFilename.equals(other.getSourceFilename()))) &&
            ((this.destFilename==null && other.getDestFilename()==null) || 
             (this.destFilename!=null &&
              this.destFilename.equals(other.getDestFilename()))) &&
            this.queueOrder == other.getQueueOrder();
        __equalsCalc = null;
        return _equals;
    }

    private boolean __hashCodeCalc;
    public synchronized int hashCode() {
        if (__hashCodeCalc) {
            return 0;
        }
        __hashCodeCalc = true;
        int _hashCode = super.hashCode();
        if (getState() != null) {
            _hashCode += getState().hashCode();
        }
        _hashCode += getFileId();
        if (getTURL() != null) {
            _hashCode += getTURL().hashCode();
        }
        _hashCode += getEstSecondsToStart();
        if (getSourceFilename() != null) {
            _hashCode += getSourceFilename().hashCode();
        }
        if (getDestFilename() != null) {
            _hashCode += getDestFilename().hashCode();
        }
        _hashCode += getQueueOrder();
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(RequestFileStatus.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://www.themindelectric.com/package/diskCacheV111.srm/", "RequestFileStatus"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("state");
        elemField.setXmlName(new javax.xml.namespace.QName("", "state"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("fileId");
        elemField.setXmlName(new javax.xml.namespace.QName("", "fileId"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("TURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "TURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("estSecondsToStart");
        elemField.setXmlName(new javax.xml.namespace.QName("", "estSecondsToStart"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sourceFilename");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sourceFilename"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("destFilename");
        elemField.setXmlName(new javax.xml.namespace.QName("", "destFilename"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("queueOrder");
        elemField.setXmlName(new javax.xml.namespace.QName("", "queueOrder"));
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

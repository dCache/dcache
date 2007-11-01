/**
 * SrmPrepareToGetRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmPrepareToGetRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.ArrayOfTGetFileRequest arrayOfFileRequests;

    private org.dcache.srm.v2_1.ArrayOf_xsd_string arrayOfTransferProtocols;

    private java.lang.String userRequestDescription;

    private org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds totalRetryTime;

    public SrmPrepareToGetRequest() {
    }

    public SrmPrepareToGetRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.ArrayOfTGetFileRequest arrayOfFileRequests,
           org.dcache.srm.v2_1.ArrayOf_xsd_string arrayOfTransferProtocols,
           java.lang.String userRequestDescription,
           org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo,
           org.dcache.srm.v2_1.TLifeTimeInSeconds totalRetryTime) {
           this.userID = userID;
           this.arrayOfFileRequests = arrayOfFileRequests;
           this.arrayOfTransferProtocols = arrayOfTransferProtocols;
           this.userRequestDescription = userRequestDescription;
           this.storageSystemInfo = storageSystemInfo;
           this.totalRetryTime = totalRetryTime;
    }


    /**
     * Gets the userID value for this SrmPrepareToGetRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmPrepareToGetRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the arrayOfFileRequests value for this SrmPrepareToGetRequest.
     * 
     * @return arrayOfFileRequests
     */
    public org.dcache.srm.v2_1.ArrayOfTGetFileRequest getArrayOfFileRequests() {
        return arrayOfFileRequests;
    }


    /**
     * Sets the arrayOfFileRequests value for this SrmPrepareToGetRequest.
     * 
     * @param arrayOfFileRequests
     */
    public void setArrayOfFileRequests(org.dcache.srm.v2_1.ArrayOfTGetFileRequest arrayOfFileRequests) {
        this.arrayOfFileRequests = arrayOfFileRequests;
    }


    /**
     * Gets the arrayOfTransferProtocols value for this SrmPrepareToGetRequest.
     * 
     * @return arrayOfTransferProtocols
     */
    public org.dcache.srm.v2_1.ArrayOf_xsd_string getArrayOfTransferProtocols() {
        return arrayOfTransferProtocols;
    }


    /**
     * Sets the arrayOfTransferProtocols value for this SrmPrepareToGetRequest.
     * 
     * @param arrayOfTransferProtocols
     */
    public void setArrayOfTransferProtocols(org.dcache.srm.v2_1.ArrayOf_xsd_string arrayOfTransferProtocols) {
        this.arrayOfTransferProtocols = arrayOfTransferProtocols;
    }


    /**
     * Gets the userRequestDescription value for this SrmPrepareToGetRequest.
     * 
     * @return userRequestDescription
     */
    public java.lang.String getUserRequestDescription() {
        return userRequestDescription;
    }


    /**
     * Sets the userRequestDescription value for this SrmPrepareToGetRequest.
     * 
     * @param userRequestDescription
     */
    public void setUserRequestDescription(java.lang.String userRequestDescription) {
        this.userRequestDescription = userRequestDescription;
    }


    /**
     * Gets the storageSystemInfo value for this SrmPrepareToGetRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_1.TStorageSystemInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmPrepareToGetRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_1.TStorageSystemInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the totalRetryTime value for this SrmPrepareToGetRequest.
     * 
     * @return totalRetryTime
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getTotalRetryTime() {
        return totalRetryTime;
    }


    /**
     * Sets the totalRetryTime value for this SrmPrepareToGetRequest.
     * 
     * @param totalRetryTime
     */
    public void setTotalRetryTime(org.dcache.srm.v2_1.TLifeTimeInSeconds totalRetryTime) {
        this.totalRetryTime = totalRetryTime;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmPrepareToGetRequest)) return false;
        SrmPrepareToGetRequest other = (SrmPrepareToGetRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.userID==null && other.getUserID()==null) || 
             (this.userID!=null &&
              this.userID.equals(other.getUserID()))) &&
            ((this.arrayOfFileRequests==null && other.getArrayOfFileRequests()==null) || 
             (this.arrayOfFileRequests!=null &&
              this.arrayOfFileRequests.equals(other.getArrayOfFileRequests()))) &&
            ((this.arrayOfTransferProtocols==null && other.getArrayOfTransferProtocols()==null) || 
             (this.arrayOfTransferProtocols!=null &&
              this.arrayOfTransferProtocols.equals(other.getArrayOfTransferProtocols()))) &&
            ((this.userRequestDescription==null && other.getUserRequestDescription()==null) || 
             (this.userRequestDescription!=null &&
              this.userRequestDescription.equals(other.getUserRequestDescription()))) &&
            ((this.storageSystemInfo==null && other.getStorageSystemInfo()==null) || 
             (this.storageSystemInfo!=null &&
              this.storageSystemInfo.equals(other.getStorageSystemInfo()))) &&
            ((this.totalRetryTime==null && other.getTotalRetryTime()==null) || 
             (this.totalRetryTime!=null &&
              this.totalRetryTime.equals(other.getTotalRetryTime())));
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
        if (getUserID() != null) {
            _hashCode += getUserID().hashCode();
        }
        if (getArrayOfFileRequests() != null) {
            _hashCode += getArrayOfFileRequests().hashCode();
        }
        if (getArrayOfTransferProtocols() != null) {
            _hashCode += getArrayOfTransferProtocols().hashCode();
        }
        if (getUserRequestDescription() != null) {
            _hashCode += getUserRequestDescription().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        if (getTotalRetryTime() != null) {
            _hashCode += getTotalRetryTime().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmPrepareToGetRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGetRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfFileRequests");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfFileRequests"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGetFileRequest"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfTransferProtocols");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfTransferProtocols"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOf_xsd_string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userRequestDescription");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userRequestDescription"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("storageSystemInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "storageSystemInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TStorageSystemInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("totalRetryTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "totalRetryTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
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

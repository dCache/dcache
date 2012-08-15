/**
 * SrmPrepareToPutRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmPrepareToPutRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -7499676436685112271L;
    private java.lang.String authorizationID;

    private org.dcache.srm.v2_2.ArrayOfTPutFileRequest arrayOfFileRequests;

    private java.lang.String userRequestDescription;

    private org.dcache.srm.v2_2.TOverwriteMode overwriteOption;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo;

    private java.lang.Integer desiredTotalRequestTime;

    private java.lang.Integer desiredPinLifeTime;

    private java.lang.Integer desiredFileLifeTime;

    private org.dcache.srm.v2_2.TFileStorageType desiredFileStorageType;

    private java.lang.String targetSpaceToken;

    private org.dcache.srm.v2_2.TRetentionPolicyInfo targetFileRetentionPolicyInfo;

    private org.dcache.srm.v2_2.TTransferParameters transferParameters;

    public SrmPrepareToPutRequest() {
    }

    public SrmPrepareToPutRequest(
           java.lang.String authorizationID,
           org.dcache.srm.v2_2.ArrayOfTPutFileRequest arrayOfFileRequests,
           java.lang.String userRequestDescription,
           org.dcache.srm.v2_2.TOverwriteMode overwriteOption,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo,
           java.lang.Integer desiredTotalRequestTime,
           java.lang.Integer desiredPinLifeTime,
           java.lang.Integer desiredFileLifeTime,
           org.dcache.srm.v2_2.TFileStorageType desiredFileStorageType,
           java.lang.String targetSpaceToken,
           org.dcache.srm.v2_2.TRetentionPolicyInfo targetFileRetentionPolicyInfo,
           org.dcache.srm.v2_2.TTransferParameters transferParameters) {
           this.authorizationID = authorizationID;
           this.arrayOfFileRequests = arrayOfFileRequests;
           this.userRequestDescription = userRequestDescription;
           this.overwriteOption = overwriteOption;
           this.storageSystemInfo = storageSystemInfo;
           this.desiredTotalRequestTime = desiredTotalRequestTime;
           this.desiredPinLifeTime = desiredPinLifeTime;
           this.desiredFileLifeTime = desiredFileLifeTime;
           this.desiredFileStorageType = desiredFileStorageType;
           this.targetSpaceToken = targetSpaceToken;
           this.targetFileRetentionPolicyInfo = targetFileRetentionPolicyInfo;
           this.transferParameters = transferParameters;
    }


    /**
     * Gets the authorizationID value for this SrmPrepareToPutRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmPrepareToPutRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the arrayOfFileRequests value for this SrmPrepareToPutRequest.
     * 
     * @return arrayOfFileRequests
     */
    public org.dcache.srm.v2_2.ArrayOfTPutFileRequest getArrayOfFileRequests() {
        return arrayOfFileRequests;
    }


    /**
     * Sets the arrayOfFileRequests value for this SrmPrepareToPutRequest.
     * 
     * @param arrayOfFileRequests
     */
    public void setArrayOfFileRequests(org.dcache.srm.v2_2.ArrayOfTPutFileRequest arrayOfFileRequests) {
        this.arrayOfFileRequests = arrayOfFileRequests;
    }


    /**
     * Gets the userRequestDescription value for this SrmPrepareToPutRequest.
     * 
     * @return userRequestDescription
     */
    public java.lang.String getUserRequestDescription() {
        return userRequestDescription;
    }


    /**
     * Sets the userRequestDescription value for this SrmPrepareToPutRequest.
     * 
     * @param userRequestDescription
     */
    public void setUserRequestDescription(java.lang.String userRequestDescription) {
        this.userRequestDescription = userRequestDescription;
    }


    /**
     * Gets the overwriteOption value for this SrmPrepareToPutRequest.
     * 
     * @return overwriteOption
     */
    public org.dcache.srm.v2_2.TOverwriteMode getOverwriteOption() {
        return overwriteOption;
    }


    /**
     * Sets the overwriteOption value for this SrmPrepareToPutRequest.
     * 
     * @param overwriteOption
     */
    public void setOverwriteOption(org.dcache.srm.v2_2.TOverwriteMode overwriteOption) {
        this.overwriteOption = overwriteOption;
    }


    /**
     * Gets the storageSystemInfo value for this SrmPrepareToPutRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmPrepareToPutRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the desiredTotalRequestTime value for this SrmPrepareToPutRequest.
     * 
     * @return desiredTotalRequestTime
     */
    public java.lang.Integer getDesiredTotalRequestTime() {
        return desiredTotalRequestTime;
    }


    /**
     * Sets the desiredTotalRequestTime value for this SrmPrepareToPutRequest.
     * 
     * @param desiredTotalRequestTime
     */
    public void setDesiredTotalRequestTime(java.lang.Integer desiredTotalRequestTime) {
        this.desiredTotalRequestTime = desiredTotalRequestTime;
    }


    /**
     * Gets the desiredPinLifeTime value for this SrmPrepareToPutRequest.
     * 
     * @return desiredPinLifeTime
     */
    public java.lang.Integer getDesiredPinLifeTime() {
        return desiredPinLifeTime;
    }


    /**
     * Sets the desiredPinLifeTime value for this SrmPrepareToPutRequest.
     * 
     * @param desiredPinLifeTime
     */
    public void setDesiredPinLifeTime(java.lang.Integer desiredPinLifeTime) {
        this.desiredPinLifeTime = desiredPinLifeTime;
    }


    /**
     * Gets the desiredFileLifeTime value for this SrmPrepareToPutRequest.
     * 
     * @return desiredFileLifeTime
     */
    public java.lang.Integer getDesiredFileLifeTime() {
        return desiredFileLifeTime;
    }


    /**
     * Sets the desiredFileLifeTime value for this SrmPrepareToPutRequest.
     * 
     * @param desiredFileLifeTime
     */
    public void setDesiredFileLifeTime(java.lang.Integer desiredFileLifeTime) {
        this.desiredFileLifeTime = desiredFileLifeTime;
    }


    /**
     * Gets the desiredFileStorageType value for this SrmPrepareToPutRequest.
     * 
     * @return desiredFileStorageType
     */
    public org.dcache.srm.v2_2.TFileStorageType getDesiredFileStorageType() {
        return desiredFileStorageType;
    }


    /**
     * Sets the desiredFileStorageType value for this SrmPrepareToPutRequest.
     * 
     * @param desiredFileStorageType
     */
    public void setDesiredFileStorageType(org.dcache.srm.v2_2.TFileStorageType desiredFileStorageType) {
        this.desiredFileStorageType = desiredFileStorageType;
    }


    /**
     * Gets the targetSpaceToken value for this SrmPrepareToPutRequest.
     * 
     * @return targetSpaceToken
     */
    public java.lang.String getTargetSpaceToken() {
        return targetSpaceToken;
    }


    /**
     * Sets the targetSpaceToken value for this SrmPrepareToPutRequest.
     * 
     * @param targetSpaceToken
     */
    public void setTargetSpaceToken(java.lang.String targetSpaceToken) {
        this.targetSpaceToken = targetSpaceToken;
    }


    /**
     * Gets the targetFileRetentionPolicyInfo value for this SrmPrepareToPutRequest.
     * 
     * @return targetFileRetentionPolicyInfo
     */
    public org.dcache.srm.v2_2.TRetentionPolicyInfo getTargetFileRetentionPolicyInfo() {
        return targetFileRetentionPolicyInfo;
    }


    /**
     * Sets the targetFileRetentionPolicyInfo value for this SrmPrepareToPutRequest.
     * 
     * @param targetFileRetentionPolicyInfo
     */
    public void setTargetFileRetentionPolicyInfo(org.dcache.srm.v2_2.TRetentionPolicyInfo targetFileRetentionPolicyInfo) {
        this.targetFileRetentionPolicyInfo = targetFileRetentionPolicyInfo;
    }


    /**
     * Gets the transferParameters value for this SrmPrepareToPutRequest.
     * 
     * @return transferParameters
     */
    public org.dcache.srm.v2_2.TTransferParameters getTransferParameters() {
        return transferParameters;
    }


    /**
     * Sets the transferParameters value for this SrmPrepareToPutRequest.
     * 
     * @param transferParameters
     */
    public void setTransferParameters(org.dcache.srm.v2_2.TTransferParameters transferParameters) {
        this.transferParameters = transferParameters;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmPrepareToPutRequest)) {
            return false;
        }
        SrmPrepareToPutRequest other = (SrmPrepareToPutRequest) obj;
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
            ((this.authorizationID==null && other.getAuthorizationID()==null) || 
             (this.authorizationID!=null &&
              this.authorizationID.equals(other.getAuthorizationID()))) &&
            ((this.arrayOfFileRequests==null && other.getArrayOfFileRequests()==null) || 
             (this.arrayOfFileRequests!=null &&
              this.arrayOfFileRequests.equals(other.getArrayOfFileRequests()))) &&
            ((this.userRequestDescription==null && other.getUserRequestDescription()==null) || 
             (this.userRequestDescription!=null &&
              this.userRequestDescription.equals(other.getUserRequestDescription()))) &&
            ((this.overwriteOption==null && other.getOverwriteOption()==null) || 
             (this.overwriteOption!=null &&
              this.overwriteOption.equals(other.getOverwriteOption()))) &&
            ((this.storageSystemInfo==null && other.getStorageSystemInfo()==null) || 
             (this.storageSystemInfo!=null &&
              this.storageSystemInfo.equals(other.getStorageSystemInfo()))) &&
            ((this.desiredTotalRequestTime==null && other.getDesiredTotalRequestTime()==null) || 
             (this.desiredTotalRequestTime!=null &&
              this.desiredTotalRequestTime.equals(other.getDesiredTotalRequestTime()))) &&
            ((this.desiredPinLifeTime==null && other.getDesiredPinLifeTime()==null) || 
             (this.desiredPinLifeTime!=null &&
              this.desiredPinLifeTime.equals(other.getDesiredPinLifeTime()))) &&
            ((this.desiredFileLifeTime==null && other.getDesiredFileLifeTime()==null) || 
             (this.desiredFileLifeTime!=null &&
              this.desiredFileLifeTime.equals(other.getDesiredFileLifeTime()))) &&
            ((this.desiredFileStorageType==null && other.getDesiredFileStorageType()==null) || 
             (this.desiredFileStorageType!=null &&
              this.desiredFileStorageType.equals(other.getDesiredFileStorageType()))) &&
            ((this.targetSpaceToken==null && other.getTargetSpaceToken()==null) || 
             (this.targetSpaceToken!=null &&
              this.targetSpaceToken.equals(other.getTargetSpaceToken()))) &&
            ((this.targetFileRetentionPolicyInfo==null && other.getTargetFileRetentionPolicyInfo()==null) || 
             (this.targetFileRetentionPolicyInfo!=null &&
              this.targetFileRetentionPolicyInfo.equals(other.getTargetFileRetentionPolicyInfo()))) &&
            ((this.transferParameters==null && other.getTransferParameters()==null) || 
             (this.transferParameters!=null &&
              this.transferParameters.equals(other.getTransferParameters())));
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
        if (getAuthorizationID() != null) {
            _hashCode += getAuthorizationID().hashCode();
        }
        if (getArrayOfFileRequests() != null) {
            _hashCode += getArrayOfFileRequests().hashCode();
        }
        if (getUserRequestDescription() != null) {
            _hashCode += getUserRequestDescription().hashCode();
        }
        if (getOverwriteOption() != null) {
            _hashCode += getOverwriteOption().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        if (getDesiredTotalRequestTime() != null) {
            _hashCode += getDesiredTotalRequestTime().hashCode();
        }
        if (getDesiredPinLifeTime() != null) {
            _hashCode += getDesiredPinLifeTime().hashCode();
        }
        if (getDesiredFileLifeTime() != null) {
            _hashCode += getDesiredFileLifeTime().hashCode();
        }
        if (getDesiredFileStorageType() != null) {
            _hashCode += getDesiredFileStorageType().hashCode();
        }
        if (getTargetSpaceToken() != null) {
            _hashCode += getTargetSpaceToken().hashCode();
        }
        if (getTargetFileRetentionPolicyInfo() != null) {
            _hashCode += getTargetFileRetentionPolicyInfo().hashCode();
        }
        if (getTransferParameters() != null) {
            _hashCode += getTransferParameters().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmPrepareToPutRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPutRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfFileRequests");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfFileRequests"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTPutFileRequest"));
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
        elemField.setFieldName("overwriteOption");
        elemField.setXmlName(new javax.xml.namespace.QName("", "overwriteOption"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOverwriteMode"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("storageSystemInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "storageSystemInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTExtraInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("desiredTotalRequestTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredTotalRequestTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("desiredPinLifeTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredPinLifeTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("desiredFileLifeTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredFileLifeTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("desiredFileStorageType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredFileStorageType"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileStorageType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("targetSpaceToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "targetSpaceToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("targetFileRetentionPolicyInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "targetFileRetentionPolicyInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicyInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("transferParameters");
        elemField.setXmlName(new javax.xml.namespace.QName("", "transferParameters"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TTransferParameters"));
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

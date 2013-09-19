/**
 * SrmCopyRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmCopyRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -5187602915952139088L;
    private java.lang.String authorizationID;

    private org.dcache.srm.v2_2.ArrayOfTCopyFileRequest arrayOfFileRequests;

    private java.lang.String userRequestDescription;

    private org.dcache.srm.v2_2.TOverwriteMode overwriteOption;

    private java.lang.Integer desiredTotalRequestTime;

    private java.lang.Integer desiredTargetSURLLifeTime;

    private org.dcache.srm.v2_2.TFileStorageType targetFileStorageType;

    private java.lang.String targetSpaceToken;

    private org.dcache.srm.v2_2.TRetentionPolicyInfo targetFileRetentionPolicyInfo;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo sourceStorageSystemInfo;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo targetStorageSystemInfo;

    public SrmCopyRequest() {
    }

    public SrmCopyRequest(
           java.lang.String authorizationID,
           org.dcache.srm.v2_2.ArrayOfTCopyFileRequest arrayOfFileRequests,
           java.lang.String userRequestDescription,
           org.dcache.srm.v2_2.TOverwriteMode overwriteOption,
           java.lang.Integer desiredTotalRequestTime,
           java.lang.Integer desiredTargetSURLLifeTime,
           org.dcache.srm.v2_2.TFileStorageType targetFileStorageType,
           java.lang.String targetSpaceToken,
           org.dcache.srm.v2_2.TRetentionPolicyInfo targetFileRetentionPolicyInfo,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo sourceStorageSystemInfo,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo targetStorageSystemInfo) {
           this.authorizationID = authorizationID;
           this.arrayOfFileRequests = arrayOfFileRequests;
           this.userRequestDescription = userRequestDescription;
           this.overwriteOption = overwriteOption;
           this.desiredTotalRequestTime = desiredTotalRequestTime;
           this.desiredTargetSURLLifeTime = desiredTargetSURLLifeTime;
           this.targetFileStorageType = targetFileStorageType;
           this.targetSpaceToken = targetSpaceToken;
           this.targetFileRetentionPolicyInfo = targetFileRetentionPolicyInfo;
           this.sourceStorageSystemInfo = sourceStorageSystemInfo;
           this.targetStorageSystemInfo = targetStorageSystemInfo;
    }


    /**
     * Gets the authorizationID value for this SrmCopyRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmCopyRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the arrayOfFileRequests value for this SrmCopyRequest.
     * 
     * @return arrayOfFileRequests
     */
    public org.dcache.srm.v2_2.ArrayOfTCopyFileRequest getArrayOfFileRequests() {
        return arrayOfFileRequests;
    }


    /**
     * Sets the arrayOfFileRequests value for this SrmCopyRequest.
     * 
     * @param arrayOfFileRequests
     */
    public void setArrayOfFileRequests(org.dcache.srm.v2_2.ArrayOfTCopyFileRequest arrayOfFileRequests) {
        this.arrayOfFileRequests = arrayOfFileRequests;
    }


    /**
     * Gets the userRequestDescription value for this SrmCopyRequest.
     * 
     * @return userRequestDescription
     */
    public java.lang.String getUserRequestDescription() {
        return userRequestDescription;
    }


    /**
     * Sets the userRequestDescription value for this SrmCopyRequest.
     * 
     * @param userRequestDescription
     */
    public void setUserRequestDescription(java.lang.String userRequestDescription) {
        this.userRequestDescription = userRequestDescription;
    }


    /**
     * Gets the overwriteOption value for this SrmCopyRequest.
     * 
     * @return overwriteOption
     */
    public org.dcache.srm.v2_2.TOverwriteMode getOverwriteOption() {
        return overwriteOption;
    }


    /**
     * Sets the overwriteOption value for this SrmCopyRequest.
     * 
     * @param overwriteOption
     */
    public void setOverwriteOption(org.dcache.srm.v2_2.TOverwriteMode overwriteOption) {
        this.overwriteOption = overwriteOption;
    }


    /**
     * Gets the desiredTotalRequestTime value for this SrmCopyRequest.
     * 
     * @return desiredTotalRequestTime
     */
    public java.lang.Integer getDesiredTotalRequestTime() {
        return desiredTotalRequestTime;
    }


    /**
     * Sets the desiredTotalRequestTime value for this SrmCopyRequest.
     * 
     * @param desiredTotalRequestTime
     */
    public void setDesiredTotalRequestTime(java.lang.Integer desiredTotalRequestTime) {
        this.desiredTotalRequestTime = desiredTotalRequestTime;
    }


    /**
     * Gets the desiredTargetSURLLifeTime value for this SrmCopyRequest.
     * 
     * @return desiredTargetSURLLifeTime
     */
    public java.lang.Integer getDesiredTargetSURLLifeTime() {
        return desiredTargetSURLLifeTime;
    }


    /**
     * Sets the desiredTargetSURLLifeTime value for this SrmCopyRequest.
     * 
     * @param desiredTargetSURLLifeTime
     */
    public void setDesiredTargetSURLLifeTime(java.lang.Integer desiredTargetSURLLifeTime) {
        this.desiredTargetSURLLifeTime = desiredTargetSURLLifeTime;
    }


    /**
     * Gets the targetFileStorageType value for this SrmCopyRequest.
     * 
     * @return targetFileStorageType
     */
    public org.dcache.srm.v2_2.TFileStorageType getTargetFileStorageType() {
        return targetFileStorageType;
    }


    /**
     * Sets the targetFileStorageType value for this SrmCopyRequest.
     * 
     * @param targetFileStorageType
     */
    public void setTargetFileStorageType(org.dcache.srm.v2_2.TFileStorageType targetFileStorageType) {
        this.targetFileStorageType = targetFileStorageType;
    }


    /**
     * Gets the targetSpaceToken value for this SrmCopyRequest.
     * 
     * @return targetSpaceToken
     */
    public java.lang.String getTargetSpaceToken() {
        return targetSpaceToken;
    }


    /**
     * Sets the targetSpaceToken value for this SrmCopyRequest.
     * 
     * @param targetSpaceToken
     */
    public void setTargetSpaceToken(java.lang.String targetSpaceToken) {
        this.targetSpaceToken = targetSpaceToken;
    }


    /**
     * Gets the targetFileRetentionPolicyInfo value for this SrmCopyRequest.
     * 
     * @return targetFileRetentionPolicyInfo
     */
    public org.dcache.srm.v2_2.TRetentionPolicyInfo getTargetFileRetentionPolicyInfo() {
        return targetFileRetentionPolicyInfo;
    }


    /**
     * Sets the targetFileRetentionPolicyInfo value for this SrmCopyRequest.
     * 
     * @param targetFileRetentionPolicyInfo
     */
    public void setTargetFileRetentionPolicyInfo(org.dcache.srm.v2_2.TRetentionPolicyInfo targetFileRetentionPolicyInfo) {
        this.targetFileRetentionPolicyInfo = targetFileRetentionPolicyInfo;
    }


    /**
     * Gets the sourceStorageSystemInfo value for this SrmCopyRequest.
     * 
     * @return sourceStorageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getSourceStorageSystemInfo() {
        return sourceStorageSystemInfo;
    }


    /**
     * Sets the sourceStorageSystemInfo value for this SrmCopyRequest.
     * 
     * @param sourceStorageSystemInfo
     */
    public void setSourceStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo sourceStorageSystemInfo) {
        this.sourceStorageSystemInfo = sourceStorageSystemInfo;
    }


    /**
     * Gets the targetStorageSystemInfo value for this SrmCopyRequest.
     * 
     * @return targetStorageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getTargetStorageSystemInfo() {
        return targetStorageSystemInfo;
    }


    /**
     * Sets the targetStorageSystemInfo value for this SrmCopyRequest.
     * 
     * @param targetStorageSystemInfo
     */
    public void setTargetStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo targetStorageSystemInfo) {
        this.targetStorageSystemInfo = targetStorageSystemInfo;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmCopyRequest)) {
            return false;
        }
        SrmCopyRequest other = (SrmCopyRequest) obj;
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
            ((this.desiredTotalRequestTime==null && other.getDesiredTotalRequestTime()==null) || 
             (this.desiredTotalRequestTime!=null &&
              this.desiredTotalRequestTime.equals(other.getDesiredTotalRequestTime()))) &&
            ((this.desiredTargetSURLLifeTime==null && other.getDesiredTargetSURLLifeTime()==null) || 
             (this.desiredTargetSURLLifeTime!=null &&
              this.desiredTargetSURLLifeTime.equals(other.getDesiredTargetSURLLifeTime()))) &&
            ((this.targetFileStorageType==null && other.getTargetFileStorageType()==null) || 
             (this.targetFileStorageType!=null &&
              this.targetFileStorageType.equals(other.getTargetFileStorageType()))) &&
            ((this.targetSpaceToken==null && other.getTargetSpaceToken()==null) || 
             (this.targetSpaceToken!=null &&
              this.targetSpaceToken.equals(other.getTargetSpaceToken()))) &&
            ((this.targetFileRetentionPolicyInfo==null && other.getTargetFileRetentionPolicyInfo()==null) || 
             (this.targetFileRetentionPolicyInfo!=null &&
              this.targetFileRetentionPolicyInfo.equals(other.getTargetFileRetentionPolicyInfo()))) &&
            ((this.sourceStorageSystemInfo==null && other.getSourceStorageSystemInfo()==null) || 
             (this.sourceStorageSystemInfo!=null &&
              this.sourceStorageSystemInfo.equals(other.getSourceStorageSystemInfo()))) &&
            ((this.targetStorageSystemInfo==null && other.getTargetStorageSystemInfo()==null) || 
             (this.targetStorageSystemInfo!=null &&
              this.targetStorageSystemInfo.equals(other.getTargetStorageSystemInfo())));
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
        if (getDesiredTotalRequestTime() != null) {
            _hashCode += getDesiredTotalRequestTime().hashCode();
        }
        if (getDesiredTargetSURLLifeTime() != null) {
            _hashCode += getDesiredTargetSURLLifeTime().hashCode();
        }
        if (getTargetFileStorageType() != null) {
            _hashCode += getTargetFileStorageType().hashCode();
        }
        if (getTargetSpaceToken() != null) {
            _hashCode += getTargetSpaceToken().hashCode();
        }
        if (getTargetFileRetentionPolicyInfo() != null) {
            _hashCode += getTargetFileRetentionPolicyInfo().hashCode();
        }
        if (getSourceStorageSystemInfo() != null) {
            _hashCode += getSourceStorageSystemInfo().hashCode();
        }
        if (getTargetStorageSystemInfo() != null) {
            _hashCode += getTargetStorageSystemInfo().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmCopyRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyRequest"));
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
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTCopyFileRequest"));
        elemField.setNillable(false);
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
        elemField.setFieldName("desiredTotalRequestTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredTotalRequestTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("desiredTargetSURLLifeTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredTargetSURLLifeTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("targetFileStorageType");
        elemField.setXmlName(new javax.xml.namespace.QName("", "targetFileStorageType"));
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
        elemField.setFieldName("sourceStorageSystemInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sourceStorageSystemInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTExtraInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("targetStorageSystemInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "targetStorageSystemInfo"));
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

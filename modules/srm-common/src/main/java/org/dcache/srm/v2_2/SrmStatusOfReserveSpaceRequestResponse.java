/**
 * SrmStatusOfReserveSpaceRequestResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmStatusOfReserveSpaceRequestResponse  implements java.io.Serializable {
    private static final long serialVersionUID = -3251588725081746368L;
    private org.dcache.srm.v2_2.TReturnStatus returnStatus;

    private java.lang.Integer estimatedProcessingTime;

    private org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo;

    private org.apache.axis.types.UnsignedLong sizeOfTotalReservedSpace;

    private org.apache.axis.types.UnsignedLong sizeOfGuaranteedReservedSpace;

    private java.lang.Integer lifetimeOfReservedSpace;

    private java.lang.String spaceToken;

    public SrmStatusOfReserveSpaceRequestResponse() {
    }

    public SrmStatusOfReserveSpaceRequestResponse(
           org.dcache.srm.v2_2.TReturnStatus returnStatus,
           java.lang.Integer estimatedProcessingTime,
           org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo,
           org.apache.axis.types.UnsignedLong sizeOfTotalReservedSpace,
           org.apache.axis.types.UnsignedLong sizeOfGuaranteedReservedSpace,
           java.lang.Integer lifetimeOfReservedSpace,
           java.lang.String spaceToken) {
           this.returnStatus = returnStatus;
           this.estimatedProcessingTime = estimatedProcessingTime;
           this.retentionPolicyInfo = retentionPolicyInfo;
           this.sizeOfTotalReservedSpace = sizeOfTotalReservedSpace;
           this.sizeOfGuaranteedReservedSpace = sizeOfGuaranteedReservedSpace;
           this.lifetimeOfReservedSpace = lifetimeOfReservedSpace;
           this.spaceToken = spaceToken;
    }


    /**
     * Gets the returnStatus value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_2.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_2.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }


    /**
     * Gets the estimatedProcessingTime value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @return estimatedProcessingTime
     */
    public java.lang.Integer getEstimatedProcessingTime() {
        return estimatedProcessingTime;
    }


    /**
     * Sets the estimatedProcessingTime value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @param estimatedProcessingTime
     */
    public void setEstimatedProcessingTime(java.lang.Integer estimatedProcessingTime) {
        this.estimatedProcessingTime = estimatedProcessingTime;
    }


    /**
     * Gets the retentionPolicyInfo value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @return retentionPolicyInfo
     */
    public org.dcache.srm.v2_2.TRetentionPolicyInfo getRetentionPolicyInfo() {
        return retentionPolicyInfo;
    }


    /**
     * Sets the retentionPolicyInfo value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @param retentionPolicyInfo
     */
    public void setRetentionPolicyInfo(org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo) {
        this.retentionPolicyInfo = retentionPolicyInfo;
    }


    /**
     * Gets the sizeOfTotalReservedSpace value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @return sizeOfTotalReservedSpace
     */
    public org.apache.axis.types.UnsignedLong getSizeOfTotalReservedSpace() {
        return sizeOfTotalReservedSpace;
    }


    /**
     * Sets the sizeOfTotalReservedSpace value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @param sizeOfTotalReservedSpace
     */
    public void setSizeOfTotalReservedSpace(org.apache.axis.types.UnsignedLong sizeOfTotalReservedSpace) {
        this.sizeOfTotalReservedSpace = sizeOfTotalReservedSpace;
    }


    /**
     * Gets the sizeOfGuaranteedReservedSpace value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @return sizeOfGuaranteedReservedSpace
     */
    public org.apache.axis.types.UnsignedLong getSizeOfGuaranteedReservedSpace() {
        return sizeOfGuaranteedReservedSpace;
    }


    /**
     * Sets the sizeOfGuaranteedReservedSpace value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @param sizeOfGuaranteedReservedSpace
     */
    public void setSizeOfGuaranteedReservedSpace(org.apache.axis.types.UnsignedLong sizeOfGuaranteedReservedSpace) {
        this.sizeOfGuaranteedReservedSpace = sizeOfGuaranteedReservedSpace;
    }


    /**
     * Gets the lifetimeOfReservedSpace value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @return lifetimeOfReservedSpace
     */
    public java.lang.Integer getLifetimeOfReservedSpace() {
        return lifetimeOfReservedSpace;
    }


    /**
     * Sets the lifetimeOfReservedSpace value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @param lifetimeOfReservedSpace
     */
    public void setLifetimeOfReservedSpace(java.lang.Integer lifetimeOfReservedSpace) {
        this.lifetimeOfReservedSpace = lifetimeOfReservedSpace;
    }


    /**
     * Gets the spaceToken value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @return spaceToken
     */
    public java.lang.String getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this SrmStatusOfReserveSpaceRequestResponse.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(java.lang.String spaceToken) {
        this.spaceToken = spaceToken;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmStatusOfReserveSpaceRequestResponse)) {
            return false;
        }
        SrmStatusOfReserveSpaceRequestResponse other = (SrmStatusOfReserveSpaceRequestResponse) obj;
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
            ((this.returnStatus==null && other.getReturnStatus()==null) || 
             (this.returnStatus!=null &&
              this.returnStatus.equals(other.getReturnStatus()))) &&
            ((this.estimatedProcessingTime==null && other.getEstimatedProcessingTime()==null) || 
             (this.estimatedProcessingTime!=null &&
              this.estimatedProcessingTime.equals(other.getEstimatedProcessingTime()))) &&
            ((this.retentionPolicyInfo==null && other.getRetentionPolicyInfo()==null) || 
             (this.retentionPolicyInfo!=null &&
              this.retentionPolicyInfo.equals(other.getRetentionPolicyInfo()))) &&
            ((this.sizeOfTotalReservedSpace==null && other.getSizeOfTotalReservedSpace()==null) || 
             (this.sizeOfTotalReservedSpace!=null &&
              this.sizeOfTotalReservedSpace.equals(other.getSizeOfTotalReservedSpace()))) &&
            ((this.sizeOfGuaranteedReservedSpace==null && other.getSizeOfGuaranteedReservedSpace()==null) || 
             (this.sizeOfGuaranteedReservedSpace!=null &&
              this.sizeOfGuaranteedReservedSpace.equals(other.getSizeOfGuaranteedReservedSpace()))) &&
            ((this.lifetimeOfReservedSpace==null && other.getLifetimeOfReservedSpace()==null) || 
             (this.lifetimeOfReservedSpace!=null &&
              this.lifetimeOfReservedSpace.equals(other.getLifetimeOfReservedSpace()))) &&
            ((this.spaceToken==null && other.getSpaceToken()==null) || 
             (this.spaceToken!=null &&
              this.spaceToken.equals(other.getSpaceToken())));
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
        if (getReturnStatus() != null) {
            _hashCode += getReturnStatus().hashCode();
        }
        if (getEstimatedProcessingTime() != null) {
            _hashCode += getEstimatedProcessingTime().hashCode();
        }
        if (getRetentionPolicyInfo() != null) {
            _hashCode += getRetentionPolicyInfo().hashCode();
        }
        if (getSizeOfTotalReservedSpace() != null) {
            _hashCode += getSizeOfTotalReservedSpace().hashCode();
        }
        if (getSizeOfGuaranteedReservedSpace() != null) {
            _hashCode += getSizeOfGuaranteedReservedSpace().hashCode();
        }
        if (getLifetimeOfReservedSpace() != null) {
            _hashCode += getLifetimeOfReservedSpace().hashCode();
        }
        if (getSpaceToken() != null) {
            _hashCode += getSpaceToken().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmStatusOfReserveSpaceRequestResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfReserveSpaceRequestResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("returnStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("", "returnStatus"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("estimatedProcessingTime");
        elemField.setXmlName(new javax.xml.namespace.QName("", "estimatedProcessingTime"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("retentionPolicyInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "retentionPolicyInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicyInfo"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sizeOfTotalReservedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfTotalReservedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sizeOfGuaranteedReservedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfGuaranteedReservedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetimeOfReservedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetimeOfReservedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("spaceToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "spaceToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
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

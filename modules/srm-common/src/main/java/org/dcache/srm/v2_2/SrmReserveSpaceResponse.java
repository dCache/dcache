/**
 * SrmReserveSpaceResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmReserveSpaceResponse  implements java.io.Serializable {
    private static final long serialVersionUID = -4989628031554742994L;
    private org.dcache.srm.v2_2.TReturnStatus returnStatus;

    private java.lang.String requestToken;

    private java.lang.Integer estimatedProcessingTime;

    private org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo;

    private org.apache.axis.types.UnsignedLong sizeOfTotalReservedSpace;

    private org.apache.axis.types.UnsignedLong sizeOfGuaranteedReservedSpace;

    private java.lang.Integer lifetimeOfReservedSpace;

    private java.lang.String spaceToken;

    public SrmReserveSpaceResponse() {
    }

    public SrmReserveSpaceResponse(
           org.dcache.srm.v2_2.TReturnStatus returnStatus,
           java.lang.String requestToken,
           java.lang.Integer estimatedProcessingTime,
           org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo,
           org.apache.axis.types.UnsignedLong sizeOfTotalReservedSpace,
           org.apache.axis.types.UnsignedLong sizeOfGuaranteedReservedSpace,
           java.lang.Integer lifetimeOfReservedSpace,
           java.lang.String spaceToken) {
           this.returnStatus = returnStatus;
           this.requestToken = requestToken;
           this.estimatedProcessingTime = estimatedProcessingTime;
           this.retentionPolicyInfo = retentionPolicyInfo;
           this.sizeOfTotalReservedSpace = sizeOfTotalReservedSpace;
           this.sizeOfGuaranteedReservedSpace = sizeOfGuaranteedReservedSpace;
           this.lifetimeOfReservedSpace = lifetimeOfReservedSpace;
           this.spaceToken = spaceToken;
    }


    /**
     * Gets the returnStatus value for this SrmReserveSpaceResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_2.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmReserveSpaceResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_2.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }


    /**
     * Gets the requestToken value for this SrmReserveSpaceResponse.
     * 
     * @return requestToken
     */
    public java.lang.String getRequestToken() {
        return requestToken;
    }


    /**
     * Sets the requestToken value for this SrmReserveSpaceResponse.
     * 
     * @param requestToken
     */
    public void setRequestToken(java.lang.String requestToken) {
        this.requestToken = requestToken;
    }


    /**
     * Gets the estimatedProcessingTime value for this SrmReserveSpaceResponse.
     * 
     * @return estimatedProcessingTime
     */
    public java.lang.Integer getEstimatedProcessingTime() {
        return estimatedProcessingTime;
    }


    /**
     * Sets the estimatedProcessingTime value for this SrmReserveSpaceResponse.
     * 
     * @param estimatedProcessingTime
     */
    public void setEstimatedProcessingTime(java.lang.Integer estimatedProcessingTime) {
        this.estimatedProcessingTime = estimatedProcessingTime;
    }


    /**
     * Gets the retentionPolicyInfo value for this SrmReserveSpaceResponse.
     * 
     * @return retentionPolicyInfo
     */
    public org.dcache.srm.v2_2.TRetentionPolicyInfo getRetentionPolicyInfo() {
        return retentionPolicyInfo;
    }


    /**
     * Sets the retentionPolicyInfo value for this SrmReserveSpaceResponse.
     * 
     * @param retentionPolicyInfo
     */
    public void setRetentionPolicyInfo(org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo) {
        this.retentionPolicyInfo = retentionPolicyInfo;
    }


    /**
     * Gets the sizeOfTotalReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @return sizeOfTotalReservedSpace
     */
    public org.apache.axis.types.UnsignedLong getSizeOfTotalReservedSpace() {
        return sizeOfTotalReservedSpace;
    }


    /**
     * Sets the sizeOfTotalReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @param sizeOfTotalReservedSpace
     */
    public void setSizeOfTotalReservedSpace(org.apache.axis.types.UnsignedLong sizeOfTotalReservedSpace) {
        this.sizeOfTotalReservedSpace = sizeOfTotalReservedSpace;
    }


    /**
     * Gets the sizeOfGuaranteedReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @return sizeOfGuaranteedReservedSpace
     */
    public org.apache.axis.types.UnsignedLong getSizeOfGuaranteedReservedSpace() {
        return sizeOfGuaranteedReservedSpace;
    }


    /**
     * Sets the sizeOfGuaranteedReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @param sizeOfGuaranteedReservedSpace
     */
    public void setSizeOfGuaranteedReservedSpace(org.apache.axis.types.UnsignedLong sizeOfGuaranteedReservedSpace) {
        this.sizeOfGuaranteedReservedSpace = sizeOfGuaranteedReservedSpace;
    }


    /**
     * Gets the lifetimeOfReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @return lifetimeOfReservedSpace
     */
    public java.lang.Integer getLifetimeOfReservedSpace() {
        return lifetimeOfReservedSpace;
    }


    /**
     * Sets the lifetimeOfReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @param lifetimeOfReservedSpace
     */
    public void setLifetimeOfReservedSpace(java.lang.Integer lifetimeOfReservedSpace) {
        this.lifetimeOfReservedSpace = lifetimeOfReservedSpace;
    }


    /**
     * Gets the spaceToken value for this SrmReserveSpaceResponse.
     * 
     * @return spaceToken
     */
    public java.lang.String getSpaceToken() {
        return spaceToken;
    }


    /**
     * Sets the spaceToken value for this SrmReserveSpaceResponse.
     * 
     * @param spaceToken
     */
    public void setSpaceToken(java.lang.String spaceToken) {
        this.spaceToken = spaceToken;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmReserveSpaceResponse)) {
            return false;
        }
        SrmReserveSpaceResponse other = (SrmReserveSpaceResponse) obj;
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
            ((this.requestToken==null && other.getRequestToken()==null) || 
             (this.requestToken!=null &&
              this.requestToken.equals(other.getRequestToken()))) &&
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
        if (getRequestToken() != null) {
            _hashCode += getRequestToken().hashCode();
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
        new org.apache.axis.description.TypeDesc(SrmReserveSpaceResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceResponse"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("returnStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("", "returnStatus"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("requestToken");
        elemField.setXmlName(new javax.xml.namespace.QName("", "requestToken"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
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

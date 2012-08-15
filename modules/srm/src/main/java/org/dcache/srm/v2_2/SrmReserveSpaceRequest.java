/**
 * SrmReserveSpaceRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmReserveSpaceRequest  implements java.io.Serializable {
    private static final long serialVersionUID = 8148353068115545193L;
    private java.lang.String authorizationID;

    private java.lang.String userSpaceTokenDescription;

    private org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo;

    private org.apache.axis.types.UnsignedLong desiredSizeOfTotalSpace;

    private org.apache.axis.types.UnsignedLong desiredSizeOfGuaranteedSpace;

    private java.lang.Integer desiredLifetimeOfReservedSpace;

    private org.dcache.srm.v2_2.ArrayOfUnsignedLong arrayOfExpectedFileSizes;

    private org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo;

    private org.dcache.srm.v2_2.TTransferParameters transferParameters;

    public SrmReserveSpaceRequest() {
    }

    public SrmReserveSpaceRequest(
           java.lang.String authorizationID,
           java.lang.String userSpaceTokenDescription,
           org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo,
           org.apache.axis.types.UnsignedLong desiredSizeOfTotalSpace,
           org.apache.axis.types.UnsignedLong desiredSizeOfGuaranteedSpace,
           java.lang.Integer desiredLifetimeOfReservedSpace,
           org.dcache.srm.v2_2.ArrayOfUnsignedLong arrayOfExpectedFileSizes,
           org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo,
           org.dcache.srm.v2_2.TTransferParameters transferParameters) {
           this.authorizationID = authorizationID;
           this.userSpaceTokenDescription = userSpaceTokenDescription;
           this.retentionPolicyInfo = retentionPolicyInfo;
           this.desiredSizeOfTotalSpace = desiredSizeOfTotalSpace;
           this.desiredSizeOfGuaranteedSpace = desiredSizeOfGuaranteedSpace;
           this.desiredLifetimeOfReservedSpace = desiredLifetimeOfReservedSpace;
           this.arrayOfExpectedFileSizes = arrayOfExpectedFileSizes;
           this.storageSystemInfo = storageSystemInfo;
           this.transferParameters = transferParameters;
    }


    /**
     * Gets the authorizationID value for this SrmReserveSpaceRequest.
     * 
     * @return authorizationID
     */
    public java.lang.String getAuthorizationID() {
        return authorizationID;
    }


    /**
     * Sets the authorizationID value for this SrmReserveSpaceRequest.
     * 
     * @param authorizationID
     */
    public void setAuthorizationID(java.lang.String authorizationID) {
        this.authorizationID = authorizationID;
    }


    /**
     * Gets the userSpaceTokenDescription value for this SrmReserveSpaceRequest.
     * 
     * @return userSpaceTokenDescription
     */
    public java.lang.String getUserSpaceTokenDescription() {
        return userSpaceTokenDescription;
    }


    /**
     * Sets the userSpaceTokenDescription value for this SrmReserveSpaceRequest.
     * 
     * @param userSpaceTokenDescription
     */
    public void setUserSpaceTokenDescription(java.lang.String userSpaceTokenDescription) {
        this.userSpaceTokenDescription = userSpaceTokenDescription;
    }


    /**
     * Gets the retentionPolicyInfo value for this SrmReserveSpaceRequest.
     * 
     * @return retentionPolicyInfo
     */
    public org.dcache.srm.v2_2.TRetentionPolicyInfo getRetentionPolicyInfo() {
        return retentionPolicyInfo;
    }


    /**
     * Sets the retentionPolicyInfo value for this SrmReserveSpaceRequest.
     * 
     * @param retentionPolicyInfo
     */
    public void setRetentionPolicyInfo(org.dcache.srm.v2_2.TRetentionPolicyInfo retentionPolicyInfo) {
        this.retentionPolicyInfo = retentionPolicyInfo;
    }


    /**
     * Gets the desiredSizeOfTotalSpace value for this SrmReserveSpaceRequest.
     * 
     * @return desiredSizeOfTotalSpace
     */
    public org.apache.axis.types.UnsignedLong getDesiredSizeOfTotalSpace() {
        return desiredSizeOfTotalSpace;
    }


    /**
     * Sets the desiredSizeOfTotalSpace value for this SrmReserveSpaceRequest.
     * 
     * @param desiredSizeOfTotalSpace
     */
    public void setDesiredSizeOfTotalSpace(org.apache.axis.types.UnsignedLong desiredSizeOfTotalSpace) {
        this.desiredSizeOfTotalSpace = desiredSizeOfTotalSpace;
    }


    /**
     * Gets the desiredSizeOfGuaranteedSpace value for this SrmReserveSpaceRequest.
     * 
     * @return desiredSizeOfGuaranteedSpace
     */
    public org.apache.axis.types.UnsignedLong getDesiredSizeOfGuaranteedSpace() {
        return desiredSizeOfGuaranteedSpace;
    }


    /**
     * Sets the desiredSizeOfGuaranteedSpace value for this SrmReserveSpaceRequest.
     * 
     * @param desiredSizeOfGuaranteedSpace
     */
    public void setDesiredSizeOfGuaranteedSpace(org.apache.axis.types.UnsignedLong desiredSizeOfGuaranteedSpace) {
        this.desiredSizeOfGuaranteedSpace = desiredSizeOfGuaranteedSpace;
    }


    /**
     * Gets the desiredLifetimeOfReservedSpace value for this SrmReserveSpaceRequest.
     * 
     * @return desiredLifetimeOfReservedSpace
     */
    public java.lang.Integer getDesiredLifetimeOfReservedSpace() {
        return desiredLifetimeOfReservedSpace;
    }


    /**
     * Sets the desiredLifetimeOfReservedSpace value for this SrmReserveSpaceRequest.
     * 
     * @param desiredLifetimeOfReservedSpace
     */
    public void setDesiredLifetimeOfReservedSpace(java.lang.Integer desiredLifetimeOfReservedSpace) {
        this.desiredLifetimeOfReservedSpace = desiredLifetimeOfReservedSpace;
    }


    /**
     * Gets the arrayOfExpectedFileSizes value for this SrmReserveSpaceRequest.
     * 
     * @return arrayOfExpectedFileSizes
     */
    public org.dcache.srm.v2_2.ArrayOfUnsignedLong getArrayOfExpectedFileSizes() {
        return arrayOfExpectedFileSizes;
    }


    /**
     * Sets the arrayOfExpectedFileSizes value for this SrmReserveSpaceRequest.
     * 
     * @param arrayOfExpectedFileSizes
     */
    public void setArrayOfExpectedFileSizes(org.dcache.srm.v2_2.ArrayOfUnsignedLong arrayOfExpectedFileSizes) {
        this.arrayOfExpectedFileSizes = arrayOfExpectedFileSizes;
    }


    /**
     * Gets the storageSystemInfo value for this SrmReserveSpaceRequest.
     * 
     * @return storageSystemInfo
     */
    public org.dcache.srm.v2_2.ArrayOfTExtraInfo getStorageSystemInfo() {
        return storageSystemInfo;
    }


    /**
     * Sets the storageSystemInfo value for this SrmReserveSpaceRequest.
     * 
     * @param storageSystemInfo
     */
    public void setStorageSystemInfo(org.dcache.srm.v2_2.ArrayOfTExtraInfo storageSystemInfo) {
        this.storageSystemInfo = storageSystemInfo;
    }


    /**
     * Gets the transferParameters value for this SrmReserveSpaceRequest.
     * 
     * @return transferParameters
     */
    public org.dcache.srm.v2_2.TTransferParameters getTransferParameters() {
        return transferParameters;
    }


    /**
     * Sets the transferParameters value for this SrmReserveSpaceRequest.
     * 
     * @param transferParameters
     */
    public void setTransferParameters(org.dcache.srm.v2_2.TTransferParameters transferParameters) {
        this.transferParameters = transferParameters;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmReserveSpaceRequest)) {
            return false;
        }
        SrmReserveSpaceRequest other = (SrmReserveSpaceRequest) obj;
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
            ((this.userSpaceTokenDescription==null && other.getUserSpaceTokenDescription()==null) || 
             (this.userSpaceTokenDescription!=null &&
              this.userSpaceTokenDescription.equals(other.getUserSpaceTokenDescription()))) &&
            ((this.retentionPolicyInfo==null && other.getRetentionPolicyInfo()==null) || 
             (this.retentionPolicyInfo!=null &&
              this.retentionPolicyInfo.equals(other.getRetentionPolicyInfo()))) &&
            ((this.desiredSizeOfTotalSpace==null && other.getDesiredSizeOfTotalSpace()==null) || 
             (this.desiredSizeOfTotalSpace!=null &&
              this.desiredSizeOfTotalSpace.equals(other.getDesiredSizeOfTotalSpace()))) &&
            ((this.desiredSizeOfGuaranteedSpace==null && other.getDesiredSizeOfGuaranteedSpace()==null) || 
             (this.desiredSizeOfGuaranteedSpace!=null &&
              this.desiredSizeOfGuaranteedSpace.equals(other.getDesiredSizeOfGuaranteedSpace()))) &&
            ((this.desiredLifetimeOfReservedSpace==null && other.getDesiredLifetimeOfReservedSpace()==null) || 
             (this.desiredLifetimeOfReservedSpace!=null &&
              this.desiredLifetimeOfReservedSpace.equals(other.getDesiredLifetimeOfReservedSpace()))) &&
            ((this.arrayOfExpectedFileSizes==null && other.getArrayOfExpectedFileSizes()==null) || 
             (this.arrayOfExpectedFileSizes!=null &&
              this.arrayOfExpectedFileSizes.equals(other.getArrayOfExpectedFileSizes()))) &&
            ((this.storageSystemInfo==null && other.getStorageSystemInfo()==null) || 
             (this.storageSystemInfo!=null &&
              this.storageSystemInfo.equals(other.getStorageSystemInfo()))) &&
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
        if (getUserSpaceTokenDescription() != null) {
            _hashCode += getUserSpaceTokenDescription().hashCode();
        }
        if (getRetentionPolicyInfo() != null) {
            _hashCode += getRetentionPolicyInfo().hashCode();
        }
        if (getDesiredSizeOfTotalSpace() != null) {
            _hashCode += getDesiredSizeOfTotalSpace().hashCode();
        }
        if (getDesiredSizeOfGuaranteedSpace() != null) {
            _hashCode += getDesiredSizeOfGuaranteedSpace().hashCode();
        }
        if (getDesiredLifetimeOfReservedSpace() != null) {
            _hashCode += getDesiredLifetimeOfReservedSpace().hashCode();
        }
        if (getArrayOfExpectedFileSizes() != null) {
            _hashCode += getArrayOfExpectedFileSizes().hashCode();
        }
        if (getStorageSystemInfo() != null) {
            _hashCode += getStorageSystemInfo().hashCode();
        }
        if (getTransferParameters() != null) {
            _hashCode += getTransferParameters().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmReserveSpaceRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("authorizationID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "authorizationID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userSpaceTokenDescription");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userSpaceTokenDescription"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "string"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("retentionPolicyInfo");
        elemField.setXmlName(new javax.xml.namespace.QName("", "retentionPolicyInfo"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicyInfo"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("desiredSizeOfTotalSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredSizeOfTotalSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("desiredSizeOfGuaranteedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredSizeOfGuaranteedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("desiredLifetimeOfReservedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "desiredLifetimeOfReservedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("arrayOfExpectedFileSizes");
        elemField.setXmlName(new javax.xml.namespace.QName("", "arrayOfExpectedFileSizes"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfUnsignedLong"));
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

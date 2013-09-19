/**
 * SrmUpdateSpaceResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmUpdateSpaceResponse  implements java.io.Serializable {
    private static final long serialVersionUID = -8838448879658291118L;
    private org.dcache.srm.v2_2.TReturnStatus returnStatus;

    private java.lang.String requestToken;

    private org.apache.axis.types.UnsignedLong sizeOfTotalSpace;

    private org.apache.axis.types.UnsignedLong sizeOfGuaranteedSpace;

    private java.lang.Integer lifetimeGranted;

    public SrmUpdateSpaceResponse() {
    }

    public SrmUpdateSpaceResponse(
           org.dcache.srm.v2_2.TReturnStatus returnStatus,
           java.lang.String requestToken,
           org.apache.axis.types.UnsignedLong sizeOfTotalSpace,
           org.apache.axis.types.UnsignedLong sizeOfGuaranteedSpace,
           java.lang.Integer lifetimeGranted) {
           this.returnStatus = returnStatus;
           this.requestToken = requestToken;
           this.sizeOfTotalSpace = sizeOfTotalSpace;
           this.sizeOfGuaranteedSpace = sizeOfGuaranteedSpace;
           this.lifetimeGranted = lifetimeGranted;
    }


    /**
     * Gets the returnStatus value for this SrmUpdateSpaceResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_2.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmUpdateSpaceResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_2.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }


    /**
     * Gets the requestToken value for this SrmUpdateSpaceResponse.
     * 
     * @return requestToken
     */
    public java.lang.String getRequestToken() {
        return requestToken;
    }


    /**
     * Sets the requestToken value for this SrmUpdateSpaceResponse.
     * 
     * @param requestToken
     */
    public void setRequestToken(java.lang.String requestToken) {
        this.requestToken = requestToken;
    }


    /**
     * Gets the sizeOfTotalSpace value for this SrmUpdateSpaceResponse.
     * 
     * @return sizeOfTotalSpace
     */
    public org.apache.axis.types.UnsignedLong getSizeOfTotalSpace() {
        return sizeOfTotalSpace;
    }


    /**
     * Sets the sizeOfTotalSpace value for this SrmUpdateSpaceResponse.
     * 
     * @param sizeOfTotalSpace
     */
    public void setSizeOfTotalSpace(org.apache.axis.types.UnsignedLong sizeOfTotalSpace) {
        this.sizeOfTotalSpace = sizeOfTotalSpace;
    }


    /**
     * Gets the sizeOfGuaranteedSpace value for this SrmUpdateSpaceResponse.
     * 
     * @return sizeOfGuaranteedSpace
     */
    public org.apache.axis.types.UnsignedLong getSizeOfGuaranteedSpace() {
        return sizeOfGuaranteedSpace;
    }


    /**
     * Sets the sizeOfGuaranteedSpace value for this SrmUpdateSpaceResponse.
     * 
     * @param sizeOfGuaranteedSpace
     */
    public void setSizeOfGuaranteedSpace(org.apache.axis.types.UnsignedLong sizeOfGuaranteedSpace) {
        this.sizeOfGuaranteedSpace = sizeOfGuaranteedSpace;
    }


    /**
     * Gets the lifetimeGranted value for this SrmUpdateSpaceResponse.
     * 
     * @return lifetimeGranted
     */
    public java.lang.Integer getLifetimeGranted() {
        return lifetimeGranted;
    }


    /**
     * Sets the lifetimeGranted value for this SrmUpdateSpaceResponse.
     * 
     * @param lifetimeGranted
     */
    public void setLifetimeGranted(java.lang.Integer lifetimeGranted) {
        this.lifetimeGranted = lifetimeGranted;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmUpdateSpaceResponse)) {
            return false;
        }
        SrmUpdateSpaceResponse other = (SrmUpdateSpaceResponse) obj;
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
            ((this.sizeOfTotalSpace==null && other.getSizeOfTotalSpace()==null) || 
             (this.sizeOfTotalSpace!=null &&
              this.sizeOfTotalSpace.equals(other.getSizeOfTotalSpace()))) &&
            ((this.sizeOfGuaranteedSpace==null && other.getSizeOfGuaranteedSpace()==null) || 
             (this.sizeOfGuaranteedSpace!=null &&
              this.sizeOfGuaranteedSpace.equals(other.getSizeOfGuaranteedSpace()))) &&
            ((this.lifetimeGranted==null && other.getLifetimeGranted()==null) || 
             (this.lifetimeGranted!=null &&
              this.lifetimeGranted.equals(other.getLifetimeGranted())));
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
        if (getSizeOfTotalSpace() != null) {
            _hashCode += getSizeOfTotalSpace().hashCode();
        }
        if (getSizeOfGuaranteedSpace() != null) {
            _hashCode += getSizeOfGuaranteedSpace().hashCode();
        }
        if (getLifetimeGranted() != null) {
            _hashCode += getLifetimeGranted().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmUpdateSpaceResponse.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceResponse"));
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
        elemField.setFieldName("sizeOfTotalSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfTotalSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sizeOfGuaranteedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfGuaranteedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "unsignedLong"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetimeGranted");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetimeGranted"));
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

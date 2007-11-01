/**
 * SrmUpdateSpaceResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmUpdateSpaceResponse  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TSizeInBytes sizeOfTotalSpace;

    private org.dcache.srm.v2_1.TSizeInBytes sizeOfGuaranteedSpace;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeGranted;

    private org.dcache.srm.v2_1.TReturnStatus returnStatus;

    public SrmUpdateSpaceResponse() {
    }

    public SrmUpdateSpaceResponse(
           org.dcache.srm.v2_1.TSizeInBytes sizeOfTotalSpace,
           org.dcache.srm.v2_1.TSizeInBytes sizeOfGuaranteedSpace,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeGranted,
           org.dcache.srm.v2_1.TReturnStatus returnStatus) {
           this.sizeOfTotalSpace = sizeOfTotalSpace;
           this.sizeOfGuaranteedSpace = sizeOfGuaranteedSpace;
           this.lifetimeGranted = lifetimeGranted;
           this.returnStatus = returnStatus;
    }


    /**
     * Gets the sizeOfTotalSpace value for this SrmUpdateSpaceResponse.
     * 
     * @return sizeOfTotalSpace
     */
    public org.dcache.srm.v2_1.TSizeInBytes getSizeOfTotalSpace() {
        return sizeOfTotalSpace;
    }


    /**
     * Sets the sizeOfTotalSpace value for this SrmUpdateSpaceResponse.
     * 
     * @param sizeOfTotalSpace
     */
    public void setSizeOfTotalSpace(org.dcache.srm.v2_1.TSizeInBytes sizeOfTotalSpace) {
        this.sizeOfTotalSpace = sizeOfTotalSpace;
    }


    /**
     * Gets the sizeOfGuaranteedSpace value for this SrmUpdateSpaceResponse.
     * 
     * @return sizeOfGuaranteedSpace
     */
    public org.dcache.srm.v2_1.TSizeInBytes getSizeOfGuaranteedSpace() {
        return sizeOfGuaranteedSpace;
    }


    /**
     * Sets the sizeOfGuaranteedSpace value for this SrmUpdateSpaceResponse.
     * 
     * @param sizeOfGuaranteedSpace
     */
    public void setSizeOfGuaranteedSpace(org.dcache.srm.v2_1.TSizeInBytes sizeOfGuaranteedSpace) {
        this.sizeOfGuaranteedSpace = sizeOfGuaranteedSpace;
    }


    /**
     * Gets the lifetimeGranted value for this SrmUpdateSpaceResponse.
     * 
     * @return lifetimeGranted
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifetimeGranted() {
        return lifetimeGranted;
    }


    /**
     * Sets the lifetimeGranted value for this SrmUpdateSpaceResponse.
     * 
     * @param lifetimeGranted
     */
    public void setLifetimeGranted(org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeGranted) {
        this.lifetimeGranted = lifetimeGranted;
    }


    /**
     * Gets the returnStatus value for this SrmUpdateSpaceResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_1.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmUpdateSpaceResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_1.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmUpdateSpaceResponse)) return false;
        SrmUpdateSpaceResponse other = (SrmUpdateSpaceResponse) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.sizeOfTotalSpace==null && other.getSizeOfTotalSpace()==null) || 
             (this.sizeOfTotalSpace!=null &&
              this.sizeOfTotalSpace.equals(other.getSizeOfTotalSpace()))) &&
            ((this.sizeOfGuaranteedSpace==null && other.getSizeOfGuaranteedSpace()==null) || 
             (this.sizeOfGuaranteedSpace!=null &&
              this.sizeOfGuaranteedSpace.equals(other.getSizeOfGuaranteedSpace()))) &&
            ((this.lifetimeGranted==null && other.getLifetimeGranted()==null) || 
             (this.lifetimeGranted!=null &&
              this.lifetimeGranted.equals(other.getLifetimeGranted()))) &&
            ((this.returnStatus==null && other.getReturnStatus()==null) || 
             (this.returnStatus!=null &&
              this.returnStatus.equals(other.getReturnStatus())));
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
        if (getSizeOfTotalSpace() != null) {
            _hashCode += getSizeOfTotalSpace().hashCode();
        }
        if (getSizeOfGuaranteedSpace() != null) {
            _hashCode += getSizeOfGuaranteedSpace().hashCode();
        }
        if (getLifetimeGranted() != null) {
            _hashCode += getLifetimeGranted().hashCode();
        }
        if (getReturnStatus() != null) {
            _hashCode += getReturnStatus().hashCode();
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
        elemField.setFieldName("sizeOfTotalSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfTotalSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sizeOfGuaranteedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfGuaranteedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetimeGranted");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetimeGranted"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("returnStatus");
        elemField.setXmlName(new javax.xml.namespace.QName("", "returnStatus"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus"));
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

/**
 * SrmReserveSpaceResponse.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmReserveSpaceResponse  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TSpaceType typeOfReservedSpace;

    private org.dcache.srm.v2_1.TSizeInBytes sizeOfTotalReservedSpace;

    private org.dcache.srm.v2_1.TSizeInBytes sizeOfGuaranteedReservedSpace;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeOfReservedSpace;

    private org.dcache.srm.v2_1.TSpaceToken referenceHandleOfReservedSpace;

    private org.dcache.srm.v2_1.TReturnStatus returnStatus;

    public SrmReserveSpaceResponse() {
    }

    public SrmReserveSpaceResponse(
           org.dcache.srm.v2_1.TSpaceType typeOfReservedSpace,
           org.dcache.srm.v2_1.TSizeInBytes sizeOfTotalReservedSpace,
           org.dcache.srm.v2_1.TSizeInBytes sizeOfGuaranteedReservedSpace,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeOfReservedSpace,
           org.dcache.srm.v2_1.TSpaceToken referenceHandleOfReservedSpace,
           org.dcache.srm.v2_1.TReturnStatus returnStatus) {
           this.typeOfReservedSpace = typeOfReservedSpace;
           this.sizeOfTotalReservedSpace = sizeOfTotalReservedSpace;
           this.sizeOfGuaranteedReservedSpace = sizeOfGuaranteedReservedSpace;
           this.lifetimeOfReservedSpace = lifetimeOfReservedSpace;
           this.referenceHandleOfReservedSpace = referenceHandleOfReservedSpace;
           this.returnStatus = returnStatus;
    }


    /**
     * Gets the typeOfReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @return typeOfReservedSpace
     */
    public org.dcache.srm.v2_1.TSpaceType getTypeOfReservedSpace() {
        return typeOfReservedSpace;
    }


    /**
     * Sets the typeOfReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @param typeOfReservedSpace
     */
    public void setTypeOfReservedSpace(org.dcache.srm.v2_1.TSpaceType typeOfReservedSpace) {
        this.typeOfReservedSpace = typeOfReservedSpace;
    }


    /**
     * Gets the sizeOfTotalReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @return sizeOfTotalReservedSpace
     */
    public org.dcache.srm.v2_1.TSizeInBytes getSizeOfTotalReservedSpace() {
        return sizeOfTotalReservedSpace;
    }


    /**
     * Sets the sizeOfTotalReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @param sizeOfTotalReservedSpace
     */
    public void setSizeOfTotalReservedSpace(org.dcache.srm.v2_1.TSizeInBytes sizeOfTotalReservedSpace) {
        this.sizeOfTotalReservedSpace = sizeOfTotalReservedSpace;
    }


    /**
     * Gets the sizeOfGuaranteedReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @return sizeOfGuaranteedReservedSpace
     */
    public org.dcache.srm.v2_1.TSizeInBytes getSizeOfGuaranteedReservedSpace() {
        return sizeOfGuaranteedReservedSpace;
    }


    /**
     * Sets the sizeOfGuaranteedReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @param sizeOfGuaranteedReservedSpace
     */
    public void setSizeOfGuaranteedReservedSpace(org.dcache.srm.v2_1.TSizeInBytes sizeOfGuaranteedReservedSpace) {
        this.sizeOfGuaranteedReservedSpace = sizeOfGuaranteedReservedSpace;
    }


    /**
     * Gets the lifetimeOfReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @return lifetimeOfReservedSpace
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifetimeOfReservedSpace() {
        return lifetimeOfReservedSpace;
    }


    /**
     * Sets the lifetimeOfReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @param lifetimeOfReservedSpace
     */
    public void setLifetimeOfReservedSpace(org.dcache.srm.v2_1.TLifeTimeInSeconds lifetimeOfReservedSpace) {
        this.lifetimeOfReservedSpace = lifetimeOfReservedSpace;
    }


    /**
     * Gets the referenceHandleOfReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @return referenceHandleOfReservedSpace
     */
    public org.dcache.srm.v2_1.TSpaceToken getReferenceHandleOfReservedSpace() {
        return referenceHandleOfReservedSpace;
    }


    /**
     * Sets the referenceHandleOfReservedSpace value for this SrmReserveSpaceResponse.
     * 
     * @param referenceHandleOfReservedSpace
     */
    public void setReferenceHandleOfReservedSpace(org.dcache.srm.v2_1.TSpaceToken referenceHandleOfReservedSpace) {
        this.referenceHandleOfReservedSpace = referenceHandleOfReservedSpace;
    }


    /**
     * Gets the returnStatus value for this SrmReserveSpaceResponse.
     * 
     * @return returnStatus
     */
    public org.dcache.srm.v2_1.TReturnStatus getReturnStatus() {
        return returnStatus;
    }


    /**
     * Sets the returnStatus value for this SrmReserveSpaceResponse.
     * 
     * @param returnStatus
     */
    public void setReturnStatus(org.dcache.srm.v2_1.TReturnStatus returnStatus) {
        this.returnStatus = returnStatus;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmReserveSpaceResponse)) return false;
        SrmReserveSpaceResponse other = (SrmReserveSpaceResponse) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.typeOfReservedSpace==null && other.getTypeOfReservedSpace()==null) || 
             (this.typeOfReservedSpace!=null &&
              this.typeOfReservedSpace.equals(other.getTypeOfReservedSpace()))) &&
            ((this.sizeOfTotalReservedSpace==null && other.getSizeOfTotalReservedSpace()==null) || 
             (this.sizeOfTotalReservedSpace!=null &&
              this.sizeOfTotalReservedSpace.equals(other.getSizeOfTotalReservedSpace()))) &&
            ((this.sizeOfGuaranteedReservedSpace==null && other.getSizeOfGuaranteedReservedSpace()==null) || 
             (this.sizeOfGuaranteedReservedSpace!=null &&
              this.sizeOfGuaranteedReservedSpace.equals(other.getSizeOfGuaranteedReservedSpace()))) &&
            ((this.lifetimeOfReservedSpace==null && other.getLifetimeOfReservedSpace()==null) || 
             (this.lifetimeOfReservedSpace!=null &&
              this.lifetimeOfReservedSpace.equals(other.getLifetimeOfReservedSpace()))) &&
            ((this.referenceHandleOfReservedSpace==null && other.getReferenceHandleOfReservedSpace()==null) || 
             (this.referenceHandleOfReservedSpace!=null &&
              this.referenceHandleOfReservedSpace.equals(other.getReferenceHandleOfReservedSpace()))) &&
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
        if (getTypeOfReservedSpace() != null) {
            _hashCode += getTypeOfReservedSpace().hashCode();
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
        if (getReferenceHandleOfReservedSpace() != null) {
            _hashCode += getReferenceHandleOfReservedSpace().hashCode();
        }
        if (getReturnStatus() != null) {
            _hashCode += getReturnStatus().hashCode();
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
        elemField.setFieldName("typeOfReservedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "typeOfReservedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSpaceType"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sizeOfTotalReservedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfTotalReservedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sizeOfGuaranteedReservedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sizeOfGuaranteedReservedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifetimeOfReservedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifetimeOfReservedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("referenceHandleOfReservedSpace");
        elemField.setXmlName(new javax.xml.namespace.QName("", "referenceHandleOfReservedSpace"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSpaceToken"));
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

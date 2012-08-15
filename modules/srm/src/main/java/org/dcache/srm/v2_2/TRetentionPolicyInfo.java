/**
 * TRetentionPolicyInfo.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TRetentionPolicyInfo  implements java.io.Serializable {
    private static final long serialVersionUID = 427779808779759071L;
    private org.dcache.srm.v2_2.TRetentionPolicy retentionPolicy;

    private org.dcache.srm.v2_2.TAccessLatency accessLatency;

    public TRetentionPolicyInfo() {
    }

    public TRetentionPolicyInfo(
           org.dcache.srm.v2_2.TRetentionPolicy retentionPolicy,
           org.dcache.srm.v2_2.TAccessLatency accessLatency) {
           this.retentionPolicy = retentionPolicy;
           this.accessLatency = accessLatency;
    }


    /**
     * Gets the retentionPolicy value for this TRetentionPolicyInfo.
     * 
     * @return retentionPolicy
     */
    public org.dcache.srm.v2_2.TRetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }


    /**
     * Sets the retentionPolicy value for this TRetentionPolicyInfo.
     * 
     * @param retentionPolicy
     */
    public void setRetentionPolicy(org.dcache.srm.v2_2.TRetentionPolicy retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }


    /**
     * Gets the accessLatency value for this TRetentionPolicyInfo.
     * 
     * @return accessLatency
     */
    public org.dcache.srm.v2_2.TAccessLatency getAccessLatency() {
        return accessLatency;
    }


    /**
     * Sets the accessLatency value for this TRetentionPolicyInfo.
     * 
     * @param accessLatency
     */
    public void setAccessLatency(org.dcache.srm.v2_2.TAccessLatency accessLatency) {
        this.accessLatency = accessLatency;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TRetentionPolicyInfo)) {
            return false;
        }
        TRetentionPolicyInfo other = (TRetentionPolicyInfo) obj;
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
            ((this.retentionPolicy==null && other.getRetentionPolicy()==null) || 
             (this.retentionPolicy!=null &&
              this.retentionPolicy.equals(other.getRetentionPolicy()))) &&
            ((this.accessLatency==null && other.getAccessLatency()==null) || 
             (this.accessLatency!=null &&
              this.accessLatency.equals(other.getAccessLatency())));
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
        if (getRetentionPolicy() != null) {
            _hashCode += getRetentionPolicy().hashCode();
        }
        if (getAccessLatency() != null) {
            _hashCode += getAccessLatency().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TRetentionPolicyInfo.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicyInfo"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("retentionPolicy");
        elemField.setXmlName(new javax.xml.namespace.QName("", "retentionPolicy"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicy"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("accessLatency");
        elemField.setXmlName(new javax.xml.namespace.QName("", "accessLatency"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TAccessLatency"));
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

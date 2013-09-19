/**
 * TGetFileRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TGetFileRequest  implements java.io.Serializable {
    private static final long serialVersionUID = -8440284190624384837L;
    private org.apache.axis.types.URI sourceSURL;

    private org.dcache.srm.v2_2.TDirOption dirOption;

    public TGetFileRequest() {
    }

    public TGetFileRequest(
           org.apache.axis.types.URI sourceSURL,
           org.dcache.srm.v2_2.TDirOption dirOption) {
           this.sourceSURL = sourceSURL;
           this.dirOption = dirOption;
    }


    /**
     * Gets the sourceSURL value for this TGetFileRequest.
     * 
     * @return sourceSURL
     */
    public org.apache.axis.types.URI getSourceSURL() {
        return sourceSURL;
    }


    /**
     * Sets the sourceSURL value for this TGetFileRequest.
     * 
     * @param sourceSURL
     */
    public void setSourceSURL(org.apache.axis.types.URI sourceSURL) {
        this.sourceSURL = sourceSURL;
    }


    /**
     * Gets the dirOption value for this TGetFileRequest.
     * 
     * @return dirOption
     */
    public org.dcache.srm.v2_2.TDirOption getDirOption() {
        return dirOption;
    }


    /**
     * Sets the dirOption value for this TGetFileRequest.
     * 
     * @param dirOption
     */
    public void setDirOption(org.dcache.srm.v2_2.TDirOption dirOption) {
        this.dirOption = dirOption;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TGetFileRequest)) {
            return false;
        }
        TGetFileRequest other = (TGetFileRequest) obj;
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
            ((this.sourceSURL==null && other.getSourceSURL()==null) || 
             (this.sourceSURL!=null &&
              this.sourceSURL.equals(other.getSourceSURL()))) &&
            ((this.dirOption==null && other.getDirOption()==null) || 
             (this.dirOption!=null &&
              this.dirOption.equals(other.getDirOption())));
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
        if (getSourceSURL() != null) {
            _hashCode += getSourceSURL().hashCode();
        }
        if (getDirOption() != null) {
            _hashCode += getDirOption().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TGetFileRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGetFileRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("sourceSURL");
        elemField.setXmlName(new javax.xml.namespace.QName("", "sourceSURL"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "anyURI"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("dirOption");
        elemField.setXmlName(new javax.xml.namespace.QName("", "dirOption"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TDirOption"));
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

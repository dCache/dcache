/**
 * TOverwriteMode.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TOverwriteMode implements java.io.Serializable {
    private static final long serialVersionUID = -4649129205758258364L;
    private java.lang.String _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected TOverwriteMode(java.lang.String value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final java.lang.String _NEVER = "NEVER";
    public static final java.lang.String _ALWAYS = "ALWAYS";
    public static final java.lang.String _WHEN_FILES_ARE_DIFFERENT = "WHEN_FILES_ARE_DIFFERENT";
    public static final TOverwriteMode NEVER = new TOverwriteMode(_NEVER);
    public static final TOverwriteMode ALWAYS = new TOverwriteMode(_ALWAYS);
    public static final TOverwriteMode WHEN_FILES_ARE_DIFFERENT = new TOverwriteMode(_WHEN_FILES_ARE_DIFFERENT);
    public java.lang.String getValue() { return _value_;}
    public static TOverwriteMode fromValue(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        TOverwriteMode enumeration = (TOverwriteMode)
            _table_.get(value);
        if (enumeration==null) {
            throw new IllegalArgumentException();
        }
        return enumeration;
    }
    public static TOverwriteMode fromString(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        return fromValue(value);
    }
    public boolean equals(java.lang.Object obj) {return (obj == this);}
    public int hashCode() { return toString().hashCode();}
    public java.lang.String toString() { return _value_;}
    public java.lang.Object readResolve() throws java.io.ObjectStreamException { return fromValue(_value_);}
    public static org.apache.axis.encoding.Serializer getSerializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new org.apache.axis.encoding.ser.EnumSerializer(
            _javaType, _xmlType);
    }
    public static org.apache.axis.encoding.Deserializer getDeserializer(
           java.lang.String mechType, 
           java.lang.Class _javaType,  
           javax.xml.namespace.QName _xmlType) {
        return 
          new org.apache.axis.encoding.ser.EnumDeserializer(
            _javaType, _xmlType);
    }
    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TOverwriteMode.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOverwriteMode"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

}

/**
 * TPermissionMode.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TPermissionMode implements java.io.Serializable {
    private static final long serialVersionUID = -110189838551472565L;
    private java.lang.String _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected TPermissionMode(java.lang.String value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final java.lang.String _NONE = "NONE";
    public static final java.lang.String _X = "X";
    public static final java.lang.String _W = "W";
    public static final java.lang.String _WX = "WX";
    public static final java.lang.String _R = "R";
    public static final java.lang.String _RX = "RX";
    public static final java.lang.String _RW = "RW";
    public static final java.lang.String _RWX = "RWX";
    public static final TPermissionMode NONE = new TPermissionMode(_NONE);
    public static final TPermissionMode X = new TPermissionMode(_X);
    public static final TPermissionMode W = new TPermissionMode(_W);
    public static final TPermissionMode WX = new TPermissionMode(_WX);
    public static final TPermissionMode R = new TPermissionMode(_R);
    public static final TPermissionMode RX = new TPermissionMode(_RX);
    public static final TPermissionMode RW = new TPermissionMode(_RW);
    public static final TPermissionMode RWX = new TPermissionMode(_RWX);
    public java.lang.String getValue() { return _value_;}
    public static TPermissionMode fromValue(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        TPermissionMode enumeration = (TPermissionMode)
            _table_.get(value);
        if (enumeration==null) {
            throw new IllegalArgumentException();
        }
        return enumeration;
    }
    public static TPermissionMode fromString(java.lang.String value)
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
        new org.apache.axis.description.TypeDesc(TPermissionMode.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionMode"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

}

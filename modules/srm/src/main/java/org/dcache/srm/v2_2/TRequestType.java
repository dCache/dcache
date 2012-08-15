/**
 * TRequestType.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TRequestType implements java.io.Serializable {
    private static final long serialVersionUID = 6433240101557904415L;
    private java.lang.String _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected TRequestType(java.lang.String value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final java.lang.String _PREPARE_TO_GET = "PREPARE_TO_GET";
    public static final java.lang.String _PREPARE_TO_PUT = "PREPARE_TO_PUT";
    public static final java.lang.String _COPY = "COPY";
    public static final java.lang.String _BRING_ONLINE = "BRING_ONLINE";
    public static final java.lang.String _RESERVE_SPACE = "RESERVE_SPACE";
    public static final java.lang.String _UPDATE_SPACE = "UPDATE_SPACE";
    public static final java.lang.String _CHANGE_SPACE_FOR_FILES = "CHANGE_SPACE_FOR_FILES";
    public static final java.lang.String _LS = "LS";
    public static final TRequestType PREPARE_TO_GET = new TRequestType(_PREPARE_TO_GET);
    public static final TRequestType PREPARE_TO_PUT = new TRequestType(_PREPARE_TO_PUT);
    public static final TRequestType COPY = new TRequestType(_COPY);
    public static final TRequestType BRING_ONLINE = new TRequestType(_BRING_ONLINE);
    public static final TRequestType RESERVE_SPACE = new TRequestType(_RESERVE_SPACE);
    public static final TRequestType UPDATE_SPACE = new TRequestType(_UPDATE_SPACE);
    public static final TRequestType CHANGE_SPACE_FOR_FILES = new TRequestType(_CHANGE_SPACE_FOR_FILES);
    public static final TRequestType LS = new TRequestType(_LS);
    public java.lang.String getValue() { return _value_;}
    public static TRequestType fromValue(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        TRequestType enumeration = (TRequestType)
            _table_.get(value);
        if (enumeration==null) {
            throw new IllegalArgumentException();
        }
        return enumeration;
    }
    public static TRequestType fromString(java.lang.String value)
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
        new org.apache.axis.description.TypeDesc(TRequestType.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestType"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

}

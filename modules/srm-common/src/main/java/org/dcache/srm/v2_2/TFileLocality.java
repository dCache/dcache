/**
 * TFileLocality.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TFileLocality implements java.io.Serializable {
    private static final long serialVersionUID = -3277627070020864025L;
    private java.lang.String _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected TFileLocality(java.lang.String value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final java.lang.String _ONLINE = "ONLINE";
    public static final java.lang.String _NEARLINE = "NEARLINE";
    public static final java.lang.String _ONLINE_AND_NEARLINE = "ONLINE_AND_NEARLINE";
    public static final java.lang.String _LOST = "LOST";
    public static final java.lang.String _NONE = "NONE";
    public static final java.lang.String _UNAVAILABLE = "UNAVAILABLE";
    public static final TFileLocality ONLINE = new TFileLocality(_ONLINE);
    public static final TFileLocality NEARLINE = new TFileLocality(_NEARLINE);
    public static final TFileLocality ONLINE_AND_NEARLINE = new TFileLocality(_ONLINE_AND_NEARLINE);
    public static final TFileLocality LOST = new TFileLocality(_LOST);
    public static final TFileLocality NONE = new TFileLocality(_NONE);
    public static final TFileLocality UNAVAILABLE = new TFileLocality(_UNAVAILABLE);
    public java.lang.String getValue() { return _value_;}
    public static TFileLocality fromValue(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        TFileLocality enumeration = (TFileLocality)
            _table_.get(value);
        if (enumeration==null) {
            throw new IllegalArgumentException();
        }
        return enumeration;
    }
    public static TFileLocality fromString(java.lang.String value)
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
        new org.apache.axis.description.TypeDesc(TFileLocality.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileLocality"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

}

/**
 * TRetentionPolicy.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TRetentionPolicy implements java.io.Serializable {
    private static final long serialVersionUID = 8061143506699926498L;
    private java.lang.String _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected TRetentionPolicy(java.lang.String value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final java.lang.String _REPLICA = "REPLICA";
    public static final java.lang.String _OUTPUT = "OUTPUT";
    public static final java.lang.String _CUSTODIAL = "CUSTODIAL";
    public static final TRetentionPolicy REPLICA = new TRetentionPolicy(_REPLICA);
    public static final TRetentionPolicy OUTPUT = new TRetentionPolicy(_OUTPUT);
    public static final TRetentionPolicy CUSTODIAL = new TRetentionPolicy(_CUSTODIAL);
    public java.lang.String getValue() { return _value_;}
    public static TRetentionPolicy fromValue(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        TRetentionPolicy enumeration = (TRetentionPolicy)
            _table_.get(value);
        if (enumeration==null) {
            throw new IllegalArgumentException();
        }
        return enumeration;
    }
    public static TRetentionPolicy fromString(java.lang.String value)
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
        new org.apache.axis.description.TypeDesc(TRetentionPolicy.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicy"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

}

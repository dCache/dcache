/**
 * TStatusCode.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TStatusCode implements java.io.Serializable {
    private static final long serialVersionUID = 4922474801320959743L;
    private java.lang.String _value_;
    private static java.util.HashMap _table_ = new java.util.HashMap();

    // Constructor
    protected TStatusCode(java.lang.String value) {
        _value_ = value;
        _table_.put(_value_,this);
    }

    public static final java.lang.String _SRM_SUCCESS = "SRM_SUCCESS";
    public static final java.lang.String _SRM_FAILURE = "SRM_FAILURE";
    public static final java.lang.String _SRM_AUTHENTICATION_FAILURE = "SRM_AUTHENTICATION_FAILURE";
    public static final java.lang.String _SRM_AUTHORIZATION_FAILURE = "SRM_AUTHORIZATION_FAILURE";
    public static final java.lang.String _SRM_INVALID_REQUEST = "SRM_INVALID_REQUEST";
    public static final java.lang.String _SRM_INVALID_PATH = "SRM_INVALID_PATH";
    public static final java.lang.String _SRM_FILE_LIFETIME_EXPIRED = "SRM_FILE_LIFETIME_EXPIRED";
    public static final java.lang.String _SRM_SPACE_LIFETIME_EXPIRED = "SRM_SPACE_LIFETIME_EXPIRED";
    public static final java.lang.String _SRM_EXCEED_ALLOCATION = "SRM_EXCEED_ALLOCATION";
    public static final java.lang.String _SRM_NO_USER_SPACE = "SRM_NO_USER_SPACE";
    public static final java.lang.String _SRM_NO_FREE_SPACE = "SRM_NO_FREE_SPACE";
    public static final java.lang.String _SRM_DUPLICATION_ERROR = "SRM_DUPLICATION_ERROR";
    public static final java.lang.String _SRM_NON_EMPTY_DIRECTORY = "SRM_NON_EMPTY_DIRECTORY";
    public static final java.lang.String _SRM_TOO_MANY_RESULTS = "SRM_TOO_MANY_RESULTS";
    public static final java.lang.String _SRM_INTERNAL_ERROR = "SRM_INTERNAL_ERROR";
    public static final java.lang.String _SRM_FATAL_INTERNAL_ERROR = "SRM_FATAL_INTERNAL_ERROR";
    public static final java.lang.String _SRM_NOT_SUPPORTED = "SRM_NOT_SUPPORTED";
    public static final java.lang.String _SRM_REQUEST_QUEUED = "SRM_REQUEST_QUEUED";
    public static final java.lang.String _SRM_REQUEST_INPROGRESS = "SRM_REQUEST_INPROGRESS";
    public static final java.lang.String _SRM_REQUEST_SUSPENDED = "SRM_REQUEST_SUSPENDED";
    public static final java.lang.String _SRM_ABORTED = "SRM_ABORTED";
    public static final java.lang.String _SRM_RELEASED = "SRM_RELEASED";
    public static final java.lang.String _SRM_FILE_PINNED = "SRM_FILE_PINNED";
    public static final java.lang.String _SRM_FILE_IN_CACHE = "SRM_FILE_IN_CACHE";
    public static final java.lang.String _SRM_SPACE_AVAILABLE = "SRM_SPACE_AVAILABLE";
    public static final java.lang.String _SRM_LOWER_SPACE_GRANTED = "SRM_LOWER_SPACE_GRANTED";
    public static final java.lang.String _SRM_DONE = "SRM_DONE";
    public static final java.lang.String _SRM_PARTIAL_SUCCESS = "SRM_PARTIAL_SUCCESS";
    public static final java.lang.String _SRM_REQUEST_TIMED_OUT = "SRM_REQUEST_TIMED_OUT";
    public static final java.lang.String _SRM_LAST_COPY = "SRM_LAST_COPY";
    public static final java.lang.String _SRM_FILE_BUSY = "SRM_FILE_BUSY";
    public static final java.lang.String _SRM_FILE_LOST = "SRM_FILE_LOST";
    public static final java.lang.String _SRM_FILE_UNAVAILABLE = "SRM_FILE_UNAVAILABLE";
    public static final java.lang.String _SRM_CUSTOM_STATUS = "SRM_CUSTOM_STATUS";
    public static final TStatusCode SRM_SUCCESS = new TStatusCode(_SRM_SUCCESS);
    public static final TStatusCode SRM_FAILURE = new TStatusCode(_SRM_FAILURE);
    public static final TStatusCode SRM_AUTHENTICATION_FAILURE = new TStatusCode(_SRM_AUTHENTICATION_FAILURE);
    public static final TStatusCode SRM_AUTHORIZATION_FAILURE = new TStatusCode(_SRM_AUTHORIZATION_FAILURE);
    public static final TStatusCode SRM_INVALID_REQUEST = new TStatusCode(_SRM_INVALID_REQUEST);
    public static final TStatusCode SRM_INVALID_PATH = new TStatusCode(_SRM_INVALID_PATH);
    public static final TStatusCode SRM_FILE_LIFETIME_EXPIRED = new TStatusCode(_SRM_FILE_LIFETIME_EXPIRED);
    public static final TStatusCode SRM_SPACE_LIFETIME_EXPIRED = new TStatusCode(_SRM_SPACE_LIFETIME_EXPIRED);
    public static final TStatusCode SRM_EXCEED_ALLOCATION = new TStatusCode(_SRM_EXCEED_ALLOCATION);
    public static final TStatusCode SRM_NO_USER_SPACE = new TStatusCode(_SRM_NO_USER_SPACE);
    public static final TStatusCode SRM_NO_FREE_SPACE = new TStatusCode(_SRM_NO_FREE_SPACE);
    public static final TStatusCode SRM_DUPLICATION_ERROR = new TStatusCode(_SRM_DUPLICATION_ERROR);
    public static final TStatusCode SRM_NON_EMPTY_DIRECTORY = new TStatusCode(_SRM_NON_EMPTY_DIRECTORY);
    public static final TStatusCode SRM_TOO_MANY_RESULTS = new TStatusCode(_SRM_TOO_MANY_RESULTS);
    public static final TStatusCode SRM_INTERNAL_ERROR = new TStatusCode(_SRM_INTERNAL_ERROR);
    public static final TStatusCode SRM_FATAL_INTERNAL_ERROR = new TStatusCode(_SRM_FATAL_INTERNAL_ERROR);
    public static final TStatusCode SRM_NOT_SUPPORTED = new TStatusCode(_SRM_NOT_SUPPORTED);
    public static final TStatusCode SRM_REQUEST_QUEUED = new TStatusCode(_SRM_REQUEST_QUEUED);
    public static final TStatusCode SRM_REQUEST_INPROGRESS = new TStatusCode(_SRM_REQUEST_INPROGRESS);
    public static final TStatusCode SRM_REQUEST_SUSPENDED = new TStatusCode(_SRM_REQUEST_SUSPENDED);
    public static final TStatusCode SRM_ABORTED = new TStatusCode(_SRM_ABORTED);
    public static final TStatusCode SRM_RELEASED = new TStatusCode(_SRM_RELEASED);
    public static final TStatusCode SRM_FILE_PINNED = new TStatusCode(_SRM_FILE_PINNED);
    public static final TStatusCode SRM_FILE_IN_CACHE = new TStatusCode(_SRM_FILE_IN_CACHE);
    public static final TStatusCode SRM_SPACE_AVAILABLE = new TStatusCode(_SRM_SPACE_AVAILABLE);
    public static final TStatusCode SRM_LOWER_SPACE_GRANTED = new TStatusCode(_SRM_LOWER_SPACE_GRANTED);
    public static final TStatusCode SRM_DONE = new TStatusCode(_SRM_DONE);
    public static final TStatusCode SRM_PARTIAL_SUCCESS = new TStatusCode(_SRM_PARTIAL_SUCCESS);
    public static final TStatusCode SRM_REQUEST_TIMED_OUT = new TStatusCode(_SRM_REQUEST_TIMED_OUT);
    public static final TStatusCode SRM_LAST_COPY = new TStatusCode(_SRM_LAST_COPY);
    public static final TStatusCode SRM_FILE_BUSY = new TStatusCode(_SRM_FILE_BUSY);
    public static final TStatusCode SRM_FILE_LOST = new TStatusCode(_SRM_FILE_LOST);
    public static final TStatusCode SRM_FILE_UNAVAILABLE = new TStatusCode(_SRM_FILE_UNAVAILABLE);
    public static final TStatusCode SRM_CUSTOM_STATUS = new TStatusCode(_SRM_CUSTOM_STATUS);
    public java.lang.String getValue() { return _value_;}
    public static TStatusCode fromValue(java.lang.String value)
          throws java.lang.IllegalArgumentException {
        TStatusCode enumeration = (TStatusCode)
            _table_.get(value);
        if (enumeration==null) {
            throw new IllegalArgumentException();
        }
        return enumeration;
    }
    public static TStatusCode fromString(java.lang.String value)
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
        new org.apache.axis.description.TypeDesc(TStatusCode.class);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TStatusCode"));
    }
    /**
     * Return type metadata object
     */
    public static org.apache.axis.description.TypeDesc getTypeDesc() {
        return typeDesc;
    }

    public boolean isProcessing()
    {
        return this == SRM_REQUEST_QUEUED || this == SRM_REQUEST_INPROGRESS;
    }
}

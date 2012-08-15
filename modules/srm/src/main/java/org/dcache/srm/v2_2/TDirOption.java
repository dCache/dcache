/**
 * TDirOption.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class TDirOption  implements java.io.Serializable {
    private static final long serialVersionUID = -3698115653557285152L;
    private boolean isSourceADirectory;

    private java.lang.Boolean allLevelRecursive;

    private java.lang.Integer numOfLevels;

    public TDirOption() {
    }

    public TDirOption(
           boolean isSourceADirectory,
           java.lang.Boolean allLevelRecursive,
           java.lang.Integer numOfLevels) {
           this.isSourceADirectory = isSourceADirectory;
           this.allLevelRecursive = allLevelRecursive;
           this.numOfLevels = numOfLevels;
    }


    /**
     * Gets the isSourceADirectory value for this TDirOption.
     * 
     * @return isSourceADirectory
     */
    public boolean isIsSourceADirectory() {
        return isSourceADirectory;
    }


    /**
     * Sets the isSourceADirectory value for this TDirOption.
     * 
     * @param isSourceADirectory
     */
    public void setIsSourceADirectory(boolean isSourceADirectory) {
        this.isSourceADirectory = isSourceADirectory;
    }


    /**
     * Gets the allLevelRecursive value for this TDirOption.
     * 
     * @return allLevelRecursive
     */
    public java.lang.Boolean getAllLevelRecursive() {
        return allLevelRecursive;
    }


    /**
     * Sets the allLevelRecursive value for this TDirOption.
     * 
     * @param allLevelRecursive
     */
    public void setAllLevelRecursive(java.lang.Boolean allLevelRecursive) {
        this.allLevelRecursive = allLevelRecursive;
    }


    /**
     * Gets the numOfLevels value for this TDirOption.
     * 
     * @return numOfLevels
     */
    public java.lang.Integer getNumOfLevels() {
        return numOfLevels;
    }


    /**
     * Sets the numOfLevels value for this TDirOption.
     * 
     * @param numOfLevels
     */
    public void setNumOfLevels(java.lang.Integer numOfLevels) {
        this.numOfLevels = numOfLevels;
    }

    private java.lang.Object __equalsCalc;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof TDirOption)) {
            return false;
        }
        TDirOption other = (TDirOption) obj;
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
            this.isSourceADirectory == other.isIsSourceADirectory() &&
            ((this.allLevelRecursive==null && other.getAllLevelRecursive()==null) || 
             (this.allLevelRecursive!=null &&
              this.allLevelRecursive.equals(other.getAllLevelRecursive()))) &&
            ((this.numOfLevels==null && other.getNumOfLevels()==null) || 
             (this.numOfLevels!=null &&
              this.numOfLevels.equals(other.getNumOfLevels())));
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
        _hashCode += (isIsSourceADirectory() ? Boolean.TRUE : Boolean.FALSE).hashCode();
        if (getAllLevelRecursive() != null) {
            _hashCode += getAllLevelRecursive().hashCode();
        }
        if (getNumOfLevels() != null) {
            _hashCode += getNumOfLevels().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(TDirOption.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TDirOption"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("isSourceADirectory");
        elemField.setXmlName(new javax.xml.namespace.QName("", "isSourceADirectory"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("allLevelRecursive");
        elemField.setXmlName(new javax.xml.namespace.QName("", "allLevelRecursive"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "boolean"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("numOfLevels");
        elemField.setXmlName(new javax.xml.namespace.QName("", "numOfLevels"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://www.w3.org/2001/XMLSchema", "int"));
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

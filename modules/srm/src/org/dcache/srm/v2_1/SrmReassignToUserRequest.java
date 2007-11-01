/**
 * SrmReassignToUserRequest.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmReassignToUserRequest  implements java.io.Serializable {
    private org.dcache.srm.v2_1.TUserID userID;

    private org.dcache.srm.v2_1.TUserID assignedUser;

    private org.dcache.srm.v2_1.TLifeTimeInSeconds lifeTimeOfThisAssignment;

    private org.dcache.srm.v2_1.TSURLInfo path;

    public SrmReassignToUserRequest() {
    }

    public SrmReassignToUserRequest(
           org.dcache.srm.v2_1.TUserID userID,
           org.dcache.srm.v2_1.TUserID assignedUser,
           org.dcache.srm.v2_1.TLifeTimeInSeconds lifeTimeOfThisAssignment,
           org.dcache.srm.v2_1.TSURLInfo path) {
           this.userID = userID;
           this.assignedUser = assignedUser;
           this.lifeTimeOfThisAssignment = lifeTimeOfThisAssignment;
           this.path = path;
    }


    /**
     * Gets the userID value for this SrmReassignToUserRequest.
     * 
     * @return userID
     */
    public org.dcache.srm.v2_1.TUserID getUserID() {
        return userID;
    }


    /**
     * Sets the userID value for this SrmReassignToUserRequest.
     * 
     * @param userID
     */
    public void setUserID(org.dcache.srm.v2_1.TUserID userID) {
        this.userID = userID;
    }


    /**
     * Gets the assignedUser value for this SrmReassignToUserRequest.
     * 
     * @return assignedUser
     */
    public org.dcache.srm.v2_1.TUserID getAssignedUser() {
        return assignedUser;
    }


    /**
     * Sets the assignedUser value for this SrmReassignToUserRequest.
     * 
     * @param assignedUser
     */
    public void setAssignedUser(org.dcache.srm.v2_1.TUserID assignedUser) {
        this.assignedUser = assignedUser;
    }


    /**
     * Gets the lifeTimeOfThisAssignment value for this SrmReassignToUserRequest.
     * 
     * @return lifeTimeOfThisAssignment
     */
    public org.dcache.srm.v2_1.TLifeTimeInSeconds getLifeTimeOfThisAssignment() {
        return lifeTimeOfThisAssignment;
    }


    /**
     * Sets the lifeTimeOfThisAssignment value for this SrmReassignToUserRequest.
     * 
     * @param lifeTimeOfThisAssignment
     */
    public void setLifeTimeOfThisAssignment(org.dcache.srm.v2_1.TLifeTimeInSeconds lifeTimeOfThisAssignment) {
        this.lifeTimeOfThisAssignment = lifeTimeOfThisAssignment;
    }


    /**
     * Gets the path value for this SrmReassignToUserRequest.
     * 
     * @return path
     */
    public org.dcache.srm.v2_1.TSURLInfo getPath() {
        return path;
    }


    /**
     * Sets the path value for this SrmReassignToUserRequest.
     * 
     * @param path
     */
    public void setPath(org.dcache.srm.v2_1.TSURLInfo path) {
        this.path = path;
    }

    private java.lang.Object __equalsCalc = null;
    public synchronized boolean equals(java.lang.Object obj) {
        if (!(obj instanceof SrmReassignToUserRequest)) return false;
        SrmReassignToUserRequest other = (SrmReassignToUserRequest) obj;
        if (obj == null) return false;
        if (this == obj) return true;
        if (__equalsCalc != null) {
            return (__equalsCalc == obj);
        }
        __equalsCalc = obj;
        boolean _equals;
        _equals = true && 
            ((this.userID==null && other.getUserID()==null) || 
             (this.userID!=null &&
              this.userID.equals(other.getUserID()))) &&
            ((this.assignedUser==null && other.getAssignedUser()==null) || 
             (this.assignedUser!=null &&
              this.assignedUser.equals(other.getAssignedUser()))) &&
            ((this.lifeTimeOfThisAssignment==null && other.getLifeTimeOfThisAssignment()==null) || 
             (this.lifeTimeOfThisAssignment!=null &&
              this.lifeTimeOfThisAssignment.equals(other.getLifeTimeOfThisAssignment()))) &&
            ((this.path==null && other.getPath()==null) || 
             (this.path!=null &&
              this.path.equals(other.getPath())));
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
        if (getUserID() != null) {
            _hashCode += getUserID().hashCode();
        }
        if (getAssignedUser() != null) {
            _hashCode += getAssignedUser().hashCode();
        }
        if (getLifeTimeOfThisAssignment() != null) {
            _hashCode += getLifeTimeOfThisAssignment().hashCode();
        }
        if (getPath() != null) {
            _hashCode += getPath().hashCode();
        }
        __hashCodeCalc = false;
        return _hashCode;
    }

    // Type metadata
    private static org.apache.axis.description.TypeDesc typeDesc =
        new org.apache.axis.description.TypeDesc(SrmReassignToUserRequest.class, true);

    static {
        typeDesc.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReassignToUserRequest"));
        org.apache.axis.description.ElementDesc elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("userID");
        elemField.setXmlName(new javax.xml.namespace.QName("", "userID"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setMinOccurs(0);
        elemField.setNillable(true);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("assignedUser");
        elemField.setXmlName(new javax.xml.namespace.QName("", "assignedUser"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("lifeTimeOfThisAssignment");
        elemField.setXmlName(new javax.xml.namespace.QName("", "lifeTimeOfThisAssignment"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds"));
        elemField.setNillable(false);
        typeDesc.addFieldDesc(elemField);
        elemField = new org.apache.axis.description.ElementDesc();
        elemField.setFieldName("path");
        elemField.setXmlName(new javax.xml.namespace.QName("", "path"));
        elemField.setXmlType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLInfo"));
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

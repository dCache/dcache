/**
 * SrmSoapBindingStub.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_1;

public class SrmSoapBindingStub extends org.apache.axis.client.Stub implements org.dcache.srm.v2_1.ISRM {
    private java.util.Vector cachedSerClasses = new java.util.Vector();
    private java.util.Vector cachedSerQNames = new java.util.Vector();
    private java.util.Vector cachedSerFactories = new java.util.Vector();
    private java.util.Vector cachedDeserFactories = new java.util.Vector();

    static org.apache.axis.description.OperationDesc [] _operations;

    static {
        _operations = new org.apache.axis.description.OperationDesc[31];
        _initOperationDesc1();
        _initOperationDesc2();
        _initOperationDesc3();
        _initOperationDesc4();
    }

    private static void _initOperationDesc1(){
        org.apache.axis.description.OperationDesc oper;
        org.apache.axis.description.ParameterDesc param;
        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmReserveSpace");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmReserveSpaceRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceRequest"), org.dcache.srm.v2_1.SrmReserveSpaceRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmReserveSpaceResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmReserveSpaceResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[0] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmReleaseSpace");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmReleaseSpaceRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpaceRequest"), org.dcache.srm.v2_1.SrmReleaseSpaceRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpaceResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmReleaseSpaceResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmReleaseSpaceResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[1] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmUpdateSpace");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmUpdateSpaceRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceRequest"), org.dcache.srm.v2_1.SrmUpdateSpaceRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmUpdateSpaceResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmUpdateSpaceResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[2] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmCompactSpace");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmCompactSpaceRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCompactSpaceRequest"), org.dcache.srm.v2_1.SrmCompactSpaceRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCompactSpaceResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmCompactSpaceResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmCompactSpaceResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[3] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetSpaceMetaData");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetSpaceMetaDataRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataRequest"), org.dcache.srm.v2_1.SrmGetSpaceMetaDataRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmGetSpaceMetaDataResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetSpaceMetaDataResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[4] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmChangeFileStorageType");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmChangeFileStorageTypeRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeFileStorageTypeRequest"), org.dcache.srm.v2_1.SrmChangeFileStorageTypeRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeFileStorageTypeResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmChangeFileStorageTypeResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmChangeFileStorageTypeResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[5] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetSpaceToken");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetSpaceTokenRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokenRequest"), org.dcache.srm.v2_1.SrmGetSpaceTokenRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokenResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmGetSpaceTokenResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetSpaceTokenResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[6] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmSetPermission");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmSetPermissionRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermissionRequest"), org.dcache.srm.v2_1.SrmSetPermissionRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermissionResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmSetPermissionResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmSetPermissionResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[7] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmReassignToUser");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmReassignToUserRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReassignToUserRequest"), org.dcache.srm.v2_1.SrmReassignToUserRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReassignToUserResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmReassignToUserResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmReassignToUserResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[8] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmCheckPermission");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmCheckPermissionRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermissionRequest"), org.dcache.srm.v2_1.SrmCheckPermissionRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermissionResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmCheckPermissionResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmCheckPermissionResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[9] = oper;

    }

    private static void _initOperationDesc2(){
        org.apache.axis.description.OperationDesc oper;
        org.apache.axis.description.ParameterDesc param;
        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmMkdir");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmMkdirRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdirRequest"), org.dcache.srm.v2_1.SrmMkdirRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdirResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmMkdirResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmMkdirResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[10] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmRmdir");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmRmdirRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdirRequest"), org.dcache.srm.v2_1.SrmRmdirRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdirResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmRmdirResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmRmdirResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[11] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmRm");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmRmRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmRequest"), org.dcache.srm.v2_1.SrmRmRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmRmResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmRmResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[12] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmLs");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmLsRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLsRequest"), org.dcache.srm.v2_1.SrmLsRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLsResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmLsResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmLsResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[13] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmMv");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmMvRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvRequest"), org.dcache.srm.v2_1.SrmMvRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmMvResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmMvResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[14] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmPrepareToGet");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmPrepareToGetRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGetRequest"), org.dcache.srm.v2_1.SrmPrepareToGetRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGetResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmPrepareToGetResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmPrepareToGetResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[15] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmPrepareToPut");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmPrepareToPutRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPutRequest"), org.dcache.srm.v2_1.SrmPrepareToPutRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPutResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmPrepareToPutResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmPrepareToPutResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[16] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmCopy");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmCopyRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyRequest"), org.dcache.srm.v2_1.SrmCopyRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmCopyResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmCopyResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[17] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmRemoveFiles");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmRemoveFilesRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRemoveFilesRequest"), org.dcache.srm.v2_1.SrmRemoveFilesRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRemoveFilesResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmRemoveFilesResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmRemoveFilesResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[18] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmReleaseFiles");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmReleaseFilesRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseFilesRequest"), org.dcache.srm.v2_1.SrmReleaseFilesRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseFilesResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmReleaseFilesResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmReleaseFilesResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[19] = oper;

    }

    private static void _initOperationDesc3(){
        org.apache.axis.description.OperationDesc oper;
        org.apache.axis.description.ParameterDesc param;
        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmPutDone");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmPutDoneRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPutDoneRequest"), org.dcache.srm.v2_1.SrmPutDoneRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPutDoneResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmPutDoneResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmPutDoneResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[20] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmAbortRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmAbortRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortRequestRequest"), org.dcache.srm.v2_1.SrmAbortRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmAbortRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmAbortRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[21] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmAbortFiles");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmAbortFilesRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortFilesRequest"), org.dcache.srm.v2_1.SrmAbortFilesRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortFilesResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmAbortFilesResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmAbortFilesResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[22] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmSuspendRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmSuspendRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSuspendRequestRequest"), org.dcache.srm.v2_1.SrmSuspendRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSuspendRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmSuspendRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmSuspendRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[23] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmResumeRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmResumeRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmResumeRequestRequest"), org.dcache.srm.v2_1.SrmResumeRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmResumeRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmResumeRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmResumeRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[24] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfGetRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfGetRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfGetRequestRequest"), org.dcache.srm.v2_1.SrmStatusOfGetRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfGetRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmStatusOfGetRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfGetRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[25] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfPutRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfPutRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequestRequest"), org.dcache.srm.v2_1.SrmStatusOfPutRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmStatusOfPutRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfPutRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[26] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfCopyRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfCopyRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequestRequest"), org.dcache.srm.v2_1.SrmStatusOfCopyRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmStatusOfCopyRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfCopyRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[27] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetRequestSummary");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetRequestSummaryRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummaryRequest"), org.dcache.srm.v2_1.SrmGetRequestSummaryRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummaryResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmGetRequestSummaryResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetRequestSummaryResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[28] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmExtendFileLifeTime");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmExtendFileLifeTimeRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeRequest"), org.dcache.srm.v2_1.SrmExtendFileLifeTimeRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmExtendFileLifeTimeResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmExtendFileLifeTimeResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[29] = oper;

    }

    private static void _initOperationDesc4(){
        org.apache.axis.description.OperationDesc oper;
        org.apache.axis.description.ParameterDesc param;
        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetRequestID");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetRequestIDRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestIDRequest"), org.dcache.srm.v2_1.SrmGetRequestIDRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestIDResponse"));
        oper.setReturnClass(org.dcache.srm.v2_1.SrmGetRequestIDResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetRequestIDResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[30] = oper;

    }

    public SrmSoapBindingStub() throws org.apache.axis.AxisFault {
         this(null);
    }

    public SrmSoapBindingStub(java.net.URL endpointURL, javax.xml.rpc.Service service) throws org.apache.axis.AxisFault {
         this(service);
         super.cachedEndpoint = endpointURL;
    }

    public SrmSoapBindingStub(javax.xml.rpc.Service service) throws org.apache.axis.AxisFault {
        if (service == null) {
            super.service = new org.apache.axis.client.Service();
        } else {
            super.service = service;
        }
        ((org.apache.axis.client.Service)super.service).setTypeMappingVersion("1.2");
            java.lang.Class cls;
            javax.xml.namespace.QName qName;
            javax.xml.namespace.QName qName2;
            java.lang.Class beansf = org.apache.axis.encoding.ser.BeanSerializerFactory.class;
            java.lang.Class beandf = org.apache.axis.encoding.ser.BeanDeserializerFactory.class;
            java.lang.Class enumsf = org.apache.axis.encoding.ser.EnumSerializerFactory.class;
            java.lang.Class enumdf = org.apache.axis.encoding.ser.EnumDeserializerFactory.class;
            java.lang.Class arraysf = org.apache.axis.encoding.ser.ArraySerializerFactory.class;
            java.lang.Class arraydf = org.apache.axis.encoding.ser.ArrayDeserializerFactory.class;
            java.lang.Class simplesf = org.apache.axis.encoding.ser.SimpleSerializerFactory.class;
            java.lang.Class simpledf = org.apache.axis.encoding.ser.SimpleDeserializerFactory.class;
            java.lang.Class simplelistsf = org.apache.axis.encoding.ser.SimpleListSerializerFactory.class;
            java.lang.Class simplelistdf = org.apache.axis.encoding.ser.SimpleListDeserializerFactory.class;
        addBindings0();
        addBindings1();
    }

    private void addBindings0() {
            java.lang.Class cls;
            javax.xml.namespace.QName qName;
            javax.xml.namespace.QName qName2;
            java.lang.Class beansf = org.apache.axis.encoding.ser.BeanSerializerFactory.class;
            java.lang.Class beandf = org.apache.axis.encoding.ser.BeanDeserializerFactory.class;
            java.lang.Class enumsf = org.apache.axis.encoding.ser.EnumSerializerFactory.class;
            java.lang.Class enumdf = org.apache.axis.encoding.ser.EnumDeserializerFactory.class;
            java.lang.Class arraysf = org.apache.axis.encoding.ser.ArraySerializerFactory.class;
            java.lang.Class arraydf = org.apache.axis.encoding.ser.ArrayDeserializerFactory.class;
            java.lang.Class simplesf = org.apache.axis.encoding.ser.SimpleSerializerFactory.class;
            java.lang.Class simpledf = org.apache.axis.encoding.ser.SimpleDeserializerFactory.class;
            java.lang.Class simplelistsf = org.apache.axis.encoding.ser.SimpleListSerializerFactory.class;
            java.lang.Class simplelistdf = org.apache.axis.encoding.ser.SimpleListDeserializerFactory.class;
            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOf_xsd_string");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOf_xsd_string.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTCopyFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTCopyFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTCopyRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTCopyRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGetFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTGetFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGetRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTGetRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGroupPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTGroupPermission.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTMetaDataPathDetail");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTMetaDataPathDetail.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTMetaDataSpace");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTMetaDataSpace.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTPutFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTPutFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTPutRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTPutRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTRequestSummary");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTRequestSummary.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTRequestToken");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTRequestToken.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSpaceToken");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTSpaceToken.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURL");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTSURL.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLInfo");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTSURLInfo.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLPermissionReturn");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTSURLPermissionReturn.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLReturnStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTSURLReturnStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTUserPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.ArrayOfTUserPermission.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortFilesRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmAbortFilesRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortFilesResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmAbortFilesResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmAbortRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmAbortRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeFileStorageTypeRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmChangeFileStorageTypeRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeFileStorageTypeResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmChangeFileStorageTypeResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermissionRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmCheckPermissionRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermissionResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmCheckPermissionResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCompactSpaceRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmCompactSpaceRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCompactSpaceResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmCompactSpaceResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmCopyRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmCopyResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmExtendFileLifeTimeRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmExtendFileLifeTimeResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestIDRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmGetRequestIDRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestIDResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmGetRequestIDResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummaryRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmGetRequestSummaryRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummaryResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmGetRequestSummaryResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmGetSpaceMetaDataRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmGetSpaceMetaDataResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokenRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmGetSpaceTokenRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokenResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmGetSpaceTokenResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLsRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmLsRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLsResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmLsResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdirRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmMkdirRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdirResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmMkdirResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmMvRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmMvResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGetRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmPrepareToGetRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGetResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmPrepareToGetResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPutRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmPrepareToPutRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPutResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmPrepareToPutResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPutDoneRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmPutDoneRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPutDoneResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmPutDoneResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReassignToUserRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmReassignToUserRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReassignToUserResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmReassignToUserResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseFilesRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmReleaseFilesRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseFilesResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmReleaseFilesResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpaceRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmReleaseSpaceRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpaceResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmReleaseSpaceResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRemoveFilesRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmRemoveFilesRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRemoveFilesResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmRemoveFilesResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmReserveSpaceRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmReserveSpaceResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmResumeRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmResumeRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmResumeRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmResumeRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdirRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmRmdirRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdirResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmRmdirResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmRmRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmRmResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermissionRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmSetPermissionRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermissionResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmSetPermissionResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmStatusOfCopyRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmStatusOfCopyRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfGetRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmStatusOfGetRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfGetRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmStatusOfGetRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmStatusOfPutRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmStatusOfPutRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSuspendRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmSuspendRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSuspendRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmSuspendRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmUpdateSpaceRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.SrmUpdateSpaceResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCheckSumType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TCheckSumType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCheckSumValue");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TCheckSumValue.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCopyFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TCopyFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCopyRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TCopyRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TDirOption");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TDirOption.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileStorageType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TFileStorageType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TFileType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGetFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TGetFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGetRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TGetRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGMTTime");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TGMTTime.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGroupID");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TGroupID.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGroupPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TGroupPermission.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TLifeTimeInSeconds");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TLifeTimeInSeconds.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TMetaDataPathDetail");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TMetaDataPathDetail.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TMetaDataSpace");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TMetaDataSpace.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOtherPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TOtherPermission.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOverwriteMode");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TOverwriteMode.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOwnerPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TOwnerPermission.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionMode");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TPermissionMode.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TPermissionType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

    }
    private void addBindings1() {
            java.lang.Class cls;
            javax.xml.namespace.QName qName;
            javax.xml.namespace.QName qName2;
            java.lang.Class beansf = org.apache.axis.encoding.ser.BeanSerializerFactory.class;
            java.lang.Class beandf = org.apache.axis.encoding.ser.BeanDeserializerFactory.class;
            java.lang.Class enumsf = org.apache.axis.encoding.ser.EnumSerializerFactory.class;
            java.lang.Class enumdf = org.apache.axis.encoding.ser.EnumDeserializerFactory.class;
            java.lang.Class arraysf = org.apache.axis.encoding.ser.ArraySerializerFactory.class;
            java.lang.Class arraydf = org.apache.axis.encoding.ser.ArrayDeserializerFactory.class;
            java.lang.Class simplesf = org.apache.axis.encoding.ser.SimpleSerializerFactory.class;
            java.lang.Class simpledf = org.apache.axis.encoding.ser.SimpleDeserializerFactory.class;
            java.lang.Class simplelistsf = org.apache.axis.encoding.ser.SimpleListSerializerFactory.class;
            java.lang.Class simplelistdf = org.apache.axis.encoding.ser.SimpleListDeserializerFactory.class;
            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPutFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TPutFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPutRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TPutRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestSummary");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TRequestSummary.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestToken");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TRequestToken.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TRequestType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TReturnStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSizeInBytes");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TSizeInBytes.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSpaceToken");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TSpaceToken.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSpaceType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TSpaceType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TStatusCode");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TStatusCode.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TStorageSystemInfo");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TStorageSystemInfo.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURL");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TSURL.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLInfo");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TSURLInfo.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLPermissionReturn");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TSURLPermissionReturn.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLReturnStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TSURLReturnStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TTURL");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TTURL.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserID");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TUserID.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_1.TUserPermission.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

    }

    protected org.apache.axis.client.Call createCall() throws java.rmi.RemoteException {
        try {
            org.apache.axis.client.Call _call = super._createCall();
            if (super.maintainSessionSet) {
                _call.setMaintainSession(super.maintainSession);
            }
            if (super.cachedUsername != null) {
                _call.setUsername(super.cachedUsername);
            }
            if (super.cachedPassword != null) {
                _call.setPassword(super.cachedPassword);
            }
            if (super.cachedEndpoint != null) {
                _call.setTargetEndpointAddress(super.cachedEndpoint);
            }
            if (super.cachedTimeout != null) {
                _call.setTimeout(super.cachedTimeout);
            }
            if (super.cachedPortName != null) {
                _call.setPortName(super.cachedPortName);
            }
            java.util.Enumeration keys = super.cachedProperties.keys();
            while (keys.hasMoreElements()) {
                java.lang.String key = (java.lang.String) keys.nextElement();
                _call.setProperty(key, super.cachedProperties.get(key));
            }
            // All the type mapping information is registered
            // when the first call is made.
            // The type mapping information is actually registered in
            // the TypeMappingRegistry of the service, which
            // is the reason why registration is only needed for the first call.
            synchronized (this) {
                if (firstCall()) {
                    // must set encoding style before registering serializers
                    _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
                    _call.setEncodingStyle(org.apache.axis.Constants.URI_SOAP11_ENC);
                    for (int i = 0; i < cachedSerFactories.size(); ++i) {
                        java.lang.Class cls = (java.lang.Class) cachedSerClasses.get(i);
                        javax.xml.namespace.QName qName =
                                (javax.xml.namespace.QName) cachedSerQNames.get(i);
                        java.lang.Object x = cachedSerFactories.get(i);
                        if (x instanceof Class) {
                            java.lang.Class sf = (java.lang.Class)
                                 cachedSerFactories.get(i);
                            java.lang.Class df = (java.lang.Class)
                                 cachedDeserFactories.get(i);
                            _call.registerTypeMapping(cls, qName, sf, df, false);
                        }
                        else if (x instanceof javax.xml.rpc.encoding.SerializerFactory) {
                            org.apache.axis.encoding.SerializerFactory sf = (org.apache.axis.encoding.SerializerFactory)
                                 cachedSerFactories.get(i);
                            org.apache.axis.encoding.DeserializerFactory df = (org.apache.axis.encoding.DeserializerFactory)
                                 cachedDeserFactories.get(i);
                            _call.registerTypeMapping(cls, qName, sf, df, false);
                        }
                    }
                }
            }
            return _call;
        }
        catch (java.lang.Throwable _t) {
            throw new org.apache.axis.AxisFault("Failure trying to get the Call object", _t);
        }
    }

    public org.dcache.srm.v2_1.SrmReserveSpaceResponse srmReserveSpace(org.dcache.srm.v2_1.SrmReserveSpaceRequest srmReserveSpaceRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[0]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpace"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmReserveSpaceRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmReserveSpaceResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmReserveSpaceResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmReserveSpaceResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmReleaseSpaceResponse srmReleaseSpace(org.dcache.srm.v2_1.SrmReleaseSpaceRequest srmReleaseSpaceRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[1]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpace"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmReleaseSpaceRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmReleaseSpaceResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmReleaseSpaceResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmReleaseSpaceResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmUpdateSpaceResponse srmUpdateSpace(org.dcache.srm.v2_1.SrmUpdateSpaceRequest srmUpdateSpaceRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[2]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpace"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmUpdateSpaceRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmUpdateSpaceResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmUpdateSpaceResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmUpdateSpaceResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmCompactSpaceResponse srmCompactSpace(org.dcache.srm.v2_1.SrmCompactSpaceRequest srmCompactSpaceRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[3]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCompactSpace"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmCompactSpaceRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmCompactSpaceResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmCompactSpaceResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmCompactSpaceResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmGetSpaceMetaDataResponse srmGetSpaceMetaData(org.dcache.srm.v2_1.SrmGetSpaceMetaDataRequest srmGetSpaceMetaDataRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[4]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaData"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmGetSpaceMetaDataRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmGetSpaceMetaDataResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmGetSpaceMetaDataResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmGetSpaceMetaDataResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmChangeFileStorageTypeResponse srmChangeFileStorageType(org.dcache.srm.v2_1.SrmChangeFileStorageTypeRequest srmChangeFileStorageTypeRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[5]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeFileStorageType"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmChangeFileStorageTypeRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmChangeFileStorageTypeResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmChangeFileStorageTypeResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmChangeFileStorageTypeResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmGetSpaceTokenResponse srmGetSpaceToken(org.dcache.srm.v2_1.SrmGetSpaceTokenRequest srmGetSpaceTokenRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[6]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceToken"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmGetSpaceTokenRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmGetSpaceTokenResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmGetSpaceTokenResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmGetSpaceTokenResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmSetPermissionResponse srmSetPermission(org.dcache.srm.v2_1.SrmSetPermissionRequest srmSetPermissionRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[7]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermission"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmSetPermissionRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmSetPermissionResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmSetPermissionResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmSetPermissionResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmReassignToUserResponse srmReassignToUser(org.dcache.srm.v2_1.SrmReassignToUserRequest srmReassignToUserRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[8]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReassignToUser"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmReassignToUserRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmReassignToUserResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmReassignToUserResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmReassignToUserResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmCheckPermissionResponse srmCheckPermission(org.dcache.srm.v2_1.SrmCheckPermissionRequest srmCheckPermissionRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[9]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermission"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmCheckPermissionRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmCheckPermissionResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmCheckPermissionResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmCheckPermissionResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmMkdirResponse srmMkdir(org.dcache.srm.v2_1.SrmMkdirRequest srmMkdirRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[10]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdir"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmMkdirRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmMkdirResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmMkdirResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmMkdirResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmRmdirResponse srmRmdir(org.dcache.srm.v2_1.SrmRmdirRequest srmRmdirRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[11]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdir"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmRmdirRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmRmdirResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmRmdirResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmRmdirResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmRmResponse srmRm(org.dcache.srm.v2_1.SrmRmRequest srmRmRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[12]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRm"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmRmRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmRmResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmRmResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmRmResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmLsResponse srmLs(org.dcache.srm.v2_1.SrmLsRequest srmLsRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[13]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLs"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmLsRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmLsResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmLsResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmLsResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmMvResponse srmMv(org.dcache.srm.v2_1.SrmMvRequest srmMvRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[14]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMv"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmMvRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmMvResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmMvResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmMvResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmPrepareToGetResponse srmPrepareToGet(org.dcache.srm.v2_1.SrmPrepareToGetRequest srmPrepareToGetRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[15]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGet"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmPrepareToGetRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmPrepareToGetResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmPrepareToGetResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmPrepareToGetResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmPrepareToPutResponse srmPrepareToPut(org.dcache.srm.v2_1.SrmPrepareToPutRequest srmPrepareToPutRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[16]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPut"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmPrepareToPutRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmPrepareToPutResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmPrepareToPutResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmPrepareToPutResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmCopyResponse srmCopy(org.dcache.srm.v2_1.SrmCopyRequest srmCopyRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[17]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopy"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmCopyRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmCopyResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmCopyResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmCopyResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmRemoveFilesResponse srmRemoveFiles(org.dcache.srm.v2_1.SrmRemoveFilesRequest srmRemoveFilesRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[18]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRemoveFiles"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmRemoveFilesRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmRemoveFilesResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmRemoveFilesResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmRemoveFilesResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmReleaseFilesResponse srmReleaseFiles(org.dcache.srm.v2_1.SrmReleaseFilesRequest srmReleaseFilesRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[19]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseFiles"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmReleaseFilesRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmReleaseFilesResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmReleaseFilesResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmReleaseFilesResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmPutDoneResponse srmPutDone(org.dcache.srm.v2_1.SrmPutDoneRequest srmPutDoneRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[20]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPutDone"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmPutDoneRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmPutDoneResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmPutDoneResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmPutDoneResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmAbortRequestResponse srmAbortRequest(org.dcache.srm.v2_1.SrmAbortRequestRequest srmAbortRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[21]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmAbortRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmAbortRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmAbortRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmAbortRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmAbortFilesResponse srmAbortFiles(org.dcache.srm.v2_1.SrmAbortFilesRequest srmAbortFilesRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[22]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortFiles"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmAbortFilesRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmAbortFilesResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmAbortFilesResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmAbortFilesResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmSuspendRequestResponse srmSuspendRequest(org.dcache.srm.v2_1.SrmSuspendRequestRequest srmSuspendRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[23]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSuspendRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmSuspendRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmSuspendRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmSuspendRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmSuspendRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmResumeRequestResponse srmResumeRequest(org.dcache.srm.v2_1.SrmResumeRequestRequest srmResumeRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[24]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmResumeRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmResumeRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmResumeRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmResumeRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmResumeRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmStatusOfGetRequestResponse srmStatusOfGetRequest(org.dcache.srm.v2_1.SrmStatusOfGetRequestRequest srmStatusOfGetRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[25]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfGetRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmStatusOfGetRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmStatusOfGetRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmStatusOfGetRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmStatusOfGetRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmStatusOfPutRequestResponse srmStatusOfPutRequest(org.dcache.srm.v2_1.SrmStatusOfPutRequestRequest srmStatusOfPutRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[26]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmStatusOfPutRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmStatusOfPutRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmStatusOfPutRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmStatusOfPutRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest(org.dcache.srm.v2_1.SrmStatusOfCopyRequestRequest srmStatusOfCopyRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[27]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmStatusOfCopyRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmStatusOfCopyRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmStatusOfCopyRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmStatusOfCopyRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmGetRequestSummaryResponse srmGetRequestSummary(org.dcache.srm.v2_1.SrmGetRequestSummaryRequest srmGetRequestSummaryRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[28]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummary"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmGetRequestSummaryRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmGetRequestSummaryResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmGetRequestSummaryResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmGetRequestSummaryResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmExtendFileLifeTimeResponse srmExtendFileLifeTime(org.dcache.srm.v2_1.SrmExtendFileLifeTimeRequest srmExtendFileLifeTimeRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[29]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTime"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmExtendFileLifeTimeRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmExtendFileLifeTimeResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmExtendFileLifeTimeResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmExtendFileLifeTimeResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    public org.dcache.srm.v2_1.SrmGetRequestIDResponse srmGetRequestID(org.dcache.srm.v2_1.SrmGetRequestIDRequest srmGetRequestIDRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[30]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestID"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmGetRequestIDRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_1.SrmGetRequestIDResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_1.SrmGetRequestIDResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_1.SrmGetRequestIDResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

}

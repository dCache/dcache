/**
 * SrmSoapBindingStub.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.3 Oct 05, 2005 (05:23:37 EDT) WSDL2Java emitter.
 */

package org.dcache.srm.v2_2;

public class SrmSoapBindingStub extends org.apache.axis.client.Stub implements org.dcache.srm.v2_2.ISRM {
    private java.util.Vector cachedSerClasses = new java.util.Vector();
    private java.util.Vector cachedSerQNames = new java.util.Vector();
    private java.util.Vector cachedSerFactories = new java.util.Vector();
    private java.util.Vector cachedDeserFactories = new java.util.Vector();

    static org.apache.axis.description.OperationDesc [] _operations;

    static {
        _operations = new org.apache.axis.description.OperationDesc[39];
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
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmReserveSpaceRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceRequest"), org.dcache.srm.v2_2.SrmReserveSpaceRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmReserveSpaceResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmReserveSpaceResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[0] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfReserveSpaceRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfReserveSpaceRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfReserveSpaceRequestRequest"), org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfReserveSpaceRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfReserveSpaceRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[1] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmReleaseSpace");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmReleaseSpaceRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpaceRequest"), org.dcache.srm.v2_2.SrmReleaseSpaceRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpaceResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmReleaseSpaceResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmReleaseSpaceResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[2] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmUpdateSpace");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmUpdateSpaceRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceRequest"), org.dcache.srm.v2_2.SrmUpdateSpaceRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmUpdateSpaceResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmUpdateSpaceResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[3] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfUpdateSpaceRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfUpdateSpaceRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfUpdateSpaceRequestRequest"), org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfUpdateSpaceRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfUpdateSpaceRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[4] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetSpaceMetaData");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetSpaceMetaDataRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataRequest"), org.dcache.srm.v2_2.SrmGetSpaceMetaDataRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetSpaceMetaDataResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[5] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmChangeSpaceForFiles");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmChangeSpaceForFilesRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeSpaceForFilesRequest"), org.dcache.srm.v2_2.SrmChangeSpaceForFilesRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeSpaceForFilesResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmChangeSpaceForFilesResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmChangeSpaceForFilesResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[6] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfChangeSpaceForFilesRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfChangeSpaceForFilesRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfChangeSpaceForFilesRequestRequest"), org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfChangeSpaceForFilesRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfChangeSpaceForFilesRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[7] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmExtendFileLifeTimeInSpace");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmExtendFileLifeTimeInSpaceRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeInSpaceRequest"), org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeInSpaceResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmExtendFileLifeTimeInSpaceResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[8] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmPurgeFromSpace");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmPurgeFromSpaceRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPurgeFromSpaceRequest"), org.dcache.srm.v2_2.SrmPurgeFromSpaceRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPurgeFromSpaceResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmPurgeFromSpaceResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmPurgeFromSpaceResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[9] = oper;

    }

    private static void _initOperationDesc2(){
        org.apache.axis.description.OperationDesc oper;
        org.apache.axis.description.ParameterDesc param;
        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetSpaceTokens");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetSpaceTokensRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokensRequest"), org.dcache.srm.v2_2.SrmGetSpaceTokensRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokensResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmGetSpaceTokensResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetSpaceTokensResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[10] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmSetPermission");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmSetPermissionRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermissionRequest"), org.dcache.srm.v2_2.SrmSetPermissionRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermissionResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmSetPermissionResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmSetPermissionResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[11] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmCheckPermission");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmCheckPermissionRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermissionRequest"), org.dcache.srm.v2_2.SrmCheckPermissionRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermissionResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmCheckPermissionResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmCheckPermissionResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[12] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetPermission");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetPermissionRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetPermissionRequest"), org.dcache.srm.v2_2.SrmGetPermissionRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetPermissionResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmGetPermissionResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetPermissionResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[13] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmMkdir");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmMkdirRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdirRequest"), org.dcache.srm.v2_2.SrmMkdirRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdirResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmMkdirResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmMkdirResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[14] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmRmdir");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmRmdirRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdirRequest"), org.dcache.srm.v2_2.SrmRmdirRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdirResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmRmdirResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmRmdirResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[15] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmRm");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmRmRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmRequest"), org.dcache.srm.v2_2.SrmRmRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmRmResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmRmResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[16] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmLs");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmLsRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLsRequest"), org.dcache.srm.v2_2.SrmLsRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLsResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmLsResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmLsResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[17] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfLsRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfLsRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfLsRequestRequest"), org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfLsRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfLsRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[18] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmMv");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmMvRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvRequest"), org.dcache.srm.v2_2.SrmMvRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmMvResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmMvResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[19] = oper;

    }

    private static void _initOperationDesc3(){
        org.apache.axis.description.OperationDesc oper;
        org.apache.axis.description.ParameterDesc param;
        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmPrepareToGet");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmPrepareToGetRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGetRequest"), org.dcache.srm.v2_2.SrmPrepareToGetRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGetResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmPrepareToGetResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmPrepareToGetResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[20] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfGetRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfGetRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfGetRequestRequest"), org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfGetRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfGetRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[21] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmBringOnline");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmBringOnlineRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmBringOnlineRequest"), org.dcache.srm.v2_2.SrmBringOnlineRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmBringOnlineResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmBringOnlineResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmBringOnlineResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[22] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfBringOnlineRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfBringOnlineRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfBringOnlineRequestRequest"), org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfBringOnlineRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfBringOnlineRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[23] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmPrepareToPut");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmPrepareToPutRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPutRequest"), org.dcache.srm.v2_2.SrmPrepareToPutRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPutResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmPrepareToPutResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmPrepareToPutResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[24] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfPutRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfPutRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequestRequest"), org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfPutRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[25] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmCopy");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmCopyRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyRequest"), org.dcache.srm.v2_2.SrmCopyRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmCopyResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmCopyResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[26] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmStatusOfCopyRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmStatusOfCopyRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequestRequest"), org.dcache.srm.v2_2.SrmStatusOfCopyRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmStatusOfCopyRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[27] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmReleaseFiles");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmReleaseFilesRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseFilesRequest"), org.dcache.srm.v2_2.SrmReleaseFilesRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseFilesResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmReleaseFilesResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmReleaseFilesResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[28] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmPutDone");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmPutDoneRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPutDoneRequest"), org.dcache.srm.v2_2.SrmPutDoneRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPutDoneResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmPutDoneResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmPutDoneResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[29] = oper;

    }

    private static void _initOperationDesc4(){
        org.apache.axis.description.OperationDesc oper;
        org.apache.axis.description.ParameterDesc param;
        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmAbortRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmAbortRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortRequestRequest"), org.dcache.srm.v2_2.SrmAbortRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmAbortRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmAbortRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[30] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmAbortFiles");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmAbortFilesRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortFilesRequest"), org.dcache.srm.v2_2.SrmAbortFilesRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortFilesResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmAbortFilesResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmAbortFilesResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[31] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmSuspendRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmSuspendRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSuspendRequestRequest"), org.dcache.srm.v2_2.SrmSuspendRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSuspendRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmSuspendRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmSuspendRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[32] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmResumeRequest");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmResumeRequestRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmResumeRequestRequest"), org.dcache.srm.v2_2.SrmResumeRequestRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmResumeRequestResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmResumeRequestResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmResumeRequestResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[33] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetRequestSummary");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetRequestSummaryRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummaryRequest"), org.dcache.srm.v2_2.SrmGetRequestSummaryRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummaryResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmGetRequestSummaryResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetRequestSummaryResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[34] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmExtendFileLifeTime");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmExtendFileLifeTimeRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeRequest"), org.dcache.srm.v2_2.SrmExtendFileLifeTimeRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmExtendFileLifeTimeResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[35] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetRequestTokens");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetRequestTokensRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestTokensRequest"), org.dcache.srm.v2_2.SrmGetRequestTokensRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestTokensResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmGetRequestTokensResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetRequestTokensResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[36] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmGetTransferProtocols");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmGetTransferProtocolsRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetTransferProtocolsRequest"), org.dcache.srm.v2_2.SrmGetTransferProtocolsRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetTransferProtocolsResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmGetTransferProtocolsResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[37] = oper;

        oper = new org.apache.axis.description.OperationDesc();
        oper.setName("srmPing");
        param = new org.apache.axis.description.ParameterDesc(new javax.xml.namespace.QName("", "srmPingRequest"), org.apache.axis.description.ParameterDesc.IN, new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPingRequest"), org.dcache.srm.v2_2.SrmPingRequest.class, false, false);
        oper.addParameter(param);
        oper.setReturnType(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPingResponse"));
        oper.setReturnClass(org.dcache.srm.v2_2.SrmPingResponse.class);
        oper.setReturnQName(new javax.xml.namespace.QName("", "srmPingResponse"));
        oper.setStyle(org.apache.axis.constants.Style.RPC);
        oper.setUse(org.apache.axis.constants.Use.ENCODED);
        _operations[38] = oper;

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
            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfAnyURI");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfAnyURI.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfString");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfString.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTBringOnlineRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTBringOnlineRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTCopyFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTCopyFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTCopyRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTCopyRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTExtraInfo");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTExtraInfo.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGetFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTGetFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGetRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTGetRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTGroupPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTGroupPermission.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTMetaDataPathDetail");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTMetaDataSpace");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTMetaDataSpace.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTPermissionReturn");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTPermissionReturn.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTPutFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTPutFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTPutRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTPutRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTRequestSummary");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTRequestSummary.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTRequestTokenReturn");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTRequestTokenReturn.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSupportedTransferProtocol");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTSupportedTransferProtocol.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLLifetimeReturnStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTSURLLifetimeReturnStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLPermissionReturn");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTSURLPermissionReturn.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTSURLReturnStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfTUserPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfTUserPermission.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "ArrayOfUnsignedLong");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.ArrayOfUnsignedLong.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortFilesRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmAbortFilesRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortFilesResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmAbortFilesResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmAbortRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmAbortRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmAbortRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmBringOnlineRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmBringOnlineRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmBringOnlineResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmBringOnlineResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeSpaceForFilesRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmChangeSpaceForFilesRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeSpaceForFilesResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmChangeSpaceForFilesResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermissionRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmCheckPermissionRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCheckPermissionResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmCheckPermissionResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmCopyRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmCopyResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmCopyResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeInSpaceRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeInSpaceResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmExtendFileLifeTimeRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetPermissionRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetPermissionRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetPermissionResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetPermissionResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummaryRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetRequestSummaryRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestSummaryResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetRequestSummaryResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestTokensRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetRequestTokensRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestTokensResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetRequestTokensResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetSpaceMetaDataRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceMetaDataResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokensRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetSpaceTokensRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokensResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetSpaceTokensResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetTransferProtocolsRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetTransferProtocolsRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetTransferProtocolsResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLsRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmLsRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmLsResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmLsResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdirRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmMkdirRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMkdirResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmMkdirResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmMvRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmMvResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmMvResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPingRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPingRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPingResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPingResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGetRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPrepareToGetRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToGetResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPrepareToGetResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPutRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPrepareToPutRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPrepareToPutResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPrepareToPutResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPurgeFromSpaceRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPurgeFromSpaceRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPurgeFromSpaceResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPurgeFromSpaceResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPutDoneRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPutDoneRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPutDoneResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmPutDoneResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseFilesRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmReleaseFilesRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseFilesResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmReleaseFilesResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpaceRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmReleaseSpaceRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReleaseSpaceResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmReleaseSpaceResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmReserveSpaceRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmReserveSpaceResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmReserveSpaceResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmResumeRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmResumeRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmResumeRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmResumeRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdirRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmRmdirRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmdirResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmRmdirResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmRmRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmRmResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmRmResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermissionRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmSetPermissionRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSetPermissionResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmSetPermissionResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfBringOnlineRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfBringOnlineRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfChangeSpaceForFilesRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfChangeSpaceForFilesRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfCopyRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfCopyRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfGetRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfGetRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfLsRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfLsRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfPutRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfReserveSpaceRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfReserveSpaceRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfUpdateSpaceRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfUpdateSpaceRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSuspendRequestRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmSuspendRequestRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmSuspendRequestResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmSuspendRequestResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmUpdateSpaceRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmUpdateSpaceResponse");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.SrmUpdateSpaceResponse.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

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
            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TAccessLatency");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TAccessLatency.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TAccessPattern");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TAccessPattern.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TBringOnlineRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TBringOnlineRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TConnectionType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TConnectionType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCopyFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TCopyFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TCopyRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TCopyRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TDirOption");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TDirOption.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TExtraInfo");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TExtraInfo.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileLocality");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TFileLocality.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileStorageType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TFileStorageType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TFileType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TFileType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGetFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TGetFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGetRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TGetRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TGroupPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TGroupPermission.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TMetaDataPathDetail");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TMetaDataPathDetail.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TMetaDataSpace");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TMetaDataSpace.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TOverwriteMode");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TOverwriteMode.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionMode");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TPermissionMode.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionReturn");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TPermissionReturn.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPermissionType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TPermissionType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPutFileRequest");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TPutFileRequest.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TPutRequestFileStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TPutRequestFileStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestSummary");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TRequestSummary.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestTokenReturn");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TRequestTokenReturn.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRequestType");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TRequestType.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicy");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TRetentionPolicy.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TRetentionPolicyInfo");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TRetentionPolicyInfo.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TReturnStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TReturnStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TStatusCode");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TStatusCode.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(enumsf);
            cachedDeserFactories.add(enumdf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSupportedTransferProtocol");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TSupportedTransferProtocol.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLLifetimeReturnStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TSURLLifetimeReturnStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLPermissionReturn");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TSURLPermissionReturn.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TSURLReturnStatus");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TSURLReturnStatus.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TTransferParameters");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TTransferParameters.class;
            cachedSerClasses.add(cls);
            cachedSerFactories.add(beansf);
            cachedDeserFactories.add(beandf);

            qName = new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "TUserPermission");
            cachedSerQNames.add(qName);
            cls = org.dcache.srm.v2_2.TUserPermission.class;
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
            for (Object o : super.cachedProperties.keySet()) {
                String key = (String) o;
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

    @Override
    public org.dcache.srm.v2_2.SrmReserveSpaceResponse srmReserveSpace(org.dcache.srm.v2_2.SrmReserveSpaceRequest srmReserveSpaceRequest) throws java.rmi.RemoteException {
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
                return (org.dcache.srm.v2_2.SrmReserveSpaceResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmReserveSpaceResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmReserveSpaceResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse srmStatusOfReserveSpaceRequest(org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestRequest srmStatusOfReserveSpaceRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[1]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfReserveSpaceRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmStatusOfReserveSpaceRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmReleaseSpaceResponse srmReleaseSpace(org.dcache.srm.v2_2.SrmReleaseSpaceRequest srmReleaseSpaceRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[2]);
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
                return (org.dcache.srm.v2_2.SrmReleaseSpaceResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmReleaseSpaceResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmReleaseSpaceResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmUpdateSpaceResponse srmUpdateSpace(org.dcache.srm.v2_2.SrmUpdateSpaceRequest srmUpdateSpaceRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[3]);
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
                return (org.dcache.srm.v2_2.SrmUpdateSpaceResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmUpdateSpaceResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmUpdateSpaceResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestResponse srmStatusOfUpdateSpaceRequest(org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestRequest srmStatusOfUpdateSpaceRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[4]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfUpdateSpaceRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmStatusOfUpdateSpaceRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmStatusOfUpdateSpaceRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse srmGetSpaceMetaData(org.dcache.srm.v2_2.SrmGetSpaceMetaDataRequest srmGetSpaceMetaDataRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[5]);
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
                return (org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmGetSpaceMetaDataResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmChangeSpaceForFilesResponse srmChangeSpaceForFiles(org.dcache.srm.v2_2.SrmChangeSpaceForFilesRequest srmChangeSpaceForFilesRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[6]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmChangeSpaceForFiles"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmChangeSpaceForFilesRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmChangeSpaceForFilesResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmChangeSpaceForFilesResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmChangeSpaceForFilesResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestResponse srmStatusOfChangeSpaceForFilesRequest(org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestRequest srmStatusOfChangeSpaceForFilesRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[7]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfChangeSpaceForFilesRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmStatusOfChangeSpaceForFilesRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmStatusOfChangeSpaceForFilesRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceResponse srmExtendFileLifeTimeInSpace(org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceRequest srmExtendFileLifeTimeInSpaceRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[8]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmExtendFileLifeTimeInSpace"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmExtendFileLifeTimeInSpaceRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmExtendFileLifeTimeInSpaceResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmPurgeFromSpaceResponse srmPurgeFromSpace(org.dcache.srm.v2_2.SrmPurgeFromSpaceRequest srmPurgeFromSpaceRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[9]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPurgeFromSpace"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmPurgeFromSpaceRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmPurgeFromSpaceResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmPurgeFromSpaceResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmPurgeFromSpaceResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmGetSpaceTokensResponse srmGetSpaceTokens(org.dcache.srm.v2_2.SrmGetSpaceTokensRequest srmGetSpaceTokensRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[10]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetSpaceTokens"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmGetSpaceTokensRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmGetSpaceTokensResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmGetSpaceTokensResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmGetSpaceTokensResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmSetPermissionResponse srmSetPermission(org.dcache.srm.v2_2.SrmSetPermissionRequest srmSetPermissionRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[11]);
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
                return (org.dcache.srm.v2_2.SrmSetPermissionResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmSetPermissionResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmSetPermissionResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmCheckPermissionResponse srmCheckPermission(org.dcache.srm.v2_2.SrmCheckPermissionRequest srmCheckPermissionRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[12]);
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
                return (org.dcache.srm.v2_2.SrmCheckPermissionResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmCheckPermissionResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmCheckPermissionResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmGetPermissionResponse srmGetPermission(org.dcache.srm.v2_2.SrmGetPermissionRequest srmGetPermissionRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[13]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetPermission"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmGetPermissionRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmGetPermissionResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmGetPermissionResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmGetPermissionResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmMkdirResponse srmMkdir(org.dcache.srm.v2_2.SrmMkdirRequest srmMkdirRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[14]);
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
                return (org.dcache.srm.v2_2.SrmMkdirResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmMkdirResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmMkdirResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmRmdirResponse srmRmdir(org.dcache.srm.v2_2.SrmRmdirRequest srmRmdirRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[15]);
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
                return (org.dcache.srm.v2_2.SrmRmdirResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmRmdirResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmRmdirResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmRmResponse srmRm(org.dcache.srm.v2_2.SrmRmRequest srmRmRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[16]);
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
                return (org.dcache.srm.v2_2.SrmRmResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmRmResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmRmResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmLsResponse srmLs(org.dcache.srm.v2_2.SrmLsRequest srmLsRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[17]);
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
                return (org.dcache.srm.v2_2.SrmLsResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmLsResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmLsResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse srmStatusOfLsRequest(org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest srmStatusOfLsRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[18]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfLsRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmStatusOfLsRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmMvResponse srmMv(org.dcache.srm.v2_2.SrmMvRequest srmMvRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[19]);
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
                return (org.dcache.srm.v2_2.SrmMvResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmMvResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmMvResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmPrepareToGetResponse srmPrepareToGet(org.dcache.srm.v2_2.SrmPrepareToGetRequest srmPrepareToGetRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[20]);
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
                return (org.dcache.srm.v2_2.SrmPrepareToGetResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmPrepareToGetResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmPrepareToGetResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse srmStatusOfGetRequest(org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest srmStatusOfGetRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[21]);
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
                return (org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmBringOnlineResponse srmBringOnline(org.dcache.srm.v2_2.SrmBringOnlineRequest srmBringOnlineRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[22]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmBringOnline"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmBringOnlineRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmBringOnlineResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmBringOnlineResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmBringOnlineResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequest(org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestRequest srmStatusOfBringOnlineRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[23]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmStatusOfBringOnlineRequest"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmStatusOfBringOnlineRequestRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmPrepareToPutResponse srmPrepareToPut(org.dcache.srm.v2_2.SrmPrepareToPutRequest srmPrepareToPutRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[24]);
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
                return (org.dcache.srm.v2_2.SrmPrepareToPutResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmPrepareToPutResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmPrepareToPutResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse srmStatusOfPutRequest(org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest srmStatusOfPutRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[25]);
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
                return (org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmCopyResponse srmCopy(org.dcache.srm.v2_2.SrmCopyRequest srmCopyRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[26]);
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
                return (org.dcache.srm.v2_2.SrmCopyResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmCopyResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmCopyResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest(org.dcache.srm.v2_2.SrmStatusOfCopyRequestRequest srmStatusOfCopyRequestRequest) throws java.rmi.RemoteException {
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
                return (org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmReleaseFilesResponse srmReleaseFiles(org.dcache.srm.v2_2.SrmReleaseFilesRequest srmReleaseFilesRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[28]);
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
                return (org.dcache.srm.v2_2.SrmReleaseFilesResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmReleaseFilesResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmReleaseFilesResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmPutDoneResponse srmPutDone(org.dcache.srm.v2_2.SrmPutDoneRequest srmPutDoneRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[29]);
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
                return (org.dcache.srm.v2_2.SrmPutDoneResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmPutDoneResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmPutDoneResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmAbortRequestResponse srmAbortRequest(org.dcache.srm.v2_2.SrmAbortRequestRequest srmAbortRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[30]);
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
                return (org.dcache.srm.v2_2.SrmAbortRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmAbortRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmAbortRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmAbortFilesResponse srmAbortFiles(org.dcache.srm.v2_2.SrmAbortFilesRequest srmAbortFilesRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[31]);
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
                return (org.dcache.srm.v2_2.SrmAbortFilesResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmAbortFilesResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmAbortFilesResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmSuspendRequestResponse srmSuspendRequest(org.dcache.srm.v2_2.SrmSuspendRequestRequest srmSuspendRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[32]);
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
                return (org.dcache.srm.v2_2.SrmSuspendRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmSuspendRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmSuspendRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmResumeRequestResponse srmResumeRequest(org.dcache.srm.v2_2.SrmResumeRequestRequest srmResumeRequestRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[33]);
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
                return (org.dcache.srm.v2_2.SrmResumeRequestResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmResumeRequestResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmResumeRequestResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmGetRequestSummaryResponse srmGetRequestSummary(org.dcache.srm.v2_2.SrmGetRequestSummaryRequest srmGetRequestSummaryRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[34]);
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
                return (org.dcache.srm.v2_2.SrmGetRequestSummaryResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmGetRequestSummaryResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmGetRequestSummaryResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse srmExtendFileLifeTime(org.dcache.srm.v2_2.SrmExtendFileLifeTimeRequest srmExtendFileLifeTimeRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[35]);
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
                return (org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmExtendFileLifeTimeResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmGetRequestTokensResponse srmGetRequestTokens(org.dcache.srm.v2_2.SrmGetRequestTokensRequest srmGetRequestTokensRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[36]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetRequestTokens"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmGetRequestTokensRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmGetRequestTokensResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmGetRequestTokensResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmGetRequestTokensResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse srmGetTransferProtocols(org.dcache.srm.v2_2.SrmGetTransferProtocolsRequest srmGetTransferProtocolsRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[37]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmGetTransferProtocols"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmGetTransferProtocolsRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmGetTransferProtocolsResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

    @Override
    public org.dcache.srm.v2_2.SrmPingResponse srmPing(org.dcache.srm.v2_2.SrmPingRequest srmPingRequest) throws java.rmi.RemoteException {
        if (super.cachedEndpoint == null) {
            throw new org.apache.axis.NoEndPointException();
        }
        org.apache.axis.client.Call _call = createCall();
        _call.setOperation(_operations[38]);
        _call.setUseSOAPAction(true);
        _call.setSOAPActionURI("");
        _call.setSOAPVersion(org.apache.axis.soap.SOAPConstants.SOAP11_CONSTANTS);
        _call.setOperationName(new javax.xml.namespace.QName("http://srm.lbl.gov/StorageResourceManager", "srmPing"));

        setRequestHeaders(_call);
        setAttachments(_call);
 try {        java.lang.Object _resp = _call.invoke(new java.lang.Object[] {srmPingRequest});

        if (_resp instanceof java.rmi.RemoteException) {
            throw (java.rmi.RemoteException)_resp;
        }
        else {
            extractAttachments(_call);
            try {
                return (org.dcache.srm.v2_2.SrmPingResponse) _resp;
            } catch (java.lang.Exception _exception) {
                return (org.dcache.srm.v2_2.SrmPingResponse) org.apache.axis.utils.JavaUtils.convert(_resp, org.dcache.srm.v2_2.SrmPingResponse.class);
            }
        }
  } catch (org.apache.axis.AxisFault axisFaultException) {
  throw axisFaultException;
}
    }

}

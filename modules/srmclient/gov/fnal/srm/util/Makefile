# to descend to directories, construct
# the list of directories apending '_target'
# and inserting Makefile in each directory.
SRM_PATH=../../../..
PACKAGE=gov.fnal.srm.util
include $(SRM_PATH)/conf/ostype.inc

TARGETS =  \
           
JAVA_TARGET =ArgParser.class \
	     Configuration.class \
	     Copier.class \
	     CopyJob.class \
	     SRMClient.class \
	     SRMGetClientV1.class \
             SRMGetClientV2.class \
	     SRMPutClientV1.class \
	     SRMPutClientV2.class \
             SRMLsClientV2.class \
	     ShellCommandExecuter.class \
             SRMDispatcher.class \
             KFTPArgsConverter.class \
             DccpArgsConverter.class \
             CopyJobFileReader.class \
             SRMGetFileMetaDataClientV1.class \
             SRMAdvisoryDeleteClientV1.class \
             SRMGetRequestStatusClientV1.class \
             Report.class \
             SRMRmClientV2.class \
             SRMMkDirClientV2.class \
             SRMRmdirClientV2.class \
             SRMMvClientV2.class \
             Version.class \
             SRMV1CopyJob.class \
             SRMV2CopyJob.class \
             SRMCopyClientV2.class \
             SRMCopyClientV1.class \
             SRMStageClientV1.class \
             SRMReserveSpaceClientV2.class \
             SRMReleaseSpaceClientV2.class \
             SRMGetPermissionClientV2.class \
             SRMCheckPermissionClientV2.class \
             SRMSetPermissionClientV2.class \
             SRMBringOnlineClientV2.class \
             SRMExtendFileLifeTimeClientV2.class \
             SRMPingClientV1.class \
             SRMPingClientV2.class \
             SRMGetSpaceMetaDataClientV2.class \
             SRMGetSpaceTokensClientV2.class \
             SRMGetRequestSummaryClientV2.class \
             SRMAbortRequestClientV2.class \
             SRMAbortFilesClientV2.class \
             SRMReleaseFilesClientV2.class \
             OptionParser.class \
             Option.class \
             SrmUrl.class \
             SRMGetRequestTokensClientV2.class 
              

MAKE_TARGETS = $(TARGETS:_target=_make)

#NOTHING = 
#DLR=$$

JAVA_ARCHIVE_FILES = true
JAVA_CLIENT_ARCHIVE_FILES = true

include $(SRM_PATH)/conf/config.inc



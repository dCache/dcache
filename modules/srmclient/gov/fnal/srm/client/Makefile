# to descend to directories, construct
# the list of directories apending '_target'
# and inserting Makefile in each directory.
SRM_PATH=../../../..
PACKAGE=gov.fnal.srm.client
include $(SRM_PATH)/conf/ostype.inc

TARGETS =  \

JAVA_TARGET =  \
              ClientV1.class \
              InformationProviderClientV1.class
#              Client.class \
#              CommandLineClient.class \
#              CommandLineClientV1.class \
#              CommandLineClientV1WithFactory.class

MAKE_TARGETS = $(TARGETS:_target=_make)

JAVA_CLIENT_ARCHIVE_FILES = true

include $(SRM_PATH)/conf/config.inc



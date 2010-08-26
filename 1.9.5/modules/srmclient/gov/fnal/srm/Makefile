# to descend to directories, construct
# the list of directories apending '_target'
# and inserting Makefile in each directory.
SRM_PATH=../../..
PACKAGE=gov.fnal.srm
include $(SRM_PATH)/conf/ostype.inc

TARGETS =  util_target \
           client_target 
#           security_target

MAKE_TARGETS = $(TARGETS:_target=_make)

include $(SRM_PATH)/conf/config.inc



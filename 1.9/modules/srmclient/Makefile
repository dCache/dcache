# to descend to directories, construct
# the list of directories apending '_target'
# and inserting Makefile in each directory.
SRM_PATH=.
PACKAGE=.
include $(SRM_PATH)/conf/ostype.inc

TARGETS = 		\
  gov_target \
  bin_target \
  lib_target 
ifdef SRM_C_CPP
TARGETS := $(TARGETS)  c_cpp_client_target 
endif
  

MAKE_TARGETS = $(TARGETS:_target=_make)

include $(SRM_PATH)/conf/config.inc



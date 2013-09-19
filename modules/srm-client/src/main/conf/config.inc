DEBUG=true
include $(SRM_PATH)/conf/ostype.inc
SRM_PATH       := $(subst \,/,$(SRM_PATH))
REL_PATH := $(subst .,/,$(PACKAGE))
CLEAN_TARGETS := $(MAKE_TARGETS:_make=_clean)
include $(SRM_PATH)/conf/$(OS_TYPE).cfg
include $(SRM_PATH)/conf/make.inc


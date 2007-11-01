# to descend to directories, construct
# the list of directories apending '_target'
# and inserting Makefile in each directory.
SRM_PATH=..
PACKAGE=lib
include $(SRM_PATH)/conf/ostype.inc


MAKE_TARGETS = $(TARGETS:_target=_make)
EXTRA_CLEAN = srm_*.jar
include $(SRM_PATH)/conf/config.inc



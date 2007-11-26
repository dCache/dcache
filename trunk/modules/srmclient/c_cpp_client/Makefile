# to descend to directories, construct
# the list of directories apending '_target'
# and inserting Makefile in each directory.
SRM_PATH=..
PACKAGE=c_cpp_client
include $(SRM_PATH)/conf/ostype.inc

TARGETS = 		
  
#C_OBJS = srm.o 
CPP_OBJS = soapC.o soapClient.o stdsoap2.o srm.o

EXE_OBJ = srmget.exe 

ARCHIVE_OBJS := $(C_OBJS) $(CPP_OBJS)

MAKE_TARGETS = $(TARGETS:_target=_make)


include $(SRM_PATH)/conf/config.inc



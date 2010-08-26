# to descend to directories, construct
# the list of directories apending '_target'
# and inserting Makefile in each directory.
SRM_PATH=..
PACKAGE=cppClientDevel
include $(SRM_PATH)/conf/ostype.inc

all :  SrmGetOpt.exe XParser.exe

TARGETS =

CFLAGS   = -g -O2
CXXFLAGS = -g -O2

C_OBJS = 
CPP_OBJS = soapC.o soapClient.o stdsoap2.o srm.o 

EXE_OBJ = srmget.exe

ARCHIVE_OBJS := $(C_OBJS) $(CPP_OBJS)

MAKE_TARGETS = $(TARGETS:_target=_make)

include $(SRM_PATH)/conf/config.inc


#-------------
#domtst.o : domtst.cpp XercesString.h
#	$(CC) -c domtst.cpp $(INCLUDES)
#
#XercesString.o : XercesString.cpp XercesString.h
#	$(CC) -c XercesString.cpp $(INCLUDES)
#
#domtst: domtst.o XercesString.o
#	g++ domtst.o XercesString.o -lm $(LIBRARIES) -o domtst
#

XParser.o: XParser.cpp
	$(CC) -I$(XERCES_DIR)/include \
	-c XParser.cpp 

XParser.exe: XParser.o
	$(CXX) XParser.o \
	-lm $(LIBRARIES) -L$(XERCES_DIR)/lib -lxerces-c \
	-o XParser.exe
#------
SrmGetOpt.exe: SrmGetOpt.o
	$(CXX) SrmGetOpt.o -lm $(LIBRARIES) -o SrmGetOpt.exe

# g++ -lm -pthread -ldl -lX11 -rdynamic \
#	-L../lib/ -L/usr/X11R6/lib/ \
#	SrmGetOpt.o  -o SrmGetOpt.exe 
#-------
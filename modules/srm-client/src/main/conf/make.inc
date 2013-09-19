C_OBJS := $(C_OBJS:.o=$(LIB_EXT))

ifdef ARCHIVE_OBJS
   ifeq (windows,$(OS_TYPE)) 
       ARCHIVE_OBJS := $(ARCHIVE_OBJS:.o=.obj)
   endif
endif


ifdef LIB_PREFIX
  C_OBJS := $(addprefix $(LIB_PREFIX), $(C_OBJS))
  CPP_OBJS := $(addprefix $(LIB_PREFIX), $(CPP_OBJS))
endif

CPP_OBJS := $(CPP_OBJS:.o=$(LIB_EXT))

C_LIB_DEPS := $(C_OBJS)
C_LIB_DEPS := $(C_LIB_DEPS:.obj=$(LIB_EXT))

CPP_LIB_DEPS := $(CPP_OBJS)
CPP_LIB_DEPS := $(CPP_LIB_DEPS:.obj=$(LIB_EXT))


ifndef MAKE_TARGETS
  MAKE_TARGETS = $(TARGETS:_target=_make)
  CLEAN_TARGETS = $(TARGETS:_target=_clean)
endif

ifndef FINAL_EXE
FINAL_EXE = $(EXE_OBJ)
endif



SRM_CLASSPATH := $(SRM_PATH)

#dcache
SRM_CLASSPATH := $(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/srm.jar


# glue staff
SRM_CLASSPATH := $(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/glue/GLUE-STD.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/glue/dom.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/glue/servlet.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/glue/jnet.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/glue/jsse.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/glue/jcert.jar


#globus cog kit staff
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/globus/cog-jglobus.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/globus/cryptix-asn1.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/globus/jce-jdk13-119.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/globus/puretls.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/globus/cryptix.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/globus/junit.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/globus/cryptix32.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/globus/cog-axis.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/slf4j/jcl-over-slf4j-1.6.0.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/slf4j/log4j-over-slf4j-1.6.0.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/slf4j/slf4j-api-1.6.0.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/logback/logback-classic-0.9.24.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/logback/logback-core-0.9.24.jar

SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/axis/jaxrpc.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/axis/saaj.jar
SRM_CLASSPATH :=$(SRM_CLASSPATH)$(PATH_SEPARATOR)$(SRM_PATH)/lib/axis/axis.jar

# Targets

.PHONY: all clean target_dir 

ifndef OS_TYPE
  all:
	@echo "please define SRM_TARGET!"
else
  ifdef JAVA_ARCHIVE_FILES
    all: target_dir  $(JAVA_TARGET) java_archive $(C_OBJS) $(C_LIB_DEPS) \
    $(CPP_LIB_DEPS) archive $(MAKE_TARGETS)  $(EXE_OBJ) 
  else
    ifdef JAVA_CLIENT_ARCHIVE_FILES 
      all: target_dir  $(JAVA_TARGET) java_archive $(C_OBJS) $(C_LIB_DEPS) \
      $(CPP_LIB_DEPS) archive $(MAKE_TARGETS)  $(EXE_OBJ) 
    else
       all: target_dir  $(JAVA_TARGET) $(C_OBJS) $(C_LIB_DEPS) \
       $(CPP_LIB_DEPS) archive $(MAKE_TARGETS)  $(EXE_OBJ)
    endif
  endif
endif


clean: clean_local $(CLEAN_TARGETS)

clean_local: 
	@echo "removing $(CLEAN_FILES)"
	$(RM) $(CLEAN_FILES)

archive: $(ARCHIVE_OBJS)
  ifdef ARCHIVE_OBJS
	@echo "archaiving $?"
	@echo "into archive: $(ARCHIVE_NAME) "
	@if [ -f $(ARCHIVE_NAME) ] ; then $(AR) $(ADD_LIB_OPTS) $?; \
	else $(AR) $(CREATE_LIB_OPTS) $?; fi
	@sleep 1
  endif

java_archive: 
  ifdef JAVA_ARCHIVE_FILES
	@echo "java archaiving *.class in $(REL_PATH)"
	cd $(SRM_PATH);if [ -f $(JAVA_ARCHIVE_NAME) ]; then \
        $(JAR) $(JAR_ADD_OPTS) $(REL_PATH)/*.class ; \
        else $(JAR) $(JAR_CREATE_OPTS) $(REL_PATH)/*.class ; fi
	@sleep 1
  endif
  ifdef JAVA_CLIENT_ARCHIVE_FILES
	@echo "java archaiving *.class in $(REL_PATH)"
	@cd $(SRM_PATH);if [ -f $(JAVA_CLIENT_ARCHIVE_NAME) ]; then \
        $(JAR) $(JAR_CLIENT_ADD_OPTS) $(REL_PATH)/*.class ; \
        else $(JAR) $(JAR_CLIENT_CREATE_OPTS) $(REL_PATH)/*.class ; fi
	@sleep 1
  endif


$(EXE_OBJ): $(ENTRY_OBJ) $(ARCHIVE_NAME)
	$(LINK)  $(LINK_FLAGS)
	$(COPY) $(EXE_OBJ) $(SRM_PATH)/bin/$(FINAL_EXE)
         

# rules for building 

.SUFFIXES: .o .obj $(LIB_EXT) .c .cpp .class .java


$(SRM_PATH)/lib/$(OS_TYPE)/$(LIB_PREFIX)%$(LIB_EXT) $(LIB_PREFIX)%$(LIB_EXT) : %.c
	@echo " compyling *.c into $(LIB_EXT) in $(REL_PATH)"
	$(CC) $(C_INCLUDES) $(C_OPTIONS) $< $(C_OBJ_FLAGS)
  ifdef C_LIB_ARCHIVE
	$(C_LIB_ARCHIVE)#C_LIB_DEPS := $(addprefix $(SRM_PATH)/lib/$(OS_TYPE)/, $(C_OBJS))
#C_LIB_DEPS := $(addprefix $(SRM_PATH)/lib/$(OS_TYPE)/, $(C_OBJS))

  endif


$(SRM_PATH)/lib/$(OS_TYPE)/$(LIB_PREFIX)%$(LIB_EXT) $(LIB_PREFIX)%$(LIB_EXT) : %.cpp
	@echo "compyling .cpp into $(LIB_EXT)"
	$(CPP) $(CPP_INCLUDES) $(CPP_OPTIONS) $< $(CPP_OBJ_FLAGS)
	$(CPP_LIB_ARCHIVE)

%.class : %.java
	@echo "compyling .java into .class"
	$(JAVAC) $(JAVA_OPTIONS) -classpath $(SRM_CLASSPATH)  $< 

# building sub-directories                                          |

$(MAKE_TARGETS):
	@echo "making"
	@echo "<(`pwd`/$(@:_make=))>"
	@$(MAKE) -C $(@:_make=) all

$(CLEAN_TARGETS):
	@echo
	@echo "cleaning"
	@echo "<(`pwd`/$(@:_make=))>"
	@$(MAKE) -C $(@:_clean=) clean

#archiving from the list of files
#       @if [ -f $(JAVA_ARCHIVE_NAME) ]; then \
#        $(JAR) $(JAR_ADD_OPTS) $(addprefix $(JARPRF),$(addprefix $(REL_PATH),$(addprefix /,$?))) ; \
#        else $(JAR) $(JAR_CREATE_OPTS) $(addprefix $(JARPRF),$(addprefix $(REL_PATH),$(addprefix /,$?))) ; fi

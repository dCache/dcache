ifdef SRM_TARGET
  ifeq (linux,$(SRM_TARGET))
    OS_TYPE := linux
  endif
  ifeq (windows,$(SRM_TARGET))
	OS_TYPE := windows
  endif
endif

ifndef OS_TYPE
	ifeq (Linux,$(shell uname))
		OS_TYPE :=linux
	endif
endif

ifndef OS_TYPE
	ifeq (Darwin,$(shell uname))
		OS_TYPE :=linux
	endif
endif

ifndef OS_TYPE
  ifneq (,$(findstring CYGWIN_NT-4,$(shell uname)))
    OS_TYPE := windows
  endif
endif

ifndef OS_TYPE
  ifneq (,$(findstring CYGWIN_NT-5,$(shell uname)))
    OS_TYPE := windows
  endif
endif

ifeq ($(OS_TYPE),linux)
  PWD_PATH := $(shell pwd)
endif

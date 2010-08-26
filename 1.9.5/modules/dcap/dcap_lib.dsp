# Microsoft Developer Studio Project File - Name="dcap_lib" - Package Owner=<4>
# Microsoft Developer Studio Generated Build File, Format Version 6.00
# ** DO NOT EDIT **

# TARGTYPE "Win32 (x86) Static Library" 0x0104

CFG=dcap_lib - Win32 Debug
!MESSAGE This is not a valid makefile. To build this project using NMAKE,
!MESSAGE use the Export Makefile command and run
!MESSAGE 
!MESSAGE NMAKE /f "dcap_lib.mak".
!MESSAGE 
!MESSAGE You can specify a configuration when running NMAKE
!MESSAGE by defining the macro CFG on the command line. For example:
!MESSAGE 
!MESSAGE NMAKE /f "dcap_lib.mak" CFG="dcap_lib - Win32 Debug"
!MESSAGE 
!MESSAGE Possible choices for configuration are:
!MESSAGE 
!MESSAGE "dcap_lib - Win32 Release" (based on "Win32 (x86) Static Library")
!MESSAGE "dcap_lib - Win32 Debug" (based on "Win32 (x86) Static Library")
!MESSAGE 

# Begin Project
# PROP AllowPerConfigDependencies 0
# PROP Scc_ProjName ""
# PROP Scc_LocalPath ""
CPP=cl.exe
RSC=rc.exe

!IF  "$(CFG)" == "dcap_lib - Win32 Release"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 0
# PROP BASE Output_Dir "Release"
# PROP BASE Intermediate_Dir "Release"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 0
# PROP Output_Dir "Release"
# PROP Intermediate_Dir "Release"
# PROP Target_Dir ""
# ADD BASE CPP /nologo /W3 /GX /O2 /D "WIN32" /D "NDEBUG" /D "_MBCS" /D "_LIB" /YX /FD /c
# ADD CPP /nologo /W3 /GX /O2 /D "NDEBUG" /D "_LIB" /D "NOT_THREAD_SAFE" /D "__CLAGS__ win32" /D "__CFLAGS__=\"win32build\"" /D "WIN32" /D "_MBCS" /YX /FD /c
# ADD BASE RSC /l 0x409 /d "NDEBUG"
# ADD RSC /l 0x409 /d "NDEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LIB32=link.exe -lib
# ADD BASE LIB32 /nologo
# ADD LIB32 /nologo

!ELSEIF  "$(CFG)" == "dcap_lib - Win32 Debug"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 1
# PROP BASE Output_Dir "dcap_lib___Win32_Debug"
# PROP BASE Intermediate_Dir "dcap_lib___Win32_Debug"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 1
# PROP Output_Dir "dcap_lib___Win32_Debug"
# PROP Intermediate_Dir "dcap_lib___Win32_Debug"
# PROP Target_Dir ""
# ADD BASE CPP /nologo /W3 /Gm /GX /ZI /Od /D "WIN32" /D "_DEBUG" /D "_MBCS" /D "_LIB" /YX /FD /GZ /c
# ADD CPP /nologo /W3 /Gm /GX /ZI /Od /D "_LIB" /D "NOT_THREAD_SAFE" /D "_DEBUG" /D "__CFLAGS__=\"win32build\"" /D "WIN32" /D "_MBCS" /FR /YX /FD /GZ /c
# ADD BASE RSC /l 0x409 /d "_DEBUG"
# ADD RSC /l 0x409 /d "_DEBUG  __CFLAGS__" /d "_DEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LIB32=link.exe -lib
# ADD BASE LIB32 /nologo
# ADD LIB32 /nologo /out:"dcap_lib___Win32_Debug\dcap.lib"

!ENDIF 

# Begin Target

# Name "dcap_lib - Win32 Release"
# Name "dcap_lib - Win32 Debug"
# Begin Group "Source Files"

# PROP Default_Filter "cpp;c;cxx;rc;def;r;odl;idl;hpj;bat"
# Begin Source File

SOURCE=.\adler32.c
# End Source File
# Begin Source File

SOURCE=.\array.c
# End Source File
# Begin Source File

SOURCE=.\char2crc.c
# End Source File
# Begin Source File

SOURCE=.\dcap.c
# End Source File
# Begin Source File

SOURCE=.\dcap_accept.c
# End Source File
# Begin Source File

SOURCE=.\dcap_access.c
# End Source File
# Begin Source File

SOURCE=.\dcap_ahead.c
# End Source File
# Begin Source File

SOURCE=.\dcap_checksum.c
# End Source File
# Begin Source File

SOURCE=.\dcap_checksum.h
# End Source File
# Begin Source File

SOURCE=.\dcap_close.c
# End Source File
# Begin Source File

SOURCE=.\dcap_command.c
# End Source File
# Begin Source File

SOURCE=.\dcap_debug.c
# End Source File
# Begin Source File

SOURCE=.\dcap_dup.c
# End Source File
# Begin Source File

SOURCE=.\dcap_error.c
# End Source File
# Begin Source File

SOURCE=.\dcap_fsync.c
# End Source File
# Begin Source File

SOURCE=.\dcap_interpreter.c
# End Source File
# Begin Source File

SOURCE=.\dcap_lseek.c
# End Source File
# Begin Source File

SOURCE=.\dcap_mqueue.c
# End Source File
# Begin Source File

SOURCE=.\dcap_open.c
# End Source File
# Begin Source File

SOURCE=.\dcap_poll.c
# End Source File
# Begin Source File

SOURCE=.\dcap_protocol.c
# End Source File
# Begin Source File

SOURCE=.\dcap_read.c
# End Source File
# Begin Source File

SOURCE=.\dcap_reconnect.c
# End Source File
# Begin Source File

SOURCE=.\dcap_shared.h
# End Source File
# Begin Source File

SOURCE=.\dcap_stat.c
# End Source File
# Begin Source File

SOURCE=.\dcap_stream.c
# End Source File
# Begin Source File

SOURCE=.\dcap_unix2win.c
# End Source File
# Begin Source File

SOURCE=.\dcap_url.c
# End Source File
# Begin Source File

SOURCE=.\dcap_version.c
# End Source File
# Begin Source File

SOURCE=.\dcap_write.c
# End Source File
# Begin Source File

SOURCE=.\input_parser.c
# End Source File
# Begin Source File

SOURCE=.\io.c
# End Source File
# Begin Source File

SOURCE=.\lineparser.c
# End Source File
# Begin Source File

SOURCE=.\links.c
# End Source File
# Begin Source File

SOURCE=.\node_plays.c
# End Source File
# Begin Source File

SOURCE=.\parser.c
# End Source File
# Begin Source File

SOURCE=.\pnfs.c
# End Source File
# Begin Source File

SOURCE=.\socket_nio.c
# End Source File
# Begin Source File

SOURCE=.\str2errno.c
# End Source File
# Begin Source File

SOURCE=.\string2stat.c
# End Source File
# Begin Source File

SOURCE=.\system_io.c
# End Source File
# Begin Source File

SOURCE=.\tunnelManager.c
# End Source File
# Begin Source File

SOURCE=.\win32_libdl.c
# End Source File
# Begin Source File

SOURCE=.\xutil.c
# End Source File
# End Group
# Begin Group "Header Files"

# PROP Default_Filter "h;hpp;hxx;hm;inl"
# Begin Source File

SOURCE=.\array.h
# End Source File
# Begin Source File

SOURCE=.\dcap_accept.h
# End Source File
# Begin Source File

SOURCE=.\dcap_ahead.h
# End Source File
# Begin Source File

SOURCE=.\dcap_command.h
# End Source File
# Begin Source File

SOURCE=.\dcap_debug.h
# End Source File
# Begin Source File

SOURCE=.\dcap_errno.h
# End Source File
# Begin Source File

SOURCE=.\dcap_error.h
# End Source File
# Begin Source File

SOURCE=.\dcap_interpreter.h
# End Source File
# Begin Source File

SOURCE=.\dcap_mqueue.h
# End Source File
# Begin Source File

SOURCE=.\dcap_nodes.h
# End Source File
# Begin Source File

SOURCE=.\dcap_poll.h
# End Source File
# Begin Source File

SOURCE=.\dcap_protocol.h
# End Source File
# Begin Source File

SOURCE=.\dcap_reconnect.h
# End Source File
# Begin Source File

SOURCE=.\dcap_types.h
# End Source File
# Begin Source File

SOURCE=.\dcap_url.h
# End Source File
# Begin Source File

SOURCE=.\dcap_win32.h
# End Source File
# Begin Source File

SOURCE=.\dcap_win_poll.h
# End Source File
# Begin Source File

SOURCE=.\debug_level.h
# End Source File
# Begin Source File

SOURCE=.\debug_map.h
# End Source File
# Begin Source File

SOURCE=.\input_parser.h
# End Source File
# Begin Source File

SOURCE=.\io.h
# End Source File
# Begin Source File

SOURCE=.\ioTunnel.h
# End Source File
# Begin Source File

SOURCE=.\links.h
# End Source File
# Begin Source File

SOURCE=.\parser.h
# End Source File
# Begin Source File

SOURCE=.\pnfs.h
# End Source File
# Begin Source File

SOURCE=.\socket_nio.h
# End Source File
# Begin Source File

SOURCE=.\sysdep.h
# End Source File
# Begin Source File

SOURCE=.\system_io.h
# End Source File
# Begin Source File

SOURCE=.\tunnelManager.h
# End Source File
# Begin Source File

SOURCE=.\win32_dlfcn.h
# End Source File
# End Group
# End Target
# End Project

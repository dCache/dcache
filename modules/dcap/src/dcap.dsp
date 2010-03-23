# Microsoft Developer Studio Project File - Name="dcap" - Package Owner=<4>
# Microsoft Developer Studio Generated Build File, Format Version 6.00
# ** DO NOT EDIT **

# TARGTYPE "Win32 (x86) Dynamic-Link Library" 0x0102

CFG=dcap - Win32 Debug
!MESSAGE This is not a valid makefile. To build this project using NMAKE,
!MESSAGE use the Export Makefile command and run
!MESSAGE 
!MESSAGE NMAKE /f "dcap.mak".
!MESSAGE 
!MESSAGE You can specify a configuration when running NMAKE
!MESSAGE by defining the macro CFG on the command line. For example:
!MESSAGE 
!MESSAGE NMAKE /f "dcap.mak" CFG="dcap - Win32 Debug"
!MESSAGE 
!MESSAGE Possible choices for configuration are:
!MESSAGE 
!MESSAGE "dcap - Win32 Release" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE "dcap - Win32 Debug" (based on "Win32 (x86) Dynamic-Link Library")
!MESSAGE 

# Begin Project
# PROP AllowPerConfigDependencies 0
# PROP Scc_ProjName ""
# PROP Scc_LocalPath ""
CPP=cl.exe
MTL=midl.exe
RSC=rc.exe

!IF  "$(CFG)" == "dcap - Win32 Release"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 0
# PROP BASE Output_Dir "Release"
# PROP BASE Intermediate_Dir "Release"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 0
# PROP Output_Dir "Release"
# PROP Intermediate_Dir "Release"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MT /W3 /GX /O2 /D "WIN32" /D "NDEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "DCAP_EXPORTS" /YX /FD /c
# ADD CPP /nologo /MT /W3 /Gi /GX /O2 /D "NDEBUG" /D "_WINDOWS" /D "_USRDLL" /D "DCAP_EXPORTS" /D "NOT_THREAD_SAFE" /D "__CLAGS__ win32" /D "__CFLAGS__=\"win32build\"" /D "WIN32" /D "_MBCS" /YX /FD /c
# ADD BASE MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "NDEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x409 /d "NDEBUG"
# ADD RSC /l 0x409 /d "NDEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /machine:I386
# ADD LINK32 dcap.lib kernel32.lib ws2_32.lib winmm.lib /nologo /dll /incremental:yes /machine:I386
# SUBTRACT LINK32 /nodefaultlib /force

!ELSEIF  "$(CFG)" == "dcap - Win32 Debug"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 1
# PROP BASE Output_Dir "Debug"
# PROP BASE Intermediate_Dir "Debug"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 1
# PROP Output_Dir "Debug"
# PROP Intermediate_Dir "Debug"
# PROP Ignore_Export_Lib 0
# PROP Target_Dir ""
# ADD BASE CPP /nologo /MTd /W3 /Gm /GX /ZI /Od /D "WIN32" /D "_DEBUG" /D "_WINDOWS" /D "_MBCS" /D "_USRDLL" /D "DCAP_EXPORTS" /YX /FD /GZ /c
# ADD CPP /nologo /MTd /W3 /Gm /Gi /GX /ZI /Od /D "_WINDOWS" /D "_USRDLL" /D "DCAP_EXPORTS" /D "NOT_THREAD_SAFE" /D "_DEBUG" /D "__CFLAGS__=\"win32build\"" /D "WIN32" /D "_MBCS" /FR /YX /FD /GZ /c
# ADD BASE MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD MTL /nologo /D "_DEBUG" /mktyplib203 /win32
# ADD BASE RSC /l 0x409 /d "_DEBUG"
# ADD RSC /l 0x409 /d "_DEBUG  __CFLAGS__" /d "_DEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LINK32=link.exe
# ADD BASE LINK32 kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /nologo /dll /debug /machine:I386 /pdbtype:sept
# ADD LINK32 kernel32.lib ws2_32.lib winmm.lib /nologo /dll /debug /machine:I386 /pdbtype:sept
# SUBTRACT LINK32 /pdb:none

!ENDIF 

# Begin Target

# Name "dcap - Win32 Release"
# Name "dcap - Win32 Debug"
# Begin Group "Source Files"

# PROP Default_Filter "cpp;c;cxx;rc;def;r;odl;idl;hpj;bat"
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

SOURCE=.\dcap.def
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
# Begin Group "Resource Files"

# PROP Default_Filter "ico;cur;bmp;dlg;rc2;rct;bin;rgs;gif;jpg;jpeg;jpe"
# End Group
# End Target
# End Project

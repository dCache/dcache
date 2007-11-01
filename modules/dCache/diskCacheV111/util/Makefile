all:  libSysTimer.so

JAVABASE=/opt/IBMJava2-13

SysTimerImpl.o: SysTimerImpl.c diskCacheV111_util_SysTimer.h
	gcc -c SysTimerImpl.c -I${JAVABASE}/include

SysTimer.class: SysTimer.java
	javac SysTimer.java

diskCacheV111_util_SysTimer.h: SysTimer.class
	javah -jni diskCacheV111.util.SysTimer

libSysTimer.so: SysTimerImpl.o
	ld -o libSysTimer.so -G -L${JAVABASE}/lib SysTimerImpl.o  

all: Unix.so

Unix.o: Unix.c dmg_apps_libraryServer_unix_Unix.h
	gcc  -c  -I/usr/javaIbm/jdk118/include Unix.c -I/usr/javaIbm/jdk118/include/linux

Unix.so: Unix.o
	gcc -shared -o libUnix.so Unix.o

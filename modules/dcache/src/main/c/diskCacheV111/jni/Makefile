
JAVA_HOME = /usr/lib/java
SYS = linux


CFLAGS = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(SYS)
# LDFLAGS = -G  # Sun ld
LDFLAGS = -shared  # gnu ld
LIBS = -lpam

OBJS = java_pam.o PAM_Auth.o

all: lib auth_shell

lib: $(OBJS)
	$(LD) $(LDFLAGS) -o libpam4java.so $(OBJS) $(LIBS)
	
test: java_pam.c
	$(CC) -o pam_test -DMAIN $(CFLAGS) java_pam.c $(LIBS)

auth_shell: auth_shell.c java_pam.o
	$(CC) -o auth_shell auth_shell.c $(CFLAGS) java_pam.o $(LIBS)

clean:
	rm -f a.out core *.o libpam4java.so PAM_Auth.class

.c.o:
	$(CC) -c $(CFLAGS) $<

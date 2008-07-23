//
//
// to build:
//
//    swig -Wall -python pydcap.i
//    gcc pydcap_wrap.c -I/usr/include/python2.5 -L/opt/d-cache/dcap/lib -ldcap -lpython2.5 \
//                    -Xlinker -expoert-dynamic -shared -o _pydcap.so
%module pydcap

%{
#include <dirent.h>

static PyObject* my_t_output_helper(PyObject* target, PyObject* o) {
    PyObject*   o2;
    PyObject*   o3;

    if (!target) {
        target = o;
    } else {
        if (!PyTuple_Check(target)) {
            o2 = target;
            target = PyTuple_New(1);
            PyTuple_SetItem(target, 0, o2);
        }
        o3 = PyTuple_New(1);
        PyTuple_SetItem(o3, 0, o);

        o2 = target;
        target = PySequence_Concat(o2, o3);
        Py_DECREF(o2);
        Py_DECREF(o3);
    }
    return target;
}

%}

%typemap(in, numinputs=0) (struct stat64 *)(struct stat64 statbuf) {
    $1 = &statbuf;
    memset ($1, 0, sizeof (struct stat64));
}

// convert output C 'struct stat64' into a python list exactly (in the same order) as the system os.stat() function
%typemap(argout) (struct stat64 *){
    PyObject *statlist = Py_None;

    if ($1) {
        statlist = PyList_New (10);
        PyList_SetItem (statlist, 0, PyInt_FromLong ((long)((*$1).st_mode)));
        PyList_SetItem (statlist, 1, PyLong_FromLongLong ((long long)((*$1).st_ino)));
        PyList_SetItem (statlist, 2, PyInt_FromLong ((long)((*$1).st_dev)));
        PyList_SetItem (statlist, 3, PyInt_FromLong ((long)((*$1).st_nlink)));
        PyList_SetItem (statlist, 4, PyInt_FromLong ((long)((*$1).st_uid)));
        PyList_SetItem (statlist, 5, PyInt_FromLong ((long)((*$1).st_gid)));
        PyList_SetItem (statlist, 6, PyLong_FromLongLong ((long long)((*$1).st_size)));
        PyList_SetItem (statlist, 7, PyLong_FromLong ((long)((*$1).st_atime)));
        PyList_SetItem (statlist, 8, PyLong_FromLong ((long)((*$1).st_mtime)));
        PyList_SetItem (statlist, 9, PyLong_FromLong ((long)((*$1).st_ctime)));
    }

    $result = my_t_output_helper ($result, statlist);
}//end of typemap

%typemap(in, numinputs=0) (struct dirent64 *)(struct dirent64 dir) {
    $1 = &dir;
    memset ($1, 0, sizeof (struct dirent64));
}

// convert output C 'struct dirent64' into python string
%typemap(out)(struct dirent64 *){
        if( $1 ) {
            $result = PyString_FromString( (char *)((*$1).d_name) );
        }else {
            $result = Py_None;
        }
}//end of typemap

extern int      dc_open(const char *, int, ...);
extern int      dc_creat(const char *, mode_t);
extern int      dc_close(int);
extern int      dc_close2(int);
extern ssize_t  dc_read(int, void *, size_t);
extern ssize_t  dc_readv(int, const struct iovec *, int);
extern ssize_t  dc_pread64(int, void  *,  size_t, off64_t);
extern ssize_t  dc_write(int, const void *, size_t);
extern ssize_t  dc_writev(int, const struct iovec *, int);
extern ssize_t  dc_pwrite64(int, const void  *,  size_t, off64_t);
extern off64_t  dc_lseek64(int, off64_t, int);
extern int      dc_fsync(int);
extern int      dc_dup(int);
extern int      dc_access( const char *, int);
extern int      dc_unlink( const char *);
extern int      dc_rmdir( const char *);
extern int      dc_mkdir( const char *, mode_t);
extern int      dc_chmod( const char *, mode_t);
extern int      dc_chown( const char *, uid_t, gid_t);
extern int      dc_stat64(const char *, struct stat64 *);
extern int      dc_lstat64(const char *, struct stat64 *);
extern int      dc_fstat64(int , struct stat64 *);
extern int      dc_rename(const char *, const char *);
extern void dc_perror(const char *);
extern DIR *    dc_opendir(const char *);
extern struct dirent64 * dc_readdir64(DIR *);
extern int      dc_closedir(DIR *);
extern off_t    dc_telldir(DIR *);
extern void     dc_seekdir(DIR *, off_t);

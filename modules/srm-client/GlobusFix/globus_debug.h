
#ifndef GLOBUS_DEBUG_H
#define GLOBUS_DEBUG_H

#include "globus_common_include.h"

#ifndef EXTERN_C_BEGIN
#ifdef __cplusplus
#define EXTERN_C_BEGIN extern "C" {
#define EXTERN_C_END }
#else
#define EXTERN_C_BEGIN
#define EXTERN_C_END
#endif
#endif

EXTERN_C_BEGIN

#ifdef BUILD_DEBUG

void
globus_debug_init(
    const char *                        env_name,
    const char *                        level_names,
    int *                               debug_level,
    FILE **                             out_file,
    globus_bool_t *                     using_file,
    globus_bool_t *                     show_tids);
    
/* call in same file as module_activate func (before (de)activate funcs) */
#define GlobusDebugDefine(module_name)                                      \
    static FILE * globus_i_##module_name##_debug_file;                      \
    static globus_bool_t globus_i_##module_name##_using_file;               \
    static globus_bool_t globus_i_##module_name##_print_threadids;          \
    void globus_i_##module_name##_debug_printf(const char * fmt, ...)       \
    {                                                                       \
        va_list ap;                                                         \
        va_start(ap, fmt);                                                  \
        if(globus_i_##module_name##_print_threadids)                        \
        {                                                                   \
            char buf[4096]; /* XXX better not use a fmt bigger than this */ \
            sprintf(                                                        \
                buf, "%lu::%s", (unsigned long) globus_thread_self(), fmt); \
            vfprintf(globus_i_##module_name##_debug_file, buf, ap);         \
        }                                                                   \
        else                                                                \
        {                                                                   \
            vfprintf(globus_i_##module_name##_debug_file, fmt, ap);         \
        }                                                                   \
        va_end(ap);                                                         \
    }                                                                       \
    int globus_i_##module_name##_debug_level

/* call this in a header file (if needed externally) */
#define GlobusDebugDeclare(module_name)                                     \
    extern void globus_i_##module_name##_debug_printf(const char *, ...);   \
    extern int globus_i_##module_name##_debug_level

/* call this in module activate func
 *
 * 'levels' is a space separated list of level names that can be used in env
 *    they will map to a 2^i value (so, list them in same order as value)
 *
 * will look in env for {module_name}_DEBUG whose value is:
 * <levels> [ , [ <file name> ] [ , <show tids>] ]
 * where <levels> can be a single numeric or '|' separated level names
 * <file name> is a debug output file... can be empty.  stderr by default
 * <show tids> is 0 or 1 to show thread ids on all messages.  0 by default
 */
#define GlobusDebugInit(module_name, levels)                                \
    globus_debug_init(                                                      \
        #module_name "_DEBUG",                                              \
        #levels,                                                            \
        &globus_i_##module_name##_debug_level,                              \
        &globus_i_##module_name##_debug_file,                               \
        &globus_i_##module_name##_using_file,                               \
        &globus_i_##module_name##_print_threadids)

/* call this in module deactivate func */
#define GlobusDebugDestroy(module_name)                                     \
    do                                                                      \
    {                                                                       \
        if(globus_i_##module_name##_using_file)                             \
        {                                                                   \
            fclose(globus_i_##module_name##_debug_file);                    \
        }                                                                   \
    } while(0)

/* most likely wrap this with your own macro,
 * so you dont need to pass module_name all the time
 * 'message' needs to be wrapped with parens and contains a format and
 * possibly var args
 */
#define GlobusDebugPrintf(module_name, level, message)                      \
    do                                                                      \
    {                                                                       \
        if(globus_i_##module_name##_debug_level & (level))                  \
        {                                                                   \
            globus_i_##module_name##_debug_printf message;                  \
        }                                                                   \
    } while(0)

#else

#define GlobusDebugDeclare(module_name)
#define GlobusDebugDefine(module_name)
#define GlobusDebugInit(module_name, levels)
#define GlobusDebugDestroy(module_name)
#define GlobusDebugPrintf(module_name, level, message)

#endif

EXTERN_C_END

#endif /* GLOBUS_DEBUG_H */

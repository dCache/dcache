#include <sys/times.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <jni.h>
#include "diskCacheV111_util_SysTimer.h"

JNIEXPORT void JNICALL 
  Java_diskCacheV111_util_SysTimer_times(
     JNIEnv *env , jobject thisObject , jobject  timestamp ){
     
    struct tms tmsTimestamp ;
    
    jclass   tsClass  = (*env)->GetObjectClass( env, timestamp );
    jfieldID utimeFid = (*env)->GetFieldID(env, tsClass, "_utime", "J");
    jfieldID stimeFid = (*env)->GetFieldID(env, tsClass, "_stime", "J");
    jfieldID rtimeFid = (*env)->GetFieldID(env, tsClass, "_rtime", "J");
/*
    printf( "SysTimerImpl: %d %d %d\n", tsClass , utimeFid , stimeFid ) ; 
*/    
    clock_t rtime = times( &tmsTimestamp ) ;
/*
    printf( "SysTimerImpl: %ld %ld\n",
            tmsTimestamp.tms_utime,tmsTimestamp.tms_stime ) ;
*/            
    (*env)->SetLongField( env , timestamp, utimeFid , tmsTimestamp.tms_utime );
    (*env)->SetLongField( env , timestamp, stimeFid , tmsTimestamp.tms_stime );
    (*env)->SetLongField( env , timestamp, rtimeFid , rtime );
    
     return ;
}
/*
 * Class:     diskCacheV111_util_SysTimer
 * Method:    rusage
 * Signature: (LdiskCacheV111/util/SysTimer$Timestamp;)V
 */
JNIEXPORT void JNICALL Java_diskCacheV111_util_SysTimer_rusage
  (JNIEnv *env , jobject thisObject , jobject timestamp ){
  
   struct rusage usage ;
   struct timeval daytime ;
   long tmp ;
    jclass   tsClass  = (*env)->GetObjectClass( env, timestamp );
    jfieldID utimeFid = (*env)->GetFieldID(env, tsClass, "_utime", "J");
    jfieldID stimeFid = (*env)->GetFieldID(env, tsClass, "_stime", "J");
    jfieldID rtimeFid = (*env)->GetFieldID(env, tsClass, "_rtime", "J");
 
    getrusage( RUSAGE_SELF , &usage ) ;
    
    tmp = usage.ru_stime.tv_sec * 1000 + usage.ru_stime.tv_usec / 1000 ;
    (*env)->SetLongField( env , timestamp, stimeFid , tmp );
    
    tmp = usage.ru_utime.tv_sec * 1000 + usage.ru_utime.tv_usec / 1000 ;
    (*env)->SetLongField( env , timestamp, utimeFid , tmp );
    
    gettimeofday( &daytime , NULL ) ;
    
    tmp = daytime.tv_sec * 1000 + daytime.tv_usec / 1000 ;
    (*env)->SetLongField( env , timestamp, rtimeFid , tmp );
    return ;
} 

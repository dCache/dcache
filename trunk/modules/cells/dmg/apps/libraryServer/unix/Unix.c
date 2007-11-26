#include "stdio.h"
#include "stdlib.h"
#include "dmg_apps_libraryServer_unix_Unix.h"

/*
 * Class:     dmg_apps_libraryServer_unix_Unix
 * Method:    getPid
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_dmg_apps_libraryServer_unix_Unix_getPid
  (JNIEnv *env, jclass class ){
  
  return getpid() ; 
  
}

/*
 * Class:     dmg_apps_libraryServer_unix_Unix
 * Method:    getParentId
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_dmg_apps_libraryServer_unix_Unix_getParentId
  (JNIEnv *env , jclass class ){
  
  return getppid() ;
}

/*
 * Class:     dmg_apps_libraryServer_unix_Unix
 * Method:    kill
 * Signature: (II)Z
 */
JNIEXPORT jint JNICALL Java_dmg_apps_libraryServer_unix_Unix_kill
  (JNIEnv *env , jclass class , jint pid , jint mode ){
  
    return kill( pid , mode ) ;
}

/*
 * Class:     dmg_apps_libraryServer_unix_Unix
 * Method:    open
 * Signature: (Ljava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_dmg_apps_libraryServer_unix_Unix_open
  (JNIEnv *env , jclass class , jstring filename , jint mode ){
  
  return -1 ;
}

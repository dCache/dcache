#include <jni.h>

extern int checkUser(const char *, const char *, const char *);

JNIEXPORT jint JNICALL 
Java_diskCacheV111_admin_PAM_1Auth_checkUser(JNIEnv *env, jobject obj, jstring srv, jstring user, jstring pass)
{

	jint rc;

	const char *service = (*env)->GetStringUTFChars(env, srv, 0);
	const char *usr = (*env)->GetStringUTFChars(env, user, 0);
	const char *pas = (*env)->GetStringUTFChars(env, pass, 0);
	
	rc = checkUser(service, usr, pas);

	(*env)->ReleaseStringUTFChars(env, srv, service);
	(*env)->ReleaseStringUTFChars(env, user, usr);
	(*env)->ReleaseStringUTFChars(env, pass, pas);
		
	return rc;
}

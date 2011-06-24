// $Id: KPWDAuthorizationPlugin.java,v 1.12 2007-04-17 21:46:54 tdh Exp $
// $Log: not supported by cvs2svn $

/*
 * KPWDAuthorizationPlugin.java
 *
 * Created on January 29, 2005
 */

package diskCacheV111.services.authorization;

import java.net.Socket;
import java.security.cert.X509Certificate;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.auth.KAuthFile;
import org.dcache.auth.UserAuthRecord;
import gplazma.authz.AuthorizationException;
import gplazma.authz.AuthorizationConfig;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.plugins.CachingPlugin;


/**
 * @author Abhishek Singh Rana, Timur Perelmutov, Ted Hesselroth
 */

public class KPWDAuthorizationPlugin extends CachingPlugin {

    private static Logger logger = LoggerFactory.getLogger(KPWDAuthorizationPlugin.class);
	private String kAuthFilePath;
    private long authRequestID=0;
	GSSContext context;
	String desiredUserName;
    AuthorizationConfig authConfig;

    public KPWDAuthorizationPlugin(long authRequestID)
            throws AuthorizationException {
        super(authRequestID);
        logger.debug("kpwd plugin now loaded");
  }

    public KPWDAuthorizationPlugin(String authConfigFilePath, long authRequestID)
            throws AuthorizationException {
        this(authRequestID);
        try {
            authConfig = new AuthorizationConfig(authConfigFilePath, authRequestID);
        } catch(java.io.IOException ioe) {
            logger.error("Exception in AuthorizationConfig instantiation :" + ioe);
            throw new AuthorizationException(ioe.toString());
  }
        try {
            this.kAuthFilePath = authConfig.getKpwdPath();
        } catch(Exception e) {
            logger.error("Exception getting Kpwd Path from configuration :" +e);
            throw new AuthorizationException(e.toString());
	}
  }

    public gPlazmaAuthorizationRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

    this.context = context;
    GSSName GSSId;
		String subjectDN;

    try {
			GSSId = context.getSrcName();
			subjectDN = GSSId.toString();
            logger.debug("Subject DN from GSSContext extracted as: " +subjectDN);
		}
		catch(org.ietf.jgss.GSSException gsse ) {
            logger.error("Error extracting Subject DN from GSSContext: " +gsse);
            throw new AuthorizationException(gsse.toString());
		}

    return authorize(subjectDN, null, null, desiredUserName, serviceUrl, socket);
  }

    public gPlazmaAuthorizationRecord authorize(String subjectDN, String role, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        logger.debug("Using dcache.kpwd configuration: " + kAuthFilePath);
        KAuthFile authF;
		this.desiredUserName = desiredUserName;

		String user_name;

		try {
			authF = new KAuthFile(kAuthFilePath);
		}
		catch(Exception e) {
            logger.error("Exception in KAuthFile instantiation: " +e);
            throw new AuthorizationException(e.toString());
		}

		if (desiredUserName != null) {
            logger.debug("Desired Username requested as: " + desiredUserName);
			user_name = desiredUserName;
		}
		else {
            logger.info("Requesting mapping for User with DN: " + subjectDN);
			user_name = authF.getIdMapping(subjectDN);
            logger.info("dcache.kpwd service returned Username: " + user_name);
			if (user_name == null) {
				String denied = DENIED_MESSAGE + ": Cannot determine Username for DN " + subjectDN;
                logger.warn(denied);
                throw new AuthorizationException(denied);
			}
		}

		UserAuthRecord authRecord = authF.getUserRecord(user_name);
		if (authRecord == null) {
            logger.error("User " +user_name+ " is not found in authorization records");
            logger.error("dcache.kpwd Authorization Service plugin: Authorization denied for user: " + user_name + " with subject DN: " + subjectDN);
            throw new AuthorizationException("User " +user_name+ " is not found in authorization records");
    }

		if (!authRecord.hasSecureIdentity(subjectDN)) {
            logger.error("dcache.kpwd Authorization Service plugin: Authorization denied for user: "+user_name + " with subject DN: " +subjectDN);
            throw new AuthorizationException("dcache.kpwd Authorization Plugin: Authorization denied for user " +user_name+ " with Subject DN " +subjectDN);
    }

        authRecord.DN = subjectDN;

        gPlazmaAuthorizationRecord gauthrec = getgPlazmaAuthorizationRecord(authRecord);
        return gauthrec;

	}


    /** Extract values from UserAuthRecord and write in getgPlazmaAuthorizationRecord
     * @return A filled-in gPlazmaAuthorizationRecord.
     * @param authrec The record to be converted.
     */
    public static gPlazmaAuthorizationRecord getgPlazmaAuthorizationRecord(UserAuthRecord authrec) {
        if (authrec == null) return new gPlazmaAuthorizationRecord();

        return new gPlazmaAuthorizationRecord(authrec.Username,
                authrec.ReadOnly,
                authrec.priority,
                authrec.UID,
                authrec.GIDs,
                authrec.Home,
                authrec.Root,
                authrec.FsRoot);
	}


} //end of class KPWDAuthorizationPlugin

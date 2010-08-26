// $Id: VORoleMapAuthzPlugin.java,v 1.20 2007-04-17 21:46:15 tdh Exp $
// $Log: not supported by cvs2svn $

/*
 * VORoleMapAuthzPlugin.java
 * 
 * Created on March 30, 2005
 */

package gplazma.authz.plugins.vorolemap;

import java.util.*;
import java.lang.*;
import java.net.Socket;
import java.security.cert.X509Certificate;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.gridforum.jgss.ExtendedGSSContext;
import org.apache.log4j.*;

import gplazma.authz.records.*;
import gplazma.authz.AuthorizationException;
import gplazma.authz.util.X509CertUtil;
import gplazma.authz.plugins.RecordMappingPlugin;

/**
 *
 * @author Abhishek Singh Rana, Ted Hesselroth
 */

public class VORoleMapAuthzPlugin extends RecordMappingPlugin {

    private String gridVORoleMapPath;
    private long authRequestID=0;
    gPlazmaAuthorizationRecord authRecord;
    GSSContext context;
    String desiredUserName;

    public static final String capnull = "/Capability=NULL";
    public static final int capnulllen = capnull.length();
    public static final String rolenull ="/Role=NULL";
    public static final int rolenulllen = rolenull.length();

    public VORoleMapAuthzPlugin(String gridVORoleMapPath, String storageAuthzPath, long authRequestID)
            throws AuthorizationException {
        super(storageAuthzPath, authRequestID);
        this.gridVORoleMapPath = gridVORoleMapPath;
        getLogger().info("grid-vorolemap plugin will use " + gridVORoleMapPath);
    }

    public gPlazmaAuthorizationRecord authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        String gssIdentity;
        String fqanValue;
        ExtendedGSSContext extendedcontext;
        if (context instanceof ExtendedGSSContext) {
            extendedcontext = (ExtendedGSSContext) context;
        }
        else {
            getLogger().error("Received context not instance of ExtendedGSSContext, Plugin exiting ...");
            return null;
        }

        try {
            gssIdentity = context.getSrcName().toString();
        } catch (GSSException gsse) {
            getLogger().error("Caught GSSException in getting DN " + gsse);
            return null;
        }

        try {
            Iterator<String> fqans = X509CertUtil.getFQANsFromContext(extendedcontext).iterator();
            fqanValue = fqans.hasNext() ? fqans.next() : "";
        } catch (Exception e) {
            getLogger().error("Caught Exception in extracting group and role " + e);
            return null;
        }

        return authorize(gssIdentity, fqanValue, null, desiredUserName, serviceUrl, socket);
    }

    public gPlazmaAuthorizationRecord authorize(String subjectDN, String role, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        String user_name=null;

        String identity = (role!=null) ? subjectDN.concat(role) : subjectDN;

        VORoleMapHandler voRoleMapHandler;

        try {
            voRoleMapHandler = new VORoleMapHandler(gridVORoleMapPath, getAuthRequestID());
            voRoleMapHandler.setLogLevel(getLogger().getLevel());
        } catch(Exception ase) {
            getLogger().error("Exception in reading authz-vorole-mapping configuration file: ");
            getLogger().error(gridVORoleMapPath + " " + ase);
            throw new AuthorizationException(ase.toString());
        }

        if (desiredUserName == null) getLogger().debug("Desired Username not requested. Will attempt a mapping.");
        // Do even if username is requested, in order to check blacklist
        try {
            getLogger().info("Requesting mapping for User with DN and role: " + identity);
            user_name = mapUsername(voRoleMapHandler, identity);
            // Check for wildcard DN
            if(user_name==null && role!=null) {
                identity = "*".concat(role);
                user_name = mapUsername(voRoleMapHandler, identity);
            }
        } catch(Exception e) {
            throw new AuthorizationException(e.toString());
        }
        if (desiredUserName == null) getLogger().info("Subject DN + Grid Vo Role are mapped to Username: " + user_name);

        if (user_name == null) {
            String denied = DENIED_MESSAGE + ": Cannot determine Username from grid-vorolemap for DN " + subjectDN + " and role " + role;
            getLogger().warn(denied);
            throw new AuthorizationException(denied);
        }
        if (user_name.equals("-")) {
            String why = ": for DN " + subjectDN + " and role " + role;
            throw new AuthorizationException(REVOCATION_MESSAGE + why);
        }

        if (desiredUserName != null) {
            getLogger().debug("Desired Username requested as: " +desiredUserName);
            try {
                user_name = mapUsername(voRoleMapHandler, identity, desiredUserName);
                if(user_name==null) {
                    identity = "*".concat(role);
                    user_name = mapUsername(voRoleMapHandler, identity, desiredUserName);
                }
            } catch(Exception e) {
                throw new AuthorizationException(e.toString());
            }
            if (user_name == null) {
                String denied = DENIED_MESSAGE + ": Requested username " + desiredUserName + " not found for " + subjectDN + " and role " + role;
                getLogger().warn(denied);
                throw new AuthorizationException(denied);
            }
            if (user_name.equals("-")) {
                String why = ": for DN " + subjectDN + " and role " + role;
                throw new AuthorizationException(REVOCATION_MESSAGE + why);
            }
        }

        gPlazmaAuthorizationRecord gauthrec = getgPlazmaAuthorizationRecord(user_name, subjectDN, role);

        return gauthrec;
    }

    private String mapUsername(VORoleMapHandler gridVORolemapServ, String gridFineGrainIdentity) throws Exception {
        String user_name;
        try {
            user_name = gridVORolemapServ.getMappedUsername(gridFineGrainIdentity);
            if(user_name==null) {
                // Remove "/Capability=NULL" and "/Role=NULL"
                if(gridFineGrainIdentity.endsWith(capnull))
                    gridFineGrainIdentity = gridFineGrainIdentity.substring(0, gridFineGrainIdentity.length() - capnulllen);
                if(gridFineGrainIdentity.endsWith(rolenull))
                    gridFineGrainIdentity = gridFineGrainIdentity.substring(0, gridFineGrainIdentity.length() - rolenulllen);
                user_name = gridVORolemapServ.getMappedUsername(gridFineGrainIdentity);
            }
        } catch (Exception e) {
            throw e;
        }

        return user_name;
    }

    private String mapUsername(VORoleMapHandler gridVORolemapServ, String gridFineGrainIdentity, String username) throws Exception {
        String user_name;
        try {
            user_name = gridVORolemapServ.getMappedUsername(gridFineGrainIdentity, username);
            if(user_name==null) {
                // Remove "/Capability=NULL" and "/Role=NULL"
                if(gridFineGrainIdentity.endsWith(capnull))
                    gridFineGrainIdentity = gridFineGrainIdentity.substring(0, gridFineGrainIdentity.length() - capnulllen);
                if(gridFineGrainIdentity.endsWith(rolenull))
                    gridFineGrainIdentity = gridFineGrainIdentity.substring(0, gridFineGrainIdentity.length() - rolenulllen);
                user_name = gridVORolemapServ.getMappedUsername(gridFineGrainIdentity, username);
            }
        } catch (Exception e) {
            throw e;
        }

        return user_name;
    }

} //end of class VORoleMapAuthzPlugin

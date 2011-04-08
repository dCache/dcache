// $Id: GridMapFileAuthzPlugin.java,v 1.15 2007-04-17 21:46:15 tdh Exp $
// $Log: not supported by cvs2svn $

/*
 * GridMapFileAuthzPlugin.java
 *
 * Created on March 30, 2005
 */

package gplazma.authz.plugins.gridmapfile;

import java.util.*;
import java.lang.*;
import java.net.Socket;
import java.security.cert.X509Certificate;
import java.io.File;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gplazma.authz.records.*;
import gplazma.authz.AuthorizationException;
import gplazma.authz.plugins.RecordMappingPlugin;

/**
 *
 * @author Abhishek Singh Rana, Ted Hesselroth
 */

public class GridMapFileAuthzPlugin extends RecordMappingPlugin {
    private static final Logger logger = LoggerFactory.getLogger(GridMapFileAuthzPlugin.class);
    private static final String GRIDMAP_FILENAME = "grid-mapfile";
    private static GridMapFile gridmap;
    gPlazmaAuthorizationRecord authRecord;
    GSSContext context;
    String desiredUserName;

    public GridMapFileAuthzPlugin(String gridMapFilePath, String storageAuthzPath, long authRequestID)
    {
        super(storageAuthzPath, authRequestID);
        logger.info("grid-mapfile plugin will use {}", gridMapFilePath);

        synchronized (GridMapFileAuthzPlugin.class) {
            if (gridmap == null) {
                File file = new File(gridMapFilePath);
                if (!file.getName().equals(GRIDMAP_FILENAME)) {
                    logger.warn("The grid-mapfile filename {} is not as expected.", file);
                    logger.warn("WARNING: Possible security violation.");
                }

                gridmap = new GridMapFile(file);
            }
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
            logger.info("Subject DN from GSSContext extracted as: " +subjectDN);
        }
        catch(org.ietf.jgss.GSSException gsse ) {
            logger.error(" Error extracting Subject DN from GSSContext: " +gsse);
            throw new AuthorizationException(gsse.toString());
        }

        return authorize(subjectDN, null, null, desiredUserName, serviceUrl, socket);
    }

    @Override
    public gPlazmaAuthorizationRecord authorize(String subjectDN, String role, X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
            throws AuthorizationException {

        this.desiredUserName = desiredUserName;
        String user_name;

        gridmap.refresh();

        if (desiredUserName != null) {
            logger.debug("Desired Username requested as: " +desiredUserName);
        }

        logger.info("Requesting mapping for User with DN: " + subjectDN);

        try {
            user_name = gridmap.getMappedUsername(subjectDN);
        }
        catch(Exception e) {
            throw new AuthorizationException(e.toString());
        }
        logger.info("Subject DN is mapped to Username: " + user_name);

        if (user_name == null) {
            String denied = DENIED_MESSAGE + ": Cannot determine Username from grid-mapfile for DN " + subjectDN;
            logger.warn(denied);
            throw new AuthorizationException(denied);
        }
        if (desiredUserName != null && !user_name.equals(desiredUserName)) {
            String denied = DENIED_MESSAGE + ": Requested username " + desiredUserName + " does not match returned username " + user_name + " for " + subjectDN;
            logger.warn(denied);
            throw new AuthorizationException(denied);
        }

        gPlazmaAuthorizationRecord gauthrec = getgPlazmaAuthorizationRecord(user_name, subjectDN, role);

        return gauthrec;
    }
} //end of class GridMapFileAuthzPlugin

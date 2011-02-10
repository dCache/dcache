package gplazma.authz.plugins;

import gplazma.authz.records.DCacheSRMauthzRecordsService;
import gplazma.authz.records.DynamicAuthorizationRecord;
import gplazma.authz.records.gPlazmaAuthorizationRecord;
import gplazma.authz.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RecordMappingPlugin.java
 * User: tdh
 * Date: Sep 15, 2008
 * Time: 4:11:01 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class RecordMappingPlugin extends CachingPlugin {
    private static final Logger logger = LoggerFactory.getLogger(RecordMappingPlugin.class);
    public String storageAuthzPath;


    public RecordMappingPlugin(String storageAuthzPath, long authRequestID) {
        super(authRequestID);
        this.storageAuthzPath = storageAuthzPath;
        logger.debug("RecordMappingPlugin will use " + storageAuthzPath);
    }

    public String getStorageAuthzPath() {
        return storageAuthzPath;
    }

    public gPlazmaAuthorizationRecord getgPlazmaAuthorizationRecord(String username, String subjectDN, String role) throws AuthorizationException {

        DCacheSRMauthzRecordsService storageRecordsServ;

        try {
            storageRecordsServ = new DCacheSRMauthzRecordsService(storageAuthzPath);
        } catch(Exception ase) {
            logger.error("Exception in reading storage-authzdb configuration file: ");
            logger.error(storageAuthzPath + " " + ase);
            throw new AuthorizationException(ase.toString());
        }

        gPlazmaAuthorizationRecord authRecord = storageRecordsServ.getStorageUserRecord(username);

        if (authRecord == null) {
            logger.error("No record found in "+storageAuthzPath+
                    " for username=\""+  username+"\"");
            return null;
        }

        if(authRecord instanceof DynamicAuthorizationRecord) {
            DynamicAuthorizationRecord dynrecord = (DynamicAuthorizationRecord) authRecord;
            dynrecord.subjectDN = subjectDN;
            dynrecord.role = role;
            authRecord = getDynamicRecord(username, dynrecord);
        }

        String  user=authRecord.getUsername(); if(user==null) {
        String denied = DENIED_MESSAGE + ": received null username " + user;
        logger.warn(denied);
        throw new AuthorizationException(denied);
    }

        //Integer uid = localId.getUID(); if(uid==null) {
        int uid = authRecord.getUID(); if(uid==-1) {
        String denied = DENIED_MESSAGE + ": uid not found for " + user;
        logger.warn(denied);
        throw new AuthorizationException(denied);
    }

        //Integer gid = localId.getGID(); if(gid==null) {
        int[] gids = authRecord.getGIDs(); if(gids[0]==-1) {
        String denied = DENIED_MESSAGE + ": gids not found for " + user;
        logger.warn(denied);
        throw new AuthorizationException(denied);
    }

        //String home = localId.getRelativeHomePath(); if(home==null) {
        String home = authRecord.getHome(); if(home==null) {
        String denied = DENIED_MESSAGE + ": relative home path not found for " + user;
        logger.warn(denied);
        throw new AuthorizationException(denied);
    }

        //String root = localId.getRootPath(); if(root==null) {
        String root = authRecord.getRoot(); if(root==null) {
        String denied = DENIED_MESSAGE + ": root path not found for " + user;
        logger.warn(denied);
        throw new AuthorizationException(denied);
    }

        return authRecord;
    }

}

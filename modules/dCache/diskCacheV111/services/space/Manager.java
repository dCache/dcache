// $Id: Manager.java,v 1.63 2007-11-01 00:57:07 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.62  2007/10/27 02:44:46  timur
// new option spaceManagerEnabled tells space manager if it is enabled in configuration
//
// Revision 1.61  2007/10/24 04:54:40  timur
// removing references and usage of the PoolLinkGroupInfo.getAllowedVOs() method, so from now on LinkGroup authorization without the usage of LinkGroupAuthorizationFile will not be possible
//
// Revision 1.60  2007/10/23 15:45:25  litvinse
// striving for correct English : fixed this typo:
//
//
// linkGroupAuthiorizationFileName -> linkGroupAuthorizationFileName
//
// Revision 1.59  2007/10/16 16:27:32  timur
// Commiting the changes of Gregory J. Sharp that add the commands for listing files in expired and released spaces
//
// Revision 1.58  2007/10/15 22:37:04  timur
// removed usage of the getHsmType() of LinkGroupInfo, added the usage of the isOnlineAllowed(), isNearlineAllowed(), isReplicaAllowed(), isOutputAllowed() and isCustodialAllowed(), users need to use psu set linkGroup custodialAllowed <group name> <true|false>
// psu set linkGroup onlineAllowed <group name> <true|false>
// psu set linkGroup outputAllowed <group name> <true|false>
// psu set linkGroup replicaAllowed <group name> <true|false>
// psu set linkGroup custodialAllowed <group name> <true|false>
//
// Revision 1.57  2007/10/05 21:55:06  timur
// Parsing of the Space Manager's LinkGroup Authorization file now works
// File syntax:
//
// LinkGroup Name followed by the list of the FQAN, one FQAN on separate line, followed by an empty line, which is used as a record separator, or by the end of file. any line that starts with # is a comment, can appear anywhere.
//
// Revision 1.56  2007/10/04 22:33:36  timur
// added the code for parsing LinkGroup Authorization file, made changes to Space Manager to use this file, have not tested that it works, but tested that the old behavior is still the same
//
// Revision 1.55  2007/10/03 18:45:30  timur
//  added schema version table, added schema migration code, modified schema for link group to support new fields: onlineAllowed, nearlineAllowed, replicaAllowed, onlineAllowed and custodialAllowed updated the link group selection code. Waiting for Tigran to commit the changes to the link group code in the pool manager and the link group info structure to make final changes.
//
// Revision 1.54  2007/09/14 22:18:18  timur
// implemented returnign space back to the reservation upon file deletion, did not test it yet
//
// Revision 1.53  2007/08/28 19:02:13  timur
// fix bugs in list commands
//
// Revision 1.51  2007/08/27 22:57:04  timur
// allow link group name in reserve cmd, print correct expiration for infinite reservations
//
// Revision 1.50  2007/08/23 15:29:08  timur
// fixed a bug in link group vo based selection
//
// Revision 1.49  2007/08/22 20:29:52  timur
// space manager understand lifetime=-1 as infinite, get-space-tokens does not check ownership, reserve space admin command takes lifetime in seconds, or -1 for infinite
//
// Revision 1.48  2007/08/14 22:56:43  timur
// use ThreadManager for message processing
//
// Revision 1.47  2007/08/08 22:07:16  timur
// fix LinkGroups update issues found by Gerd, new admin commands for space reservation, release of space
//
// Revision 1.46  2007/08/03 20:20:01  timur
// implementing some of the findbug bugs and recommendations, avoid selfassignment, possible nullpointer exceptions, syncronization issues, etc
//
// Revision 1.45  2007/08/03 15:46:03  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes etc, per findbug recommendations
//
// Revision 1.44  2007/06/18 21:37:47  timur
// better reporting of the expired space reservations, better singletons
//
// Revision 1.43  2007/06/12 22:13:32  timur
// prevent expired tokens from being used even if they were not collected by the release process yet
//
// Revision 1.42  2007/06/08 22:58:59  timur
// more debug info for flush notification processing, streaming custodial space seems to be working
//
// Revision 1.41  2007/05/17 14:27:41  timur
// fixed another bug related to the storage of the new flushed state in database
//
// Revision 1.40  2007/05/16 20:50:20  timur
// fixed a bug related to the storage of the new flushed state in database
//
// Revision 1.39  2007/05/08 00:18:50  timur
// add the ability to return space occupied by a file back to space reservation, if the file is flushed to tape
//
// Revision 1.38  2007/04/16 23:26:06  timur
// make default access latency and retention policy configurable, control behavior (allow implicit reservation or no reservation at all, just fording to PoolManager) for non-srm transfers
//
// Revision 1.37  2007/04/11 23:34:42  timur
// Propagate SrmNoFreeSpace and SrmSpaceReleased errors in case of useSpace function
//
// Revision 1.36  2007/04/07 01:41:58  timur
// make space manager tollerant to the empty string pgPass and make jdbcConnectionPool report errors better
//
// Revision 1.35  2007/03/03 00:44:19  timur
//  make srm reserve space and space get metadata return correct values
//
// Revision 1.34  2007/02/17 05:49:28  timur
// propagate SRM_NO_FREE_SPACE to reserveSpace
//
// Revision 1.33  2007/02/10 04:48:14  timur
//  first version of SrmExtendFileLifetime
//
// Revision 1.32  2007/01/12 21:07:50  timur
// made correction for the CUSTODIAL space selection
//
// Revision 1.31  2007/01/10 23:05:52  timur
// implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
// Revision 1.30  2006/12/29 01:45:11  timur
// removed check for storageGroupsFile parameter, it's not used anymore
//
// Revision 1.29  2006/12/21 17:40:51  timur
// when space is released, available space should be 0
//
// Revision 1.28  2006/11/28 20:29:52  timur
// normalize path, one extra debug print
//
// Revision 1.27  2006/11/16 16:13:52  timur
// getSpaceTokens implemented
//
// Revision 1.26  2006/11/09 22:32:54  timur
// implementation of SrmGetSpaceMetaData function
//
// Revision 1.25  2006/11/02 22:50:21  timur
// make space reservations expire
//
// Revision 1.24  2006/11/01 23:52:52  timur
// first approximation of working, made it understand forwarded and direct select pool messages
//
// Revision 1.23  2006/10/27 21:32:13  timur
// changes to support LinkGroups by space manager
//
// Revision 1.22  2006/10/10 21:03:49  timur
// fixed sql syntax, do implicit space reservation for write operations only
//
// Revision 1.20  2006/09/29 16:57:34  timur
// v2.2 space reservation mostly works, but not for long, since we now change design and logic one more time
//
// Revision 1.19  2006/09/27 23:27:25  timur
// first working version of srmCopy working with the new space manager
//
// Revision 1.18  2006/09/25 21:48:06  timur
// move AccessLatency and RetentionPolicy classes to diskCacheV111.util
//
// Revision 1.17  2006/09/19 00:37:47  timur
// more work for space reservation
//
// Revision 1.16  2006/09/15 22:37:44  timur
// started on storing the RetentionPolicy/AccessLatency in Pnfs
//
// Revision 1.15  2006/08/25 00:16:54  timur
// first complete version of space reservation working with srmPrepareToPut and gridftp door
//
// Revision 1.14  2006/08/22 23:21:54  timur
// srmReleaseSpace is implementedM
//
// Revision 1.13  2006/08/22 00:11:11  timur
// first working version of reserve space, works with prepareToPut and gridftp door only
//
// Revision 1.12  2006/08/18 22:06:43  timur
// srm usage of space by srmPrepareToPut implemented
//
// Revision 1.11  2006/08/16 20:37:22  timur
// few more changes
//
// Revision 1.10  2006/08/15 22:09:45  timur
// got the messages to get through to space manager
//
// Revision 1.8  2006/08/07 02:49:22  timur
// more space reservation code
//
// Revision 1.7  2006/08/04 22:08:22  timur
// include storage groups in linkGroups
//
// Revision 1.6  2006/08/02 22:09:54  timur
// more work for srm space reservation, included voGroup and voRole support
//
// Revision 1.5  2006/08/01 00:19:09  timur
// more space reservation code
//
// Revision 1.4  2006/07/24 22:13:51  timur
// implemented use space cmd
//
// Revision 1.3  2006/07/23 07:31:41  timur
// first implementation of reserve function
//
// Revision 1.1  2006/07/16 05:48:57  timur
// new explicit space manger
//


/*
 * PinManager.java
 *
 * Created on April 28, 2004, 12:54 PM
 */

package diskCacheV111.services.space;
import diskCacheV111.services.space.message.Reserve;
import diskCacheV111.services.space.message.Release;
import diskCacheV111.services.space.message.Use;
import diskCacheV111.services.space.message.CancelUse;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;
import  dmg.cells.nucleus.SystemCell;
import  dmg.cells.nucleus.Cell;
//import  dmg.util.*;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellVersion;
import dmg.util.Args;
import dmg.cells.nucleus.ExceptionEvent;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;
import java.sql.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.List;
import java.util.ArrayList;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FQAN;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.DCapProtocolInfo;

import diskCacheV111.vehicles.PoolMgrGetPoolLinkGroups;
import diskCacheV111.vehicles.PoolLinkGroupInfo;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Hashtable;
import java.util.Iterator;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import java.util.Date;
import diskCacheV111.util.Pgpass;
import diskCacheV111.services.JdbcConnectionPool;
import diskCacheV111.vehicles.PoolMgrSelectPoolMsg;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.PnfsSetStorageInfoMessage;
import diskCacheV111.util.VOInfo;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.GridProtocolInfo;
/**
 *   <pre> Space Manager dCache service provides ability
 *    \to reserve space in the pool linkGroups
 *
 *
 * @author  timur
 */
public class Manager
        extends CellAdapter
        implements Runnable {
    
    /*
     *Configuration variables
     */
    private long spaceReservationCleanupPeriodInSeconds = 60*60; // one hour in seconds
    private String jdbcUrl;
    private String jdbcClass;
    private String user;
    private String pass;
    private String pwdfile;
    private long updateLinkGroupsPeriod = 3*60*1000;  //3 minutes default
    private long expireSpaceReservationsPeriod     = 3*60*1000;  //3 minutes default
    
    private boolean deleteStoredFileRecord = false;
    private String pnfsManager = "PnfsManager";
    private String poolManager = "PoolManager";
    private String quotaManager = "QuotaManager";
    private Thread updateLinkGroups;
    private Thread expireSpaceReservations;
    private AccessLatency defaultLatency = AccessLatency.NEARLINE;
    private RetentionPolicy defaultPolicy = RetentionPolicy.CUSTODIAL;
    private boolean reserveSpaceForNonSRMTransfers;
    private boolean returnFlushedSpaceToReservation=true; 
    private boolean returnRemovedSpaceToReservation=true; 
    
    private String linkGroupAuthorizationFileName = null;
    private boolean spaceManagerEnabled =true;

    /*
     * Database storage related variables
     */
    private static final int currentSchemaVersion = 1;
    private int previousSchemaVersion;
    private static final String SpaceManagerSchemaVersionTableName =
            "srmspacemanagerschemaversion" ;
    
    
    private static final String SpaceManagerNextIdTableName =
            "srmspacemanagernextid";
    private static final String LinkGroupTableName =
            "srmLinkGroup".toLowerCase();
    
    // LinkGroup Storage Groups
    private static final String LinkGroupVOsTableName =
            "srmLinkGroupVOs".toLowerCase();
// Have no complete idea how to implement the quotas
//    private String SpaceReservationQuotaTableName =
//        "srmSpaceReservationQuota".toLowerCase();
    private static final String RetentionPolicyTableName =
            "srmRetentionPolicy".toLowerCase();
    private static final String AccessLatencyTableName =
            "srmAccessLatency".toLowerCase();
    private static final String SpaceTableName =
            "srmSpace".toLowerCase();
    private static final String SpaceFileTableName =
            "srmSpaceFile".toLowerCase();
// assume the states are known numbers for now
//    private String SpaceReservationFileStateTableName =
//        "srmSpaceReservationFileState".toLowerCase();
    
    protected static final String stringType=" VARCHAR(32672) ";
    protected static final String longType=" BIGINT ";
    protected static final String intType=" INTEGER ";
    protected static final String dateTimeType= " TIMESTAMP ";
    protected static final String booleanType= " INT ";
    private JdbcConnectionPool connection_pool;
    private static final String CreateSpaceManagerSchemaVersionTable =
            "CREATE TABLE "+SpaceManagerSchemaVersionTableName+
            " ( version "+ intType + " )";
    private static final String CreateSpaceManagerNextIdTable =
            "CREATE TABLE "+SpaceManagerNextIdTableName+
            " ( NextToken "+ longType + " )";
    
    private static final String CreateLinkGroupTable =
            "CREATE TABLE "+ LinkGroupTableName+" ( "+
            " id "+longType+" NOT NULL PRIMARY KEY "+
            ", name"+stringType+" " +
            ", freeSpaceInBytes "+longType+" "+
            ", lastUpdateTime "+longType +
            ", onlineAllowed"+booleanType+" "+
            ", nearlineAllowed"+booleanType+" "+
            ", replicaAllowed"+booleanType+" "+
            ", outputAllowed"+booleanType+" "+
            ", custodialAllowed"+booleanType+" "+
            ")";
    
    private static final String CreateLinkGroupVOsTable =
            "CREATE TABLE "+ LinkGroupVOsTableName+" ( "+
            " VOGroup "+stringType+" NOT NULL "+
            ", VORole "+stringType+" NOT NULL "+
            ", linkGroupId "+longType+" NOT NULL "+
            ", PRIMARY KEY (VOGroup, VORole, linkGroupId) "+
            ", CONSTRAINT fk_"+LinkGroupVOsTableName+
            "_L FOREIGN KEY (linkGroupId) REFERENCES "+
            LinkGroupTableName +" (id) "+
            " ON DELETE RESTRICT"+
            ")";
    
    private static final String CreateRetentionPolicyTable =
            "CREATE TABLE "+ RetentionPolicyTableName+" ( "+
            " id "+intType+" NOT NULL PRIMARY KEY "+
            ", name "+stringType+" )";
    
    private static final String CreateAccessLatencyTable =
            "CREATE TABLE "+ AccessLatencyTableName+" ( "+
            " id "+intType+" NOT NULL PRIMARY KEY "+
            ", name "+stringType+" )";
    
    private static final String CreateSpaceTable =
            "CREATE TABLE "+ SpaceTableName+" ( "+
            " id "+longType+" NOT NULL PRIMARY KEY "+
            ", voGroup "+stringType+" "+
            ", voRole "+stringType+" "+
            ", retentionPolicy "+intType+" "+
            ", accessLatency "+intType+" "+
            ", linkGroupId "+longType+" "+
            ", sizeInBytes "+longType+" "+
            ", creationTime "+longType+" "+
            ", lifetime "+longType+" "+
            ", description "+stringType+" "+
            ", state "+intType+" "+
            ", CONSTRAINT fk_"+SpaceTableName+
            "_L FOREIGN KEY (linkGroupId) REFERENCES "+
            LinkGroupTableName +" (id) "+
            ", CONSTRAINT fk_"+SpaceTableName+
            "_A FOREIGN KEY (accessLatency) REFERENCES "+
            AccessLatencyTableName +" (id) "+
            ", CONSTRAINT fk_"+SpaceTableName+
            "_R FOREIGN KEY (retentionPolicy) REFERENCES "+
            RetentionPolicyTableName +" (id) "+
            " ON DELETE RESTRICT"+
            ")";
    
    private static final String CreateSpaceFileTable =
            "CREATE TABLE "+ SpaceFileTableName+" ( "+
            " id "+longType+" NOT NULL PRIMARY KEY "+
            ", voGroup "+stringType+" "+
            ", voRole "+stringType+" "+
            ", spaceReservationId "+longType+" "+
            ", sizeInBytes "+longType+" "+
            ", creationTime "+longType+" "+
            ", lifetime "+longType+" "+
            ", pnfsPath "+stringType+" "+
            ", pnfsId "+stringType+" "+
            ", state "+intType+" "+
            ", CONSTRAINT fk_"+SpaceFileTableName+
            "_L FOREIGN KEY (spaceReservationId) REFERENCES "+
            SpaceTableName +" (id) "+
            " ON DELETE RESTRICT"+
            ")";
    private Args _args;
    
    /** Creates a new instance of SpaceManager */
    public Manager(String name, String argString) throws Exception {
        
        super( name ,Manager.class.getName(), argString , false );

        _args = getArgs();
        jdbcUrl = _args.getOpt("jdbcUrl");
        jdbcClass = _args.getOpt("jdbcDriver");
        user = _args.getOpt("dbUser");
        pass = _args.getOpt("dbPass");
        pwdfile = _args.getOpt("pgPass");
        String pgBasedPass = null;
        if (pwdfile != null && !(pwdfile.trim().equals(""))) {
            Pgpass pgpass = new Pgpass(pwdfile);      //VP
            pgBasedPass = pgpass.getPgpass(jdbcUrl, user);   //VP
        }
        if(pgBasedPass != null) {
            pass = pgBasedPass;
        }
        
        connection_pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
        
        spaceManagerEnabled = 
                isOptionSetToTrueOrYes("spaceManagerEnabled",spaceManagerEnabled);
        say("spaceManagerEnabled="+spaceManagerEnabled);
        
        if(_args.getOpt("poolManager") != null) {
            poolManager = _args.getOpt("poolManager");
        }
        if(_args.getOpt("quotaManager") != null) {
            quotaManager = _args.getOpt("quotaManager");
        }
        
        if(_args.getOpt("pnfsManager") != null) {
            pnfsManager = _args.getOpt("pnfsManager");
        }
        
        if(_args.getOpt("updateLinkGroupsPeriod") != null) {
            updateLinkGroupsPeriod = Long.parseLong(_args.getOpt("updateLinkGroupsPeriod"));
        }
        if(_args.getOpt("expireSpaceReservationsPeriod") != null) {
            expireSpaceReservationsPeriod = Long.parseLong(_args.getOpt("expireSpaceReservationsPeriod"));
        }
        
        if(_args.getOpt("cleanupPeriod") != null) {
            spaceReservationCleanupPeriodInSeconds = Long.parseLong(_args.getOpt("cleanupPeriod"));
        }
        if(_args.getOpt("defaultRetentionPolicy") != null) {
            defaultPolicy = RetentionPolicy.getRetentionPolicy(_args.getOpt("defaultRetentionPolicy"));
        }
        if(_args.getOpt("defaultAccessLatency") != null) {
            defaultLatency = AccessLatency.getAccessLatency(_args.getOpt("defaultAccessLatency"));
        }
        if(_args.getOpt("reserveSpaceForNonSRMTransfers") != null) {
            reserveSpaceForNonSRMTransfers=
                    _args.getOpt("reserveSpaceForNonSRMTransfers").equalsIgnoreCase("true");
        }
        if(_args.getOpt("deleteStoredFileRecord") != null) {
            deleteStoredFileRecord=
                    _args.getOpt("deleteStoredFileRecord").equalsIgnoreCase("true");
        }
        if(_args.getOpt("returnFlushedSpaceToReservation") != null) {
            returnFlushedSpaceToReservation=
                    _args.getOpt("returnFlushedSpaceToReservation").equalsIgnoreCase("true");
        }
        if(_args.getOpt("returnRemovedSpaceToReservation") != null) {
            returnRemovedSpaceToReservation=
                    _args.getOpt("returnRemovedSpaceToReservation").equalsIgnoreCase("true");
        }
        
        if(_args.getOpt("linkGroupAuthorizationFileName") != null) {
            String tmp = _args.getOpt("linkGroupAuthorizationFileName").trim();
            if(tmp.length() >0) {
                linkGroupAuthorizationFileName = tmp;
            }
        }
        
        if(deleteStoredFileRecord  && returnRemovedSpaceToReservation) {
            esay("configuration conflict: returnRemovedSpaceToReservation == true and deleteStoredFileRecord == true");
            throw new IllegalArgumentException("configuration conflict: returnRemovedSpaceToReservation == true and deleteStoredFileRecord == true");
        }
        
        try {
            dbinit();
            //initializeDatabasePinRequests();
            
        } catch (Throwable t) {
            esay("error starting space.Manager");
            esay(t);
            start();
            kill();
        }
        //restoreTimers();
        start();
        getNucleus().setPrintoutLevel(3);
        (updateLinkGroups = getNucleus().newThread(this,"UpdateLinkGroups")).start();
        (expireSpaceReservations = getNucleus().newThread(this,"ExpireThreadReservations")).start();
        
    }
      private boolean isOptionSetToTrueOrYes(String value) {
        String tmpstr = _args.getOpt(value);
        return tmpstr != null &&
            (tmpstr.equalsIgnoreCase("true") || 
             tmpstr.equalsIgnoreCase("on")   ||
             tmpstr.equalsIgnoreCase("yes")  ||
             tmpstr.equalsIgnoreCase("enabled") ) ;
    }
    
    private boolean isOptionSetToTrueOrYes(String value, boolean default_value) {
        String tmpstr = _args.getOpt(value);
       if( tmpstr != null) {
            return
             tmpstr.equalsIgnoreCase("true") || 
             tmpstr.equalsIgnoreCase("on")   ||
             tmpstr.equalsIgnoreCase("yes")  ||
             tmpstr.equalsIgnoreCase("enabled") ;
       } else {
            return default_value;
       }
    }
 
    
    public CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.63 $" ); }
    
    public void getInfo(java.io.PrintWriter printWriter) {
        printWriter.println("space.Manager "+getCellName());
        printWriter.println("spaceManagerEnabled="+spaceManagerEnabled);
        printWriter.println("JdbcUrl="+jdbcUrl);
        printWriter.println("jdbcClass="+jdbcClass);
        printWriter.println("databse user="+user);
        printWriter.println("reservation space cleanup period in secs : "+spaceReservationCleanupPeriodInSeconds);
        printWriter.println("updateLinkGroupsPeriod="+updateLinkGroupsPeriod);
        printWriter.println("expireSpaceReservationsPeriod="+expireSpaceReservationsPeriod);
        printWriter.println("deleteStoredFileRecord="+deleteStoredFileRecord);
        printWriter.println("pnfsManager="+pnfsManager);
        printWriter.println("poolManager="+poolManager);
        printWriter.println("defaultLatency="+defaultLatency);
        printWriter.println("defaultPolicy="+defaultPolicy);
        printWriter.println("reserveSpaceForNonSRMTransfers="+reserveSpaceForNonSRMTransfers);
        printWriter.println("returnFlushedSpaceToReservation="+returnFlushedSpaceToReservation);
        printWriter.println("returnRemovedSpaceToReservation="+returnRemovedSpaceToReservation);
        printWriter.println("linkGroupAuthorizationFileName="+linkGroupAuthorizationFileName);
    }
    
    public String hh_release = " <spaceToken> [ <bytes> ] # release the space " +
        "reservation identified by <spaceToken>" ;
    public String ac_release_$_1_2(Args args) throws Exception {
        boolean force = args.getOpt("force") != null;
        long reservationId = Long.parseLong( args.argv(0));
        Long spaceToReleaseInBytes = null;
        if(args.argc() == 1) {
            updateSpaceState(reservationId,SpaceState.RELEASED);
        StringBuffer sb = new StringBuffer();
        Space space = getSpaceWithUsedSize(reservationId);
                space.toStringBuffer(sb);
            return sb.toString();
        } else {
            return "partial release is not supported yet";
        }
    }
    public String hh_update_link_groups = " #triggers update of the link groups";
    public String ac_update_link_groups_$_0(Args args) throws Exception {
        synchronized(updateLinkGroupsSyncObject) {
            updateLinkGroupsSyncObject.notify();
        }
        return "update started";
    }
    
    public String hh_ls = " [-l] <id> # list space reservations";
    public String ac_ls_$_0_1(Args args) throws Exception {
        boolean isLongFormat = args.getOpt("l") != null;
        String id = null;
        if (args.argc() == 1) {
            id = args.argv(0);
        }
        StringBuffer sb = new StringBuffer();
        sb.append("Reservations:\n");
        listSpaceReservations(isLongFormat,id,sb);
        sb.append("\n\nLinkGroups:\n");
        listLinkGroups(isLongFormat,false,id,sb);
        return sb.toString();
    }

     public String hh_ls_link_groups = " [-l] [-a]  <id> # list link groups";
     public String ac_ls_link_groups_$_0_1(Args args) throws Exception {
        boolean isLongFormat = args.getOpt("l") != null;
        boolean all = args.getOpt("a") != null;
        String id = null;
        if (args.argc() == 1) {
            id = args.argv(0);
        }
        StringBuffer sb = new StringBuffer();
        sb.append("\n\nLinkGroups:\n");
        listLinkGroups(isLongFormat,all,id,sb);
        return sb.toString();
    }
     
    public String hh_ls_file_space_tokens = " <pnfsId>|<pnfsPath> # list space tokens " +
        "that contain a file";
     public String ac_ls_file_space_tokens_$_1(Args args) throws Exception {
        String  pnfsPath = args.argv(0);
        PnfsId pnfsId = null;
        try {
            pnfsId = new PnfsId(pnfsPath);
            pnfsPath = null;
        }
        catch(Exception e) {
            pnfsId = null;
        }
        StringBuffer sb = new StringBuffer();
        long[] tokens= getFileSpaceTokens(pnfsId, pnfsPath);
        if(tokens != null && tokens.length >0) {
            for(long token:tokens) {
                sb.append('\n').append(token);
            }
        } else {
            sb.append("\nno space tokens found for file:").append( args.argv(0));
        }
        return sb.toString();
    }
    
    public String hh_reserve = "  [-vog=voGroup] [-vor=voRole] " +
         "[-acclat=AccessLatency] [-retpol=RetentionPolicy] [-desc=Description] " +
        " [-lgid=LinkGroupId]" +
        " [-lg=LinkGroupName]" +
         " <sizeInBytes> <lifetimeInSecs (use quotes around negative one)>";   
    public String ac_reserve_$_2(Args args) throws Exception {
        long sizeInBytes = Long.parseLong(args.argv(0));
        long lifetime=Long.parseLong(args.argv(1));
        if(lifetime > 0) {
            lifetime *= 1000;
        }
        String voGroup=args.getOpt("vog");
        String voRole=args.getOpt("vor");
        String description = args.getOpt("desc");
        String latencyString = args.getOpt("acclat");
        String policyString = args.getOpt("retpol");
        
        AccessLatency latency = latencyString==null?
            defaultLatency:AccessLatency.getAccessLatency(latencyString);
        RetentionPolicy policy = policyString==null?
            defaultPolicy:RetentionPolicy.getRetentionPolicy(policyString);
        
        String lgIdString = args.getOpt("lgid");
        String lgName = args.getOpt("lg");
        if(lgIdString != null && lgName != null) {
            return "Error: both exclusive options -lg and -lgid are specified";
        }
        long reservationId;
        if(lgIdString == null && lgName == null) {
            reservationId = reserveSpace(voGroup,
                voRole,
                sizeInBytes,
                latency , 
                policy, 
                lifetime,
                description);
        } else {
            
          long lgId;
            if (lgIdString != null){
                lgId =Long.parseLong(lgIdString);
            } else {
              LinkGroup lg = getLinkGroupByName(lgName);
              if(lg ==null) {
                  return "Error, could not find link group with name = '"+lgName+"'";
              }
              lgId = lg.getId();
            }
          
            reservationId = reserveSpaceInLinkGroup(
                lgId,
                voGroup,
                voRole,
                sizeInBytes,
                latency , 
                policy, 
                lifetime,
                description);
        }
        StringBuffer sb = new StringBuffer();
        Space space = getSpaceWithUsedSize(reservationId);
                space.toStringBuffer(sb);
        return sb.toString();
    }

    public String hh_listInvalidSpaces = " [-e] [-r]" +
                                         " # e=expired, r=released, default is both";
    
    private static final int RELEASED = 1;
    private static final int EXPIRED = 2;

    private static String[] badSpaceType= { "released",
                                            "expired",
                                            "released or expired" };

    // @return a string containing a newline-separated list of all the fields
    //         of each expired/released space.
    public String ac_listInvalidSpaces_$_0_2( Args args )
        throws Exception
    {
        // Parse options.
        int argCount = args.optc();
        boolean doExpired = args.getOpt( "e" ) != null;
        boolean doReleased = args.getOpt( "r" ) != null;
        // If no options were given, do both released and expired objects.
        int listOptions = RELEASED | EXPIRED;
        if ( doExpired || doReleased )
        {
            // Add in the options that were specified
            listOptions = 0;
            if ( doExpired )
            {
                listOptions = EXPIRED;
                --argCount;
            }
            if ( doReleased )
            {
                listOptions |= RELEASED;
                --argCount;
            }
        }

        if ( argCount != 0 )
        {
            return "Unrecognized option.\nUsage: listInvalidSpaces" +
                   hh_listInvalidSpaces;
        }
        
        // Get a list of the expired spaces
        List< Space > expiredSpaces = listInvalidSpaces( listOptions );
        if ( expiredSpaces.isEmpty() )
        {
            return "There are no " + badSpaceType[ listOptions-1 ] + " spaces.";
        }
        // For each space, convert it to a string, one per line.
        StringBuffer report = new StringBuffer();
        for ( Space es : expiredSpaces )
        {
            report.append( es.toString() ).append( '\n' );
        }
        return report.toString();
    }


    private static final String selectInvalidSpaces = "SELECT * FROM "
            + SpaceTableName + " WHERE state = '";

    // This method returns an array of all the spaces that have exceeded
    // their lifetime.
    // NB. We may need to also list all the spaces that have been released,
    //     since they may also contain files that need to be deleted.
    public List< Space > listInvalidSpaces( int spaceTypes )
        throws SQLException,
               Exception
    {
        String query;
        switch ( spaceTypes )
        {
        case EXPIRED: // do just expired
            query = selectInvalidSpaces + SpaceState.EXPIRED.getStateId() + "'";
            break;

        case RELEASED: // do just released
            query = selectInvalidSpaces + SpaceState.RELEASED.getStateId() + "'";
            break;

        case RELEASED | EXPIRED: // do both
            query = selectInvalidSpaces + SpaceState.EXPIRED.getStateId() +
                
            "' OR state = '" + SpaceState.RELEASED.getStateId() + "'";
            break;

        default: // something is broken
            String msg = "listInvalidSpaces: got invalid space type "
                         + spaceTypes;
            esay( msg );
            throw new Exception( msg );
        }

        Connection con = null;
        // Note that we return an empty list if "set" is empty.
        List< Space > result = new ArrayList< Space >();

        try
        {
            say( "executing statement: " + selectInvalidSpaces );
            con = connection_pool.getConnection();
            PreparedStatement sqlStatement = con.prepareStatement( query );
            ResultSet set = sqlStatement.executeQuery();
            while ( set.next() )
            {
                Space space = new Space(
                    set.getLong( "id" ),
                    set.getString( "voGroup" ),
                    set.getString( "voRole" ),
                    RetentionPolicy.getRetentionPolicy( set.getInt( "retentionPolicy" ) ),
                    AccessLatency.getAccessLatency( set.getInt( "accessLatency" ) ),
                    set.getLong( "linkGroupId" ),
                    set.getLong( "sizeInBytes" ),
                    set.getLong( "creationTime" ),
                    set.getLong( "lifetime" ),
                    set.getString( "description" ),
                    SpaceState.getState( set.getInt( "state" ) ) );
                result.add( space );
            }
            set.close();
            sqlStatement.close();
            connection_pool.returnConnection( con );
            con = null;
        }
        catch ( SQLException sqe )
        {
            esay( sqe );
            con.rollback();
            connection_pool.returnFailedConnection( con );
            con = null;
            throw sqe;
        }
        finally
        {
            if ( con != null )
            {
                connection_pool.returnConnection( con );
            }
        }
        return result;
    }
    
    
    private static String selectFilesInSpace =  "SELECT * FROM " +
        SpaceFileTableName + " WHERE spaceReservationId = ?";

    public String hh_listFilesInSpace = " <space-id>";
    // @return a string containing a newline-separated list of the files in
    //         the space specified by <i>space-id</i>.
    public String ac_listFilesInSpace_$_1( Args args )
        throws Exception
    {
        long spaceId = Long.parseLong( args.argv( 0 ) );
        // Get a list of the Invalid spaces
        List< File > filesInSpace = listFilesInSpace( spaceId );
        if ( filesInSpace.isEmpty() )
        {
            return "There are no files in this space.";
        }
        // For each space, convert it to a string, one per line.
        StringBuffer report = new StringBuffer();
        for ( File file : filesInSpace )
        {
            report.append( file.toString() ).append( '\n' );
        }
        return report.toString();
    }
    
    
    // This method returns an array of all the files in the specified space.
    public List< File > listFilesInSpace( long spaceId )
        throws SQLException
    {
        Connection con = null;
        List< File > result = new ArrayList< File >();
        try
        {
            say( "executing statement: " + selectFilesInSpace );
            con = connection_pool.getConnection();
            PreparedStatement sqlStatement = con.prepareStatement( selectFilesInSpace );
            sqlStatement.setLong( 1, spaceId );
            ResultSet fileSet = sqlStatement.executeQuery();
            // Note that we return an empty list if "fileSet" is empty.
            while ( fileSet.next() )
            {       
                File file = extractFileFromResultSet( fileSet );
                result.add( file );
            }
            fileSet.close();
            sqlStatement.close();
            connection_pool.returnConnection( con );
            con = null;
        }
        catch ( SQLException sqe )
        {
            esay( sqe );
            con.rollback();
            connection_pool.returnFailedConnection( con );
            con = null;
            throw sqe;
        }
        finally
        {
            if ( con != null )
            {
                connection_pool.returnConnection( con );
            }
        }
        return result;
    }
            
    private void dbinit() throws SQLException {
        try {
            
            // Add driver to JDBC
            Class.forName(jdbcClass);
            
            //connect
            Connection _con = connection_pool.getConnection();
            _con.setAutoCommit(true);
            //get database info
            DatabaseMetaData md = _con.getMetaData();
            // SpaceManagerNextIdTableName
            // LinkGroupTableName
            // SpaceTableName
            // SpaceFileTableName
            
            String tables[] = new String[] {
                SpaceManagerSchemaVersionTableName,
                SpaceManagerNextIdTableName,
                LinkGroupTableName,
                LinkGroupVOsTableName,
                RetentionPolicyTableName,
                AccessLatencyTableName,
                SpaceTableName,
                SpaceFileTableName};
            String createTables[] =
                    new String[] {
                CreateSpaceManagerSchemaVersionTable,
                CreateSpaceManagerNextIdTable,
                CreateLinkGroupTable,
                CreateLinkGroupVOsTable,
                CreateRetentionPolicyTable,
                CreateAccessLatencyTable,
                CreateSpaceTable,
                CreateSpaceFileTable};
            Map<String,Boolean> created = new Hashtable<String,Boolean>();
            for (int i =0; i<tables.length;++i) {
                
                created.put(tables[i], Boolean.FALSE);
                
                ResultSet tableRs = md.getTables(null, null, tables[i] , null );
                
                
                if(!tableRs.next()) {
                    try {
                        Statement s = _con.createStatement();
                        say("dbinit trying "+createTables[i]);
                        int result = s.executeUpdate(createTables[i]);
                        s.close();
                       created.put(tables[i], Boolean.TRUE);
                    } catch(SQLException sqle) {
                        
                        esay("SQL Exception (relation "+tables[i]+" could already exist)");
                        esay(sqle);
                        
                    }
                }
            }
            
            updateSchemaVersion(created,_con);
            
            // need to initialize the NextToken value
            String select = "SELECT * FROM "+SpaceManagerNextIdTableName;
            Statement s = _con.createStatement();
            ResultSet set = s.executeQuery(select);
            if(!set.next()) {
                String insert = "INSERT INTO "+ SpaceManagerNextIdTableName+
                        " VALUES ( 0 )";
                //say("dbInit trying "+insert);
                Statement s1 = _con.createStatement();
                say("dbInit trying "+insert);
                int result = s1.executeUpdate(insert);
                s1.close();
            } else {
                say("dbInit set.next() returned nonnull");
            }
            s.close();

            insertRetentionPolicies(_con);
            insertAccessLatencies(_con);
            // to support our transactions
            _con.setAutoCommit(false);
            connection_pool.returnConnection(_con);
        } catch (SQLException sqe) {
            esay(sqe);
            throw sqe;
        } catch (Exception ex) {
            esay(ex);
            throw new SQLException(ex.toString());
        }
        
        
    }
    
    private void updateSchemaVersion (Map<String,Boolean> created, Connection _con) 
        throws SQLException {
        
            if(!created.get(SpaceManagerSchemaVersionTableName)) {
                String select = "SELECT * FROM "+
                    SpaceManagerSchemaVersionTableName ;
                Statement s1 = _con.createStatement();
                 say("dbInit trying "+select);
                 ResultSet schema = s1.executeQuery(select);
                 if(schema.next()) {
                     previousSchemaVersion  = schema.getInt("version");
                     String update  = "UPDATE "+
                         SpaceManagerSchemaVersionTableName +
                         " SET version = "+currentSchemaVersion ;
                    Statement s2 = _con.createStatement();
                     say("dbInit trying "+update);
                    int result = s2.executeUpdate(update);
                    if(result != 1) {
                        esay ("update of schema version gave result="+result);
                    }
                    s2.close();
                 } else {
                     // nothing is found in the schema version table,
                     // pretend it was just created
                     created.put(SpaceManagerSchemaVersionTableName, Boolean.TRUE);
                 }
                 s1.close();
           }  
            
           if(created.get(SpaceManagerSchemaVersionTableName)) {
                if(created.get(LinkGroupTableName)) {
                    //everything is created for the first time
                    previousSchemaVersion = currentSchemaVersion;
                } else {
                    //database was created when the Schema Version was not in existance
                    previousSchemaVersion = 0;
                }
                String insert = "INSERT INTO "+SpaceManagerSchemaVersionTableName +
                    " VALUES ( "+currentSchemaVersion+" )";
                Statement s1 = _con.createStatement();
                 say("dbInit trying "+insert);
                int result = s1.executeUpdate(insert);
                s1.close();
                
            }
            
            if(previousSchemaVersion == currentSchemaVersion) {
                return;
            }
            
            if(previousSchemaVersion == 0) {
                try {
                    updateSchemaToVersion1(_con);
                }
                catch (SQLException sqle) {
                    esay("updateSchemaToVersion1 failed, shcema might have been updated already:");
                    esay(sqle.getMessage());
                }
                previousSchemaVersion = 1;
            }
    }
    
    private void updateSchemaToVersion1(Connection _con) throws SQLException{
        
        String alter = "ALTER TABLE " + LinkGroupTableName+
            " ADD COLUMN  onlineAllowed INT,"+
            " ADD COLUMN  nearlineAllowed INT,"+
            " ADD COLUMN  replicaAllowed INT,"+
            " ADD COLUMN  outputAllowed INT,"+
            " ADD COLUMN  custodialAllowed INT";
        Statement s = _con.createStatement();
         say("dbInit trying "+alter);
        s.executeUpdate(alter);
        s.close();
        
        String update = "UPDATE  "+ LinkGroupTableName+
            "\n SET onlineAllowed = 1 ," +
            "\n     nearlineAllowed = 1 ,"+
            "\n     replicaAllowed = CASE WHEN hsmType= 'None' THEN 1 ELSE 0 END,"+
            "\n     outputAllowed = CASE WHEN hsmType= 'None' THEN 1 ELSE 0 END,"+
            "\n     custodialAllowed = CASE WHEN hsmType= 'None' THEN 0 ELSE 1 END";
        s = _con.createStatement();
         say("dbInit trying "+update);
        s.executeUpdate(update);
        s.close();
        
        String alter1 = "ALTER TABLE " + LinkGroupTableName+
            " DROP  COLUMN  hsmType ";
         s = _con.createStatement();
         say("dbInit trying "+alter1);
         s.executeUpdate(alter1);
         s.close();
           
         
    }
    
    private static final String countPolicies = 
            "SELECT count(*) from "+RetentionPolicyTableName;
    private void insertRetentionPolicies(Connection _con) throws  SQLException{
        RetentionPolicy[] policies = RetentionPolicy.getAllPoliciess();
        Statement sqlStatement = _con.createStatement();
        ResultSet rs = sqlStatement.executeQuery( countPolicies);
        if(rs.next()){
            int count = rs.getInt(1);
            if(count != policies.length) {
                for(int i = 0; i<policies.length; ++i) {
                    
                    String insertPolicy = "INSERT INTO "+
                            RetentionPolicyTableName+" VALUES ("+
                            policies[i].getId()+
                            ", '"+policies[i].toString()+"' )";
                    say("executing statement: "+insertPolicy);
                    try{
                        sqlStatement = _con.createStatement();
                        int result = sqlStatement.executeUpdate( insertPolicy );
                        sqlStatement.close();
                    } catch(SQLException sqle) {
                        //ignoring, state might be already in the table
                        esay(sqle);
                    }
                }
            }
        }
    }
    
    private static final String countLatencies = 
            "SELECT count(*) from "+AccessLatencyTableName;
    private void insertAccessLatencies(Connection _con) throws  SQLException{
        AccessLatency[] latencies = AccessLatency.getAllLatencies();
        Statement sqlStatement = _con.createStatement();
        ResultSet rs = sqlStatement.executeQuery( countLatencies);
        if(rs.next()){
            int count = rs.getInt(1);
            if(count != latencies.length) {
                for(int i = 0; i<latencies.length; ++i) {
                    
                    String insertLatency = "INSERT INTO "+
                            AccessLatencyTableName+" VALUES ("+
                            latencies[i].getId()+
                            ", '"+latencies[i].toString()+"' )";
                    say("executing statement: "+insertLatency);
                    try{
                        sqlStatement = _con.createStatement();
                        int result = sqlStatement.executeUpdate( insertLatency );
                        sqlStatement.close();
                    } catch(SQLException sqle) {
                        //ignoring, state might be already in the table
                        esay(sqle);
                    }
                }
            }
        }
    }
    private static final String selectNextIdForUpdate = "SELECT * from "+
           SpaceManagerNextIdTableName+" FOR UPDATE ";
    
    private static final String increaseNextId = "UPDATE "+SpaceManagerNextIdTableName+
                        " SET NextToken=NextToken+1";
   
    public synchronized  long getNextToken() throws SQLException  {
        Connection _con = null;
        try {
            
            _con = connection_pool.getConnection();
            long nextLong;
            try {
                PreparedStatement s = _con.prepareStatement(selectNextIdForUpdate);
                say("dbInit trying "+selectNextIdForUpdate);
                ResultSet set = s.executeQuery();
                if(!set.next()) {
                    s.close();
                    throw new SQLException("table "+
                            SpaceManagerNextIdTableName+" is empty!!!");
                }
                nextLong = set.getLong(1);
                s.close();
                s = _con.prepareStatement(increaseNextId);
                int i = s.executeUpdate();
                s.close();
                _con.commit();
            } catch(SQLException e) {
                e.printStackTrace();
                _con.rollback();
                throw e;
            }
            connection_pool.returnConnection(_con);
            _con = null;
            return nextLong;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
            
        }
    }
    
    private static final String selectLinkGroupInfoForUpdate =
                    "SELECT * FROM "+ LinkGroupTableName +
                    " WHERE  name = ? FOR UPDATE";
    private static final String insertLinkGroupInfo = 
            "INSERT INTO "+LinkGroupTableName + 
            " (id, name, freeSpaceInBytes, lastUpdateTime, onlineAllowed," +
            " nearlineAllowed, replicaAllowed, outputAllowed, custodialAllowed)"+
            " VALUES ( ?,?,?,?,?,?,?,?,?)";
    private static final String selectLinkGroupVOs = 
            "SELECT VOGroup,VORole FROM "+LinkGroupVOsTableName+
                    " WHERE linkGroupId=?";
    private static final String updateLinkGroupInfo = "UPDATE "+LinkGroupTableName +
                    " SET "+
                    "freeSpaceInBytes = ? "+
                    ",lastUpdateTime= ? "+
                    ",onlineAllowed= ? "+
                    ",nearlineAllowed= ? "+
                    ",replicaAllowed= ? "+
                    ",outputAllowed= ? "+
                    ",custodialAllowed= ? "+
                    " WHERE  id = ? ";
           
    private long updateLinkGroupInfo(
            String linkGroup,
            long freeSpace,
            long updateTime,
            boolean onlineAllowed,
            boolean nearlineAllowed,
            boolean replicaAllowed,
            boolean outputAllowed,
            boolean custodialAllowed,
            VOInfo[] linkGroupVOs
            )
            throws SQLException{
        say("UpdateLinkGroupInfo( linkGroup="+linkGroup+
            ", onlineAllowed ="+ onlineAllowed+
            ", nearlineAllowed ="+ nearlineAllowed+
            ", replicaAllowed ="+ replicaAllowed+
            ", outputAllowed ="+ outputAllowed+
            ", custodialAllowed ="+ custodialAllowed+
            ", freeSpace="+freeSpace);
        if(linkGroupVOs != null) {
            for(int i=0; i<linkGroupVOs.length; ++i) {
                say("UpdateLinkGroupInfo( VO["+i+"]="+linkGroupVOs[i]);
            }
        }
        long id;
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
           // String selectLinkGroupInfoForUpdate =
           //         "SELECT * FROM "+ LinkGroupTableName +
           //         " WHERE  name = '"+linkGroup+"' FOR UPDATE";
            say("executing statement: "+selectLinkGroupInfoForUpdate+
              " ?="+linkGroup);
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectLinkGroupInfoForUpdate);
            sqlStatement.setString(1,linkGroup);
            ResultSet updateSet = sqlStatement.executeQuery();
            
            if (!updateSet.next()) {
                
                // we did not find anything, try to insert a new linkGroup record
                sqlStatement.close();
                try {
                    
                    id = getNextToken();
                    sqlStatement = _con.prepareStatement(insertLinkGroupInfo);
                    sqlStatement.setLong(1,id);
                    sqlStatement.setString(2,linkGroup);
                    sqlStatement.setLong(3,freeSpace);
                    sqlStatement.setLong(4,updateTime);
                    sqlStatement.setInt(5,onlineAllowed? 1 : 0);
                    sqlStatement.setInt(6,nearlineAllowed? 1 : 0);
                    sqlStatement.setInt(7,replicaAllowed? 1 : 0);
                    sqlStatement.setInt(8,outputAllowed? 1 : 0);
                    sqlStatement.setInt(9,custodialAllowed? 1 : 0);
                    say("executing statement: "+insertLinkGroupInfo+
                            " ?="+id+
                            " ?="+linkGroup+
                            " ?="+freeSpace+
                            " ?="+updateTime+
                            " ?="+onlineAllowed+
                            " ?="+nearlineAllowed+
                            " ?="+replicaAllowed+
                            " ?="+outputAllowed+
                            " ?="+custodialAllowed
                        );
                    sqlStatement.executeUpdate();
                    sqlStatement.close();
                    if(linkGroupVOs != null) {
                        for(int i = 0; i<linkGroupVOs.length; ++i) {
                            /*
                             *            "CREATE TABLE "+ LinkGroupVOsTableName+" ( "+
                            ", StorageGroup "+stringType+" NOT NULL PRIMARY KEY "+
                            ", linkGroupId "+longType+" "+

                             */
                            String insertLinkGroupStorageGroup = "INSERT INTO "+LinkGroupVOsTableName +
                                    " VALUES ( '"+linkGroupVOs[i].getVoGroup()+
                                    "','"+linkGroupVOs[i].getVoRole()+
                                    "',"+id+")";
                            Statement sqlStatement1 = _con.createStatement();
                            say("executing statement: "+insertLinkGroupStorageGroup);
                            sqlStatement1.executeUpdate(insertLinkGroupStorageGroup);
                            sqlStatement1.close();
                        }
                    }
                    say("COMMIT TRANSACTION");
                    _con.commit();
                    return id;
                } catch (SQLException e) {
                    
                    esay(e);
                    esay("ignoring, it might happen that someone else has created a record");
                    
                }
                say("executing statement: "+selectLinkGroupInfoForUpdate+
              " ?="+linkGroup);
                sqlStatement =
                        _con.prepareStatement(selectLinkGroupInfoForUpdate);
                sqlStatement.setString(1,linkGroup);
                updateSet = sqlStatement.executeQuery();
                if(!updateSet.next()) {
                    sqlStatement.close();
                    throw new SQLException(" can not insert or udate the linkGroup record for linkGroup:"+linkGroup);
                }
            }
            id = updateSet.getLong(1);
            sqlStatement.close();
            PreparedStatement sqlStatement2 =
                    _con.prepareStatement(selectLinkGroupVOs);
            sqlStatement2.setLong(1,id);
            say("executing statement: "+selectLinkGroupVOs+
                        " ?="+id);
            ResultSet VOsSet = sqlStatement2.executeQuery();
            Set<VOInfo> insertVOs = new HashSet<VOInfo>();
            if(linkGroupVOs != null) {
                insertVOs.addAll(java.util.Arrays.asList(linkGroupVOs));
            }
            Set<VOInfo> deleteVOs = new HashSet<VOInfo>();
            
            while(VOsSet.next()) {
                String nextVOGroup =    VOsSet.getString(1);
                String nextVORole =    VOsSet.getString(2);
                VOInfo nextVO = new VOInfo(nextVOGroup,nextVORole);
                if(insertVOs.contains(nextVO)){
                    insertVOs.remove(nextVO);
                } else {
                    deleteVOs.add(nextVO);
                }
            }
            sqlStatement2.close();
            for(Iterator<VOInfo> i = insertVOs.iterator(); i.hasNext();) {
                VOInfo nextVo=i.next();
                String insertLinkGroupVO = "INSERT INTO "+LinkGroupVOsTableName +
                        " VALUES ( '"+nextVo.getVoGroup()+
                        "','"+nextVo.getVoRole()+
                        "',"+id+")";
                Statement sqlStatement3 = _con.createStatement();
                say("executing statement: "+insertLinkGroupVO);
                sqlStatement3.executeUpdate(insertLinkGroupVO);
                sqlStatement3.close();
                
            }
            for(Iterator<VOInfo> i = deleteVOs.iterator(); i.hasNext();) {
                VOInfo nextVo=i.next();
                String insertLinkGroupVO = "DELETE FROM "+LinkGroupVOsTableName +
                        " WHERE VOGroup = '"+nextVo.getVoGroup()+
                        "' AND VORole ='"+nextVo.getVoRole()+
                        "' AND linkGroupId="+id;
                Statement sqlStatement4 = _con.createStatement();
                say("executing statement: "+insertLinkGroupVO);
                sqlStatement4.executeUpdate(insertLinkGroupVO);
                sqlStatement4.close();
            }
            
     /*
     *           "CREATE TABLE "+ LinkGroupTableName+" ( "+
            " id "+longType+" NOT NULL PRIMARY KEY "+
            ", name"+stringType+" " +
            ", freeSpaceInBytes "+longType+" "+
            ", lastUpdateTime "+longType +
            ", onlineAllowed"+booleanType+" "+
            ", nearlineAllowed"+booleanType+" "+
            ", replicaAllowed"+booleanType+" "+
            ", outputAllowed"+booleanType+" "+
            ", custodialAllowed"+booleanType+" "+
*/
           say("executing statement: "+updateLinkGroupInfo+
                " ?="+freeSpace+
                " ?="+updateTime+
                " ?="+onlineAllowed+
                " ?="+nearlineAllowed+
                " ?="+replicaAllowed+
                " ?="+outputAllowed+
                " ?="+custodialAllowed+
                " ?="+id
              );
            PreparedStatement sqlStatement5 =
                    _con.prepareStatement(updateLinkGroupInfo);
            sqlStatement5.setLong(1,freeSpace);
            sqlStatement5.setLong(2,updateTime);
            sqlStatement5.setInt(3,onlineAllowed? 1 : 0);
            sqlStatement5.setInt(4,nearlineAllowed? 1 : 0);
            sqlStatement5.setInt(5,replicaAllowed? 1 : 0);
            sqlStatement5.setInt(6,outputAllowed? 1 : 0);
            sqlStatement5.setInt(7,custodialAllowed? 1 : 0);
            sqlStatement5.setLong(8,id);
            sqlStatement5.executeUpdate();
            sqlStatement5.close();
            say("COMMIT TRANSACTION");
            _con.commit();
            return id;
            
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    private static final String onlineSelectionCondition =
        "onlineAllowed = 1 ";
    private static final String nearlineSelectionCondition =
        "nearlineAllowed = 1 ";
    private static final String replicaSelectionCondition =
        "replicaAllowed = 1 ";
    private static final String outputSelectionCondition =
        "outputAllowed = 1 ";
    private static final String custodialSelectionCondition =
        "custodialAllowed = 1 ";

    private  static final String selectLinkGroupInfoForUpdatePart1 =
        "\nSELECT id from (" +
        //FIRST SELECT THE LINKS THAT DO NOT HAVE ANY RESERVATIONS
        "\n     SELECT UNUSEDLINKS.id as id, " +
        "\n             UNUSEDLINKS.available as available " +
        "\n     FROM " +
        "\n         ("+
        "\n         SELECT " +LinkGroupTableName +".id as id, " +
        "\n                 "+LinkGroupTableName +".freeSpaceInBytes as available," +
        "\n                 "+LinkGroupTableName +".onlineAllowed as onlineAllowed," +
        "\n                 "+LinkGroupTableName +".nearlineAllowed as nearlineAllowed," +
        "\n                 "+LinkGroupTableName +".replicaAllowed as replicaAllowed," +
        "\n                 "+LinkGroupTableName +".outputAllowed as outputAllowed," +
        "\n                 "+LinkGroupTableName +".custodialAllowed as custodialAllowed," +
        "\n                 "+LinkGroupTableName +".lastUpdateTime as lastUpdateTime" +
        "\n         FROM " + LinkGroupTableName+" FULL OUTER JOIN "+SpaceTableName+
        "\n         ON "+LinkGroupTableName+".id ="+
                         SpaceTableName+".linkGroupId " +
        "\n         group by srmlinkgroup.id, " +
        "\n                  srmlinkgroup.freeSpaceInBytes," +
        "\n                  srmlinkgroup.onlineAllowed, " +
        "\n                  srmlinkgroup.nearlineAllowed, " +
        "\n                  srmlinkgroup.replicaAllowed, " +
        "\n                  srmlinkgroup.outputAllowed, " +
        "\n                  srmlinkgroup.custodialAllowed, " +
        "\n                  srmlinkgroup.lastUpdateTime"+
        "\n         ) AS UNUSEDLINKS, " +
        "\n         "+LinkGroupVOsTableName+
        "\n     WHERE UNUSEDLINKS.id ="+LinkGroupVOsTableName+".linkGroupId "+
        "\n         AND UNUSEDLINKS.";
    private static final String selectLinkGroupInfoForUpdatePart2 =
        "\n         AND UNUSEDLINKS.";
    
    private static final String selectLinkGroupInfoForUpdatePart3 =
        "\n         AND UNUSEDLINKS.lastUpdateTime >= ?"+ //?(1)
        "\n         AND ( "+LinkGroupVOsTableName+".VOGroup = ?"+ //?(2)
        "\n                OR "+LinkGroupVOsTableName+".VOGroup = '*' )"+
        "\n         AND ( "+LinkGroupVOsTableName+".VORole = ?"+ //?(3)
        "\n                OR "+LinkGroupVOsTableName+".VORole = '*' )"+
        "\n UNION "+
        // NOW SELECT THE LINKS THAT HAVE RESERVATIONS AND
        // STILL HAVE ENOUGH SPACE
        "\n     SELECT "+
        LinkGroupTableName +".id as id, " +
        LinkGroupTableName +".freeSpaceInBytes "+
        " - SUM("+SpaceTableName+".sizeInBytes)"+
        " as available " +
        "\n     FROM " + LinkGroupTableName+", "+SpaceTableName+","+LinkGroupVOsTableName+
        "\n     WHERE "+LinkGroupTableName+".id = "+
        SpaceTableName+".linkGroupId "+
        "\n         AND "+LinkGroupTableName+".id = "+
        LinkGroupVOsTableName+".linkGroupId "+
        "\n         AND "+LinkGroupTableName+".";
    
    private static final String selectLinkGroupInfoForUpdatePart4 =
        "\n         AND "+LinkGroupTableName+".";
    
    private static final String selectLinkGroupInfoForUpdatePart5 =
    
        "\n         AND "+LinkGroupTableName+".lastUpdateTime >= ?"+//?(4)
        "\n         AND ( "+LinkGroupVOsTableName+".VOGroup = ?"+//?(5)
        "\n                OR "+LinkGroupVOsTableName+".VOGroup = '*' )"+
        "\n         AND ( "+LinkGroupVOsTableName+".VORole = ?"+//?(6)
        "\n                OR "+LinkGroupVOsTableName+".VORole = '*' )"+
        "\n         GROUP BY "+LinkGroupTableName +".id, "+
        LinkGroupTableName +".freeSpaceInBytes "+
        " \n    ) AS FOO "+
        " \n WHERE available >= ?";
    
    private static final String selectOnlineReplicaLinkGroupForUpdate =
        selectLinkGroupInfoForUpdatePart1 +
        onlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart2+
        replicaSelectionCondition+
        selectLinkGroupInfoForUpdatePart3+
        onlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart4+
        replicaSelectionCondition+
        selectLinkGroupInfoForUpdatePart5;
    
    private static final String selectOnlineOutputLinkGroupForUpdate =
        selectLinkGroupInfoForUpdatePart1 +
        onlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart2+
        outputSelectionCondition+
        selectLinkGroupInfoForUpdatePart3+
        onlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart4+
        outputSelectionCondition+
        selectLinkGroupInfoForUpdatePart5;
        
    private static final String selectOnlineCustodialLinkGroupForUpdate =
        selectLinkGroupInfoForUpdatePart1 +
        onlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart2+
        custodialSelectionCondition+
        selectLinkGroupInfoForUpdatePart3+
        onlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart4+
        custodialSelectionCondition+
        selectLinkGroupInfoForUpdatePart5;

    
    private static final String selectNearlineReplicaLinkGroupForUpdate =
        selectLinkGroupInfoForUpdatePart1 +
        nearlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart2+
        replicaSelectionCondition+
        selectLinkGroupInfoForUpdatePart3+
        nearlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart4+
        replicaSelectionCondition+
        selectLinkGroupInfoForUpdatePart5;
    
    private static final String selectNearlineOutputLinkGroupForUpdate =
        selectLinkGroupInfoForUpdatePart1 +
        nearlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart2+
        outputSelectionCondition+
        selectLinkGroupInfoForUpdatePart3+
        nearlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart4+
        outputSelectionCondition+
        selectLinkGroupInfoForUpdatePart5;
        
    private static final String selectNearlineCustodialLinkGroupForUpdate =
        selectLinkGroupInfoForUpdatePart1 +
        nearlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart2+
        custodialSelectionCondition+
        selectLinkGroupInfoForUpdatePart3+
        nearlineSelectionCondition+
        selectLinkGroupInfoForUpdatePart4+
        custodialSelectionCondition+
        selectLinkGroupInfoForUpdatePart5;
    
    
    private Long[] findLinkGroupIds(
            long sizeInBytes,
            String voGroup,
            String voRole,
            AccessLatency al,
            RetentionPolicy rp
            ) throws SQLException {
        Connection _con = null;
        try {
            say("findLinkGroupIds(sizeInBytes="+sizeInBytes+
                    ", voGroup="+voGroup+" voRole="+voRole+
                    ", AccessLatency="+al+
                    ", RetentionPolicy="+rp+
                ")");
            _con = connection_pool.getConnection();
            // The following somewhat complecated sql query
            // (at least complicated up to me, Timur P.)
            // selects linkGroups that have co
            
            PreparedStatement sqlStatement;
            String select;
            if( al.equals(AccessLatency.ONLINE)) {
                if(rp.equals(RetentionPolicy.REPLICA)) {
                    select = selectOnlineReplicaLinkGroupForUpdate;
                } else 
                if ( rp.equals(RetentionPolicy.OUTPUT)) {
                    select = selectOnlineOutputLinkGroupForUpdate;
                } else {
                    select = selectOnlineCustodialLinkGroupForUpdate;
                }
                
            } else {
                if(rp.equals(RetentionPolicy.REPLICA)) {
                    select = selectNearlineReplicaLinkGroupForUpdate;
                } else 
                if ( rp.equals(RetentionPolicy.OUTPUT)) {
                    select = selectNearlineOutputLinkGroupForUpdate;
                } else {
                    select = selectNearlineCustodialLinkGroupForUpdate;
                }
                
            }
            sqlStatement = _con.prepareStatement(select);
            say("executing statement: "+select+
                "?="+latestLinkGroupUpdateTime+
                "?="+voGroup+
                "?="+voRole+
                "?="+latestLinkGroupUpdateTime+
                "?="+voGroup+
                "?="+voRole+
                "?="+sizeInBytes
                );
            sqlStatement.setLong(1,latestLinkGroupUpdateTime);
            sqlStatement.setString(2,voGroup);
            sqlStatement.setString(3,voRole);
            sqlStatement.setLong(4,latestLinkGroupUpdateTime);
            sqlStatement.setString(5,voGroup);
            sqlStatement.setString(6,voRole);
            sqlStatement.setLong(7,sizeInBytes);
                    _con.createStatement();
            ResultSet set = sqlStatement.executeQuery();
            say("execution complete");
            java.util.Set idset = new java.util.HashSet();
            while(set.next()) {
                Long id = new Long(set.getLong("id"));
                say(" linkGroupId found:"+id);
                idset.add(id);
            }
            sqlStatement.close();
            return (Long[])idset.toArray(new Long[0]);
        } catch(SQLException sqle) {
            esay("select failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        
    }
 
    
    public LinkGroup getLinkGroup(long id)  throws SQLException{
        say("getLinkGroup("+id+")");
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            return getLinkGroup(_con,id);
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public LinkGroup getLinkGroup(Connection _con, long id)  throws SQLException{
        String selectLinkGroup =
                "SELECT * FROM "+ LinkGroupTableName +
                " WHERE  id = "+id;
        say("executing statement: "+selectLinkGroup);
        Statement sqlStatement =
                _con.createStatement();
        ResultSet set = sqlStatement.executeQuery(
                selectLinkGroup);
        if(!set.next()) {
            throw new SQLException("linkGroup with id="+id+" not found");
        }
        LinkGroup linkGroup = new LinkGroup();
        linkGroup.setId(id);
        linkGroup.setName(set.getString("name"));
        linkGroup.setFreeSpace(set.getLong("freeSpaceInBytes"));
        linkGroup.setUpdateTime(set.getLong("lastUpdateTime"));
        linkGroup.setOnlineAllowed(set.getBoolean("onlineAllowed"));
        linkGroup.setNearlineAllowed(set.getBoolean("nearlineAllowed"));
        linkGroup.setReplicaAllowed(set.getBoolean("replicaAllowed"));
        linkGroup.setOutputAllowed(set.getBoolean("outputAllowed"));
        linkGroup.setCustodialAllowed(set.getBoolean("custodialAllowed"));

        sqlStatement.close();
        /*"CREATE TABLE "+ LinkGroupVOsTableName+" ( "+
        ", StorageGroup "+stringType+" NOT NULL PRIMARY KEY "+
        ", linkGroupId "+longType+" "+
         */
       /*"CREATE TABLE "+ LinkGroupVOsTableName+" ( "+
        ", StorageGroup "+stringType+" NOT NULL PRIMARY KEY "+
        ", linkGroupId "+longType+" "+*/
        String selectLinkGroupVOs =
                "SELECT voGroup,voRole FROM "+LinkGroupVOsTableName+
                " WHERE linkGroupId="+id;
        sqlStatement =
                _con.createStatement();
        set = sqlStatement.executeQuery(
                selectLinkGroupVOs);
        Set<VOInfo> vos = new HashSet<VOInfo>();
        while(set.next()) {
            String nextVOGroup =    set.getString(1);
            String nextVORole =    set.getString(2);
            VOInfo voinfo = new VOInfo(nextVOGroup,nextVORole);
            vos.add(voinfo);
        }
        linkGroup.setVOs(vos.toArray(new VOInfo[0]));
        sqlStatement.close();
        return linkGroup;
    }
    
    public LinkGroup getLinkGroupByName(String name)  throws SQLException{
        say("getLinkGroupByName("+name+")");
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            return getLinkGroupByName(_con,name);
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
        
    public LinkGroup getLinkGroupByName(Connection _con, String name)  throws SQLException{
        String selectLinkGroup =
                "SELECT * FROM "+ LinkGroupTableName +
                " WHERE  name = '"+name+"'";
        say("executing statement: "+selectLinkGroup);
        Statement sqlStatement =
                _con.createStatement();
        ResultSet set = sqlStatement.executeQuery(
                selectLinkGroup);
        if(!set.next()) {
            throw new SQLException("linkGroup with name='"+name+"' not found");
        }
        LinkGroup linkGroup = new LinkGroup();
        long id = set.getLong("id");
        linkGroup.setId(id);
        linkGroup.setName(set.getString("name"));
        linkGroup.setFreeSpace(set.getLong("freeSpaceInBytes"));
        linkGroup.setUpdateTime(set.getLong("lastUpdateTime"));
        linkGroup.setOnlineAllowed(set.getBoolean("onlineAllowed"));
        linkGroup.setNearlineAllowed(set.getBoolean("nearlineAllowed"));
        linkGroup.setReplicaAllowed(set.getBoolean("replicaAllowed"));
        linkGroup.setOutputAllowed(set.getBoolean("outputAllowed"));
        linkGroup.setCustodialAllowed(set.getBoolean("custodialAllowed"));

        sqlStatement.close();
        /*"CREATE TABLE "+ LinkGroupVOsTableName+" ( "+
        ", StorageGroup "+stringType+" NOT NULL PRIMARY KEY "+
        ", linkGroupId "+longType+" "+
         */
       /*"CREATE TABLE "+ LinkGroupVOsTableName+" ( "+
        ", StorageGroup "+stringType+" NOT NULL PRIMARY KEY "+
        ", linkGroupId "+longType+" "+*/
        String selectLinkGroupVOs =
                "SELECT voGroup,voRole FROM "+LinkGroupVOsTableName+
                " WHERE linkGroupId="+id;
        sqlStatement =
                _con.createStatement();
        set = sqlStatement.executeQuery(
                selectLinkGroupVOs);
        Set<VOInfo> vos = new HashSet<VOInfo>();
        while(set.next()) {
            String nextVOGroup =    set.getString(1);
            String nextVORole =    set.getString(2);
            VOInfo voinfo = new VOInfo(nextVOGroup,nextVORole);
            vos.add(voinfo);
        }
        linkGroup.setVOs(vos.toArray(new VOInfo[0]));
        sqlStatement.close();
        return linkGroup;
    }
    
   
    public void selectLinkGroupForUpdate(Connection _con,Long id,long sizeInBytes)  throws SQLException{
        String selectLinkGroupInfoForUpdate =
                "SELECT * FROM "+ LinkGroupTableName +
                " WHERE  id = "+id+" and freeSpaceInBytes>="+sizeInBytes+
                " FOR UPDATE";
        say("executing statement: "+selectLinkGroupInfoForUpdate);
        Statement sqlStatement =
                _con.createStatement();
        ResultSet set = sqlStatement.executeQuery(
                selectLinkGroupInfoForUpdate);
        if(!set.next()) {
            throw new SQLException("linkGroup with id="+id+" not found or does not have enough space");
        }
    }
    
    public void updateSpaceState(long id,SpaceState spaceState)throws SQLException{
        
        updateSpaceReservation(id,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new Integer(spaceState.getStateId()));
    }
    
    public void updateSpaceLifetime(Connection _con,long id, long newLifetime)
    throws SQLException {
        updateSpaceReservation(_con,
                id,
                null,
                null,
                null,
                null,
                null,
                null,
                new Long(newLifetime),
                null,
                null);
    }
    
    public void updateSpaceReservation(Connection _con,
            long id,
            String voGroup,
            String voRole,
            RetentionPolicy retentionPolicy,
            AccessLatency accessLatency,
            Long linkGroupId,
            Long sizeInBytes,
            Long lifetime,
            String description,
            Integer state) throws SQLException {
        
        String updateSpaceReservation = "UPDATE "+SpaceTableName +
                " SET ";
        boolean added = false;
        if(voGroup != null )  {
            updateSpaceReservation += " voGroup ='"+voRole+"' ";
            added = true;
        }
        if(voRole != null )  {
            
            if(added) {
                updateSpaceReservation += ",";
            }
            added = true;
            updateSpaceReservation += " voRole ='"+voRole+"'";
        }
        if(retentionPolicy != null )  {
            
            if(added) {
                updateSpaceReservation += ",";
            }
            added = true;
            updateSpaceReservation += " retentionPolicy ="+retentionPolicy.getId()+"";
        }
        if(accessLatency != null )  {
            
            if(added) {
                updateSpaceReservation += ",";
            }
            added = true;
            updateSpaceReservation += " accessLatency ="+accessLatency.getId()+"";
        }
        if(linkGroupId != null) {
            if(added) {
                updateSpaceReservation += ",";
            }
            added = true;
            updateSpaceReservation += " linkGroupId ="+linkGroupId;
        }
        if(sizeInBytes != null) {
            if(added) {
                updateSpaceReservation += ",";
            }
            added = true;
            updateSpaceReservation += " sizeInBytes = "+sizeInBytes;
        }
        if(lifetime != null) {
            if(added) {
                updateSpaceReservation += ",";
            }
            added = true;
            updateSpaceReservation += " lifetime = "+lifetime;
        }
        if(description != null)  {
            if(added) {
                updateSpaceReservation += ",";
            }
            added = true;
            updateSpaceReservation += " description = '"+description+"'" ;
        }
        if(state != null)  {
            if(added) {
                updateSpaceReservation += ",";
            }
            added = true;
            updateSpaceReservation += " state = "+state;
        }
        updateSpaceReservation += " WHERE  id = "+id;
        
        if(!added) {
            throw new SQLException("nothing to update");
        }
        
        say("executing statement: "+updateSpaceReservation);
        Statement sqlStatement =
                _con.createStatement();
        sqlStatement.executeUpdate(updateSpaceReservation);
    }
    
    public void updateSpaceReservation(long id,
            String voGroup,
            String voRole,
            RetentionPolicy retentionPolicy,
            AccessLatency accessLatency,
            Long linkGroupId,
            Long sizeInBytes,
            Long lifetime,
            String description,
            Integer state) throws SQLException {
        boolean found = false;
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectSpaceReservationUpdate =
                    "SELECT * FROM "+ SpaceTableName +
                    " WHERE  id = "+id+" FOR UPDATE ";
            say("executing statement: "+selectSpaceReservationUpdate);
            Statement sqlStatement =
                    _con.createStatement();
            ResultSet updateSet = sqlStatement.executeQuery(
                    selectSpaceReservationUpdate);
            
            long updateTime = System.currentTimeMillis();
            found = updateSet.next();
            sqlStatement.close();
            if (found) {
                
                String updateSpaceReservation = "UPDATE "+SpaceTableName +
                        " SET ";
                boolean added = false;
                if(voGroup != null )  {
                    updateSpaceReservation += " voGroup ='"+voRole+"' ";
                    added = true;
                }
                if(voRole != null )  {
                    
                    if(added) {
                        updateSpaceReservation += ",";
                    }
                    added = true;
                    updateSpaceReservation += " voRole ='"+voRole+"'";
                }
                if(retentionPolicy != null )  {
                    
                    if(added) {
                        updateSpaceReservation += ",";
                    }
                    added = true;
                    updateSpaceReservation += " retentionPolicy ="+retentionPolicy.getId()+"";
                }
                if(accessLatency != null )  {
                    
                    if(added) {
                        updateSpaceReservation += ",";
                    }
                    added = true;
                    updateSpaceReservation += " accessLatency ="+accessLatency.getId()+"";
                }
                if(linkGroupId != null) {
                    if(added) {
                        updateSpaceReservation += ",";
                    }
                    added = true;
                    updateSpaceReservation += " linkGroupId ="+linkGroupId;
                }
                if(sizeInBytes != null) {
                    if(added) {
                        updateSpaceReservation += ",";
                    }
                    added = true;
                    updateSpaceReservation += " sizeInBytes = "+sizeInBytes;
                }
                if(lifetime != null) {
                    if(added) {
                        updateSpaceReservation += ",";
                    }
                    added = true;
                    updateSpaceReservation += " lifetime = "+lifetime;
                }
                if(description != null)  {
                    if(added) {
                        updateSpaceReservation += ",";
                    }
                    added = true;
                    updateSpaceReservation += " description = '"+description+"'" ;
                }
                if(state != null)  {
                    if(added) {
                        updateSpaceReservation += ",";
                    }
                    added = true;
                    updateSpaceReservation += " state = "+state;
                }
                updateSpaceReservation += " WHERE  id = "+id;
                
                if(!added) {
                    throw new SQLException("nothing to update");
                }
                
                say("executing statement: "+updateSpaceReservation);
                sqlStatement =
                        _con.createStatement();
                sqlStatement.executeUpdate(updateSpaceReservation);
                sqlStatement.close();
                say("COMMIT TRANSACTION");
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
            } else {
                // we did not find anything, try to insert a new linkGroup record
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
                throw new SQLException("Space Reservation with id="+id+" is not found!");
            }
            
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            if(_con != null) {
                _con.rollback();
                connection_pool.returnFailedConnection(_con);
                _con = null;
            }
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        if(!found) {
            throw new SQLException(" no record of space reservation "+id+" found");
        }
    }
    
    public void expireSpaceReservations()  {
        say("expireSpaceReservations()...");
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String updateSpaceReservation = "UPDATE "+SpaceTableName +
                    " SET "+
                    " state = "+SpaceState.EXPIRED.getStateId();
            updateSpaceReservation += " WHERE state = "+SpaceState.RESERVED.getStateId()+
                    " AND lifetime != -1 AND creationTime + lifetime < "+
                    System.currentTimeMillis();
            
            
            say("executing statement: "+updateSpaceReservation);
            Statement sqlStatement =
                    _con.createStatement();
            int count = sqlStatement.executeUpdate(updateSpaceReservation);
            sqlStatement.close();
            say("COMMIT TRANSACTION");
            _con.commit();
            connection_pool.returnConnection(_con);
            _con = null;
            say(" expired "+count+" space reservations");
            
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try {
                _con.rollback();
            } catch(SQLException sqle1) {
                esay(sqle1);
            }
            connection_pool.returnFailedConnection(_con);
            _con = null;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public long insertSpaceReservation(
            String voGroup,
            String voRole,
            RetentionPolicy retentionPolicy,
            AccessLatency accessLatency,
            long linkGroupId,
            long sizeInBytes,
            long lifetime,
            String description,
            int state) throws SQLException {
        long id = getNextToken();
        boolean found = false;
        Connection _con = null;
        try {
            
            _con = connection_pool.getConnection();
            long creationTime=System.currentTimeMillis();
            String inserSpaceReservation =
                    "INSERT INTO "+ SpaceTableName +
                    " VALUES  ("+
                    id+" ,'"+
                    voGroup+"','"+
                    voRole+"',"+
                    Integer.toString(retentionPolicy==null? 0 : retentionPolicy.getId())+","+
                    Integer.toString(accessLatency==null? 0 : accessLatency.getId())+","+
                    linkGroupId+","+
                    sizeInBytes+", "+
                    creationTime+", "+
                    lifetime+",'"+
                    description+"',"+
                    state+")";
            say("executing statement: "+inserSpaceReservation);
            Statement sqlStatement =
                    _con.createStatement();
            int inserRowCount = sqlStatement.executeUpdate(inserSpaceReservation);
            if(inserRowCount !=1 ){
                throw new SQLException("insert returned row count ="+inserRowCount);
            }
            sqlStatement.close();
            say("COMMIT TRANSACTION");
            _con.commit();
            connection_pool.returnConnection(_con);
            _con = null;
        } catch(SQLException sqle) {
            esay("insert failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        return id;
    }
    
    public void insertSpaceReservation(Connection _con,
            long id,
            String voGroup,
            String voRole,
            RetentionPolicy retentionPolicy,
            AccessLatency accessLatency,
            long linkGroupId,
            long sizeInBytes,
            long lifetime,
            String description,
            int state) throws SQLException {
        boolean found = false;
        
        long creationTime=System.currentTimeMillis();
        String inserSpaceReservation =
                "INSERT INTO "+ SpaceTableName +
                " VALUES  ("+
                id+" ,'"+
                voGroup+"','"+
                voRole+"',"+
                Integer.toString(retentionPolicy==null? 0 : retentionPolicy.getId())+","+
                Integer.toString(accessLatency==null? 0 : accessLatency.getId())+","+
                linkGroupId+","+
                sizeInBytes+", "+
                creationTime+", "+
                lifetime+",'"+
                description+"',"+
                state+")";
        say("executing statement: "+inserSpaceReservation);
        Statement sqlStatement =
                _con.createStatement();
        int inserRowCount = sqlStatement.executeUpdate(inserSpaceReservation);
        if(inserRowCount !=1 ){
            throw new SQLException("insert returned row count ="+inserRowCount);
        }
    }
    
    private static final String selectCurrentSpaceIds =
                    "SELECT id FROM "+ SpaceTableName +
                " where state != "+SpaceState.EXPIRED.getStateId() +
                " AND state != "+SpaceState.RELEASED.getStateId();
    
    private void listSpaceReservations(boolean isLongFormat, String id, StringBuffer sb) throws Exception{
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            
            if(id != null) {
                long longid = Long.parseLong(id);
                Space space;
                try {
                    space = getSpaceWithUsedSize(longid);
                } catch(Exception e) {
                    sb.append("Space with id=").append(id).append(" not found ");
                    return;
                }
                space.toStringBuffer(sb);
                return;
            }
            
            say("executing statement: "+selectCurrentSpaceIds);
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectCurrentSpaceIds);
            ResultSet set = sqlStatement.executeQuery();
            int count = 0;
            long totalReserved = 0;
            while(set.next()) {
                count++;
                long longid = set.getLong("id");
                Space space = getSpaceWithUsedSize(_con,longid);
                totalReserved += space.getSizeInBytes();
                space.toStringBuffer(sb);
                sb.append('\n');
            }
            
            sb.append("total number of reservations: ").append(count).append('\n');
            sb.append("total number of bytes reserved: ").append(totalReserved);
            sqlStatement.close();
            return;
        } catch(SQLException sqle) {
            esay("select failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            return;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        
    }
    
    private static final String selectLinkGroups =
                    "SELECT id FROM "+ LinkGroupTableName+
        " WHERE "+LinkGroupTableName+".lastUpdateTime >= ?";
    private static final String selectAllLinkGroups =
                    "SELECT id FROM "+ LinkGroupTableName;
    private void listLinkGroups(boolean isLongFormat, boolean all, String id, StringBuffer sb) throws Exception{
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            
            if(id != null) {
                long longid =  Long.parseLong(id);
                LinkGroup linkGroup = getLinkGroup(longid);
                try {
                    linkGroup = getLinkGroup(longid);
                } catch(Exception e) {
                    sb.append("LinkGroup with id=").append(id).append(" not found ");
                    return;
                }
                linkGroup.toStringBuffer(sb);
                return;
            }
            PreparedStatement sqlStatement;
            if(all) {
                
                say("executing statement: "+selectAllLinkGroups);
                sqlStatement =
                        _con.prepareStatement(selectAllLinkGroups);
            } else {
                say("executing statement: "+selectLinkGroups);
                sqlStatement =
                        _con.prepareStatement(selectLinkGroups);
                sqlStatement.setLong(1,latestLinkGroupUpdateTime);
                
            }
            ResultSet set = sqlStatement.executeQuery();
            int count = 0;
            long totalReservable = 0;
            while(set.next()) {
                count++;
                long longid = set.getLong("id");
                LinkGroup linkGroup = getLinkGroup(_con,longid);
                totalReservable += linkGroup.getFreeSpace();
                linkGroup.toStringBuffer(sb);
                sb.append('\n');
            }
            
            sb.append("total number of linkGroups: ").append(count).append('\n');
            sb.append("total number of bytes reservable: ").append(totalReservable).append('\n');
            sb.append("last time all link groups were updated: ").append(latestLinkGroupUpdateTime);
            sqlStatement.close();
            return;
        } catch(SQLException sqle) {
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            return;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        
    }
    public long[] getSpaceTokens(Connection _con,
            String voGroup,
            String voRole,
            String description)  throws SQLException{
        String selectSpace =
                "SELECT id FROM "+ SpaceTableName +
                " WHERE  state = "+SpaceState.RESERVED.getStateId();
        if(description == null) {
            selectSpace +=" AND voGroup = '"+voGroup+'\'';            
            if(voRole != null) {
                selectSpace += " AND voRole = '"+voRole+'\'';
            }
        } else {
            selectSpace += " AND description = '"+description+'\'';
        }
        say("executing statement: "+selectSpace);
        Statement sqlStatement =
                _con.createStatement();
        ResultSet set = sqlStatement.executeQuery(
                selectSpace);
        Set<Long> tokenSet = new HashSet<Long>();
        while(set.next()) {
            tokenSet.add(set.getLong(1));
        }
        Long[] tokensLong =  tokenSet.toArray(new Long[0]);
        long[] tokens = new long[tokensLong.length];
        for(int i = 0; i< tokens.length;++i) {
            tokens[i] = tokensLong[i].longValue();
        }
        return tokens;
    }
    /*
     *            "CREATE TABLE "+ SpaceFileTableName+" ( "+
            " id "+longType+" NOT NULL PRIMARY KEY "+
            ", voGroup "+stringType+" "+
            ", voRole "+stringType+" "+
            ", spaceReservationId "+longType+" "+
            ", sizeInBytes "+longType+" "+
            ", creationTime "+longType+" "+
            ", lifetime "+longType+" "+
            ", pnfsPath "+stringType+" "+
            ", pnfsId "+stringType+" "+
            ", state "+intType+" "+
            ", CONSTRAINT fk_"+SpaceFileTableName+
            "_L FOREIGN KEY (spaceReservationId) REFERENCES "+
            SpaceTableName +" (id) "+
            " ON DELETE RESTRICT"+
            ")";
*/
    
    public long[] getFileSpaceTokens(Connection _con,
            PnfsId pnfsId,
            String pnfsPath)  throws SQLException{
        String selectSpaceIds =
                "SELECT spaceReservationId FROM "+ SpaceFileTableName +
                " WHERE  state = "+FileState.STORED.getStateId();
        if(pnfsId != null) {
            selectSpaceIds +=" AND pnfsId = '"+pnfsId+'\''; 
        }
        
         if(pnfsPath != null) {
            selectSpaceIds +=" AND pnfsPath = '"+pnfsPath+'\''; 
        }
        say("executing statement: "+selectSpaceIds);
        Statement sqlStatement =
                _con.createStatement();
        ResultSet set = sqlStatement.executeQuery(
                selectSpaceIds);
        Set<Long> tokenSet = new HashSet<Long>();
        while(set.next()) {
            tokenSet.add(set.getLong(1));
        }
        Long[] tokensLong =  tokenSet.toArray(new Long[0]);
        long[] tokens = new long[tokensLong.length];
        for(int i = 0; i< tokens.length;++i) {
            tokens[i] = tokensLong[i].longValue();
        }
        return tokens;
    }

    public long[] getSpaceTokens(String voGroup,
            String voRole,
            String description)  throws SQLException{
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            return getSpaceTokens(_con,voGroup,voRole,description);
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public long[] getFileSpaceTokens(PnfsId pnfsId,
            String pnfsPath)  throws SQLException{
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            return getFileSpaceTokens(_con,pnfsId,pnfsPath);
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public Space getSpace(Connection _con,long id)  throws SQLException{
        String selectSpace =
                "SELECT * FROM "+ SpaceTableName +
                " WHERE  id = "+id;
        say("executing statement: "+selectSpace);
        Statement sqlStatement =
                _con.createStatement();
        ResultSet set = sqlStatement.executeQuery(
                selectSpace);
        if(!set.next()) {
            throw new SQLException("space with id="+id+" is not found");
        }
        Space space = new Space();
        space.setId(id);
        space.setVoGroup(set.getString("voGroup"));
        space.setVoRole(set.getString("voRole"));
        space.setRetentionPolicy(RetentionPolicy.getRetentionPolicy(set.getInt("retentionPolicy")));
        space.setAccessLatency(AccessLatency.getAccessLatency(set.getInt("accessLatency")));
        long linkGroupId = set.getLong("linkGroupId");
        //say("getSpace(), linkGroupId = "+linkGroupId);
        space.setLinkGroupId(linkGroupId);
        space.setSizeInBytes(set.getLong("sizeInBytes"));
        space.setCreationTime(set.getLong("creationTime"));
        space.setLifetime(set.getLong("lifetime"));
        space.setDescription(set.getString("description"));
        space.setState(SpaceState.getState(set.getInt("state")));
        say("getSpace("+id+") returns "+space);
        return space;
    }
    
    public Space getSpace(long id)  throws SQLException{
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            return getSpace(_con,id);
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public Space getSpaceWithUsedSize(Connection _con, long id)  throws SQLException{
        say("getSpaceWithAvailableSize");
        Space space =  getSpaceForUpdate(_con,id);
        long usedSizeInBytes = 0;
        String selectTotalFilesInSpaceSize =
                "SELECT sum(sizeInBytes) FROM "+ SpaceFileTableName +
                " WHERE  spaceReservationId = "+id +" AND state != "+FileState.FLUSHED.getStateId();
        say("executing statement: "+selectTotalFilesInSpaceSize);
        Statement sqlStatement =
                _con.createStatement();
        ResultSet set = sqlStatement.executeQuery(
                selectTotalFilesInSpaceSize);
        if(set.next()) {
            usedSizeInBytes = set.getLong(1);
        }
        space.setUsedSizeInBytes(usedSizeInBytes);
        if (space.getState() == SpaceState.RELEASED) {
            space.setSizeInBytes(usedSizeInBytes);
        }
        return space;
    }
    
    public Space getSpaceWithUsedSize(long id)  throws SQLException{
        say("getSpaceWithAvailableSize");
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            Space space =  getSpaceWithUsedSize(_con,id);
            say("COMMIT TRANSACTION");
            _con.commit();
            connection_pool.returnConnection(_con);
            _con = null;
            return space;
            
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            if(_con != null){
                say("ROLLBACK TRANSACTION");
                _con.rollback();
                connection_pool.returnFailedConnection(_con);
                _con = null;
            }
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public Space getSpaceForUpdate(Connection _con,long id)  throws SQLException{
        String selectSpace =
                "SELECT * FROM "+ SpaceTableName +
                " WHERE  id = "+id+
                " FOR UPDATE ";
        say("executing statement: "+selectSpace);
        Statement sqlStatement =
                _con.createStatement();
        ResultSet set = sqlStatement.executeQuery(
                selectSpace);
        if(!set.next()) {
            throw new SQLException("space with id="+id+" is not found");
        }
        Space space = new Space();
        space.setId(id);
        space.setVoGroup(set.getString("voGroup"));
        space.setVoRole(set.getString("voRole"));
        space.setRetentionPolicy(RetentionPolicy.getRetentionPolicy(set.getInt("retentionPolicy")));
        say("getSpaceForUpdate sets retentionPolicy="+space.getRetentionPolicy());
        space.setAccessLatency(AccessLatency.getAccessLatency(set.getInt("accessLatency")));
        say("getSpaceForUpdate sets accessLatency="+space.getAccessLatency());
        space.setLinkGroupId(set.getLong("linkGroupId"));
        space.setSizeInBytes(set.getLong("sizeInBytes"));
        space.setCreationTime(set.getLong("creationTime"));
        space.setLifetime(set.getLong("lifetime"));
        space.setDescription(set.getString("description"));
        space.setState(SpaceState.getState(set.getInt("state")));
        return space;
    }
    
    public void deleteSpaceReservation(
            Connection _con,
            long id) throws SQLException {
        boolean found = false;
        String deleteSpaceReservation =
                "DELETE FROM "+ SpaceTableName +
                " WHERE  id ="+  id;
        say("executing statement: "+deleteSpaceReservation);
        Statement sqlStatement =
                _con.createStatement();
        int deleteRowCount = sqlStatement.executeUpdate(deleteSpaceReservation);
        if(deleteRowCount !=1 ){
            throw new SQLException("delete returned row count ="+deleteRowCount);
        }
    }
    
    public void deleteSpaceReservation(
            long id) throws SQLException {
        boolean found = false;
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            deleteSpaceReservation(_con,id);
            say("COMMIT TRANSACTION");
            _con.commit();
            connection_pool.returnConnection(_con);
            _con = null;
        } catch(SQLException sqle) {
            esay("delete failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public void lockSpaceIfSpaceIsAvailable(Connection _con,long id,long sizeInBytes )  throws SQLException,SpaceException{
        String selectSpaceForUpdate =
                "SELECT * FROM "+ SpaceTableName +
                " WHERE  id = "+id+
                " FOR UPDATE";
        say("executing statement: "+selectSpaceForUpdate);
        Statement sqlStatement =
                _con.createStatement();
        ResultSet set = sqlStatement.executeQuery(
                selectSpaceForUpdate);
        
        if(!set.next()) {
            throw new SQLException("space with id="+id+" not found");
        }
        long creationTime = set.getLong("creationTime");
        long lifetime  =set.getLong("lifetime");
        long currentTime = System.currentTimeMillis();
        if(lifetime != -1 && creationTime +lifetime < currentTime) {
             throw new SpaceExpiredException("space with id="+id+" has expired");           
        }
        
        SpaceState state = SpaceState.getState(set.getInt("state"));
        if(state == SpaceState.EXPIRED) {
            throw new SpaceExpiredException("space with id="+id+" has expired");
        }
        
        if(state == SpaceState.RELEASED) {
            throw new SpaceReleasedException("space with id="+id+" was released");
        }
        long availableSpaceSize = set.getLong("sizeInBytes");
say( "available space size = " + availableSpaceSize );
        String  selectSumOfFileSizes= "SELECT sum(sizeInBytes)  FROM "+SpaceFileTableName+
                " WHERE spaceReservationId ="+id+" AND state != "+FileState.FLUSHED.getStateId();
        say("executing statement: "+selectSumOfFileSizes);
        sqlStatement =
                _con.createStatement();
        ResultSet set1 = sqlStatement.executeQuery(
                selectSumOfFileSizes);
        if(set1.next()) {
            availableSpaceSize -= set1.getLong(1);
        }
say( "available space size = " + availableSpaceSize );
say( "size in bytes = " + sizeInBytes);
        if(availableSpaceSize <sizeInBytes){
            throw new NoFreeSpaceException("space with id="+id+" does not have enough space");
        }
        
    }
    
    
    public void updateFileInSpaceInfo(
            long id,
            String voGroup,
            String voRole,
            PnfsId pnfsId,
            Long sizeInBytes,
            Long lifetime,
            Integer state) throws SQLException {
        boolean found = false;
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectFileForUpdate =
                    "SELECT * FROM "+ SpaceFileTableName +
                    " WHERE  id = "+id+" FOR UPDATE ";
            say("executing statement: "+selectFileForUpdate);
            Statement sqlStatement =
                    _con.createStatement();
            ResultSet updateSet = sqlStatement.executeQuery(
                    selectFileForUpdate);
            
            long updateTime = System.currentTimeMillis();
            found = updateSet.next();
            if (found) {
                updateFileInSpaceInfo(
                        _con,
                        id,
                        voGroup,
                        voRole,
                        pnfsId,
                        sizeInBytes,
                        lifetime,
                        state);
                
                sqlStatement.close();
                say("COMMIT TRANSACTION");
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
            } else {
                // we did not find anything, try to insert a new linkGroup record
                sqlStatement.close();
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
            }
            
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        if(!found) {
            throw new SQLException(" no record of space reservation "+id+" found");
        }
    }
    
    public void updateFileInSpaceInfo(
            Connection _con,
            long id,
            String voGroup,
            String voRole,
            PnfsId pnfsId,
            Long sizeInBytes,
            Long lifetime,
            Integer state) throws SQLException {
        
        long updateTime = System.currentTimeMillis();
        
        boolean setField = false;
        String updateFile = "UPDATE "+SpaceFileTableName +
                " SET ";
        if(voGroup != null )  {
            updateFile += "voGroup ='"+voGroup+"'";
            setField =true;
        }
        if(voRole != null) {
            if(setField) {
                updateFile += ",";
            }
            setField =true;
            updateFile += " voRole ='"+voRole+"' ";
        }
        if(pnfsId != null) {
            if(setField) {
                updateFile += ",";
            }
            setField =true;
            updateFile += " pnfsId ='"+pnfsId+"' ";
        }
        if(sizeInBytes != null) {
            if(setField) {
                updateFile += ",";
            }
            setField =true;
            updateFile += " sizeInBytes = "+sizeInBytes;
        }
        if(lifetime != null) {
            if(setField) {
                updateFile += ",";
            }
            setField =true;
            updateFile += " lifetime = "+lifetime;
        }
        if(state != null) {
            if(setField) {
                updateFile += ",";
            }
            setField =true;
            updateFile += " state = "+state;
        }
        updateFile += " WHERE id = "+id;
        say("executing statement: "+updateFile);
        Statement sqlStatement =
                _con.createStatement();
        sqlStatement.executeUpdate(updateFile);
    }
    
    public void removePnfsIdOfFileInSpace(
            Connection _con,
            long id,
            Integer state) throws SQLException {
        
        long updateTime = System.currentTimeMillis();
        
        String updateFile = "UPDATE "+SpaceFileTableName +
                " SET  pnfsId = NULL ";
        
        if(state != null) {
                updateFile += ", state = "+state;
        }
        updateFile += " WHERE id = "+id;
        say("executing statement: "+updateFile);
        Statement sqlStatement =
                _con.createStatement();
        sqlStatement.executeUpdate(updateFile);
    }
    
    public long insertFileInSpace(
            String voGroup,
            String voRole,
            long spaceReservationId,
            long sizeInBytes,
            long lifetime,
            String pnfsPath,
            PnfsId pnfsId,
            int state) throws SQLException {
        pnfsPath =new FsPath(pnfsPath).toString();
        long id = getNextToken();
        Connection _con = null;
        try {
            
            _con = connection_pool.getConnection();
            long creationTime=System.currentTimeMillis();
            String inserFileInSpace =
                    "INSERT INTO "+ SpaceFileTableName +
                    " VALUES  ("+
                    id+" ,'"+
                    voGroup+"','"+
                    voRole+"',"+
                    spaceReservationId+","+
                    sizeInBytes+", "+
                    creationTime+", "+
                    lifetime+",'"+
                    pnfsPath+"',"+
                    (pnfsId==null?"NULL":"'"+pnfsId+"'")+","+
                    state+")";
            say("executing statement: "+inserFileInSpace);
            Statement sqlStatement =
                    _con.createStatement();
            int inserRowCount = sqlStatement.executeUpdate(inserFileInSpace);
            if(inserRowCount !=1 ){
                throw new SQLException("insert returned row count ="+inserRowCount);
            }
            sqlStatement.close();
            say("COMMIT TRANSACTION");
            _con.commit();
            connection_pool.returnConnection(_con);
            _con = null;
        } catch(SQLException sqle) {
            esay("insert failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        return id;
    }
    
    public void insertFileInSpace( Connection _con,
            long id,
            String voGroup,
            String voRole,
            long spaceReservationId,
            long sizeInBytes,
            long lifetime,
            String pnfsPath,
            PnfsId pnfsId,
            int state) throws SQLException {
        
        pnfsPath =new FsPath(pnfsPath).toString();
        long creationTime=System.currentTimeMillis();
        String inserFileInSpace =
                "INSERT INTO "+ SpaceFileTableName +
                " VALUES  ("+
                id+" ,'"+
                voGroup+"','"+
                voRole+"',"+
                spaceReservationId+","+
                sizeInBytes+", "+
                creationTime+", "+
                lifetime+",'"+
                pnfsPath+"',"+
                (pnfsId==null?"NULL":"'"+pnfsId+"'")+","+
                state+")";
        say("executing statement: "+inserFileInSpace);
        Statement sqlStatement =
                _con.createStatement();
        int inserRowCount = sqlStatement.executeUpdate(inserFileInSpace);
        if(inserRowCount !=1 ){
            throw new SQLException("insert returned row count ="+inserRowCount);
        }
    }
    
    public File getFile(long id)  throws SQLException{
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            return getFile(_con, id );
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    private File extractFileFromResultSet( ResultSet set )
        throws java.sql.SQLException
    {
        String pnfsIdString = set.getString( "pnfsId" );
        PnfsId pnfsId = null;
        if ( pnfsIdString != null ) {
            pnfsId = new PnfsId( pnfsIdString );
        }
        return new File(
                        set.getLong( "id" ),
                        set.getString( "voGroup" ),
                        set.getString( "voRole" ),
                        set.getLong( "spaceReservationId" ),
                        set.getLong( "sizeInBytes" ),
                        set.getLong( "creationTime" ),
                        set.getLong( "lifetime" ),
                        set.getString( "pnfsPath" ),
                        pnfsId,
                        FileState.getState( set.getInt( "state" ) )
                       );
    }
    
   public File getFile(Connection _con, long id)  throws SQLException{
            String selectFile =
                    "SELECT * FROM "+ SpaceFileTableName +
                    " WHERE  id = "+id;
            say("executing statement: "+selectFile);
            Statement sqlStatement =
                    _con.createStatement();
            ResultSet set = sqlStatement.executeQuery(
                    selectFile);
            if(!set.next()) {
                throw new SQLException("file with id="+id+" is not found");
            }
            File file = extractFileFromResultSet( set );
            sqlStatement.close();
            return file;
    }
    
    public File getFile(PnfsId pnfsId)  throws SQLException{
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectFile =
                    "SELECT * FROM "+ SpaceFileTableName +
                    " WHERE  pnfsId = '"+pnfsId+"'";
            say("executing statement: "+selectFile);
            Statement sqlStatement =
                    _con.createStatement();
            ResultSet set = sqlStatement.executeQuery(
                    selectFile);
            if(!set.next()) {
                throw new SQLException("file with pnfsId="+pnfsId+" is not found");
            }
            File file = extractFileFromResultSet( set );
            sqlStatement.close();
            return file;
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public File getFile(String pnfsPath)  throws SQLException{
        pnfsPath =new FsPath(pnfsPath).toString();
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectFile =
                    "SELECT * FROM "+ SpaceFileTableName +
                    " WHERE  pnfsPath = '"+pnfsPath+"'";
            say("executing statement: "+selectFile);
            Statement sqlStatement =
                    _con.createStatement();
            ResultSet set = sqlStatement.executeQuery(
                    selectFile);
            if(!set.next()) {
                throw new SQLException("file with pnfsPath="+pnfsPath+" is not found");
            }
            File file = extractFileFromResultSet( set );
            sqlStatement.close();
            return file;
        } catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }

    private static final String selectFileForUpdateByPath =
        "SELECT * FROM "+ SpaceFileTableName +
            " WHERE  pnfsPath = ?"+
            " FOR UPDATE ";

    public File getFileForUpdate(Connection _con,String pnfsPath)  throws SQLException{
        pnfsPath =new FsPath(pnfsPath).toString();
        say("executing statement: "+selectFileForUpdateByPath+" ?="+pnfsPath);
        PreparedStatement sqlStatement =
                _con.prepareStatement(selectFileForUpdateByPath);
        sqlStatement.setString(1,pnfsPath);
        ResultSet set = sqlStatement.executeQuery();
        if(!set.next()) {
            say("getFileForUpdate: file with pnfsPath="+pnfsPath+" is not found, returning null");
            return null;
        }
        File file = extractFileFromResultSet( set );
        sqlStatement.close();
        return file;
    }
    
    private static final String selectFileForUpdateByPnfsId =
        "SELECT * FROM "+ SpaceFileTableName +
            " WHERE  pnfsId = ?"+
            " FOR UPDATE ";
    
    public File getFileForUpdate(Connection _con,PnfsId pnfsId)  throws SQLException{
        say("executing statement: "+selectFileForUpdateByPnfsId+" ?="+pnfsId);
        PreparedStatement sqlStatement =
                _con.prepareStatement(selectFileForUpdateByPnfsId);
        sqlStatement.setString(1,pnfsId.toString());
        ResultSet set = sqlStatement.executeQuery();
        if(!set.next()) {
            say("getFileForUpdate: file with pnfsId="+pnfsId+" is not found, returning null");
            return null;
        }
        File file = extractFileFromResultSet( set );
        sqlStatement.close();
        return file;
    }
    
    public void deleteFileInSpaceSpace(
            long id) throws SQLException {
        boolean found = false;
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            deleteFileInSpaceSpace(_con,id);
            say("COMMIT TRANSACTION");
            _con.commit();
            connection_pool.returnConnection(_con);
            _con = null;
        } catch(SQLException sqle) {
            esay("delete failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    public void deleteFileInSpaceSpace(Connection _con,
            long id) throws SQLException {
        String deletFileInSpace =
                "DELETE FROM "+ SpaceFileTableName +
                " WHERE  id ="+  id;
        say("executing statement: "+deletFileInSpace);
        Statement sqlStatement =
                _con.createStatement();
        int deleteRowCount = sqlStatement.executeUpdate(deletFileInSpace);
        if(deleteRowCount !=1 ){
            throw new SQLException("delete returned row count ="+deleteRowCount);
        }
    }
    
    public static final void main(String[] args) throws Throwable {
        Cell system = new SystemCell( "firstDomain" ) ;
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Manager m = new Manager("m",
                " -jdbcUrl=jdbc:derby:testdb;create=true "+
                
                "-jdbcDriver=org.apache.derby.jdbc.EmbeddedDriver "+
                "-dbUser=user -dbPass=pass");
        long currentTime = System.currentTimeMillis();
        long linkGroupId = 
                m.updateLinkGroupInfo("linkGroup1",56,currentTime,true,true,true,true,true,
                new VOInfo[] {new VOInfo("cms","cmsprod")});
        linkGroupId = m.updateLinkGroupInfo("linkGroup1", 
            156,currentTime,true,true,true,true,true,new VOInfo[] {new VOInfo("cms","cmsprod")});
        LinkGroup linkGroup = m.getLinkGroup(linkGroupId);
        m.say("LinkGroup = "+linkGroup);
        long spaceId = m.insertSpaceReservation("/cms/uscms","owner1",RetentionPolicy.CUSTODIAL,AccessLatency.NEARLINE,linkGroupId,10,3600,"something",0);
        Reserve reserve = new Reserve("/cms/uscms","user",10,
                
                RetentionPolicy.CUSTODIAL,
                AccessLatency.NEARLINE,
                10000,
                "aaa");
        m.reserveSpace(reserve);
        m.updateSpaceReservation(spaceId,
                "/cms/uscms",
                "owner1",
                null,
                null,
                new Long(linkGroupId),
                new Long(10),
                new Long(3600),
                "something",
                new Integer(0));
        Space space = m.getSpace(spaceId);
        m.say("Space = "+space);
        long fileId = m.insertFileInSpace("/cms/uscms","owner",spaceId,5,1234,"/home/f1",null,0);
        m.updateFileInSpaceInfo(fileId,"/cms/uscms","owner1",null,new Long(5),new Long(1234),new Integer(0));
        File file = m.getFile(fileId);
        m.say("File = "+file);
        m.deleteFileInSpaceSpace(fileId);
        m.deleteSpaceReservation(spaceId);
    }
    
    public void messageArrived( final CellMessage cellMessage ) {
         diskCacheV111.util.ThreadManager.execute(new Runnable() {
            public void run() {
                processMessage(cellMessage);
            }
        });    
    }
    
    private void processMessage( CellMessage cellMessage ) {
        Object object = cellMessage.getMessageObject();
        say("Message  arrived: "+object +" from "+cellMessage.getSourcePath());
        if (! (object instanceof Message) ){
            esay("Unexpected message class "+object.getClass());
            return;
        }
        Message spaceMessage = (Message)object ;
        boolean replyRequired = spaceMessage.getReplyRequired() ;
        try {
            if(spaceMessage instanceof Reserve) {
                Reserve reserve =
                        (Reserve) spaceMessage;
                reserveSpace(reserve);
            } else if(spaceMessage instanceof Release) {
                Release release =
                        (Release) spaceMessage;
                releaseSpace(release);
            } else if(spaceMessage instanceof Use){
                Use use = (Use) spaceMessage;
                useSpace(use);
            } else if(spaceMessage instanceof CancelUse){
                CancelUse cancelUse = (CancelUse) spaceMessage;
                cancelUseSpace(cancelUse);
            } else if(spaceMessage instanceof PoolMgrSelectPoolMsg) {
                selectPool(cellMessage,false);
                //replyRequired should be set only after successful 
                // invocation of selectPool
                // if select pool throws exception, we need to return a message
                replyRequired = false;
            } else if(spaceMessage instanceof GetSpaceMetaData){
                GetSpaceMetaData getSpaceMetaData = (GetSpaceMetaData) spaceMessage;
                getSpaceMetaData(getSpaceMetaData);
            } else if(spaceMessage instanceof GetSpaceTokens){
                GetSpaceTokens getSpaceTokens = (GetSpaceTokens) spaceMessage;
                getSpaceTokens(getSpaceTokens);
            } else if(spaceMessage instanceof ExtendLifetime){
                ExtendLifetime extendLifetime = (ExtendLifetime) spaceMessage;
                extendLifetime(extendLifetime);
            }else if(spaceMessage instanceof PoolFileFlushedMessage) {
                PoolFileFlushedMessage fileFlushed = (PoolFileFlushedMessage) spaceMessage;
                fileFlushed(fileFlushed);
            }else if(spaceMessage instanceof PoolRemoveFilesMessage) {
                PoolRemoveFilesMessage fileRemoved = (PoolRemoveFilesMessage) spaceMessage;
                fileRemoved(fileRemoved);
            }else if(spaceMessage instanceof GetFileSpaceTokensMessage) {
                GetFileSpaceTokensMessage getFileTokens = (GetFileSpaceTokensMessage) spaceMessage;
                getFileSpaceTokens(getFileTokens);
                
            } else {
                esay("unknown Space Manager message type :"+spaceMessage.getClass().getName()+" value: "+spaceMessage);
                super.messageArrived(cellMessage);
                return;
            }
        }catch(SpaceException se) {
            say("SpaceException: "+se.getMessage());
            spaceMessage.setFailed(-2,se);
        } catch(Throwable t) {
            esay(t);
            spaceMessage.setFailed(-1,t);
        }
        if( replyRequired ) {
            try {
                say("Sending reply "+spaceMessage);
                cellMessage.revertDirection();
                sendMessage(cellMessage);
            } catch (Exception e) {
                esay("Can't reply message : "+e);
            }
        } else {
            say("reply is not required, finished processing");
        }
    }
    
    public void messageToForward(final CellMessage cellMessage ){
         diskCacheV111.util.ThreadManager.execute(new Runnable() {
            public void run() {
                processMessageToForward(cellMessage);
            }
        });    
    }
    
    public void processMessageToForward(CellMessage cellMessage ) {
        Object object = cellMessage.getMessageObject();
        //
        say("messageToForward,  arrived: type="+object.getClass().getName()+" value="+object +" from "+cellMessage.getSourcePath()+
                " going to "+cellMessage.getDestinationPath()+cellMessage.isAcknowledge());
        //
        // process the message
        //
        try {
            if( object instanceof PoolMgrSelectPoolMsg) {
                selectPool(cellMessage,true);
            } else if( object instanceof PoolAcceptFileMessage ) {
                
                PoolAcceptFileMessage poolRequest = (PoolAcceptFileMessage)object;
                if(poolRequest.isReply()) {
                    PnfsId pnfsId = poolRequest.getPnfsId();
                    
                    //mark file as being transfered
                    transferStarted(pnfsId,poolRequest.getReturnCode() == 0);
                } else {
                    // this message on its way to the pool
                    // we need to set the AccessLatency, RetentionPolicy and StorageGroup
                    transferToBeStarted(poolRequest);
                }
                
            } else if ( object instanceof DoorTransferFinishedMessage) {
                DoorTransferFinishedMessage finished = (DoorTransferFinishedMessage) object;
                transferFinished(finished);
            }
        } catch (Exception e){
            esay(e);
            // should we fail the transfer if we were not able
            // to set the correct AccessLatency and RetentionProperty attributes of the file
            //finished.setFailed(1,e)
        }
        
        //
        // and let it continue its travel
        //
        super.messageToForward(cellMessage) ;
        
    }
    
    public void exceptionArrived(ExceptionEvent ee) {
        say("Exception Arrived: "+ee);
        super.exceptionArrived(ee);
    }
        
    public void returnFailedResponse(Object reason ,
            Message spaceManagerMessage, CellMessage cellMessage) {
        if( reason != null && !(reason instanceof java.io.Serializable)) {
            reason = reason.toString();
        }
        
        try {
            spaceManagerMessage.setReply();
            spaceManagerMessage.setFailed(1, reason);
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        } catch(Exception e) {
            esay("can not send a failed responce");
            esay(e);
        }
    }
    
    public void returnMessage(Message message, CellMessage cellMessage) {
        try {
            message.setReply();
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        } catch(Exception e) {
            esay("can not send a responce");
            esay(e);
        }
        
    }
    
    private final Object updateLinkGroupsSyncObject = new Object();
    public void run(){
        if(Thread.currentThread() == expireSpaceReservations) {
            while(true) {
                expireSpaceReservations();
                try{
                    Thread.sleep(expireSpaceReservationsPeriod);
                } catch (InterruptedException ie) {
                    esay("expire SpaceReservations thread has been interrupted");
                    return;
                }
                
            }
            
        } else if(Thread.currentThread() == updateLinkGroups) {
            while(true) {
                updateLinkGroups();
                synchronized(updateLinkGroupsSyncObject) {
                    try{
                        updateLinkGroupsSyncObject.wait(updateLinkGroupsPeriod);
                    } catch (InterruptedException ie) {
                        esay("update LinkGroup thread has been interrupted");
                        return;
                    }
                }
                
            }
        }
    }
    
    private long latestLinkGroupUpdateTime =System.currentTimeMillis();
    private LinkGroupAuthorizationFile linkGroupAuthorizationFile;
    private long linkGroupAuthorizationFileLastUpdateTimestampt = 0;

    private void updateLinkGroupAuthorizationFile()
    {
        say("updateLinkGroupAuthorizationFile");
        if(linkGroupAuthorizationFileName == null) {
            return;
        }
        
        java.io.File f = new java.io.File (linkGroupAuthorizationFileName);
        if(!f.exists()) {
            esay("LinkGroupAuthorizationFile "+linkGroupAuthorizationFileName+
                " not found");
            linkGroupAuthorizationFile = null;
        }
        
        long lastModified = f.lastModified();
        if(linkGroupAuthorizationFile == null || lastModified >= linkGroupAuthorizationFileLastUpdateTimestampt) {
            linkGroupAuthorizationFileLastUpdateTimestampt = lastModified;
            try {
                say("reading "+linkGroupAuthorizationFileName);
                linkGroupAuthorizationFile = 
                    new LinkGroupAuthorizationFile(linkGroupAuthorizationFileName);
                say("done reading "+linkGroupAuthorizationFileName);
            }
            catch(Exception e) {
                esay(e);
            }
        }
    }

    private void updateLinkGroups(){
        long currentTime = System.currentTimeMillis();
        say("updateLinkGroups()... at "+currentTime);
        CellMessage cellMessage = new CellMessage(new CellPath(poolManager),
                new PoolMgrGetPoolLinkGroups());
        PoolMgrGetPoolLinkGroups getLinkGroups = null;
        try {
            cellMessage = sendAndWait(cellMessage,1000*5*60);
            say("updateLinkGroups() received reply");
            if(cellMessage == null ) {
                esay("updateLinkGroups() : request timed out");
                return;
                
            }
            if (cellMessage.getMessageObject() == null ) {
                esay("updateLinkGroups() : reply message is null");
                return;                
            }
            if( ! (cellMessage.getMessageObject() instanceof PoolMgrGetPoolLinkGroups)){
                esay("updateLinkGroups() : reply message is "+
                        cellMessage.getMessageObject().getClass().getName());
                return;                
                
            }
            
            getLinkGroups = (PoolMgrGetPoolLinkGroups)cellMessage.getMessageObject();
            if(getLinkGroups.getReturnCode() != 0) {
                esay("  PoolMgrGetPoolLinkGroups reply return code ="+getLinkGroups.getReturnCode() +
                        " error Object= "+getLinkGroups.getErrorObject());
                return;
            }
        } catch(Exception e) {
            esay("update failed");
            esay(e);
            return;
            
        }
        PoolLinkGroupInfo[] poolLinkGroupInfos = getLinkGroups.getPoolLinkGroupInfos();
        
        say("updateLinkGroups() number of poolLinkGroupInfos is "+poolLinkGroupInfos.length);
        if(poolLinkGroupInfos.length == 0) {
            return;
        }
        
        updateLinkGroupAuthorizationFile();
        
        for (int i = 0 ; i<poolLinkGroupInfos.length; ++i) {
            PoolLinkGroupInfo info = poolLinkGroupInfos[i];
            String linkGroupName = info.getName();
            long avalSpaceInBytes = info.getAvailableSpaceInBytes();
            info.getAvailableSpaceInBytes();
            VOInfo[] vos=null;
            //VOInfo[] vos = info.getAllowedVOs();
            
            boolean onlineAllowed = info.isOnlineAllowed();
            boolean nearlineAllowed = info.isNearlineAllowed();
            boolean replicaAllowed = info.isReplicaAllowed();
            boolean outputAllowed = info.isOutputAllowed();
            boolean custodialAllowed = info.isCustodialAllowed();

            //boolean replica = info.getHsmType().equals("None");
            

            say("updateLinkGroups: received LinkGroupInfo: name:"+linkGroupName+
                    " onlineAllowed:"+onlineAllowed+
                    " nearlineAllowed:"+nearlineAllowed+
                    " replicaAllowed:"+replicaAllowed+
                    " outputAllowed:"+outputAllowed+
                    " custodialAllowed:"+custodialAllowed+
                    " avalSpaceInBytes:"+avalSpaceInBytes);
            if(linkGroupAuthorizationFile != null) {
                
                 LinkGroupAuthorizationRecord record = 
                    linkGroupAuthorizationFile.getLinkGroupAuthorizationRecord(linkGroupName);
                 say("got LinkGroupAuthorizationRecord for "+linkGroupName+" record="+record);
                 if(record != null) {
                     FQAN[] fqans = record.getFqanArray();
                     if(fqans != null  && fqans.length >0) {
                         int vos_length = fqans.length;
                         vos = new VOInfo[vos_length];
                         for(int j = 0; j<fqans.length ; ++j) {
                             vos[j] = new VOInfo(fqans[j].getGroup(), fqans[j].getRole());
                         }
                         
                     }
                 }
            }
            
            if(vos != null) {
                for(int j =0; j<vos.length ; ++j ) {
                    say("updateLinkGroups: VOInfo["+j+"]="+vos[j]);
                }
            }
            
            try {
                updateLinkGroupInfo(linkGroupName,avalSpaceInBytes,currentTime,
                    onlineAllowed,
                    nearlineAllowed,
                    replicaAllowed,
                    outputAllowed,
                    custodialAllowed,
                    vos);
            }catch(SQLException sqle) {
                esay("update of linkGroup "+linkGroupName+" failed with exception:");
                esay(sqle);
            }
        }
        latestLinkGroupUpdateTime = currentTime;
        // insert update of linkGroups here
    }
    
    private void releaseSpace(Release release) throws 
       SQLException,SpaceException {
        say("releaseSpace("+release+")");
         if(!spaceManagerEnabled) {
            throw new SpaceException("SpaceManager is disabled in configuration");
        }
       long spaceToken = release.getSpaceToken();
        Long spaceToReleaseInBytes = release.getReleaseSizeInBytes();
        if(spaceToReleaseInBytes == null) {
            updateSpaceState(spaceToken,SpaceState.RELEASED);
            return;
        } else {
            throw new SQLException("partial release is not supported yet");
        }
    }
    
    java.util.Random rand = new java.util.Random();
    private void reserveSpace(Reserve reserve)
    throws SQLException,java.io.IOException,SpaceException{
        //say("reserveSpace("+reserve+")");
        if(!spaceManagerEnabled) {
            throw new SpaceException("SpaceManager is disabled in configuration");
        }
        long sizeInBytes = reserve.getSizeInBytes();
        AccessLatency latency = reserve.getAccessLatency()==null?
            defaultLatency:reserve.getAccessLatency();
        RetentionPolicy policy = reserve.getRetentionPolicy()==null?
            defaultPolicy:reserve.getRetentionPolicy();
        long lifetime = reserve.getLifetime();
        String voGroup = reserve.getVoGroup();
        String voRole = reserve.getVoRole();
        String description = reserve.getDescription();
        long reservationId = reserveSpace(voGroup,voRole,sizeInBytes,latency , policy, lifetime,description);
        reserve.setSpaceToken(reservationId);
    }
    
    public File reserveAndUseSpace(String pnfsPath,PnfsId pnfsId,long size,AccessLatency latency,RetentionPolicy policy,VOInfo voinfo)
    throws SQLException,java.io.IOException,SpaceException{
        
        long sizeInBytes = size;
        long lifetime = 1000*60*60;
        String voGroup = null;
        String voRole = null;
        if(voinfo != null){
            voGroup = voinfo.getVoGroup();
            voRole = voinfo.getVoRole();
        }
        String description = null;
        long reservationId = reserveSpace(voGroup,voRole,sizeInBytes,latency , policy, lifetime,description);
        long fileId = useSpace(reservationId,voGroup,voRole,sizeInBytes,lifetime,pnfsPath,pnfsId);
        File file = getFile(fileId);
        return file;
    }
    
    private void useSpace(Use use)
    throws SQLException, SpaceException{
        if(!spaceManagerEnabled) {
            throw new SpaceException("SpaceManager is disabled in configuration");
        }
        say("useSpace("+use+")");
        long reservationId = use.getSpaceToken();
        long sizeInBytes = use.getSizeInBytes();
        String voGroup = use.getVoGroup();
        String voRole = use.getVoRole();
        String pnfsPath = use.getPnfsName();
        PnfsId pnfsId = use.getPnfsId();
        long lifetime = use.getLifetime();
        long fileId = useSpace(reservationId,voGroup,voRole,sizeInBytes,lifetime,pnfsPath,pnfsId);
        use.setFileId(fileId);
    }
    private void transferToBeStarted(PoolAcceptFileMessage poolRequest){
        PnfsId pnfsId = poolRequest.getPnfsId();
        say("transferToBeStarted("+pnfsId+")");
        try {
            File f = getFile(pnfsId);
            Space s = getSpace(f.getSpaceId());
            StorageInfo info = poolRequest.getStorageInfo();
            
            info.setAccessLatency(s.getAccessLatency());
            info.isSetAccessLatency(true);
            info.setRetentionPolicy(s.getRetentionPolicy());
            info.isSetRetentionPolicy(true);
            say("transferToBeStarted(), set AL to "+s.getAccessLatency()+
                    " RP to "+s.getRetentionPolicy());
        } catch(SQLException sqle){
            esay("transferToBeStarted(): could bnot get space reservation related to this transfer ");
        }
    }
    
    private void transferStarted(PnfsId pnfsId,boolean success) {
        say("transferStarted("+pnfsId+","+success+")");
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            if(!success) {
                say("transfer start up failed");
                File f = getFileForUpdate(_con,pnfsId);
                if(f == null) {
                    _con.rollback();
                    connection_pool.returnConnection(_con);
                    _con = null;
                    return;
                }
                if(f.getState() == FileState.RESERVED ||
                        f.getState() == FileState.TRANSFERING) {
                    removePnfsIdOfFileInSpace(_con,f.getId(),
                            new Integer(FileState.RESERVED.getStateId()));

                    say("COMMIT TRANSACTION");
                    _con.commit();
                    connection_pool.returnConnection(_con);
                    _con = null;
                } else {
                    // we did not find anything, try to insert a new linkGroup record
                    _con.commit();
                    connection_pool.returnConnection(_con);
                    _con = null;
                }
                return;
            }
            File f = getFileForUpdate(_con,pnfsId);
            if(f == null) {
                _con.rollback();
                connection_pool.returnConnection(_con);
                _con = null;
                return;
            }
            
            if(f.getState() == FileState.RESERVED ||
                    f.getState() == FileState.TRANSFERING) {
                updateFileInSpaceInfo(_con,f.getId(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        new Integer(FileState.TRANSFERING.getStateId()));
                
                say("COMMIT TRANSACTION");
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
            } else {
                // we did not find anything, try to insert a new linkGroup record
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
            }
            
        } catch(SQLException sqle) {
            esay("transferStarted failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try {
                _con.rollback();
            } catch(SQLException sqle1) {}
            connection_pool.returnFailedConnection(_con);
            _con = null;
            
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        
    }
    
    private void transferFinished(DoorTransferFinishedMessage finished) throws Exception {
        boolean weDeleteStoredFileRecord = deleteStoredFileRecord;
        PnfsId pnfsId = finished.getPnfsId();
        StorageInfo storageInfo = finished.getStorageInfo();
        
        long size = storageInfo.getFileSize();
        boolean success = finished.getReturnCode() == 0;
        
        say("transferFinished("+pnfsId+","+success+")");
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            File f = getFileForUpdate(_con,pnfsId);
            if(f == null) {
                _con.rollback();
                connection_pool.returnConnection(_con);
                _con = null;
                return;
            }
            long spaceId = f.getSpaceId();
            
            if(f.getState() == FileState.RESERVED ||
                    f.getState() == FileState.TRANSFERING) {
                if(success) {
                    if(returnFlushedSpaceToReservation && weDeleteStoredFileRecord) {
                        RetentionPolicy rp = getSpace(_con,spaceId).getRetentionPolicy();
                        if(rp.equals(RetentionPolicy.CUSTODIAL)) {
                            //we do not delete it here, since the 
                            // file will get flushed and we will need
                            // to account for that
                            weDeleteStoredFileRecord = false;
                        }
                    }
                    
                    if(weDeleteStoredFileRecord) {
                        say("file transfered, deleting file record");
                        deleteFileInSpaceSpace(_con,f.getId());
                        Space space = getSpaceForUpdate(_con,spaceId);
                        storageInfo.setAccessLatency(space.getAccessLatency());
                        storageInfo.isSetAccessLatency(true);
                        storageInfo.setRetentionPolicy(space.getRetentionPolicy());
                        storageInfo.isSetRetentionPolicy(true);
                        long availSpaceSize = space.getSizeInBytes();
                        if(size >= availSpaceSize) {
                            say("all space utilized, deleting space record");
                            try {
                                deleteSpaceReservation(_con,space.getId());
                            } catch(Exception e) {
                                esay(e);
                                say("cant delete, updating space record");
                                updateSpaceReservation(_con,space.getId(),
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        new Long(availSpaceSize-size),
                                        null,null,new Integer(SpaceState.RELEASED.getStateId()));
                            }
                        } else {
                            updateSpaceReservation(_con,space.getId(),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    new Long(availSpaceSize-size),
                                    null,null,null);
                            
                        }
                        
                    } else {
                        say("file transfered, updating file record");
                        Space space = getSpaceForUpdate(_con,spaceId);
                        storageInfo.setAccessLatency(space.getAccessLatency());
                        storageInfo.isSetAccessLatency(true);
                        storageInfo.setRetentionPolicy(space.getRetentionPolicy());
                        storageInfo.isSetRetentionPolicy(true);
                        updateFileInSpaceInfo(_con,f.getId(),
                                null,
                                null,
                                null,
                                new Long(size),
                                null,
                                new Integer(FileState.STORED.getStateId()));
                    }
                    
                } else {
                    updateFileInSpaceInfo(_con,f.getId(),
                            null,
                            null,
                            null,
                            null,
                            null,
                            new Integer(FileState.RESERVED.getStateId()));
                    
                }
                say("COMMIT TRANSACTION");
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
            } else {
                esay("transferFinished("+pnfsId+"): file state=" +f.getState() );
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
            }
            
        } catch(SQLException sqle) {
            esay("transferStarted failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try {
                _con.rollback();
            } catch(SQLException sqle1) {}
            connection_pool.returnFailedConnection(_con);
            _con = null;
            
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        //
        // We do not do this anymore because pool code will do that for us!!!
        //PnfsSetStorageInfoMessage setStorageInfo = new PnfsSetStorageInfoMessage(pnfsId,storageInfo,0);
        //setStorageInfo.setReply(false);
        //sendMessage(new CellMessage(new CellPath(pnfsManager),setStorageInfo));
    }
    
    private void  fileFlushed(PoolFileFlushedMessage fileFlushed) throws Exception {
        PnfsId pnfsId = fileFlushed.getPnfsId();
        StorageInfo storageInfo = fileFlushed.getStorageInfo();
        say("fileFlushed("+pnfsId+")");
        if(!returnFlushedSpaceToReservation) {
            //do nothing
            return;
        }
        long size = storageInfo.getFileSize();
        
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            File f = getFileForUpdate(_con,pnfsId);
            if(f == null) {
                _con.rollback();
                connection_pool.returnConnection(_con);
                _con = null;
                //file not found, return
                say( "fileFlushed("+pnfsId+"): file not in a reservation, do nothing");
                return;
            }
            long spaceId = f.getSpaceId();
            
            if(f.getState() == FileState.STORED) {
                if(deleteStoredFileRecord) {
                    say("returnSpaceToReservation, deleting file record");
                    deleteFileInSpaceSpace(_con,f.getId());
                } else {
                    say("returnSpaceToReservation: file flushed, updating file record");
                    Space space = getSpaceForUpdate(_con,spaceId);
                    storageInfo.setAccessLatency(space.getAccessLatency());
                    storageInfo.isSetAccessLatency(true);
                    storageInfo.setRetentionPolicy(space.getRetentionPolicy());
                    storageInfo.isSetRetentionPolicy(true);
                    updateFileInSpaceInfo(_con,f.getId(),
                            null,
                            null,
                            null,
                            new Long(size),
                            null,
                                new Integer(FileState.FLUSHED.getStateId()));

                    say("COMMIT TRANSACTION");
                    _con.commit();
                    connection_pool.returnConnection(_con);
                    _con = null;
                }
            } else {
                esay("returnSpaceToReservation("+pnfsId+"): file state=" +f.getState() );
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
            }
            
        } catch(SQLException sqle) {
            esay("returnSpaceToReservation failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try {
                _con.rollback();
            } catch(SQLException sqle1) {}
            connection_pool.returnFailedConnection(_con);
            _con = null;
            
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        //
        // We do not do this anymore because pool code will do that for us!!!
        //PnfsSetStorageInfoMessage setStorageInfo = new PnfsSetStorageInfoMessage(pnfsId,storageInfo,0);
        //setStorageInfo.setReply(false);
        //sendMessage(new CellMessage(new CellPath(pnfsManager),setStorageInfo));
    }
    
    private void  fileRemoved(PoolRemoveFilesMessage fileRemoved) throws Exception {
        String[] pnfsIdStrings = fileRemoved.getFiles();
        if(pnfsIdStrings == null || pnfsIdStrings.length == 0) {
            return;
        }
        for(String pnfsIdString : pnfsIdStrings ) {
            PnfsId pnfsId ;
            try {
                pnfsId = new PnfsId(pnfsIdString);
            }
            catch(Exception e) {
                esay(e);
                continue;
            }
            
            say("fileRemoved("+pnfsId+")");
            if(!returnRemovedSpaceToReservation) {
                //do nothing
                return;
            }

            Connection _con = null;
            try {
                _con = connection_pool.getConnection();
                File f = getFileForUpdate(_con,pnfsId);
                if(f == null) {
                    _con.rollback();
                    connection_pool.returnConnection(_con);
                    _con = null;
                    //file not found, return
                    say( "fileRemoved("+pnfsId+"): file not in a reservation, do nothing");
                    return;
                }
                long spaceId = f.getSpaceId();
                deleteFileInSpaceSpace(_con,f.getId());
                say("COMMIT TRANSACTION");
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;

            } catch(SQLException sqle) {
                esay("returnSpaceToReservation failed with ");
                esay(sqle);
                say("ROLLBACK TRANSACTION");
                try {
                    _con.rollback();
                } catch(SQLException sqle1) {}
                connection_pool.returnFailedConnection(_con);
                _con = null;

            } finally {
                if(_con != null) {
                    connection_pool.returnConnection(_con);
                }
            }
        }
        //
        // We do not do this anymore because pool code will do that for us!!!
        //PnfsSetStorageInfoMessage setStorageInfo = new PnfsSetStorageInfoMessage(pnfsId,storageInfo,0);
        //setStorageInfo.setReply(false);
        //sendMessage(new CellMessage(new CellPath(pnfsManager),setStorageInfo));
    }
    
    private void cancelUseSpace(CancelUse cancelUse)
    throws SQLException,SpaceException{
        if(!spaceManagerEnabled) {
            throw new SpaceException("SpaceManager is disabled in configuration");
        }
        say("cancelUseSpace("+cancelUse+")");
        long reservationId = cancelUse.getSpaceToken();
        String pnfsPath = cancelUse.getPnfsName();
        PnfsId pnfsId = cancelUse.getPnfsId();
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            File f = getFileForUpdate(_con,pnfsPath);
            if(f == null) {
                // no file is found
                esay("cancelUseSpace("+cancelUse+"), no file is found");
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
            }
            else {
                if(f.getState() == FileState.RESERVED ||
                        f.getState() == FileState.TRANSFERING) {
                    deleteFileInSpaceSpace(_con,f.getId());

                    say("COMMIT TRANSACTION");
                    _con.commit();
                    connection_pool.returnConnection(_con);
                    _con = null;
                } else {
                    // we did not find anything, try to insert a new linkGroup record
                    _con.commit();
                    connection_pool.returnConnection(_con);
                    _con = null;
                }
            }
        } catch(SQLException sqle) {
            esay("cancelUseSpace failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    private long reserveSpace(
            String voGroup,
            String voRole,
            long sizeInBytes,
            AccessLatency latency ,
            RetentionPolicy policy,
            long lifetime,
            String description)
            throws SQLException,
            java.io.IOException,
            SpaceException
            {
        say("reserveSpace(group="+voGroup+", role="+voRole+", sz="+sizeInBytes+
                ", latency="+latency+", policy="+policy+", lifetime="+lifetime+
                ", description="+description);
        
        boolean needHsmBackup = policy.equals(RetentionPolicy.CUSTODIAL);
        say("policy is "+policy+", needHsmBackup is "+needHsmBackup);
        long reservationId = getNextToken();
        for(int i =0;i<10;++i) {
            Long[] linkGroups = findLinkGroupIds(sizeInBytes,voGroup,voRole,
                latency,
                policy);
            if(linkGroups.length == 0) {
                esay("find LinkGroup Ids returned 0 linkGroups, no linkGroups found");
                throw new NoFreeSpaceException(" no space available");
            }
            say("found linkGroups");
            Long linkGroupId = linkGroups[rand.nextInt(linkGroups.length)];
            say("selected linkGroup with id="+linkGroupId);
            Connection _con =null;
            
            try {
                _con = connection_pool.getConnection();
                // if there is not enough space in this linkGroup
                // which can happen if someone have inserted
                selectLinkGroupForUpdate(_con,linkGroupId,sizeInBytes);
                //insert
                insertSpaceReservation(
                        _con,
                        reservationId,
                        voGroup,
                        voRole,
                        policy,
                        latency,
                        linkGroupId.longValue(),
                        sizeInBytes,
                        lifetime,
                        description,
                        0);
                say("COMMIT TRANSACTION");
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;
                
                return reservationId;
            } catch(SQLException sqle) {
                esay("selectLinkGroupForUpdate failed with ");
                esay(sqle);
                say("ROLLBACK TRANSACTION");
                _con.rollback();
                connection_pool.returnFailedConnection(_con);
                _con = null;
                // we do not throw exception
                // since we want to loop here
                //throw sqle;
            } finally {
                if(_con != null) {
                    connection_pool.returnConnection(_con);
                }
            }
        }
        throw new SQLException(" can not find availabe space");
    }
    
    private long reserveSpaceInLinkGroup(
            long linkGroupId,
            String voGroup,
            String voRole,
            long sizeInBytes,
            AccessLatency latency ,
            RetentionPolicy policy,
            long lifetime,
            String description)
            throws SQLException,
            java.io.IOException,
            SpaceException
            {
        say("reserveSpaceInLinkGroup(linkGroupId="+linkGroupId+
            "group="+voGroup+", role="+voRole+", sz="+sizeInBytes+
                ", latency="+latency+", policy="+policy+", lifetime="+lifetime+
                ", description="+description);
        
        boolean needHsmBackup = policy.equals(RetentionPolicy.CUSTODIAL);
        LinkGroup lg = getLinkGroup(linkGroupId);
        if(lg == null ) {
            throw new SpaceException("Link Group with id="+linkGroupId+
                " is not found");
        }
        
        long reservationId = getNextToken();
        Connection _con =null;

        try {
            _con = connection_pool.getConnection();
            // if there is not enough space in this linkGroup
            // which can happen if someone have inserted
            selectLinkGroupForUpdate(_con,linkGroupId,sizeInBytes);
            //insert
            insertSpaceReservation(
                    _con,
                    reservationId,
                    voGroup,
                    voRole,
                    policy,
                    latency,
                    linkGroupId,
                    sizeInBytes,
                    lifetime,
                    description,
                    0);
            say("COMMIT TRANSACTION");
            _con.commit();
            connection_pool.returnConnection(_con);
            _con = null;

            return reservationId;
        } catch(SQLException sqle) {
            esay("reserveSpaceInLinkGroup failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }

    }
    
    private long useSpace(long reservationId,
            String voGroup,
            String voRole,
            long sizeInBytes,long lifetime, String pnfsPath, PnfsId pnfsId)
            throws SQLException,SpaceException{
        Connection _con =null;
        pnfsPath =new FsPath(pnfsPath).toString();
        long fileId = getNextToken();
        try {
            _con = connection_pool.getConnection();
            // if there is not enough space in this linkGroup
            // which can happen if someone have inserted
            lockSpaceIfSpaceIsAvailable(_con,reservationId,sizeInBytes);
            //insert
            insertFileInSpace(
                    _con,
                    fileId,
                    voGroup,
                    voRole,
                    reservationId,
                    sizeInBytes,
                    lifetime,
                    pnfsPath,
                    pnfsId,
                    0);
            say("COMMIT TRANSACTION");
            _con.commit();
            connection_pool.returnConnection(_con);
            _con = null;
            
            return fileId;
        } catch(SQLException sqle) {
            esay("selecSpaceForUpdate or insert file failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public void selectPool(CellMessage cellMessage, boolean willBeForwarded ) throws Exception{
        PoolMgrSelectPoolMsg selectPool = (PoolMgrSelectPoolMsg)cellMessage.getMessageObject();
        if(!spaceManagerEnabled ) {
            if(!willBeForwarded) {
                say("just forwarding the message to "+ poolManager);
                cellMessage.getDestinationPath().add( new CellPath(poolManager) ) ;
                cellMessage.nextDestination() ;
                sendMessage(cellMessage) ;
            }
            return;
        }
        
        say("selectPool("+selectPool +")");
        String pnfsPath = selectPool.getPnfsPath();
        PnfsId pnfsId = selectPool.getPnfsId();
        if( !(selectPool instanceof PoolMgrSelectWritePoolMsg) ||
                pnfsPath == null) {
            say("selectPool: pnfsPath is null");
            if(!willBeForwarded) {
                say("just forwarding the message to "+ poolManager);
                cellMessage.getDestinationPath().add( new CellPath(poolManager) ) ;
                cellMessage.nextDestination() ;
                sendMessage(cellMessage) ;
            }
            return;
        }
        File file = null;
        try {
            say("selectPool: getFile("+pnfsPath+")");
            file = getFile(pnfsPath);
        } catch (Exception e){
            esay(e);
        }
        if( file == null) {
            if(reserveSpaceForNonSRMTransfers) {
                ProtocolInfo protocolInfo = selectPool.getProtocolInfo();
                VOInfo voinfo = null;
                if(protocolInfo instanceof GridProtocolInfo) {
                    voinfo = 
                        ((GridProtocolInfo)protocolInfo).getVOInfo();
                    say("protocol info is GridProtocolInfo");
                    say(" voinfo="+voinfo);
                }
                say("selectPool: file is not found, no prior reservations for this file, calling reserveAndUseSpace()");
                StorageInfo storageInfo = selectPool.getStorageInfo();
                AccessLatency al = defaultLatency;
                RetentionPolicy rp = defaultPolicy;
                if(storageInfo != null) {
                    if(storageInfo.isSetAccessLatency()){
                        al  = storageInfo.getAccessLatency();
                    }
                    if(storageInfo.isSetRetentionPolicy()){
                        rp  = storageInfo.getRetentionPolicy();
                    }
                }
                file = reserveAndUseSpace(pnfsPath,
                        selectPool.getPnfsId(),
                        selectPool.getFileSize(),
                        al, 
                        rp,
                        voinfo);
            } else {
                say("selectPool: file is not found, no prior reservations for this file");
                if(!willBeForwarded) {
                    say("just forwarding the message to "+ poolManager);
                    cellMessage.getDestinationPath().add( new CellPath(poolManager) ) ;
                    cellMessage.nextDestination() ;
                    sendMessage(cellMessage) ;
                }
                return;
                
            }
        } else {
            say("selectPool: file is not null, calling updateFileInSpaceInfo()");
            updateFileInSpaceInfo(file.getId(),null,null,pnfsId,null,null,null);
        }
        
        long spaceId = file.getSpaceId();
        Space space = getSpace(spaceId);
        long linkGroupid = space.getLinkGroupId();
        LinkGroup linkGroup = getLinkGroup(linkGroupid);
        String linkGroupName = linkGroup.getName();
        selectPool.setLinkGroup(linkGroupName);
        StorageInfo storageInfo = selectPool.getStorageInfo();
        storageInfo.setKey("SpaceToken",Long.toString(spaceId));
        if(!willBeForwarded) {
            cellMessage.getDestinationPath().add( new CellPath(poolManager) ) ;
            cellMessage.nextDestination() ;
            say("selectPool: found linkGroup = "+linkGroupName+", forwarding message");
            sendMessage(cellMessage) ;
        }
        return;
    }
    
    public String getPnfsManager() {
        return pnfsManager;
    }
    
    public String getPoolManager() {
        return poolManager;
    }
    
    public void getSpaceMetaData(GetSpaceMetaData gsmd) throws Exception{
        if(!spaceManagerEnabled) {
            throw new SpaceException("SpaceManager is disabled in configuration");
        }
        long[] tokens = gsmd.getSpaceTokens();
        if(tokens == null) {
            throw new IllegalArgumentException("null space tokens");
        }
        Space[] spaces = new Space[tokens.length];
        for(int i=0;i<spaces.length; ++i){
            try {
                spaces[i] = getSpaceWithUsedSize(tokens[i]);
            } catch(Exception e) {
                esay(e);
                spaces[i]= null;
            }
        }
        gsmd.setSpaces(spaces);
    }
    
    public void getSpaceTokens(GetSpaceTokens gst) throws Exception{
        if(!spaceManagerEnabled) {
            throw new SpaceException("SpaceManager is disabled in configuration");
        }
        String description = gst.getDescription();
        String voGroup = gst.getVoGroup();
        String voRole = gst.getVoRole();
        if(voGroup == null) {
            throw new IllegalArgumentException("null voGroup");
        }
        long [] tokens = getSpaceTokens(voGroup, voRole, description);
        gst.setSpaceToken(tokens);
    }
    
    public void getFileSpaceTokens(GetFileSpaceTokensMessage getFileTokens) throws Exception{
        if(!spaceManagerEnabled) {
            throw new SpaceException("SpaceManager is disabled in configuration");
        }
        PnfsId pnfsId = getFileTokens.getPnfsId();
        String pnfsPath = getFileTokens.getPnfsPath();
        if(pnfsId == null && pnfsPath == null) {
            throw new IllegalArgumentException("null voGroup");
            
        }
        long [] tokens = getFileSpaceTokens(pnfsId,pnfsPath);
        getFileTokens.setSpaceToken(tokens);
        
    }
    
    public void extendLifetime(ExtendLifetime extendLifetime) throws Exception{
        if(!spaceManagerEnabled) {
            throw new SpaceException("SpaceManager is disabled in configuration");
        }
        long token = extendLifetime.getSpaceToken();
        long newLifetime = extendLifetime.getNewLifetime();
        Connection _con =null;
        try {
            _con = connection_pool.getConnection();
            Space space = getSpaceForUpdate(_con,token);
            if(SpaceState.isFinalState(space.getState())) {
                _con.rollback();
                connection_pool.returnConnection(_con);
                _con = null;
                throw new Exception("Space Is already Released");
            }
            long creationTime = space.getCreationTime();
            long lifetime = space.getLifetime();
            if(lifetime == -1) {
                _con.rollback();
                connection_pool.returnConnection(_con);
                _con = null;
                return;
            }
            if(newLifetime == -1) {
                updateSpaceLifetime(_con,token, newLifetime);
                say("COMMIT TRANSACTION");
                _con.commit();
                connection_pool.returnConnection(_con);
                _con = null;  
                return;
            }
            long currentTime = System.currentTimeMillis();
            long remainingLifetime = creationTime + lifetime - currentTime;
            if(remainingLifetime > newLifetime) {
                _con.rollback();
                connection_pool.returnConnection(_con);
                _con = null;
                return;
            }
            long newLifetimeSinceCreation = currentTime - creationTime + newLifetime;
            updateSpaceLifetime(_con,token, newLifetimeSinceCreation);
            say("COMMIT TRANSACTION");
            _con.commit();
            connection_pool.returnConnection(_con);
            _con = null;
            
        } catch(SQLException sqle) {
            esay("selecSpaceForUpdate or insert file failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
}

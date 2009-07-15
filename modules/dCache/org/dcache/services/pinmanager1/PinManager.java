// $Id: PinManager.java,v 1.42 2007-10-25 15:02:43 behrmann Exp $
// $Log: not supported by cvs2svn $
// Revision 1.41  2007/08/16 22:04:58  timur
// simplified srmRm, return SRM_INVALID_PATH if file does not exist
//
// Revision 1.38  2007/08/03 20:20:00  timur
// implementing some of the findbug bugs and recommendations,
// avoid selfassignment, possible nullpointer exceptions, syncronization issues, etc
//
// Revision 1.37  2007/08/03 15:46:01  timur
// closing sql statement, implementing hashCode functions, not passing null args,
// resing classes etc, per findbug recommendations
//
// Revision 1.36  2007/07/16 21:56:01  timur
// make sure the empty pgpass is ignored
//
// Revision 1.35  2007/05/24 13:51:13  tigran
// merge of 1.7.1 and the head
//
// Revision 1.34  2007/04/18 23:35:30  timur
// initial state of the pin should be INITIAL_STATE and not PINNED\!
//
// Revision 1.33  2007/02/10 04:48:13  timur
//  first version of SrmExtendFileLifetime
//
// Revision 1.32  2006/10/10 21:04:35  timur
// srmBringOnline related changes
//
// Revision 1.31  2006/05/12 22:44:54  litvinse
// *** empty log message ***
//
// Revision 1.30  2005/12/14 10:07:55  tigran
// setting cell type to class name
//
// Revision 1.29  2005/11/22 10:59:30  patrick
// Versioning enabled.
//
// Revision 1.28  2005/11/17 17:45:33  timur
// fixed two select statements rejected by Postgres8.1
//
// Revision 1.27  2005/08/26 21:59:33  timur
// one more bug removed
//
// Revision 1.26  2005/08/26 21:44:37  timur
// bugs removed
//
// Revision 1.25  2005/08/26 21:01:57  timur
// reorganized data structures and state transitions of PinManager
//
// Revision 1.24  2005/08/23 16:22:11  timur
// added new info messages
//
// Revision 1.23  2005/08/22 17:22:08  timur
// better recovery from unexpected states of PinRequests
//
// Revision 1.22  2005/08/19 23:45:26  timur
// added Pgpass for postgress md5 password suport
//
// Revision 1.21  2005/08/16 16:35:25  timur
// added duration control
//
// Revision 1.20  2005/08/15 19:30:59  timur
// mostly working
//
// Revision 1.19  2005/08/15 18:19:43  timur
// new PinManager first working version, needs more testing
//
// Revision 1.18  2005/08/12 17:14:54  timur
// a first approximation of the new PinManager, not tested, but compiles,
// do not deploy in production
//
// Revision 1.17  2005/03/07 22:57:43  timur
// more work on space reservation
//
// Revision 1.16  2005/03/01 23:12:09  timur
// Modified the database scema to increase database operations performance
// and to account for reserved space"and to account for reserved space
//
// Revision 1.15  2004/11/09 08:04:46  tigran
// added SerialVersion ID
//
// Revision 1.14  2004/10/20 21:32:29  timur
// adding classes for space management
//
// Revision 1.13  2004/06/22 01:32:09  timur
// Fixed an initialization bug
//

/*
 * PinManager.java
 *
 * Created on April 28, 2004, 12:54 PM
 */

package org.dcache.services.pinmanager1;

import diskCacheV111.poolManager.RequestContainerV5;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.Args;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.CellVersion;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PinManagerMessage;
import diskCacheV111.vehicles.PinManagerPinMessage;
import diskCacheV111.vehicles.PinManagerUnpinMessage;
import diskCacheV111.vehicles.PinManagerExtendLifetimeMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.regex.PatternSyntaxException;
import org.dcache.cells.Option;
import org.dcache.cells.AbstractCell;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.pool.repository.StickyRecord;

/**
 *   <pre>
 *   This cell performs "pinning and unpining service on behalf of other
 *   services cenralized pin management supports:
 *    pining/unpinning of the same resources by multiple requestors,
 *     synchronization of pinning and unpinning
 *     lifetimes for pins
 *
 * PINNING
 * 1) when pin for a file exists and another request arrives
 *  no action is taken, the database pinrequest record is created
 * 2) if pin does not exist new pinrequest record is created
 *       Pnfs flag is not set anymore
 *       the file is staged if nessesary and pinned in a read pool
 *       with PinManager, as an owner, and lifetime
 *
 *
 * UNPINNING
 *  1)if pin request expires / canseled and other pin requests
 *  for the same file exist, no action is taken, other then removal
 *  of the database  pin request record
 *  2) if last pin request is removed then the file is unpinned
 * which means sending of the "set sticky to false message" is send to all
 * locations,
 *the pnfs flag is removed
 * database  pin request record is removed
 *
 *
 *
 * @author  timur
 */
public class PinManager extends AbstractCell implements Runnable  {

    @Option(
        name = "expirationFrequency",
        description = "Frequency of running pin expiration routine",
        defaultValue = "60000", // every minute
        unit = "ms"
    )
    protected long expirationFrequency;

    @Option(
        name = "maxPinDuration",
        description = "Max. lifetime of a pin",
        defaultValue = "86400000", // one day
        unit = "ms"
    )
    protected long maxPinDuration;

    @Option(
        name = "pnfsManager",
        defaultValue = "PnfsManager",
        description = "PNFS manager name"
    )
    protected String pnfsManager;

    @Option(
        name = "poolManager",
        defaultValue = "PoolManager",
        description = "Pool manager name"
    )
    protected String poolManager;

    @Option(
        name = "jdbcUrl",
        required = true
    )
    protected String jdbcUrl;

    @Option(
        name = "jdbcDriver",
        required = true
    )
    protected String jdbcDriver;

    @Option(
        name = "dbUser",
        required = true
    )
    protected String dbUser;

    @Option(
        name = "dbPass",
        log = false
    )
    protected String dbPass;

    @Option(
        name = "pgPass"
    )
    protected String pgPass;


    @Option(
        name = "maxActiveJdbcConnections",
        defaultValue = "50", // half of default postgres max of 100
        description = "max number of active jdbc connections"
    )
    protected int maxActiveJdbcConnections;


    @Option(
        name = "maxJdbcConnectionsWaitSec",
        defaultValue = "180", // 3 min
        description = "max number of idle jdbc connections",
        unit = "sec"
    )
    protected long maxJdbcConnectionsWaitSec;

    @Option(
        name = "maxIdleJdbcConnections",
        defaultValue = "10",
        description = "max number of idle jdbc connections"
    )
    protected int maxIdleJdbcConnections;


    @Option(
        name = "pinManagerPolicy",
        defaultValue="org.dcache.services.pinmanager1.SimplePinManagerPolicyImpl"
    )
    protected String pinManagerPolicyClass;

     /**
     * File (StageConfiguration.conf) containing DNs and FQANs whose owner are allowed to STAGE files
     * (i.e. allowed to copy file from dCache in case file is stored on tape but not on disk).
     * /opt/d-cache/config/StageConfiguration.conf
     * By default, such file does not exist, so that tape protection feature is not in use.
     */
    @Option(
        name = "stageConfigurationFilePath",
        description = "File containing DNs and FQANs for which staging is allowed",
        defaultValue = ""
    )
    protected String _stageConfigurationFilePath;

    // all database oprations will be done in the lazy
    // fassion in a low priority thread
    private Thread expireRequests;

    private PinManagerDatabase db;

    // this is the difference between the expiration time of the pin and the
    // expiration time of the sticky bit in the pool. used in case if the
    // pin exiration / removal could not unpin the file in the pool
    // (due to the pool down situation)
    protected static final long  POOL_LIFETIME_MARGIN=60*60*1000L;

    private PinManagerPolicy pinManagerPolicy;

    private Map<Long, CellMessage> pinToRequestsMap =
        new ConcurrentHashMap<Long, CellMessage>();

    private Map<Long, CellMessage> pinRequestToUnpinRequestsMap =
        new ConcurrentHashMap<Long, CellMessage>();

    /** Tape Protection */
    protected CheckStagePermission _checkStagePermission;

    /** Creates a new instance of PinManager */
    public PinManager(String name , String argString)
        throws InterruptedException, ExecutionException
    {
        super(name, argString);
        doInit();
    }

    @Override
    protected void init()
        throws Exception
    {
        super.init();


        pinManagerPolicy = (PinManagerPolicy)
            Class.forName(pinManagerPolicyClass).getConstructor().newInstance();
        db = new PinManagerDatabase(this,
                                    jdbcUrl,
                                    jdbcDriver,
                                    dbUser,
                                    dbPass,
                                    pgPass,
                                    maxActiveJdbcConnections,
                                    maxJdbcConnectionsWaitSec,
                                    maxIdleJdbcConnections
                                    );
        expireRequests =
            getNucleus().newThread(this,"ExpireRequestsThread");
        //databaseUpdateThread.setPriority(Thread.MIN_PRIORITY);
        expireRequests.start();

        runInventoryBeforeStartPart();
        start();
        runInventoryAfterStartPart();

        _checkStagePermission = new CheckStagePermission(_stageConfigurationFilePath);
    }

    public void stop() {
        kill();
    }

    public long getMaxPinDuration()
    {
        return maxPinDuration;
    }

    public CellPath getPnfsManager()
    {
        return new CellPath(pnfsManager);
    }

    public CellPath getPoolManager()
    {
        return new CellPath(poolManager);
    }


    public CellVersion getCellVersion(){
        return new CellVersion(
            diskCacheV111.util.Version.getVersion(),"$Revision: 1.42 $" );
    }


    public String hh_pin_pnfsid = "<pnfsId> <seconds> " +
        "# pin a file by pnfsid for <seconds> seconds" ;
    public String ac_pin_pnfsid_$_2( Args args ) throws Exception {
        PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
        long lifetime = Long.parseLong( args.argv(1) ) ;
        if(lifetime != -1) {
            lifetime *=1000;
        }
        pin(pnfsId,null,lifetime,0,null,null,null);
        return "pin started";

    }

    public String hh_unpin = " [-force] [<pinRequestId>] <pnfsId> " +
        "# unpin a a file by pinRequestId and by pnfsId or just by pnfsId" ;
    public String ac_unpin_$_1_2( Args args ) throws Exception {
        boolean force = args.getOpt("force") != null;
        if(args.argc() == 1) {
            PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
            unpin(pnfsId,null,null,null,force);
            return "unpin started";

        }
        long pinRequestId = Long.parseLong(args.argv(0));
        PnfsId pnfsId = new PnfsId( args.argv(1) ) ;
        unpin(pnfsId, pinRequestId,null,null,null,force);
        return "unpin started";

    }

    public String hh_extend_lifetime = "<pinRequestId> <pnfsId> <seconds " +
        "# extendlifetime of a pin  by pinRequestId and by pnfsId" ;
    public String ac_extend_lifetime_$_3( Args args ) throws Exception {
        long pinRequestId = Long.parseLong(args.argv(0));
        PnfsId pnfsId = new PnfsId( args.argv(1) ) ;
        long lifetime = Long.parseLong( args.argv(2) ) ;
        lifetime *=1000;
        extendLifetime(pnfsId, pinRequestId,lifetime,null,null);
        return "extend lifetime started";
    }


    public String hh_set_max_pin_duration =
        " # sets new max pin duration value in milliseconds, -1 for infinite" ;
    public String ac_set_max_pin_duration_$_1( Args args ) throws Exception {
        StringBuilder sb = new StringBuilder();
        long newMaxPinDuration = Long.parseLong(args.argv(0));
        if(newMaxPinDuration== -1 || newMaxPinDuration >0 ) {

            sb.append("old max pin duration was ");
            sb.append(maxPinDuration).append(" milliseconds\n");
            maxPinDuration = newMaxPinDuration;
            sb.append("max pin duration value set to ");
            sb.append(maxPinDuration).append(" milliseconds\n");

        } else {
            sb.append("max pin duration value must be -1 ior nonnegative !!!");
        }

        return sb.toString();
    }
    public String hh_get_max_pin_duration =
            " # gets current max pin duration value" ;
    public String ac_get_max_pin_duration_$_0( Args args ) throws Exception {

        return Long.toString(maxPinDuration)+" milliseconds";
    }

    public String hh_ls = " [id|pnfsId] # lists all pins or a specified pin by request id or pnfsid" ;
    public String ac_ls_$_0_1(Args args) throws Exception {
        db.initDBConnection();
        try {
            if (args.argc() > 0) {
                try {
                    long  id = Long.parseLong(args.argv(0));
                    Pin pin  = db.getPin(id) ;
                    StringBuilder sb = new StringBuilder();
                    sb.append(pin.toString());
                        sb.append("\n  pinRequests: \n");
                    for (PinRequest pinReqiest:pin.getRequests()) {
                        sb.append("  ").append(pinReqiest).append('\n');
                    }
                    return sb.toString();
                } catch (NumberFormatException nfe) {
                    PnfsId pnfsId = new PnfsId(args.argv(0));
                    StringBuilder sb = new StringBuilder();

                     db.allPinsByPnfsIdToStringBuilder(sb,pnfsId);
                     return sb.toString();
                }
            }

            StringBuilder sb = new StringBuilder();

             db.allPinsToStringBuilder(sb);
             return sb.toString();
        } finally {
            db.commitDBOperations();
        }
    }

    public void getInfo( java.io.PrintWriter printWriter ) {
        StringBuilder sb = new StringBuilder();
        sb.append("PinManager\n");
        sb.append("\tjdbcDriver=").append(jdbcDriver).append('\n');
        sb.append("\tjdbcUrl=").append(jdbcUrl).append('\n');
        sb.append("\tdbUser=").append(dbUser).append('\n');
        sb.append("\tmaxPinDuration=").
                append(maxPinDuration).append(" milliseconds \n");
        //sb.append("\tnumber of files pinned=").append(pnfsIdToPins.size());
        printWriter.println(sb.toString());

    }

   private  Collection<Pin> unconnectedPins=null;
    private void runInventoryBeforeStartPart() throws PinDBException {
        // we get all the problematic pins before the pin manager starts
        // receiving new requests

        db.initDBConnection();
        try {
            unconnectedPins=db.getAllPinsThatAreNotPinned();

        } finally {
            db.commitDBOperations();
        }
    }

    private void runInventoryAfterStartPart() throws PinDBException {

        // the rest can be done in parallel
         diskCacheV111.util.ThreadManager.execute(new Runnable() {
            public void run() {
                unpinAllInitiallyUnpinnedPins();
            }
        });
    }


    private void unpinAllInitiallyUnpinnedPins() {
        for(Pin pin:unconnectedPins) {
            forceUnpinning(pin, false);
        }
        //we do not need this anymore
        unconnectedPins = null;
    }

    private void forceUnpinning(final Pin pin, boolean retry) {
        debug("forceUnpinning "+pin);
        Collection<PinRequest> pinRequests = pin.getRequests();
        if(pinRequests.isEmpty()) {
            new Unpinner(this,pin.getPnfsId(),pin,retry);
        }
        else {
            for(PinRequest pinRequest: pinRequests) {
                try {
                    unpin(pin.getPnfsId(),pinRequest.getId(),null,null,null,true);
                } catch (Exception e) {
                    error("unpinAllInitiallyUnpinnedPins "+e);
                }
            }
        }
    }



    public void messageArrived( final CellMessage cellMessage ) {
        info("messageArrived:"+cellMessage);
         diskCacheV111.util.ThreadManager.execute(new Runnable() {
            public void run() {
                processMessage(cellMessage);
            }
        });
    }

    public void processMessage( CellMessage cellMessage ) {
        Object o = cellMessage.getMessageObject();
        if(!(o instanceof Message )) {
            super.messageArrived(cellMessage);
            return;
        }
        Message message = (Message)o ;
        try {
           info("processMessage: Message  arrived:"+o +" from "+
                   cellMessage.getSourcePath());
            if(message instanceof PinManagerPinMessage) {
                PinManagerPinMessage pinRequest =
                        (PinManagerPinMessage) message;
                pin(pinRequest, cellMessage);
            } else if(message instanceof PinManagerUnpinMessage) {
                PinManagerUnpinMessage unpinRequest =
                        (PinManagerUnpinMessage) message;
                unpin(unpinRequest, cellMessage);
            } else if(message instanceof PinManagerExtendLifetimeMessage) {
                PinManagerExtendLifetimeMessage extendLifetimeRequest =
                        (PinManagerExtendLifetimeMessage) message;
                extendLifetime(extendLifetimeRequest, cellMessage);
            } else if (message instanceof  PoolRemoveFilesMessage) {
                PoolRemoveFilesMessage removeFile =
                        (PoolRemoveFilesMessage) message;
                removeFiles(removeFile);
            } else if (message instanceof  PinManagerMovePinMessage) {
                PinManagerMovePinMessage movePin =
                        (PinManagerMovePinMessage) message;
                movePin(movePin, cellMessage);
            } else {
                error("unknown to Pin Manager message type :"+
                        message.getClass().getName()+" value: "+message);
                super.messageArrived(cellMessage);
                return;
            }
        } catch(Throwable t) {
            error(t);
            message.setFailed(-1,t);
        }
         if(  message.getReplyRequired()  ) {
            try {
                message.setReply();
                info("Reverting derection "+cellMessage);
                cellMessage.revertDirection();
                info("Sending reply "+cellMessage);
                sendMessage(cellMessage);
            } catch (Exception e) {
                error("Can't reply message : "+e);
            }
        } else {
            info("reply is not required, finished processing");
        }
   }


    public void exceptionArrived(ExceptionEvent ee) {
        error("Exception Arrived: "+ee);
        error(ee.getException().toString());
        super.exceptionArrived(ee);
    }

    private void pin(PinManagerPinMessage pinRequest, CellMessage cellMessage)
    throws PinException {

        PnfsId pnfsId = pinRequest.getPnfsId();
        if(pnfsId == null ) {
            pinRequest.setFailed(1, "pnfsId == null");
            return;
        }
        long lifetime = pinRequest.getLifetime();
        if(lifetime <=0 && lifetime != -1 )
        {

            pinRequest.setFailed(1, "lifetime ="+lifetime+" <=0");
            return;
        }
        long srmRequestId = pinRequest.getRequestId();
        String clientHost = pinRequest.getClientHost();
        AuthorizationRecord ar = pinRequest.getAuthorizationRecord();
        pin(pnfsId,
           clientHost,
           lifetime,
           srmRequestId,
           ar,
           pinRequest,
           cellMessage) ;
    }

    /**
     * this function should work with pinRequestMessage and
     * cellMessage set to null as it might be invoked by an admin command
     */
    private  void pin(PnfsId pnfsId,
        String clientHost,
        long lifetime,
        long srmRequestId,
        AuthorizationRecord authRec,
        PinManagerPinMessage pinRequestMessage, CellMessage cellMessage)
    throws PinException {

        info("pin pnfsId="+pnfsId+" lifetime="+lifetime+
            " srmRequestId="+srmRequestId);

        if(maxPinDuration != -1 && lifetime > maxPinDuration) {
            lifetime = maxPinDuration;
            info("Pin lifetime exceeded maxPinDuration, " +
                "new lifetime is set to "+lifetime);
        }
        db.initDBConnection();
        boolean changedReplyRequired = false;
        // pinRequestIdLong is also a marker that
        // we have stored a message in a table
        // if it is not null
        Long pinRequestIdLong = null;
        try {
            PinRequest pinRequest =
                db.insertPinRequestIntoNewOrExistingPin(
                    pnfsId,
                    lifetime,
                    srmRequestId,
                    authRec);
            Pin pin = pinRequest.getPin();
            info("insertPinRequestIntoNewOrExistingPin gave Pin = "+pin+
                " PinRequest= "+pinRequest);
            if(pin.getState().equals(PinManagerPinState.PINNED) ){
                // we are  done here
                // pin is pinned already
                info("pinning is already pinned");
                if( pin.getExpirationTime() == -1 ||
                    pinRequest.getExpirationTime() != -1 &&
                     pin.getExpirationTime() >= pinRequest.getExpirationTime()
                    ) {
                    // no pin lifetime extention is needed
                     if(pinRequestMessage != null) {
                        pinRequestMessage.setPinId(
                            Long.toString(pinRequest.getId()));
                     }
                     return;
                }

               info("need to extend the lifetime of the request");
               if(pinRequestMessage != null) {
                    if(pinRequestMessage.getReplyRequired()) {
                        changedReplyRequired = true;
                        pinRequestMessage.setReplyRequired(false);
                    }

                    pinRequestMessage.setPinId(
                        Long.toString(pinRequest.getId()));
               }
               db.commitDBOperations();
                new Extender(this,pin,pinRequest,cellMessage,pinRequestMessage,
                    pinRequest.getExpirationTime());
                return;
            }
            else if(pin.getState().equals(PinManagerPinState.PINNING)) {
                if(pinRequestMessage != null &&
                    pinRequestMessage.getReplyRequired() ) {
                    info("pinning is in progress");
                    info("settign setReplyRequired(false)");
                    pinRequestMessage.setReplyRequired(false);
                    pinRequestIdLong = new Long(pinRequest.getId());
                    info("putting in the  pinToRequestsMap");
                    pinToRequestsMap.put(pinRequestIdLong,cellMessage);
                    return;
                }
            }
            else if(pin.getState().equals(PinManagerPinState.INITIAL)) {
                if(pinRequestMessage != null &&
                    pinRequestMessage.getReplyRequired() ) {
                    pinRequestIdLong = new Long(pinRequest.getId());
                    pinToRequestsMap.put(pinRequestIdLong,cellMessage);
                    pinRequestMessage.setReplyRequired(false);
                }
                //start a new pinner
                StorageInfo storageInfo = null;
                if(pinRequestMessage != null) {
                    storageInfo = pinRequestMessage.getStorageInfo();
                }
                db.updatePin(pin.getId(),null,null,PinManagerPinState.PINNING);
                db.commitDBOperations();
                // we need to commit the new state before we start
                // processing
                //otherwise we might not see the request in database
                // if processing succeeds before the commit is executed
                // (a race condition )

                int allowedStates = RequestContainerV5.allStatesExceptStage;
                try {
                    allowedStates = _checkStagePermission.canPerformStaging(authRec.getName(), authRec.getVoRole()) ? RequestContainerV5.allStates : RequestContainerV5.allStatesExceptStage;
                } catch (PatternSyntaxException ex) {
                    error("failed to get allowed pool manager states: " + ex);
                } catch (IOException ex) {
                    error("failed to get allowed pool manager states: " + ex);
                }
                new Pinner(this, pnfsId,clientHost, storageInfo, pin,
                   lifetime, pinRequest.getId(), allowedStates);
            } else {
                info("pin returned is in the wrong state");
                if(pinRequestMessage != null) {
                    failResponse("pin returned is in the wrong state",3,
                            pinRequestMessage);
                }
            }

        } catch (PinDBException pdbe ) {
            db.rollbackDBOperations();
            if(pinRequestMessage != null) {
                failResponse(pdbe,3,pinRequestMessage);
                if(pinRequestIdLong != null) {
                    pinRequestMessage.setReplyRequired(true);
                    pinToRequestsMap.remove(pinRequestIdLong);
                }
            }
        }
        finally {
           db.commitDBOperations();
        }
    }


    public void pinSucceeded ( Pin pin ,
        String pool,
        long expiration,
        long originalPinRequestId) throws PinException {
        boolean success = true;
        String error =null;
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {
            pin = db.getPinForUpdate(pin.getId());
            pinRequests = pin.getRequests();
            if(pin.getState().equals(PinManagerPinState.PINNING)) {

                db.updatePin(pin.getId(),expiration,pool,PinManagerPinState.PINNED);
            } else if(pin.getState().equals(PinManagerPinState.INITIAL)){
                 //weird but ok, we probably will not get here,
                // but let us still change state to Pinned and notify of success
                db.updatePin(pin.getId(),expiration,pool,PinManagerPinState.PINNED);
            } else if(pin.getState().equals(PinManagerPinState.PINNED)){
                //weird but ok, we probably will not get here,
                // but let us still notify of success
                db.updatePin(pin.getId(),expiration,pool,PinManagerPinState.PINNED);
            } else if(pin.getState().equals(PinManagerPinState.EXPIRED)) {
                success = false;
                error = "expired before we could finish pinning";
            } else if(pin.getState().equals(PinManagerPinState.UNPINNING)) {
                success = false;
                error = "unpinning started";
            } else {
                success = false;
                error = "unknown";
            }

            for(PinRequest pinRequest:pinRequests) {
                if(pinRequest.getId() ==originalPinRequestId ) {
                    if(pinRequest.getExpirationTime() < expiration) {
                        db.updatePinRequest(pinRequest.getId(), expiration);
                    }
                }
                CellMessage envelope =
                        pinToRequestsMap.remove(pinRequest.getId());
                info("pinSucceeded, pin request: "+pinRequest+
                        " its cellmessage: "+envelope);
                if(envelope != null) {
                    PinManagerPinMessage pinMessage =
                        (PinManagerPinMessage)envelope.getMessageObject();
                    pinMessage.setPinId(Long.toString(pinRequest.getId()));
                    if(success) {
                        returnResponse(pinMessage,envelope);
                    } else {
                        returnFailedResponse(error,pinMessage,envelope);
                    }
                }

                 if(!success) {
                    //deleting the pin requests that
                    db.deletePinRequest(pinRequest.getId());

                }
            }
            // start unpinner if we failed to make sure that
            // the file pinned in pool is unpinnedd
            if(!success) {
                // set the state to unpinning no matter what we were
                // since this is what we are doing now)
                db.updatePin(pin.getId(),null,pool,PinManagerPinState.UNPINNING);
                db.commitDBOperations();
                // we need to commit the new state before we start
                // processing
                //otherwise we might not see the request in database
                // if processing succeeds before the commit is executed
                // (a race condition )
                new Unpinner(this,pin.getPnfsId(),pin,false);
            }
            db.commitDBOperations();
        } catch (PinDBException pdbe ) {
            error("Exception in pinSucceeded: "+pdbe);
            db.rollbackDBOperations();
        }
        finally {
            db.commitDBOperations();
        }
    }

    public void pinFailed ( Pin pin ) throws PinException {
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {
            pin = db.getPinForUpdate(pin.getId());
            pinRequests = pin.getRequests();
            for(PinRequest pinRequest:pinRequests) {
                CellMessage envelope =
                        pinToRequestsMap.remove(pinRequest.getId());
                if(envelope != null) {
                    PinManagerPinMessage pinMessage =
                        (PinManagerPinMessage)envelope.getMessageObject();
                    returnFailedResponse("Pinning failed",pinMessage,envelope);
                }
                db.deletePinRequest(pinRequest.getId());
            }
            db.deletePin(pin.getId());
            db.commitDBOperations();
        } catch (PinDBException pdbe ) {
            db.rollbackDBOperations();
        }
        finally {
            db.commitDBOperations();
        }
    }

    public void failResponse(Object reason,int rc, PinManagerMessage request ) {
        error("failResponse: "+reason);

        if(request == null  ) {
            error("can not return failed response: pinManagerMessage is null ");
            return;
        }
        if( reason != null && !(reason instanceof java.io.Serializable)) {
            reason = reason.toString();
        }

        request.setFailed(rc, reason);

    }

    public void returnFailedResponse(Object reason,
            PinManagerMessage request,CellMessage cellMessage ) {
        failResponse(reason,1,request);
        returnResponse(request,cellMessage);
    }
    public void returnResponse(
            PinManagerMessage request,CellMessage cellMessage ) {
        info("returnResponse");

        if(request == null ||cellMessage == null ) {
            error("can not return  response: pinManagerMessage is null ");
            return;
        }

        try {
            request.setReply();
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        }
        catch(Exception e) {
            error("can not send a response");
            error(e.toString());
        }
    }


    public void extendLifetime(
            PinManagerExtendLifetimeMessage extendLifetimeRequest,
        CellMessage cellMessage) throws PinException {
        String pinIdStr = extendLifetimeRequest.getPinId();
        if(pinIdStr == null) {
            returnFailedResponse("pinIdStr == null",
                    extendLifetimeRequest, cellMessage);
            return;
        }
        PnfsId pnfsId = extendLifetimeRequest.getPnfsId();
        if(pnfsId == null ) {
            returnFailedResponse("pnfsId == null",
                    extendLifetimeRequest, cellMessage);
            return;
        }
        long pinId = Long.parseLong(pinIdStr);
        long newLifetime = extendLifetimeRequest.getNewLifetime();
        extendLifetime(pnfsId, pinId,newLifetime,
                extendLifetimeRequest, cellMessage);
    }

    /**
     * this function should work with extendLifetimeRequest and
     * cellMessage set to null as it might be invoked by an admin command
     */

    public void extendLifetime(PnfsId pnfsId,
            long pinRequestId, long newLifetime,
            PinManagerExtendLifetimeMessage extendLifetimeRequest,
            CellMessage cellMessage)
            throws PinException
    {
        info("extend lifetime pnfsId="+pnfsId+" pinId="+pinRequestId+
                " new lifetime="+newLifetime);
        if(maxPinDuration !=-1 && newLifetime > maxPinDuration) {
            newLifetime = maxPinDuration;
            info("Pin newLifetime exceeded maxPinDuration, " +
                    "newLifetime is set to "+newLifetime);
        }
        db.initDBConnection();

        boolean  changedReplyRequired = false;
        try {
            Pin pin = db.getPinForUpdateByRequestId(pinRequestId);
            if(pin == null) {
                error("extend: pin request with id = "+pinRequestId+
                        " is not found");
                if(extendLifetimeRequest != null) {
                    failResponse("pin request with id = "+pinRequestId+
                        " is not found",
                        4,extendLifetimeRequest);
                }
                return;
            }

            Set<PinRequest> pinRequests = pin.getRequests();
            PinRequest pinRequest = null;
            for(PinRequest aPinRequest: pinRequests) {
                if(aPinRequest.getId() == pinRequestId) {
                    pinRequest = aPinRequest;
                    break;
                }
            }
            if(pinRequest == null) {
                error("extend: pin request with id = "+pinRequestId+
                        " is not found");
                if(extendLifetimeRequest != null) {
                    failResponse("pin request with id = "+pinRequestId+
                        " is not found",
                        4,extendLifetimeRequest);
                }
                return;
            }
            assert pinRequest != null;
            if(!pin.getState().equals(PinManagerPinState.PINNED) &&
                pin.getState().equals(PinManagerPinState.INITIAL) &&
                pin.getState().equals(PinManagerPinState.PINNING ) ) {
                error("extend: pin request with id = "+pinRequestId+
                        " is not pinned anymore");
                if(extendLifetimeRequest != null) {
                    failResponse("pin request with id = "+pinRequestId+
                        " is not pinned anymore",
                        4,extendLifetimeRequest);
                }
                return;

            }

            long expiration = pinRequest.getExpirationTime();
            if(expiration == -1) {
               // lifetime is already infinite
                info("extend: lifetime is already infinite");
               return;
            }
            long currentTime = System.currentTimeMillis();
            long remainingTime = expiration - currentTime;
            if(newLifetime != -1 && remainingTime >= newLifetime) {

               //nothing to be done here
               info( "extendLifetime: remainingTime("+extendLifetimeRequest+
                   ") >= newLifetime("+cellMessage+")");
               return;
            }
            expiration = newLifetime == -1? -1: currentTime + newLifetime;
            if(pin.getExpirationTime() == -1  ||
                ( pin.getExpirationTime() != -1 &&
                  expiration != -1 &&
                  pin.getExpirationTime() > expiration)) {
                db.updatePinRequest(pinRequest.getId(),expiration);
                info( "extendLifetime:  overall pin lifetime " +
                        "does not need extention");
                return;
            }
            info("need to extend the lifetime of the request");
            if( extendLifetimeRequest != null &&
                extendLifetimeRequest.getReplyRequired()) {
                changedReplyRequired = true;
                extendLifetimeRequest.setReplyRequired(false);
            }
            info("starting extender");
            new Extender(this,pin,pinRequest,cellMessage,extendLifetimeRequest,
                    expiration);


        } catch (PinDBException pdbe ) {
            error("extend lifetime: "+pdbe);
            db.rollbackDBOperations();
            if( extendLifetimeRequest != null ) {
                failResponse(pdbe,3,extendLifetimeRequest);
                if(changedReplyRequired) {
                    extendLifetimeRequest.setReplyRequired(true);
                }
            }
        }
        finally {
            db.commitDBOperations();
        }

    }

       public void extendSucceeded ( Pin pin,
           PinRequest pinRequest,
           PinManagerMessage extendMessage ,
           CellMessage envelope, long expiration ) throws PinException {
        info("extendSucceeded pin="+pin+" pinRequest="+pinRequest +
            " new expiration "+expiration);
        boolean success = true;
        String error =null;
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {
            pin = db.getPinForUpdate(pin.getId());
            pinRequests = pin.getRequests();

             if(!pin.getState().equals(PinManagerPinState.PINNED) &&
                pin.getState().equals(PinManagerPinState.INITIAL) &&
                pin.getState().equals(PinManagerPinState.PINNING ) ) {
                if(extendMessage != null) {
                    failResponse("pin request with id = "+pinRequest.getId()+
                            " is not pinned anymore",
                        4,extendMessage);
                }

            } else {
                if(expiration == -1) {
                    if(pinRequest.getExpirationTime() != -1) {
                        db.updatePinRequest(pinRequest.getId(),-1);
                    }
                    if(pin.getExpirationTime() != -1 ) {
                        db.updatePin(pin.getId(),new Long(-1),null,null);
                    }
                } else {
                    if( pinRequest.getExpirationTime() !=  expiration) {
                        db.updatePinRequest(pinRequest.getId(),expiration);
                    }
                    if(pin.getExpirationTime() != -1 && pin.getExpirationTime()< expiration) {
                        db.updatePin(pin.getId(),new Long(expiration),null,null);
                    }
                }
            }

            if(extendMessage != null) {
                returnResponse(extendMessage,envelope);
            }

        } catch (PinDBException pdbe ) {
            db.rollbackDBOperations();
        }
        finally {
            db.commitDBOperations();
        }
    }

    public void extendFailed ( Pin pin ,PinRequest pinRequest,
        PinManagerMessage extendMessage ,
           CellMessage envelope,Object reason) throws PinException {
        error("extenderFailed for pin "+pin+" reason="+reason);
        if(extendMessage != null) {
            returnFailedResponse(reason,extendMessage,envelope);
        }
    }


    public void unpin(PinManagerUnpinMessage unpinRequest,
            CellMessage cellMessage)
    throws PinException {
        PnfsId pnfsId = unpinRequest.getPnfsId();
        if(pnfsId == null ) {
            failResponse("pnfsId == null",1, unpinRequest);
            return;
        }
        String pinIdStr = unpinRequest.getPinId();
        Long srmRequestId = unpinRequest.getSrmRequestId();
        AuthorizationRecord authRec = unpinRequest.getAuthorizationRecord();

        if(pinIdStr == null && srmRequestId == null ) {
            unpin(pnfsId,authRec, unpinRequest, cellMessage,false);
        } else {
            unpin(pnfsId, pinIdStr,srmRequestId ,authRec, unpinRequest, cellMessage,false);
        }
    }

       /**
     * this function should work with unpinRequest and
     * cellMessage set to null as it might be invoked by an admin command
     * or by watchdog thread
     */
    public void unpin(PnfsId pnfsId,
        String pinRequestIdStr,
        Long srmRequestId,
        AuthorizationRecord authRec,
        PinManagerUnpinMessage unpinRequest,
        CellMessage cellMessage,
        boolean force)
    throws PinException {
        info("unpin pnfsId="+pnfsId+" pinId="+pinRequestIdStr+" srmRequestId="+srmRequestId);

        long pinRequestId;
        db.initDBConnection();

        try {

            if(pinRequestIdStr != null) {
                pinRequestId = Long.parseLong(pinRequestIdStr);
            } else {
                pinRequestId =
                    db.getPinRequestIdByByPnfsIdandSrmRequestId(pnfsId,srmRequestId);
            }
        } catch (PinDBException pdbe ) {
            error("unpin: "+pdbe.toString());
            db.rollbackDBOperations();
            if(unpinRequest != null) {
                failResponse(pdbe,3,unpinRequest);
            }
            return;
        }
        finally {
            db.commitDBOperations();
        }

        unpin(pnfsId,pinRequestId,authRec,unpinRequest,cellMessage,force);

    }


    public void unpin(PnfsId pnfsId,
        AuthorizationRecord authRec,
        PinManagerUnpinMessage unpinRequest,
        CellMessage cellMessage,
        boolean force)
    throws PinException {
        info("unpin all requests for pnfsId="+pnfsId);
        if(unpinRequest != null) {
            assert unpinRequest.getPinId()==null &&
                   unpinRequest.getSrmRequestId() ==null;
        }

        db.initDBConnection();
        Long pinRequestIdLong = null;

        try {
            Pin pin = db.getAndLockActivePinWithRequestsByPnfstId(pnfsId);
            if(pin == null ) {
                error("unpin: pin request for PnfsId = "+pnfsId+
                        " is not found");
                if(unpinRequest != null) {
                    failResponse("pin requests for PnfsId = "+pnfsId+
                        " is not found",
                        4,unpinRequest);
                }
                return;
            }

            if(!force &&  !pin.getState().equals(PinManagerPinState.PINNED)) {
                if (pin.getState().equals(PinManagerPinState.INITIAL) ||
                     pin.getState().equals(PinManagerPinState.PINNING)) {
                    error("unpin: in request with PnfsId = "+pnfsId+
                            " is not pinned yet");

                    if(unpinRequest != null) {
                        failResponse("pin request with PnfsId = "+pnfsId+
                                " is not pinned yet",
                            5,unpinRequest);
                    }
                    return;
                } else  {
                            error("unpin: in request with PnfsId = "+pnfsId+
                            " is not pinned yet"+
                            " or is already being upinnned");
                    if(unpinRequest != null) {
                        failResponse("pin request with PnfsId = "+pnfsId+
                                " is not pinned, " +
                            "or is already being upinnned",
                            5,unpinRequest);
                    }
                    return;

                }
            }



            Set<PinRequest> pinRequests = pin.getRequests();
            int setSize = pinRequests.size();
            boolean skippedPins = false;
            boolean unpinedAtLeastOne = false;
            int pinReqIndx = 0;
            for(PinRequest pinRequest: pinRequests) {
                long pinRequestId = pinRequest.getId();
                if(!force && !pinManagerPolicy.canUnpin(authRec,pinRequest)) {
                    skippedPins = true;
                    continue;
                }
                unpinedAtLeastOne = true;
                pinReqIndx++;
                if( pinReqIndx < setSize || skippedPins ) {
                   info("unpin: more  requests left in this pin, " +
                           "just deleting the request");
                    db.deletePinRequest(pinRequestId);
                } else{
                    if(unpinRequest != null &&
                            unpinRequest.getReplyRequired() ) {
                        pinRequestIdLong = new Long(pinRequestId);
                        pinRequestToUnpinRequestsMap.put(
                                pinRequestIdLong,cellMessage);
                        unpinRequest.setReplyRequired(false);
                    }
                    db.updatePin(pin.getId(),null,null,
                            PinManagerPinState.UNPINNING);
                    info("starting unpinnerfor request with id = "+pinRequestId);
                    db.commitDBOperations();
                    // we need to commit the new state before we start
                    // processing
                    // otherwise we might not see the request in database
                    // if processing succeeds before the commit is executed
                    // (a race condition )

                    new Unpinner(this,pin.getPnfsId(),pin,false);
                    return;
                }
            }

            if(!unpinedAtLeastOne) {
                error("unpin: pin request with  PnfsId = "+pnfsId+
                        " can not be unpinned, authorization failure");
                if(unpinRequest != null) {
                    failResponse("pin request with  PnfsId = "+pnfsId+
                        " can not be unpinned, authorization failure",
                        4,unpinRequest);
                }
                return;
            }


        } catch (PinDBException pdbe ) {
            error("unpin: "+pdbe.toString());
            db.rollbackDBOperations();
            if(unpinRequest != null) {
                failResponse(pdbe,3,unpinRequest);
                if(pinRequestIdLong != null) {
                    unpinRequest.setReplyRequired(true);
                    pinRequestToUnpinRequestsMap.remove(pinRequestIdLong);
                }
            }
            return;
        }
        finally {
            db.commitDBOperations();
        }
    }


    /**
     * this function should work with unpinRequest and
     * cellMessage set to null as it might be invoked by an admin command
     * or by watchdog thread
     */
    public void unpin(PnfsId pnfsId,
        long pinRequestId,
        AuthorizationRecord authRec,
        PinManagerUnpinMessage unpinRequest,
        CellMessage cellMessage,
        boolean force)
    throws PinException {
        info("unpin pnfsId="+pnfsId+" pinId="+pinRequestId);
        db.initDBConnection();
        Long pinRequestIdLong = null;
        try {
            Pin pin = db.getPinForUpdateByRequestId(pinRequestId);
            if(pin == null) {
                error("unpin: pin request with id = "+pinRequestId+
                        " is not found");
                if(unpinRequest != null) {
                    failResponse("pin request with id = "+pinRequestId+
                            " is not found",
                        4,unpinRequest);
                }
                return;
            }

            if(!force &&  ! pin.getState().equals(PinManagerPinState.PINNED)) {
                if (pin.getState().equals(PinManagerPinState.INITIAL) ||
                     pin.getState().equals(PinManagerPinState.PINNING)) {
                    error("unpin: in request with id = "+pinRequestId+
                            " is not pinned yet");

                    if(unpinRequest != null) {
                        failResponse("pin request with id = "+pinRequestId+
                                " is not pinned yet",
                            5,unpinRequest);
                    }
                    return;
                } else {
                            error("unpin: in request with id = "+pinRequestId+
                            " is not pinned yet"+
                            " or is already being upinnned");
                    if(unpinRequest != null) {
                        failResponse("pin request with id = "+pinRequestId+
                                " is not pinned, " +
                            "or is already being upinnned",
                            5,unpinRequest);
                    }
                    return;

                }
            }

            Set<PinRequest> pinRequests = pin.getRequests();
            PinRequest foundPinRequest = null;
            for(PinRequest pinRequest: pinRequests) {
                if(pinRequest.getId() == pinRequestId) {
                    foundPinRequest = pinRequest;
                    break;
                }
            }
            if(foundPinRequest == null) {
                error("unpin: pin request with id = "+pinRequestId+
                        " is not found");
                if(unpinRequest != null) {
                    failResponse("pin request with id = "+pinRequestId+
                            " is not found",
                        4,unpinRequest);
                }
                return;
            }
            if(!force && !pinManagerPolicy.canUnpin(authRec,foundPinRequest)){
                error("unpin: pin request with id = "+pinRequestId+
                        " can not be unpinned, authorization failure");
                if(unpinRequest != null) {
                    failResponse("pin request with id = "+pinRequestId+
                        " can not be unpinned, authorization failure",
                        4,unpinRequest);
                }
                return;

            }
            if(pinRequests.size() > 1) {
               info("unpin: more than one requests in this pin, " +
                       "just deleting the request");
               db.deletePinRequest(pinRequestId);
                // we done, just return the message to requester now
                // will be done by process message automatically
                return;
            }

            if(unpinRequest != null &&
                    unpinRequest.getReplyRequired() ) {
                pinRequestIdLong = new Long(pinRequestId);
                pinRequestToUnpinRequestsMap.put(
                        pinRequestIdLong,cellMessage);
                unpinRequest.setReplyRequired(false);
            }
            db.updatePin(pin.getId(),null,null,
                    PinManagerPinState.UNPINNING);
            info("starting unpinnerfor request with id = "+pinRequestId);
            db.commitDBOperations();
            // we need to commit the new state before we start
            // processing
            // otherwise we might not see the request in database
            // if processing succeeds before the commit is executed
            // (a race condition )

            new Unpinner(this,pin.getPnfsId(),pin,false);
            return;
        } catch (PinDBException pdbe ) {
            error("unpin: "+pdbe.toString());
            db.rollbackDBOperations();
            if(unpinRequest != null) {
                failResponse(pdbe,3,unpinRequest);
                if(pinRequestIdLong != null) {
                    unpinRequest.setReplyRequired(true);
                    pinRequestToUnpinRequestsMap.remove(pinRequestIdLong);
                }
            }
        }
        finally {
            db.commitDBOperations();
        }
    }


    public void unpinSucceeded ( Pin pin ) throws PinException {
        info("unpinSucceeded for "+pin);
        boolean success = true;
        String error =null;
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {
            pin = db.getPinForUpdate(pin.getId());
            pinRequests = pin.getRequests();
            for(PinRequest pinRequest:pinRequests) {
                // find all the pin messages, which should not be there
                CellMessage envelope =
                        pinToRequestsMap.remove(pinRequest.getId());
                if(envelope != null) {
                    PinManagerPinMessage pinMessage =
                        (PinManagerPinMessage)envelope.getMessageObject();
                    returnFailedResponse("Pinning failed, unpin has suceeded",
                            pinMessage,envelope);
                }
                // find all unpin messages and return success
                 envelope =
                        pinRequestToUnpinRequestsMap.remove(pinRequest.getId());
                if(envelope != null) {
                    PinManagerUnpinMessage unpinMessage =
                        (PinManagerUnpinMessage)envelope.getMessageObject();
                    returnResponse(unpinMessage,envelope);
                }
                 // delete all pin requests
                db.deletePinRequest(pinRequest.getId());
            }
            // delete the pin itself
            db.deletePin(pin.getId());
        } catch (PinDBException pdbe ) {
            db.rollbackDBOperations();
        }
        finally {
            db.commitDBOperations();
        }
    }

    public void unpinFailed ( Pin pin ) throws PinException {
        error("unpinFailed for "+pin);
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {
            pin = db.getPinForUpdate(pin.getId());
            pinRequests = pin.getRequests();
            for(PinRequest pinRequest:pinRequests) {
                CellMessage envelope =
                        pinToRequestsMap.remove(pinRequest.getId());
                if(envelope != null) {
                    PinManagerPinMessage pinMessage =
                        (PinManagerPinMessage)envelope.getMessageObject();
                    returnFailedResponse(
                            "Pinning failed, unpinning is in progress",
                            pinMessage,envelope);
                }
                 envelope =
                        pinRequestToUnpinRequestsMap.remove(pinRequest.getId());
                if(envelope != null) {
                    PinManagerUnpinMessage unpinMessage =
                        (PinManagerUnpinMessage)envelope.getMessageObject();
                    returnFailedResponse(
                            "Unpinning failed, unpinning will be retried",
                        unpinMessage,envelope);
                }
                db.deletePinRequest(pinRequest.getId());
            }
            db.updatePin(pin.getId(),null,null,
                    PinManagerPinState.UNPINNINGFAILED);
        } catch (PinDBException pdbe ) {
            db.rollbackDBOperations();
        }
        finally {
            db.commitDBOperations();
        }
    }



    private void retryFailedUnpinnings() throws PinDBException {
        // we get all the problematic pins before the pin manager starts
        // receiving new requests
        Collection<Pin> failedPins=null;
        db.initDBConnection();
        try {
            failedPins=db.getPinsByState(PinManagerPinState.UNPINNINGFAILED);
        } finally {
            db.commitDBOperations();
        }

        for(Pin pin: failedPins) {
            forceUnpinning(pin,true);
        }
    }

    public void expirePinRequests() throws PinException{
        Collection<PinRequest> expiredPinRequests=null;
        db.initDBConnection();
        try {
            expiredPinRequests = db.getExpiredPinRequests();


        } finally {
            db.commitDBOperations();
        }

        for(PinRequest pinRequest:expiredPinRequests) {
           debug("expiring pin request "+pinRequest);
           unpin(pinRequest.getPin().getPnfsId(),pinRequest.getId(),
               pinRequest.getAuthorizationRecord(),null,null,false);
        }

    }
   //getExpiredPinsWithoutRequests
    public void expirePinsWithoutRequests() throws PinDBException {
        Collection<Pin> expiredPins=null;
        db.initDBConnection();
        try {
            expiredPins = db.getExpiredPinsWithoutRequests();


        } finally {
            db.commitDBOperations();
        }

        for(Pin pin:expiredPins) {
            forceUnpinning(pin,false);
        }
    }

    public void run()  {
        if(Thread.currentThread() == this.expireRequests) {
            while(true)
            {
                try {
                    retryFailedUnpinnings();
                } catch(PinException pdbe) {
                    error("retryFailedUnpinnings failed: " +pdbe);
                }

                try {
                    expirePinRequests();
                } catch(PinException pdbe) {
                    error("expirePinRequests failed: " +pdbe);
                }
                try {
                    expirePinsWithoutRequests();
                } catch(PinException pdbe) {
                    error("expirePinsWithoutRequests failed: " +pdbe);
                }

                try {
                    Thread.sleep(expirationFrequency);
                }
                catch(InterruptedException ie) {
                    error("expireRequests Thread interrupted, quiting");
                    return;
                }

            }
        }
    }

    private void removeFiles(PoolRemoveFilesMessage removeFile) {
        String[] pnfsIds = removeFile.getFiles();
        if(pnfsIds == null || pnfsIds.length == 0) {
            return;
        }

        for(String pnfsIdString: pnfsIds) {
            PnfsId pnfsId = null;
            try {
              pnfsId =  new PnfsId(pnfsIdString);
            } catch (Exception e) {
                super.error("removeFiles: PoolRemoveFilesMessage has an invalid pnfsid: "+pnfsIdString);
                continue;
            }

            assert pnfsId != null;
            try {
                try {
                    db.initDBConnection();
                    Set<Pin> pins = db.allPinsByPnfsId(pnfsId);

                    for(Pin apin : pins) {
                        info(pnfsIdString+" is  deleted, removing pin request" +apin );
                        Pin pin = db.getPinForUpdate(apin.getId());// this locks the
                                                                              // the pin
                        for(PinRequest pinRequest:pin.getRequests()) {
                            db.deletePinRequest(pinRequest.getId());
                            CellMessage envelope =
                                    pinToRequestsMap.remove(pinRequest.getId());
                            info("removeFiles: found pin request: "+pinRequest+
                                    " its cellmessage: "+envelope);
                            if(envelope != null) {
                                PinManagerPinMessage pinMessage =
                                    (PinManagerPinMessage)envelope.getMessageObject();
                                pinMessage.setPinId(Long.toString(pinRequest.getId()));
                                returnFailedResponse("File Removed",pinMessage,envelope);
                            }
                        }
                        db.deletePin(pin.getId());
                    }
                } finally {
                    db.commitDBOperations();
                }
            } catch (PinDBException pdbe) {
                error(pdbe);
            }
        }
    }

     private void removeFile(String pnfsid) {
        if(pnfsid == null || pnfsid.length() ==0 ) {
            return;
        }
    }

    private void movePin(PinManagerMovePinMessage movePin,
        CellMessage envelope ) {
        PnfsId pnfsId = movePin.getPnfsId();
        String srcPool = movePin.getSourcePool();
        String dstPool = movePin.getTargetPool();
        //Collection<StickyRecord> records = movePin.getRecords();

        if(pnfsId == null ) {
            error("pnfsid is not set");
            movePin.setFailed(CacheException.INVALID_ARGS,"pnfsid is not set");
            return;
        }
        if(srcPool == null ) {
            error(" source pool is not set");
            movePin.setFailed(CacheException.INVALID_ARGS,"source pool is not set");
            return;
        }
        if(dstPool == null ) {
            error("destination pool is not set");
            movePin.setFailed(CacheException.INVALID_ARGS,"destination pool is not set");
            return;
        }

        try {
            try {
                db.initDBConnection();
                Set<Pin> pins = db.allPinsByPnfsId(pnfsId);
                Set<Pin> pinsToMove = new HashSet<Pin>();
                for(Pin srcPin : pins) {
                    if(srcPin.getState().equals(PinManagerPinState.PINNED)
                    && srcPin.getPool().equals(srcPool)) {
                        pinsToMove.add(srcPin);
                    }
                }
                if(pinsToMove.isEmpty()) {
                    error("pins for "+pnfsId+" in "+srcPool+ " in pinned state are not  found");
                    movePin.setFailed(1,"pins for "+pnfsId+" in "+srcPool+ " in pinned state are not  found");
                    return;
                }
                if(pinsToMove.size() >1) {
                    error("more than one pin found, which is not yet supported ");
                    movePin.setFailed(1,"more than one pin found, which is not yet supported ");
                    return;
                }

                for(Pin srcPin : pinsToMove) {
                        long expirationTime = srcPin.getExpirationTime();
                        info(" file "+pnfsId+" is  being moved, changing pin request" +srcPin );
                        Pin dstPin =
                            db.newPinForPinMove(pnfsId,dstPool,expirationTime);
                        movePin.setReplyRequired(false);
                        new PinMover(this,
                            pnfsId,
                            srcPin,
                            dstPin,
                            dstPool,
                            expirationTime,
                            movePin,
                             envelope);
                }
            } finally {
                db.commitDBOperations();
            }
        } catch (PinDBException pdbe) {
            error(pdbe);
        }
    }

    /**
     * this method is called after the pinning of the file in the new pool
     * is successful, but before the unpinning has begun
     * @return true is unpinning should proceed
     *         false if not
     */
    public boolean pinMoveToNewPoolPinSucceeded(
        Pin  srcPin ,
        Pin  dstPin ,
        String pool,
        long expiration,
        PinManagerMovePinMessage movePin,
        CellMessage  envelope)
        throws PinException {
        debug("pinMoveToNewPoolPinSucceeded, srcPin="+srcPin+
            " dstPin="+dstPin+
            " pool="+pool+
            " expiration="+expiration);
        boolean success = true;
        String error =null;
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {
            srcPin = db.getPinForUpdate(srcPin.getId());
            if(srcPin == null) {
                warn("src pin "+srcPin+" has been removed by the time move succeeded");

                // there are no more requests pinning the original file
                // so the target should not be pinned either
                cleanMovedStickyFlag(dstPin);

                //return succes
                returnResponse(movePin, envelope);
                return false;
            }

            if(!srcPin.getState().equals(PinManagerPinState.PINNED)) {
                    pinMoveFailed(dstPin,movePin,envelope,
                        "state of source pin has changed to "+
                        srcPin.getState() +
                        " by the time move succeeded");
                cleanMovedStickyFlag(dstPin);

                return false;
            }

            dstPin = db.getPinForUpdate(dstPin.getId());
            if(dstPin == null) {
                pinMoveFailed(dstPin,movePin,envelope,
                    "dst pin has been removed by the time move succeeded");
                return false;
            }
            if(!dstPin.getState().equals(PinManagerPinState.MOVING)) {
                pinMoveFailed(dstPin,movePin,envelope,
                    "state of destination pin has changed to "+
                    dstPin.getState() +
                    " by the time move succeeded");
                return false;
            }
            pinRequests = srcPin.getRequests();
            //new pin is pinned
            debug("change dst pin"+dstPin+" to PINNED state");

            db.updatePin(dstPin.getId(),null,null,PinManagerPinState.PINNED);

            // move the requests to the new pin
            debug("move src pin requests to dest pin");
            for(PinRequest pinRequest:pinRequests) {
                db.movePinRequest(pinRequest.getId(),dstPin.getId());
                debug("pinRequest "+pinRequest+" moved");
            }
            debug("change src pin"+srcPin+" to UNPINNING state");
            db.updatePin(srcPin.getId(),null,null,
                PinManagerPinState.UNPINNING);
         } catch (PinDBException pdbe ) {
            error("Exception in pinMoveSucceeded: "+pdbe);
            db.rollbackDBOperations();
            pinMoveFailed(dstPin,movePin,envelope,
                "Exception in pinMoveSucceeded: "+pdbe);
            db.initDBConnection();
            try {
                cleanMovedStickyFlag(dstPin);

            } catch (PinDBException pdbe1) {
                 error("Exception in cleanMovedStickyFlag: "+pdbe1);

            } finally {
                db.commitDBOperations();
            }
            return false;
         }
        finally {
            db.commitDBOperations();
        }

        //proceed to unpinnning of the src pin
        // only if the file is safely pinned in the new pool
        // and the file requests are moved to the new record

        return true;
     }

    public void pinMoveSucceeded (
        Pin  srcPin ,
        Pin  dstPin ,
        String pool,
        long expiration,
        PinManagerMovePinMessage movePin,
        CellMessage  envelope)
        throws PinException {
        debug("pinMoveSucceeded, srcPin="+srcPin+
            " dstPin="+dstPin+
            " pool="+pool+
            " expiration="+expiration);
        boolean success = true;
        String error =null;
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {
            srcPin = db.getPinForUpdate(srcPin.getId());
            if(srcPin == null) {
                warn("src pin "+srcPin+" has been removed by the time move succeeded");

                //return succes
                returnResponse(movePin, envelope);
                return;
            }
            debug("pinMoveSucceeded, deleting original pin");
            db.deletePin(srcPin.getId());
         } catch (PinDBException pdbe ) {
            error("Exception in pinMoveSucceeded: "+pdbe);
            db.rollbackDBOperations();

            //return success anyway, as the pin was unpined in the source, but
            // db update failed, pin is in unpinning state in db
            returnResponse(movePin, envelope);
            return;
        }
        finally {
            db.commitDBOperations();
        }
        //return success
        returnResponse(movePin, envelope);
    }

    private void cleanMovedStickyFlag(final Pin dstPin) throws PinDBException {
        if(dstPin == null ) {
            error("cleanMovedStickyFlag: dstPin is null");
            return;
        }
        // start removing of the sticky flag we just set
        // in the new pool
        db.updatePin(dstPin.getId(),null,null,
        PinManagerPinState.UNPINNING);
        new Unpinner(this,dstPin.getPnfsId(),dstPin,false);
    }

    public void pinMoveFailed (
        Pin  srcPin ,
        Pin  dstPin ,
        String pool,
        long expiration,
        PinManagerMovePinMessage movePin,
        CellMessage  envelope,
        Object error) throws PinException {
        error("pinMoveFailed, error="+error+" srcPin="+srcPin+
            " dstPin="+dstPin+
            " pool="+pool+
            " expiration="+expiration);

        db.initDBConnection();
        try {
            pinMoveFailed(dstPin,movePin,envelope,error);
        } catch (PinDBException pdbe ) {
            db.rollbackDBOperations();
        }
        finally {
            db.commitDBOperations();
        }
    }

    private void pinMoveFailed (
        Pin  dstPin ,
        PinManagerMovePinMessage movePin,
        CellMessage  envelope,
        Object error) throws PinException {
        returnFailedResponse(error,movePin,envelope);
        db.deletePin(dstPin.getId());
        db.commitDBOperations();
    }


}

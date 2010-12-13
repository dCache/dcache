/*
 * PinManager.java
 *
 * Created on April 28, 2004, 12:54 PM
 */

package org.dcache.services.pinmanager1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import diskCacheV111.poolManager.RequestContainerV5;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.CellEndpoint;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PinManagerMessage;
import diskCacheV111.vehicles.PinManagerPinMessage;
import diskCacheV111.vehicles.PinManagerUnpinMessage;
import diskCacheV111.vehicles.PinManagerExtendLifetimeMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.PatternSyntaxException;
import java.util.concurrent.atomic.AtomicInteger;
import org.dcache.cells.Option;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.Subjects;
import javax.security.auth.Subject;
import org.dcache.pool.repository.StickyRecord;
import diskCacheV111.vehicles.StorageInfo;

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
public class PinManager
    extends AbstractCellComponent
    implements Runnable, CellMessageReceiver, CellCommandListener
{
    private static final Logger logger =
        LoggerFactory.getLogger(PinManager.class);

    protected long expirationFrequency;

    private long maxPinDuration;

    protected CellPath pnfsManager;
    protected CellPath poolManager;

    // all database oprations will be done in the lazy
    // fassion in a low priority thread
    private Thread expireRequests;

    private PinManagerDatabase db;

    // this is the difference between the expiration time of the pin and the
    // expiration time of the sticky bit in the pool. used in case if the
    // pin exiration / removal could not unpinAllRequestForUser the file in the pool
    // (due to the pool down situation)
    protected static final long  POOL_LIFETIME_MARGIN=60*60*1000L;

    private PinManagerPolicy pinManagerPolicy;

    private Map<Long, PinManagerJob> pinRequestToJobMap =
        new ConcurrentHashMap<Long, PinManagerJob>();

    private Map<Long, PinManagerJob> pinRequestToUnpinJobMap =
        new ConcurrentHashMap<Long, PinManagerJob>();

    private static AtomicInteger nextInteractiveJobId = new AtomicInteger(0);

    private Map<Integer,InteractiveJob> interactiveJobs =
            new ConcurrentHashMap<Integer,InteractiveJob>();

    /** Tape Protection */
    protected CheckStagePermission _checkStagePermission;

    public void init()
        throws Exception
    {
        expireRequests = new Thread( "ExpireRequestsThread");
        //databaseUpdateThread.setPriority(Thread.MIN_PRIORITY);
        expireRequests.start();

        runInventoryBeforeStartPart();
    }

    @Override
    public void afterStart()
    {
        try {
            runInventoryAfterStartPart();
        } catch (PinDBException e) {
            throw new RuntimeException("Startup failed:" + e.toString(), e);
        }
    }

    public CellEndpoint getCellEndpoint()
    {
        return super.getCellEndpoint();
    }

    public void setDb(PinManagerDatabase db)
    {
        this.db = db;
    }

    public void setExpirationFrequency(long frequency)
    {
        expirationFrequency = frequency;
    }

    public void setMaxPinDuration(long duration)
    {
        maxPinDuration = duration;
    }

    public long getMaxPinDuration()
    {
        return maxPinDuration;
    }

    public void setPinManagerPolicy(PinManagerPolicy policy)
    {
        pinManagerPolicy = policy;
    }

    public void setStagePermission(CheckStagePermission checker)
    {
        _checkStagePermission = checker;
    }

    public void setPnfsManager(CellPath path)
    {
        pnfsManager = path;
    }

    public CellPath getPnfsManager()
    {
        return pnfsManager;
    }

    public void setPoolManager(CellPath path)
    {
        poolManager = path;
    }

    public CellPath getPoolManager()
    {
        return poolManager;
    }

    public final static String hh_pin_pnfsid = "<pnfsId> <seconds> " +
        "# pin a file by pnfsid for <seconds> seconds" ;
    public String ac_pin_pnfsid_$_2( Args args ) throws Exception {
        PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
        long lifetime = Long.parseLong( args.argv(1) ) ;
        if(lifetime != -1) {
            lifetime *=1000;
        }
        PinManagerJobImpl job =
                new PinManagerJobImpl(PinManagerJobType.PIN,
                null,null,pnfsId,lifetime,null,null,null,null);
        pin(job);
        int id = nextInteractiveJobId.incrementAndGet();
        interactiveJobs.put(id,job);
        return "pin started: id="+id+", Job ="+job;
    }


    public  final static String hh_bulk_pin = "<file> <seconds> # pin pnfsids from <file> for <seconds>";
    public String fh_bulk_pin =
        "pin a list of pnfsids from a file for a specified number of seconds\n"+
        "read a list of pnfsids to pin from a file\n"+
        "each line in a file is a pnfsid\n";
    public Object ac_bulk_pin_$_2( final Args args ) throws Exception {
        final DelayedReply reply = new DelayedReply();
        Thread t = new Thread("bulk pin") {
                @Override
                public void run() {
                    try {
                        String fileName = args.argv(0) ;
                        long lifetime = Long.parseLong( args.argv(1) ) ;
                        if(lifetime != -1) {
                            lifetime *=1000;
                        }
                        File file = new File(fileName);
                        if(!file.canRead()) {
                            reply.send("file "+fileName+" can not be read");
                            return;
                        }
                        BulkPinJob pinJob = new BulkPinJob(fileName, lifetime);
                        int id = nextInteractiveJobId.incrementAndGet();
                        interactiveJobs.put(id,pinJob);
                        reply.send( "pin started, bulk job id="+id);
                    } catch (NoRouteToCellException nrtce) {
                        logger.error("bulk pin cell communication failure:  "+nrtce.getMessage());
                    } catch (IOException ioe) {
                        logger.warn("bulk pinning failed due to IO error: " +
                                ioe.getMessage());
                        try {
                            reply.send("bulk pinning failed due to IO error: " +
                                ioe.getMessage());
                        }catch(Exception e) {
                            logger.error(e.toString());
                        }
                    } catch (InterruptedException ie) {
                        logger.warn("bulk pin interrupted: "+ie.getMessage());
                    }
                }
            };
        t.start();

        return reply;

    }

    public  final static String hh_bulk_unpin = " [-force] <file> # unpin pnfsids from <file>";
    public String fh_bulk_unpin =
        "unpin a list of pnfsids from a file\n"+
        "read a list of pnfsids to pin from a file\n"+
        "each line in a file is a pnfsid\n";
    public Object ac_bulk_unpin_$_1( final Args args ) throws Exception {
       final DelayedReply reply = new DelayedReply();
        Thread t = new Thread("bulk unpin") {
                @Override
                public void run() {
                    try {
                        boolean force = args.getOpt("force") != null;
                        String fileName = args.argv(0) ;
                        File file = new File(fileName);
                        if(!file.canRead()) {
                            reply.send("file "+fileName+" can not be read");
                            return;
                        }
                        BulkUnpinJob unpinJob = new BulkUnpinJob(fileName, force);
                        int id = nextInteractiveJobId.incrementAndGet();
                        interactiveJobs.put(id,unpinJob);
                        reply.send( "bulk unpin started, bulk job id="+id);
                    } catch (NoRouteToCellException nrtce) {
                        logger.error("bulk unpin cell communication failure:  "+nrtce.getMessage());
                    } catch (IOException ioe) {
                        logger.warn("bulk unpinning failed due to IO error: " +
                                ioe.getMessage());
                        try {
                            reply.send("bulk unpinning failed due to IO error: " +
                                ioe.getMessage());
                        }catch(Exception e) {
                            logger.error(e.toString());
                        }
                    } catch (InterruptedException ie) {
                        logger.warn("bulk unpin interrupted: "+ie.getMessage());
                    }
                }
            };
        t.start();

        return reply;

    }

    public final static String hh_unpin = " [-force] [<pinRequestId>] <pnfsId> " +
        "# unpin a a file by pinRequestId and by pnfsId or just by pnfsId" ;
    public String ac_unpin_$_1_2( Args args ) throws Exception {
        boolean force = args.getOpt("force") != null;
        PnfsId pnfsId;
        Long pinRequestId = null;
        if(args.argc() == 1) {
            pnfsId = new PnfsId( args.argv(0) ) ;
            PinManagerJobImpl job =
                new PinManagerJobImpl(PinManagerJobType.UNPIN,
                null,null,pnfsId,0,null,null,null,null);
            unpinAllRequestForUser(job,force);
            Integer id = nextInteractiveJobId.incrementAndGet();
            interactiveJobs.put(id,job);
            return "unpin started: id="+id+", Job="+job;
        }
        pinRequestId = Long.parseLong(args.argv(0));
        pnfsId = new PnfsId( args.argv(1) ) ;
        PinManagerJobImpl job =
                new PinManagerJobImpl(PinManagerJobType.UNPIN,
                null,null,pnfsId,0,null,null,null,null);
        job.setPinRequestId(pinRequestId);
        unpin(job,force);
        Integer id = nextInteractiveJobId.incrementAndGet();
        interactiveJobs.put(id,job);
        return "unpin started: id="+id+", Job="+job;

    }

    public  final static String hh_extend_lifetime = "<pinRequestId> <pnfsId> <seconds " +
        "# extendlifetime of a pin  by pinRequestId and by pnfsId" ;
    public String ac_extend_lifetime_$_3( Args args ) throws Exception {
        long pinRequestId = Long.parseLong(args.argv(0));
        PnfsId pnfsId = new PnfsId( args.argv(1) ) ;
        long lifetime = Long.parseLong( args.argv(2) ) ;
        lifetime *=1000;
        PinManagerJobImpl job =
                new PinManagerJobImpl(PinManagerJobType.EXTEND_LIFETIME,
                null,null,pnfsId,lifetime,null,null,null,null);
        job.setPinRequestId(pinRequestId);
        extendLifetime(job);
        int id = nextInteractiveJobId.incrementAndGet();
        interactiveJobs.put(id,job);
        return "extend lifetime started: id="+id+", Job="+job;
    }


    public  final static String hh_set_max_pin_duration =
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
    public final static String hh_get_max_pin_duration =
            " # gets current max pin duration value" ;
    public String ac_get_max_pin_duration_$_0( Args args ) throws Exception {

        return Long.toString(maxPinDuration)+" milliseconds";
    }

    public final static String hh_ls = " [id|pnfsId] # lists all pins or a specified pin by request id or pnfsid" ;
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

   public final static String fh_jobs_clear =
        "Removes completed interactive jobs. For reference, information about\n" +
        "interactive jobs is kept until explicitly cleared.\n";
    public String ac_jobs_clear(Args args)
    {
        Iterator<InteractiveJob> i = interactiveJobs.values().iterator();
        while (i.hasNext()) {
            InteractiveJob job = i.next();
            switch (job.getState()) {
            case COMPLETED:
                i.remove();
                break;
            default:
                break;
            }
        }
        return "";
    }

    public final static String hh_jobs_ls = "[<id1> [ ... [<idN>]]] # list all or specified jobs";
    public final static String fh_jobs_ls =
        "Lists all or specified or pin manager jobs.";
    public String ac_jobs_ls_$_0_99(Args args)
    {
        StringBuilder sb = new StringBuilder();
        if(args.argc() == 0 ) {
            for (Map.Entry<Integer,InteractiveJob> e:  interactiveJobs.entrySet()) {
                sb.append(e.getKey()).append(':');
                sb.append(e.getValue()).append('\n');
            }
        } else {
            for(int i = 0 ; i<args.argc(); ++i) {
                try {
                    int id = Integer.parseInt(args.argv(i));
                    sb.append(id).append(':');
                    InteractiveJob job = interactiveJobs.get(id);
                    if(job != null) {
                        sb.append(job).append('\n');
                    } else {
                        sb.append("id not found").append('\n');
                    }
                } catch( NumberFormatException nfe) {
                    sb.append(args.argv(i)).append(':');
                    sb.append("Illegal id").append('\n');
                }
            }
        }
        return sb.toString();
    }

    @Override
    public void getInfo( java.io.PrintWriter printWriter ) {
        StringBuilder sb = new StringBuilder();
        sb.append("PinManager\n");
        // sb.append("\tjdbcDriver=").append(jdbcDriver).append('\n');
        // sb.append("\tjdbcUrl=").append(jdbcUrl).append('\n');
        // sb.append("\tdbUser=").append(dbUser).append('\n');
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
        new Thread(new Runnable() {
                public void run() {
                    unpinAllInitiallyUnpinnedPins();
                }
            }, getCellName() + "-init").start();
    }


    private void unpinAllInitiallyUnpinnedPins() {
        for(Pin pin:unconnectedPins) {
            forceUnpinning(pin, false);
        }
        //we do not need this anymore
        unconnectedPins = null;
    }

    private void forceUnpinning(final Pin pin, boolean retry) {
        logger.debug("forceUnpinning "+pin);
        Collection<PinRequest> pinRequests = pin.getRequests();
        if(pinRequests.isEmpty()) {
            PinManagerJob job =
                new PinManagerJobImpl(PinManagerJobType.UNPIN,
                    null,
                    null,
                    pin.getPnfsId(),
                    0,
                    null,
                    null,
                    null,
                    null);
            new Unpinner(this,job,pin,retry);
        }
        else {
            for(PinRequest pinRequest: pinRequests) {
                try {
                    PinManagerJob job =
                            new PinManagerJobImpl(PinManagerJobType.UNPIN,
                            null,null,pin.getPnfsId(),0,null,null,null,null);
                    job.setPinRequestId(pinRequest.getId());
                    unpin(job,true);
                } catch (Exception e) {
                    logger.error("unpinAllInitiallyUnpinnedPins "+e);
                }
            }
        }
    }

    public void messageArrived(CellMessage envelope,
                               PinManagerPinMessage message)
        throws PinException
    {
        pin(message, envelope);
    }

    public void messageArrived(CellMessage envelope,
                               PinManagerUnpinMessage message)
        throws PinException
    {
        unpin(message, envelope);
    }

    public void messageArrived(CellMessage envelope,
                               PinManagerExtendLifetimeMessage message)
        throws PinException
    {
        extendLifetime(message, envelope);
    }

    public void messageArrived(PoolRemoveFilesMessage message)
    {
        removeFiles(message);
    }

    public void messageArrived(CellMessage envelope,
                               PinManagerMovePinMessage message)
    {
        movePin(message, envelope);
    }

    private void pin(PinManagerPinMessage pinRequest, CellMessage cellMessage)
    throws PinException {

        PnfsId pnfsId = pinRequest.getPnfsId();
        long lifetime = pinRequest.getLifetime();
        PinManagerJobImpl job =
                new PinManagerJobImpl(PinManagerJobType.PIN,
                cellMessage,
                pinRequest,
                pnfsId,
                lifetime,
                pinRequest.getAuthorizationRecord(),
                pinRequest.getRequestId(),
                pinRequest.getClientHost(),
                pinRequest.getStorageInfo());
        if(pnfsId == null ) {
            job.returnFailedResponse( "pnfsId == null");
            return;
        }
        if(lifetime <=0 && lifetime != -1 )
        {

            job.returnFailedResponse( "lifetime ="+lifetime+" <=0");
            return;
        }


        pin(job) ;
    }

    /**
     * this function should work with pinRequestMessage and
     * cellMessage set to null as it might be invoked by an admin command
     */
    private  void pin(PinManagerJobImpl job)
    throws PinException {
         assert(job.getType()==PinManagerJobType.PIN);
        logger.info("pin pnfsId="+job.getPnfsId()+" lifetime="+job.getLifetime()+
            " srmRequestId="+job.getSrmRequestId());

        if(getMaxPinDuration() != -1 && job.getLifetime() > getMaxPinDuration()) {
            job.setLifetime( getMaxPinDuration());
            logger.info("Pin lifetime exceeded maxPinDuration, " +
                "new lifetime is set to "+job.getLifetime());
        }
        db.initDBConnection();
        try {
            PinRequest pinRequest =
                db.insertPinRequestIntoNewOrExistingPin(
                    job.getPnfsId(),
                    job.getLifetime(),
                    job.getSrmRequestId(),
                    job.getAuthorizationRecord());
            Pin pin = pinRequest.getPin();
            logger.info("insertPinRequestIntoNewOrExistingPin gave Pin = "+pin+
                " PinRequest= "+pinRequest);
            job.setPinRequestId(pinRequest.getId());
            if(pin.getState().equals(PinManagerPinState.PINNED) ){
                // we are  done here
                // pin is pinned already
                logger.info("pinning is already pinned");
                if( pin.getExpirationTime() == -1 ||
                    pinRequest.getExpirationTime() != -1 &&
                     pin.getExpirationTime() >= pinRequest.getExpirationTime()
                    ) {
                    job.returnResponse();// no pin lifetime extention is needed
                    return;
                }

               logger.info("need to extend the lifetime of the request");
               db.commitDBOperations();
                new Extender(this,pin,pinRequest,
                        job,
                    pinRequest.getExpirationTime());
                return;
            }
            else if(pin.getState().equals(PinManagerPinState.PINNING)) {
                logger.info("pinning is in progress, store this request in pinRequestToJobMap");
                pinRequestToJobMap.put(job.getPinRequestId(),job);
                return;
            }
            else if(pin.getState().equals(PinManagerPinState.INITIAL)) {
                logger.info("pinning will begin, store this request in pinRequestToJobMap");
                pinRequestToJobMap.put(pinRequest.getId(),job);

                //start a new pinner
                db.updatePin(pin.getId(),null,null,PinManagerPinState.PINNING);
                db.commitDBOperations();
                // we need to commit the new state before we start
                // processing
                //otherwise we might not see the request in database
                // if processing succeeds before the commit is executed
                // (a race condition )

                // if the job.getAuthorizationRecord() is null the
                // request is comming from the admin interface
                // and staging should be allowed for this case
                // so the default value for allowedStates should be
                //
                int allowedStates = RequestContainerV5.allStates;
                try {
                     allowedStates =
                             _checkStagePermission.canPerformStaging(job.getSubject(), job.storageInfo) ?
                                 RequestContainerV5.allStates :
                                 RequestContainerV5.allStatesExceptStage;
                } catch (PatternSyntaxException ex) {
                     logger.error("failed to get allowed pool manager states: " + ex);
                } catch (IOException ex) {
                     logger.error("failed to get allowed pool manager states: " + ex);
                }
                new Pinner(this, job, pin,
                   pinRequest.getId(), allowedStates);
            } else {
                job.returnFailedResponse("pin returned is in the wrong state");
            }

        } catch (PinDBException pdbe ) {
            db.rollbackDBOperations();
            job.returnFailedResponse(pdbe);
            Long pinRequestIdLong = job.getPinRequestId();
            if(pinRequestIdLong != null) {
                pinRequestToJobMap.remove(pinRequestIdLong);
            }
        }
        finally {
           db.commitDBOperations();
        }
    }


    /**
     * This Method moves all pin requests for pins for the same pnfsid
     * in REPINNING or ERROR state to this pin
     *  This method is called from the pinSucceeded method
     * where db transaction has been aready started.
     * The state of pin in REPINNING or ERROR state, once it is stripped of all
     * Pin Requests is changed to the UNPINNING state
     * so that the pin manager keeps on trying to remove the assosiated
     * sticky flag, when the pool where pin once existed is back online
     * @param pin a new pin
     * @throws org.dcache.services.pinmanager1.PinDBException
     */
    private void moveRepinningPinRequestsIntoNewPin(Pin pin) throws PinDBException {

        for (Pin apin : db.allPinsByPnfsId(pin.getPnfsId())) {
             if (apin.getState()     == PinManagerPinState.REPINNING ||
                    apin.getState() == PinManagerPinState.ERROR) {
                apin = db.getPinForUpdate(apin.getId());
                for (PinRequest pinRequest : apin.getRequests()) {
                    db.movePinRequest(pinRequest.getId(), pin.getId());
                    PinManagerJob job = pinRequestToJobMap.remove(pinRequest.getId());
                    if (job != null) {
                        job.returnResponse();
                    }
                }
                db.updatePin(apin.getId(), null, null, PinManagerPinState.UNPINNINGFAILED);
            }
        }
    }


    /**
     * This method is called by either when pinning attempt has failed
     *  This method is called from a  method
     * where db transaction has been aready started.
     * The method finds all the pins in the REPINNING state
     * and changes their state to ERROR
     * It makes all the pending requests that are assosiated with this pin
     * to fail.
     * The pin requests that are assumed to have succeeded already are preserved.
     * <p>
     * Once the request is in ERROR state, there will be further attempts to
     * repin it done periodically
     *
     * @param pin
     * @param error
     * @throws org.dcache.services.pinmanager1.PinDBException
     */
    private void errorRepinRequests(Pin pin, Object error) throws PinDBException {
        for (Pin apin : db.allPinsByPnfsId(pin.getPnfsId())) {
            if (apin.getId() == pin.getId()) {
                // we do not
                continue;
            }
            if (apin.getState() == PinManagerPinState.REPINNING) {
                apin = db.getPinForUpdate(apin.getId());
                for (PinRequest pinRequest : apin.getRequests()) {
                    PinManagerJob job = pinRequestToJobMap.remove(pinRequest.getId());
                    if (job != null) {
                        job.returnFailedResponse("original pinned copy is not accessible, " + "repinning attemt failed:" + error);
                    }
                    // the pending job we found could have been either extend
                    // or a pin request
                    if(job.getType() == PinManagerJobType.PIN) {
                       // we returned failure for this pin request
                       // no need to keep it
                       db.deletePinRequest(pinRequest.getId());
                    }
                }
                // We will keep on trying to repin this pin
                db.updatePin(apin.getId(), null, null, PinManagerPinState.ERROR);
            }
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
                error = "state is "+pin.getState();
            }

            if(success) {
                moveRepinningPinRequestsIntoNewPin(pin);
                // update the pin so it picks
                // up all the pin requests that are moved from the
                // old request
                pin = db.getPinForUpdate(pin.getId());
            } else {
                errorRepinRequests(pin, error);

            }

            for(PinRequest pinRequest:pinRequests) {
                if(pinRequest.getId() ==originalPinRequestId ) {
                    if(pinRequest.getExpirationTime() < expiration) {
                        db.updatePinRequest(pinRequest.getId(), expiration);
                    }
                }
                PinManagerJob job =
                        pinRequestToJobMap.remove(pinRequest.getId());
                if(job != null) {
                    if(success) {
                        job.returnResponse();
                    } else {
                        job.returnFailedResponse(error);
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
                PinManagerJob job =
                    new PinManagerJobImpl(PinManagerJobType.UNPIN,
                        null,
                        null,
                        pin.getPnfsId(),
                        0,
                        null,
                        null,
                        null,
                        null);
                new Unpinner(this,job,pin,false);
            }
            db.commitDBOperations();
        } catch (PinDBException pdbe ) {
            logger.error("Exception in pinSucceeded: "+pdbe);
            db.rollbackDBOperations();
        }
        finally {
            db.commitDBOperations();
        }
    }

    public void pinFailed ( Pin pin, Object reason ) throws PinException {
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {

            //If there are repin requests
            // fail them
            errorRepinRequests(pin, reason);

            pin = db.getPinForUpdate(pin.getId());
            pinRequests = pin.getRequests();
            for(PinRequest pinRequest:pinRequests) {
                PinManagerJob job =
                        pinRequestToJobMap.remove(pinRequest.getId());
                if(job != null) {
                    job.returnFailedResponse("Pinning failed: "+reason);
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
        logger.error("failResponse: "+reason);

        if(request == null  ) {
            logger.error("can not return failed response: pinManagerMessage is null ");
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
        logger.info("returnResponse");

        if(request == null ||cellMessage == null ) {
            logger.error("can not return  response: pinManagerMessage is null ");
            return;
        }

        try {
            request.setReply();
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        }
        catch(Exception e) {
            logger.error("can not send a response");
            logger.error(e.toString());
        }
    }


    public void extendLifetime(
            PinManagerExtendLifetimeMessage extendLifetimeRequest,
        CellMessage cellMessage) throws PinException {
        String pinRequestIdStr = extendLifetimeRequest.getPinRequestId();
        PnfsId pnfsId = extendLifetimeRequest.getPnfsId();
        long pinRequestId = Long.parseLong(pinRequestIdStr);
        long newLifetime = extendLifetimeRequest.getNewLifetime();

        PinManagerJobImpl job =
            new PinManagerJobImpl(PinManagerJobType.EXTEND_LIFETIME,
                cellMessage,
                extendLifetimeRequest,
                pnfsId,
                extendLifetimeRequest.getNewLifetime(),
                extendLifetimeRequest.getAuthorizationRecord(),
                null,
                null,
                null);
        if(pinRequestIdStr == null) {
            job.returnFailedResponse("pinRequestIdStr == null");
            return;
        }
        if(pnfsId == null ) {
            job.returnFailedResponse("pnfsId == null");
            return;
        }
        job.setPinRequestId(pinRequestId);

        extendLifetime(job);
    }

    /**
     * this function should work with extendLifetimeRequest and
     * cellMessage set to null as it might be invoked by an admin command
     */

    private void extendLifetime(PinManagerJobImpl job)
            throws PinException
    {
        logger.info("extend lifetime pnfsId="+job.getPnfsId()+" pinRequestId="+job.getPinRequestId()+
                " new lifetime="+job.getLifetime());
        if(getMaxPinDuration() !=-1 && job.getLifetime() > getMaxPinDuration()) {
            job.setLifetime(getMaxPinDuration());
            logger.info("Pin newLifetime exceeded maxPinDuration, " +
                    "newLifetime is set to "+job.getLifetime() );
        }
        db.initDBConnection();

        boolean  changedReplyRequired = false;
        try {
            Pin pin = db.getPinForUpdateByRequestId(job.getPinRequestId());
            if(pin == null) {
                job.returnFailedResponse("extend: pin request with id = "+job.getPinRequestId()+
                        " is not found");
                return;
            }

            Set<PinRequest> pinRequests = pin.getRequests();
            PinRequest pinRequest = null;
            for(PinRequest aPinRequest: pinRequests) {
                if(aPinRequest.getId() == job.getPinRequestId()) {
                    pinRequest = aPinRequest;
                    break;
                }
            }

            if(pinRequest == null) {
                job.returnFailedResponse("extend: pin request with id = "+job.getPinRequestId()+
                        " is not found");
                return;
            }
            assert pinRequest != null;
            if(!pin.getState().equals(PinManagerPinState.PINNED) &&
                pin.getState().equals(PinManagerPinState.INITIAL) &&
                pin.getState().equals(PinManagerPinState.PINNING ) ) {
                job.returnFailedResponse("extend: pin request with id = "+job.getPinRequestId()+
                        " is not pinned anymore");
                return;
            }

            long expiration = pinRequest.getExpirationTime();
            if(expiration == -1) {
               // lifetime is already infinite
                logger.info("extend: lifetime is already infinite");
                job.returnResponse();
               return;
            }
            long currentTime = System.currentTimeMillis();
            long remainingTime = expiration - currentTime;
            if(job.getLifetime() != -1 && remainingTime >= job.getLifetime()) {

               //nothing to be done here
               logger.info( "extendLifetime: remainingTime("+remainingTime+
                   ") >= newLifetime("+job.getLifetime()+")");
               job.returnResponse();
               return;
            }
            expiration = job.getLifetime() == -1? -1: currentTime + job.getLifetime();
            if(pin.getExpirationTime() == -1  ||
                ( pin.getExpirationTime() != -1 &&
                  expiration != -1 &&
                  pin.getExpirationTime() > expiration)) {
                db.updatePinRequest(pinRequest.getId(),expiration);
                logger.info( "extendLifetime:  overall pin lifetime " +
                        "does not need extention");
                job.returnResponse();
                return;
            }
            logger.info("need to extend the lifetime of the request");
            logger.info("starting extender");
            new Extender(this,pin,pinRequest,job,
                    expiration);
        } catch (PinDBException pdbe ) {
            logger.error("extend lifetime: "+pdbe);
            db.rollbackDBOperations();
            job.returnFailedResponse(pdbe);
        }
        finally {
            db.commitDBOperations();
        }

    }

       public void extendSucceeded ( Pin pin,
           PinRequest pinRequest,
           PinManagerJob extendJob ,
           long expiration ) throws PinException {
        logger.info("extendSucceeded pin="+pin+" pinRequest="+pinRequest +
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
                extendJob.returnFailedResponse("pin request with id = "+pinRequest.getId()+
                            " is not pinned anymore");

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
                extendJob.returnResponse();
            }
        } catch (PinDBException pdbe ) {
            db.rollbackDBOperations();
        }
        finally {
            db.commitDBOperations();
        }
    }

    public void extendFailed ( Pin pin ,PinRequest pinRequest,
        PinManagerJob extendJob  ,
           Object reason) throws PinException {
        // extend failed - pool is not available
        //make pin manager attempt to pin file in a new location

        repin(extendJob,pin);
    }

    /**
     * If the operation on already pinned replica fails,most likely failed due
     * to the pinned replica or pool inavailability, PinManager does not fail
     * this operation, but tries to pin a file again, which could lead to the
     * file being pinned in a new pool.
     * <p>
     * If pinning in a new pool succeeds, all pin requests associated with this
     * pin is moved to the new pin.
     * <p>
     * If pinning in a new pool fails, the pending requests on this pin are
     * failed, and the requests that are reported to have succeed are preserved,
     * and the pin will be attempted to be repinned over and over again
     *
     * @param job this is the job that lead to the operation on the
     * pin that failed.
     *
     * @param pin
     * @throws org.dcache.services.pinmanager1.PinException
     */
    public void repin(PinManagerJob job,Pin pin) throws PinException {
        db.initDBConnection();
        try {
            Pin oldPin = db.getPinForUpdate(pin.getId());
            if(oldPin.getState() != PinManagerPinState.PINNED) {
                throw new PinException("Pin "+oldPin+"is not pinned");
            }
            db.updatePin(oldPin.getId(), expirationFrequency,null,
                    PinManagerPinState.REPINNING);
            Pin newPin =
                    db.newPinForRepin(oldPin.getPnfsId(),
                    oldPin.getExpirationTime());
            long lifetime = oldPin.getExpirationTime() == -1 ? -1 :
                oldPin.getExpirationTime() - System.currentTimeMillis();

            PinManagerJobImpl newPinJob =
                new PinManagerJobImpl(PinManagerJobType.PIN,
                null,null,oldPin.getPnfsId(),lifetime,null,null,null,null);
            //save the extend job on pinRequestToJobMap
            // if the new pinning succeeds, the extend job will
            // receive a success notification
            // if it fails, it will cause the extend request to fail
            if(job != null && job.getPinRequestId() != null) {
                pinRequestToJobMap.put(job.getPinRequestId(),job);
            }
            new Pinner(this, newPinJob, newPin,0, RequestContainerV5.allStates);

        } catch (PinDBException pdbe ) {
            logger.error("repinFile: "+pdbe.toString());
            db.rollbackDBOperations();
            return;
        }
        finally {
            db.commitDBOperations();
        }
    }

    public void unpin(PinManagerUnpinMessage unpinRequest,
            CellMessage cellMessage)
    throws PinException {
        PnfsId pnfsId = unpinRequest.getPnfsId();
        Long srmRequestId = unpinRequest.getSrmRequestId();
        AuthorizationRecord authRec = unpinRequest.getAuthorizationRecord();
        PinManagerJob job =
                new PinManagerJobImpl(PinManagerJobType.UNPIN,
                cellMessage,
                unpinRequest,
                pnfsId,
                0,
                unpinRequest.getAuthorizationRecord(),
                unpinRequest.getSrmRequestId(),
                null,null);
        if(pnfsId == null ) {
            job.returnFailedResponse("pnfsId == null");
            return;
        }
        Long pinRequestId = null;
        if(unpinRequest.getPinRequestId() != null){
            pinRequestId = Long.parseLong(unpinRequest.getPinRequestId());
        }

        job.setPinRequestId(pinRequestId);

        if(pinRequestId == null && srmRequestId == null ) {
            unpinAllRequestForUser(job,false);
        } else {
            unpin(job,false);
        }
    }



    public void unpinAllRequestForUser(PinManagerJob job,
        boolean force)
    throws PinException {
        logger.info("unpin all requests for pnfsId="+job.getPnfsId());
        assert job.getPinId()==null &&
               job.getSrmRequestId() ==0;

        db.initDBConnection();
        Long pinRequestIdLong = null;

        try {
            Pin pin = db.getAndLockActivePinWithRequestsByPnfstId(job.getPnfsId());
            if(pin == null ) {
                job.returnFailedResponse("unpin: pin requests for PnfsId = "+job.getPnfsId()+
                        " is not found");
                return;
            }

            if(!force &&  !pin.getState().equals(PinManagerPinState.PINNED)) {
                if (pin.getState().equals(PinManagerPinState.INITIAL) ||
                     pin.getState().equals(PinManagerPinState.PINNING)) {
                   job.returnFailedResponse("unpin: pin request with PnfsId = "+job.getPnfsId()+
                                " is not pinned yet");
                    return;
                } else  {
                    job.returnFailedResponse("unpin: pin request with PnfsId = "+job.getPnfsId()+
                                " is not pinned, " +
                            "or is already being upinnned");
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
                if(!force && !pinManagerPolicy.canUnpin(job.getAuthorizationRecord(),pinRequest)) {
                    skippedPins = true;
                    continue;
                }
                unpinedAtLeastOne = true;
                pinReqIndx++;
                if( pinReqIndx < setSize || skippedPins ) {
                   logger.info("unpin: more  requests left in this pin, " +
                           "just deleting the request");
                    db.deletePinRequest(pinRequestId);
                } else{

                    pinRequestIdLong = pinRequestId;
                    pinRequestToUnpinJobMap.put(
                            pinRequestIdLong,job);
                    db.updatePin(pin.getId(),null,null,
                            PinManagerPinState.UNPINNING);
                    logger.info("starting unpinnerfor request with id = "+pinRequestId);
                    db.commitDBOperations();
                    // we need to commit the new state before we start
                    // processing
                    // otherwise we might not see the request in database
                    // if processing succeeds before the commit is executed
                    // (a race condition )

                    PinManagerJob unpinJob =
                        new PinManagerJobImpl(PinManagerJobType.UNPIN,
                            null,
                            null,
                            pin.getPnfsId(),
                            0,
                            null,
                            null,
                            null,
                            null);
                    new Unpinner(this,unpinJob,pin,false);
                    return;
                }
            }

            if(!unpinedAtLeastOne) {
                job.returnFailedResponse("pin request with  PnfsId = "+job.getPnfsId()+
                        " can not be unpinned, authorization failure");
                return;
            }


        } catch (PinDBException pdbe ) {
            logger.error("unpin: "+pdbe.toString());
            db.rollbackDBOperations();
            job.returnFailedResponse(pdbe);
            if(pinRequestIdLong != null) {
                pinRequestToUnpinJobMap.remove(pinRequestIdLong);
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
    public void unpin(PinManagerJob job,
        boolean force)
    throws PinException {
        logger.info("unpin pnfsId="+job.getPnfsId()+
                " pinRequestId="+job.getPinRequestId()+
                " srmRequestId="+job.getSrmRequestId());

        if(job.getPinRequestId() == null) {
            db.initDBConnection();

            try {
                job.setPinRequestId(
                    db.getPinRequestIdByByPnfsIdandSrmRequestId(
                        job.getPnfsId(),job.getSrmRequestId()));
            } catch (PinDBException pdbe ) {
                logger.error("unpin: "+pdbe.toString());
                db.rollbackDBOperations();
                job.returnFailedResponse(pdbe);
                return;
            }
            finally {
                db.commitDBOperations();
            }
        }

        db.initDBConnection();
        Long pinRequestIdLong = null;
        try {
            Pin pin = db.getPinForUpdateByRequestId(job.getPinRequestId());
            if(pin == null) {
                job.returnFailedResponse("unpin: pin request with id = "+
                        job.getPinRequestId()+ " is not found");
                return;
            }

            if(!force &&  ! pin.getState().equals(PinManagerPinState.PINNED)) {
                if (pin.getState().equals(PinManagerPinState.INITIAL) ||
                     pin.getState().equals(PinManagerPinState.PINNING)) {
                    job.returnFailedResponse("pin request with id = "+job.getPinRequestId()+
                                " is not pinned yet");

                    return;
                } else {
                    job.returnFailedResponse("pin request with id = "+job.getPinRequestId()+
                                " is not pinned, " +
                            "or is already being upinnned");
                    return;

                }
            }

            Set<PinRequest> pinRequests = pin.getRequests();
            PinRequest foundPinRequest = null;
            for(PinRequest pinRequest: pinRequests) {
                if(pinRequest.getId() == job.getPinRequestId()) {
                    foundPinRequest = pinRequest;
                    break;
                }
            }
            if(foundPinRequest == null) {
                job.returnFailedResponse("pin request with id = "+job.getPinRequestId()+
                            " is not found");
                return;
            }
            if(!force && !pinManagerPolicy.canUnpin(job.getAuthorizationRecord(),foundPinRequest)){
                job.returnFailedResponse("pin request with id = "+job.getPinRequestId()+
                        " can not be unpinned, authorization failure");
                return;

            }
            if(pinRequests.size() > 1) {
               logger.info("unpin: more than one requests in this pin, " +
                       "just deleting the request");
               db.deletePinRequest(job.getPinRequestId());
               job.returnResponse();
                return;
            }

            pinRequestIdLong = new Long(job.getPinRequestId());

            pinRequestToUnpinJobMap.put(
                    pinRequestIdLong,job);
            db.updatePin(pin.getId(),null,null,
                    PinManagerPinState.UNPINNING);
            logger.info("starting unpinner for request with id = "+job.getPinRequestId());
            db.commitDBOperations();
            // we need to commit the new state before we start
            // processing
            // otherwise we might not see the request in database
            // if processing succeeds before the commit is executed
            // (a race condition )
            PinManagerJob unpinJob =
                new PinManagerJobImpl(PinManagerJobType.UNPIN,
                    null,
                    null,
                    pin.getPnfsId(),
                    0,
                    null,
                    null,
                    null,
                    null);
            new Unpinner(this,unpinJob,pin,false);
            return;
        } catch (PinDBException pdbe ) {
            logger.error("unpin: "+pdbe.toString());
            db.rollbackDBOperations();
            job.returnFailedResponse(pdbe);
            if(job.getPinManagerMessage()  != null) {

                if(pinRequestIdLong != null) {
                    pinRequestToUnpinJobMap.remove(pinRequestIdLong);
                }
            }
        }
        finally {
            db.commitDBOperations();
        }
    }


    public void unpinSucceeded ( Pin pin ) throws PinException {
        logger.info("unpinSucceeded for "+pin);
        boolean success = true;
        String error =null;
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {
            pin = db.getPinForUpdate(pin.getId());
            pinRequests = pin.getRequests();
            for(PinRequest pinRequest:pinRequests) {
                // find all the pin messages, which should not be there
                PinManagerJob job =
                        pinRequestToJobMap.remove(pinRequest.getId());
                if(job != null) {
                    job.returnFailedResponse("Pinning failed, unpin has suceeded");
                }
                // find all unpinAllRequestForUser messages and return success
                PinManagerJob unpinjob =
                        pinRequestToUnpinJobMap.remove(pinRequest.getId());
                if(unpinjob != null) {
                    unpinjob.returnResponse();
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
        logger.error("unpinFailed for "+pin);
        Set<PinRequest> pinRequests ;
        db.initDBConnection();
        try {
            pin = db.getPinForUpdate(pin.getId());
            pinRequests = pin.getRequests();
            for(PinRequest pinRequest:pinRequests) {
                PinManagerJob job =
                        pinRequestToJobMap.remove(pinRequest.getId());
                if(job != null) {
                    job.returnFailedResponse(
                                "Pinning failed, unpinning is in progress");
                }

                PinManagerJob unpinjob =
                        pinRequestToUnpinJobMap.remove(pinRequest.getId());
                if(unpinjob != null) {
                    unpinjob.returnFailedResponse(
                                "Unpinning failed, unpinning will be retried");
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

    private void repinErrorPins() throws PinException {
        // we get all the problematic pins before the pin manager starts
        // receiving new requests
        Collection<Pin> errorPins=null;
        db.initDBConnection();
        try {
            errorPins=db.getPinsByState(PinManagerPinState.ERROR);
        } finally {
            db.commitDBOperations();
        }

        for(Pin pin: errorPins) {
            repin(null, pin);
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
           logger.debug("expiring pin request "+pinRequest);
           PinManagerJob job =
                   new PinManagerJobImpl(PinManagerJobType.UNPIN,
                   null,null,pinRequest.getPin().getPnfsId(),
                   0,pinRequest.getAuthorizationRecord(),null,null,null);
           job.setPinRequestId(pinRequest.getId());
           unpin(job,true);
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
                    logger.error("retryFailedUnpinnings failed: " +pdbe);
                }

                try {
                    expirePinRequests();
                } catch(PinException pdbe) {
                    logger.error("expirePinRequests failed: " +pdbe);
                }
                try {
                    expirePinsWithoutRequests();
                } catch(PinException pdbe) {
                    logger.error("expirePinsWithoutRequests failed: " +pdbe);
                }

                try {
                    repinErrorPins();
                } catch(PinException pdbe) {
                    logger.error("repinErrorPins failed: " +pdbe);
                }

                try {
                    Thread.sleep(expirationFrequency);
                }
                catch(InterruptedException ie) {
                    logger.error("expireRequests Thread interrupted, quiting");
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
                logger.error("removeFiles: PoolRemoveFilesMessage has an invalid pnfsid: "+pnfsIdString);
                continue;
            }

            assert pnfsId != null;
            try {
                try {
                    db.initDBConnection();
                    Set<Pin> pins = db.allPinsByPnfsId(pnfsId);

                    for(Pin apin : pins) {
                        logger.info(pnfsIdString+" is  deleted, removing pin request" +apin );
                        Pin pin = db.getPinForUpdate(apin.getId());// this locks the
                                                                              // the pin
                        for(PinRequest pinRequest:pin.getRequests()) {
                            db.deletePinRequest(pinRequest.getId());
                            PinManagerJob job =
                                    pinRequestToJobMap.remove(pinRequest.getId());
                            if(job != null) {
                                job.returnFailedResponse("File Removed");
                            }
                        }
                        db.deletePin(pin.getId());
                    }
                } finally {
                    db.commitDBOperations();
                }
            } catch (PinDBException pdbe) {
                logger.error(pdbe.toString());
            }
        }
    }

    private void cleanPoolStickyBitsUnknownToPinManager(
            PinManagerMovePinMessage movePin
         ) throws NoRouteToCellException
    {
        PnfsId pnfsId = movePin.getPnfsId();
        String srcPool = movePin.getSourcePool();
        Collection<StickyRecord> records = movePin.getRecords();
        for(StickyRecord record:records) {
            String stickyBitName = record.owner();
            if(stickyBitName.startsWith(getCellName())) {
                PoolSetStickyMessage setStickyRequest =
                   new PoolSetStickyMessage(srcPool,
                        pnfsId, false,stickyBitName,-1);
                setStickyRequest.setReplyRequired(false);

                this.sendMessage(
                        new CellMessage(new CellPath(srcPool),
                        setStickyRequest));
            }
        }

    }

    private void movePin(PinManagerMovePinMessage movePin,
        CellMessage envelope ) {
        PnfsId pnfsId = movePin.getPnfsId();
        String srcPool = movePin.getSourcePool();
        String dstPool = movePin.getTargetPool();

        if(pnfsId == null ) {
            logger.error("pnfsid is not set");
            movePin.setFailed(CacheException.INVALID_ARGS,"pnfsid is not set");
            returnResponse(movePin, envelope);
            return;
        }
        if(srcPool == null ) {
            logger.error(" source pool is not set");
            movePin.setFailed(CacheException.INVALID_ARGS,"source pool is not set");
            returnResponse(movePin, envelope);
            return;
        }
        if(dstPool == null ) {
            logger.error("destination pool is not set");
            movePin.setFailed(CacheException.INVALID_ARGS,"destination pool is not set");
            returnResponse(movePin, envelope);
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
                    logger.warn("pins for "+pnfsId+" in "+srcPool+
                            " in pinned state are not  found," +
                            " removing sticky flags");
                    try {
                        cleanPoolStickyBitsUnknownToPinManager(movePin);
                    } catch(NoRouteToCellException nrtce) {
                        logger.error(nrtce.toString());
                    }
                    movePin.setFailed(1,"pins for "+pnfsId+" in "+srcPool+ " in pinned state are not  found");
                    returnResponse(movePin, envelope);
                    return;
                }
                if(pinsToMove.size() >1) {
                    logger.error("more than one pin found, which is not yet supported ");
                    movePin.setFailed(1,"more than one pin found, which is not yet supported ");
                    returnResponse(movePin, envelope);
                    return;
                }

                for(Pin srcPin : pinsToMove) {
                        long expirationTime = srcPin.getExpirationTime();
                        logger.info(" file "+pnfsId+" is  being moved, changing pin request" +srcPin );
                        Pin dstPin =
                            db.newPinForPinMove(pnfsId,dstPool,expirationTime);
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
            logger.error(pdbe.toString());
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
        logger.debug("pinMoveToNewPoolPinSucceeded, srcPin="+srcPin+
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
                logger.warn("src pin "+srcPin+" has been removed by the time move succeeded");

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
            logger.debug("change dst pin"+dstPin+" to PINNED state");

            db.updatePin(dstPin.getId(),null,null,PinManagerPinState.PINNED);

            // move the requests to the new pin
            logger.debug("move src pin requests to dest pin");
            for(PinRequest pinRequest:pinRequests) {
                db.movePinRequest(pinRequest.getId(),dstPin.getId());
                logger.debug("pinRequest "+pinRequest+" moved");
            }
            logger.debug("change src pin"+srcPin+" to UNPINNING state");
            db.updatePin(srcPin.getId(),null,null,
                PinManagerPinState.UNPINNING);
         } catch (PinDBException pdbe ) {
            logger.error("Exception in pinMoveSucceeded: "+pdbe);
            db.rollbackDBOperations();
            pinMoveFailed(dstPin,movePin,envelope,
                "Exception in pinMoveSucceeded: "+pdbe);
            db.initDBConnection();
            try {
                cleanMovedStickyFlag(dstPin);

            } catch (PinDBException pdbe1) {
                 logger.error("Exception in cleanMovedStickyFlag: "+pdbe1);

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
        logger.debug("pinMoveSucceeded, srcPin="+srcPin+
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
                logger.warn("src pin "+srcPin+" has been removed by the time move succeeded");

                //return succes
                returnResponse(movePin, envelope);
                return;
            }
            logger.debug("pinMoveSucceeded, deleting original pin");
            db.deletePin(srcPin.getId());
         } catch (PinDBException pdbe ) {
            logger.error("Exception in pinMoveSucceeded: "+pdbe);
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
            logger.error("cleanMovedStickyFlag: dstPin is null");
            return;
        }
        // start removing of the sticky flag we just set
        // in the new pool
        db.updatePin(dstPin.getId(),null,null,
        PinManagerPinState.UNPINNING);
        PinManagerJob unpinJob =
            new PinManagerJobImpl(PinManagerJobType.UNPIN,
                null,
                null,
                dstPin.getPnfsId(),
                0,
                null,
                null,
                null,
                null);
        new Unpinner(this,unpinJob,dstPin,false);
    }

    public void pinMoveFailed (
        Pin  srcPin ,
        Pin  dstPin ,
        String pool,
        long expiration,
        PinManagerMovePinMessage movePin,
        CellMessage  envelope,
        Object error) throws PinException {
        logger.error("pinMoveFailed, error="+error+" srcPin="+srcPin+
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

    private enum PinManagerJobState {
        ACTIVE,
        COMPLETED
    }

    private interface InteractiveJob
    {
        public PinManagerJobState getState();
    }

    private class PinManagerJobImpl implements PinManagerJob, InteractiveJob {

        private final PinManagerJobType type;
        private PinManagerJobState state = PinManagerJobState.ACTIVE;
        private Long pinId;
        private Long pinRequestId;
        private CellMessage cellMessage;
        private PinManagerMessage pinManagerMessage;
        private String pnfsPath;
        private PnfsId pnfsId;
        private long lifetime;
        private final AuthorizationRecord authRecord;
        private final long srmRequestId;
        private final String clientHost ;
        private StorageInfo storageInfo;
        private Object errorObject;
        private int errorCode;
        private SMCTask task;

        public PinManagerJobImpl(PinManagerJobType type,
                CellMessage cellMessage,
                PinManagerMessage pinManagerMessage,
                PnfsId pnfsId,
                long lifetime,
                AuthorizationRecord authRecord,
                Long srmRequestId,
                String clientHost,
                StorageInfo storageInfo) {
            this.type = type;
            //do not even store the message, if the reply is not required
            if(pinManagerMessage != null && pinManagerMessage.getReplyRequired()) {
                this.cellMessage =  cellMessage ;
                this.pinManagerMessage = pinManagerMessage;
            }
            this.pnfsId = pnfsId;
            this.lifetime = lifetime;
            this.authRecord = authRecord;
            if(srmRequestId != null) {
                this.srmRequestId = srmRequestId;
            } else {
                this.srmRequestId = 0;
            }
            this.clientHost =  clientHost;
            this.storageInfo = storageInfo;
        }
        /**
         * @return the type
         */
        public PinManagerJobType getType() {
            return type;
        }

        /**
         * @return the cellMessage
         */
        public CellMessage getCellMessage() {
            return cellMessage;
        }

        /**
         * @return the pinManagerMessage
         */
        public PinManagerMessage getPinManagerMessage() {
            return pinManagerMessage;
        }

        /**
         * @return the pnfsPath
         */
        public String getPnfsPath() {
            return pnfsPath;
        }

        /**
         * @param pnfsPath the pnfsPath to set
         */
        public void setPnfsPath(String pnfsPath) {
            this.pnfsPath = pnfsPath;
        }

        /**
         * @return the pnfsId
         */
        public PnfsId getPnfsId() {
            return pnfsId;
        }

        /**
         * @param pnfsId the pnfsId to set
         */
        public void setPnfsId(PnfsId pnfsId) {
            this.pnfsId = pnfsId;
        }

        /**
         * @return the authRecord
         */
        public AuthorizationRecord getAuthorizationRecord() {
            return authRecord;
        }

        /**
         * @return the subject
         */
        public Subject getSubject(){
            if(authRecord == null) {
                return Subjects.ROOT ;
            }
            return Subjects.getSubject(authRecord);
        }



        /**
         * @return the srmRequestId
         */
        public long getSrmRequestId() {
            return srmRequestId;
        }

        /**
         * @return the clientHost
         */
        public String getClientHost() {
            return clientHost;
        }

        /**
         * @return the lifetime
         */
        public long getLifetime() {
            return lifetime;
        }

        /**
         * @param lifetime the lifetime to set
         */
        public void setLifetime(long lifetime) {
            this.lifetime = lifetime;
        }

        /**
         * @return the storageIInfo
         */
        public StorageInfo getStorageInfo() {
            return storageInfo;
        }

        /**
         * @param storageIInfo the storageIInfo to set
         */
        public void setStorageInfo(StorageInfo storageIInfo) {
            this.storageInfo = storageIInfo;
        }

        /**
         * @return the pinId
         */
        public Long getPinId() {
            return pinId;
        }

        /**
         * @param pinId the pinId to set
         */
        public void setPinId(Long pinId) {
            this.pinId = pinId;
        }

        /**
         * @return the pinRequestId
         */
        public Long getPinRequestId() {
            return pinRequestId;
        }

        /**
         * @param pinRequestId the pinRequestId to set
         */
        public void setPinRequestId(Long pinRequestId) {
            this.pinRequestId = pinRequestId;
            if(pinManagerMessage != null && pinRequestId != null) {
                pinManagerMessage.setPinRequestId(pinRequestId.toString());
            }
        }

        public void failResponse(Object reason,int rc ) {
            logger.error("failResponse: "+reason+" rc="+rc);
            this.errorObject = reason;
            this.errorCode = rc;
            if(  pinManagerMessage != null  ) {
                if( reason != null && !(reason instanceof java.io.Serializable)) {
                    reason = reason.toString();
                }

                pinManagerMessage.setFailed(rc, reason);
            }

        }

        public void returnFailedResponse(Object reason) {
            failResponse(reason,1);
            returnResponse();
        }

        public void returnResponse( ) {
            logger.info("returnResponse");
            state = PinManagerJobState.COMPLETED;
            if(pinManagerMessage != null &&
               cellMessage != null       &&
               pinManagerMessage.getReplyRequired()) {

                try {
                    pinManagerMessage.setReply();
                    cellMessage.revertDirection();
                    sendMessage(cellMessage);

                    //return response only once
                    pinManagerMessage = null;
                    cellMessage = null;
                }
                catch(NoRouteToCellException nrtce) {
                    logger.error("can not send a response, no route to cell "+cellMessage.getDestinationAddress());
                    logger.error(nrtce.getMessage());
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(type);
            sb.append(' ').append(pnfsId);
            sb.append(" PinRequestId:").append(pinRequestId);
            sb.append(" PinId:").append(pinId);
            sb.append(" lifetime:").append(lifetime);
            if(task != null) sb.append(" smc:").append(task);
            sb.append(" state:").append(state);
            if(errorCode != 0) {
                sb.append(" rc:").append(errorCode);
                sb.append(' ').append(errorObject);
            }
            return sb.toString();
        }

        public void setSMCTask(SMCTask task) {
            this.task = task;
        }

        public PinManagerJobState getState() {
            return state;
        }
    }

    private class BulkPinJob implements InteractiveJob  {

        private PinManagerJobState state = PinManagerJobState.ACTIVE;
        Map<Integer,PinManagerJobImpl> jobs =
                new HashMap<Integer,PinManagerJobImpl>();
        BulkPinJob(String fileName, long lifetime ) throws IOException {
            FileReader fr = new FileReader(fileName);
            BufferedReader reader = new BufferedReader(fr);
            try {
                String line;
                while((line = reader.readLine()) != null) {
                    line = line.trim();
                    if(line.isEmpty() || line.startsWith("#")) continue;
                    PnfsId pnfsId = new PnfsId(line);
                    PinManagerJobImpl job =
                        new PinManagerJobImpl(PinManagerJobType.PIN,
                        null,null,pnfsId,lifetime,null,null,null,null);
                    Integer id= nextInteractiveJobId.incrementAndGet();
                    jobs.put(id,job);
                    interactiveJobs.put(id, job);
                }
            } finally {
                reader.close();
            }
            for(PinManagerJobImpl job:jobs.values()) {
                try {
                    pin(job);
                } catch (PinException pinException) {
                    job.returnFailedResponse(pinException);
                }
            }

        }

        public PinManagerJobState getState() {
            if(state == PinManagerJobState.COMPLETED) {
                return state;
            }
            //state is active
            for(PinManagerJobImpl job:jobs.values()) {
                //there is at least one active job,
                // leave state as active
                if(job.getState() ==  PinManagerJobState.ACTIVE) {
                    return state;
                }
            }
            //no active job found
            return state =  PinManagerJobState.COMPLETED;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            //state is active
            sb.append("BulkPinJob ").append(getState());
            sb.append('\n');

            for (Map.Entry<Integer,PinManagerJobImpl> e:  jobs.entrySet()) {
                sb.append(' ').append(e.getKey()).append(':');
                sb.append(e.getValue()).append('\n');
            }
            return sb.toString();
        }
    }

    private class BulkUnpinJob implements InteractiveJob  {

        private PinManagerJobState state = PinManagerJobState.ACTIVE;
        Map<Integer,PinManagerJobImpl> jobs =
                new HashMap<Integer,PinManagerJobImpl>();
        BulkUnpinJob(String fileName, boolean force) throws IOException {
            FileReader fr = new FileReader(fileName);
            BufferedReader reader = new BufferedReader(fr);
            try {
                String line;
                while((line = reader.readLine()) != null) {
                    line = line.trim();
                    if(line.isEmpty() || line.startsWith("#")) continue;
                    PnfsId pnfsId = new PnfsId(line);

                    PinManagerJobImpl job =
                        new PinManagerJobImpl(PinManagerJobType.UNPIN,
                        null,null,pnfsId,0,null,null,null,null);
                    Integer id= nextInteractiveJobId.incrementAndGet();
                    jobs.put(id,job);
                    interactiveJobs.put(id, job);
                }
            } finally {
                reader.close();
            }
            for(PinManagerJobImpl job:jobs.values()) {
                try {
                    unpin(job,force);
                } catch (PinException pinException) {
                    job.returnFailedResponse(pinException);
                }
            }

        }

        public PinManagerJobState getState() {
            if(state == PinManagerJobState.COMPLETED) {
                return state;
            }
            //state is active
            for(PinManagerJobImpl job:jobs.values()) {
                //there is at least one active job,
                // leave state as active
                if(job.getState() ==  PinManagerJobState.ACTIVE) {
                    return state;
                }
            }
            //no active job found
            return state =  PinManagerJobState.COMPLETED;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            //state is active
            sb.append("BulkUnpinJob ").append(getState());
            sb.append('\n');

            for (Map.Entry<Integer,PinManagerJobImpl> e:  jobs.entrySet()) {
                sb.append(' ').append(e.getKey()).append(':');
                sb.append(e.getValue()).append('\n');
            }
            return sb.toString();
        }
    }
}

package org.dcache.services.pinmanager1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import diskCacheV111.poolManager.RequestContainerV5;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.CellEndpoint;
import diskCacheV111.vehicles.Message;
import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.pinmanager.PinManagerExtendPinMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.InvalidMessageCacheException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.PatternSyntaxException;
import org.dcache.cells.Option;
import org.dcache.cells.CellStub;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.MessageReply;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.Subjects;
import javax.security.auth.Subject;
import org.dcache.pool.repository.StickyRecord;
import diskCacheV111.vehicles.StorageInfo;
import org.springframework.beans.factory.annotation.Required;

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
    implements Runnable
{
    private static final Logger logger =
        LoggerFactory.getLogger(PinManager.class);

    private long expirationFrequency;

    private long maxPinDuration;

    private CellStub _pnfsManagerStub;
    private CellStub _poolManagerStub;
    private CellStub _poolStub;

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

    private final Map<Long, PinManagerJob<?>> pinRequestToJobMap =
        new ConcurrentHashMap<Long, PinManagerJob<?>>();

    private final Map<Long, PinManagerJob<?>> pinRequestToUnpinJobMap =
        new ConcurrentHashMap<Long, PinManagerJob<?>>();

    private Collection<Pin> unconnectedPins;

    protected CheckStagePermission _checkStagePermission;

    public void init()
        throws PinDBException
    {
        expireRequests = new Thread(this, "ExpireRequestsThread");
        //databaseUpdateThread.setPriority(Thread.MIN_PRIORITY);
        expireRequests.start();

        runInventoryBeforeStartPart();
    }

    @Override
    public void afterStart()
    {
        runInventoryAfterStartPart();
    }

    @Override
    public CellEndpoint getCellEndpoint()
    {
        return super.getCellEndpoint();
    }

    @Required
    public void setDb(PinManagerDatabase db)
    {
        this.db = db;
    }

    @Required
    public void setExpirationFrequency(long frequency)
    {
        expirationFrequency = frequency;
    }

    @Required
    public void setMaxPinDuration(long duration)
    {
        maxPinDuration = duration;
    }

    @Required
    public long getMaxPinDuration()
    {
        return maxPinDuration;
    }

    @Required
    public void setPinManagerPolicy(PinManagerPolicy policy)
    {
        pinManagerPolicy = policy;
    }

    @Required
    public void setStagePermission(CheckStagePermission checker)
    {
        _checkStagePermission = checker;
    }

    @Required
    public void setPnfsManagerStub(CellStub stub)
    {
        _pnfsManagerStub = stub;
    }

    @Required
    public void setPoolManagerStub(CellStub stub)
    {
        _poolManagerStub = stub;
    }

    @Required
    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    @Override
    public void getInfo(PrintWriter printWriter)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("PinManager\n");
        sb.append("\tmaxPinDuration=").
                append(maxPinDuration).append(" milliseconds \n");
        // sb.append("\tnumber of files pinned=").append(pnfsIdToPins.size());
        printWriter.println(sb.toString());

    }

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

    private void runInventoryAfterStartPart()
    {
        // the rest can be done in parallel
        new Thread(getCellName() + "-init") {
            public void run() {
                unpinAllInitiallyUnpinnedPins();
            }
        }.start();
    }

    public InteractiveJob pin(PnfsId pnfsId, long lifetime)
        throws CacheException
    {
        PinManagerJobImpl<PinManagerPinMessage> job =
            new PinManagerJobImpl<PinManagerPinMessage>(PinManagerJobType.PIN, pnfsId,
                                                        lifetime);
        pin(job);
        return job;
    }

    public InteractiveJob unpin(PnfsId pnfsId, boolean force)
        throws CacheException
    {
        PinManagerJobImpl<PinManagerUnpinMessage> job =
            new PinManagerJobImpl<PinManagerUnpinMessage>(PinManagerJobType.UNPIN,
                                                          pnfsId,0);
        unpinAllRequestForUser(job, force);
        return job;
    }

    public InteractiveJob unpin(PnfsId pnfsId, long requestId, boolean force)
        throws CacheException
    {
        PinManagerJobImpl<PinManagerUnpinMessage> job =
            new PinManagerJobImpl<PinManagerUnpinMessage>(PinManagerJobType.UNPIN,
                                                          pnfsId,0);
        job.setPinRequestId(requestId);
        unpin(job,force);
        return job;
    }

    public InteractiveJob extendLifetime(PnfsId pnfsId, long requestId, long lifetime)
        throws CacheException
    {
        PinManagerJobImpl<PinManagerExtendPinMessage> job =
            new PinManagerJobImpl<PinManagerExtendPinMessage>(PinManagerJobType.EXTEND_LIFETIME,
                                                     pnfsId,lifetime);
        job.setPinRequestId(requestId);
        extendLifetime(job);
        return job;
    }

    public String list()
        throws PinDBException
    {
        db.initDBConnection();
        try {
            StringBuilder sb = new StringBuilder();
            db.allPinsToStringBuilder(sb);
            return sb.toString();
        } finally {
            db.commitDBOperations();
        }
    }

    public String list(PnfsId pnfsId)
        throws PinDBException
    {
        db.initDBConnection();
        try {
            StringBuilder sb = new StringBuilder();
            db.allPinsByPnfsIdToStringBuilder(sb, pnfsId);
            return sb.toString();
        } finally {
            db.commitDBOperations();
        }
    }

    public String list(long id)
        throws PinDBException
    {
        db.initDBConnection();
        try {
            Pin pin = db.getPin(id);
            StringBuilder sb = new StringBuilder();
            sb.append(pin);
            sb.append("\n  pinRequests: \n");
            for (PinRequest pinRequest: pin.getRequests()) {
                sb.append("  ").append(pinRequest).append('\n');
            }
            return sb.toString();
        } finally {
            db.commitDBOperations();
        }
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
            PinManagerJob<PinManagerUnpinMessage> job =
                new PinManagerJobImpl<PinManagerUnpinMessage>(PinManagerJobType.UNPIN,
                    pin.getPnfsId(),
                    0);
            new Unpinner(this,job,pin,retry, _pnfsManagerStub, _poolStub);
        }
        else {
            for(PinRequest pinRequest: pinRequests) {
                try {
                    PinManagerJobImpl<PinManagerUnpinMessage> job =
                        new PinManagerJobImpl<PinManagerUnpinMessage>(PinManagerJobType.UNPIN,
                            pin.getPnfsId(),0);
                    job.setPinRequestId(pinRequest.getId());
                    unpin(job,true);
                } catch (Exception e) {
                    logger.error("unpinAllInitiallyUnpinnedPins "+e);
                }
            }
        }
    }

    public MessageReply<PinManagerPinMessage> pin(PinManagerPinMessage pinRequest)
        throws CacheException
    {
        PnfsId pnfsId = pinRequest.getPnfsId();
        long lifetime = pinRequest.getLifetime();
        if (pnfsId == null ) {
            throw new InvalidMessageCacheException("PnfsId is null");
        }
        if (lifetime <=0 && lifetime != -1 ) {
            throw new InvalidMessageCacheException("Invalid lifetime");
        }
        String requestId = pinRequest.getRequestId();
        PinManagerJobImpl<PinManagerPinMessage> job =
            new PinManagerJobImpl(PinManagerJobType.PIN,
                                  pinRequest,
                                  pnfsId,
                                  lifetime,
                                  (requestId == null) ? null : Long.parseLong(requestId));
        pin(job);
        return job;
    }

    /**
     * This method MUST work with pinRequestMessage set to null as
     * it might be invoked by an admin command.
     */
    private void pin(PinManagerJobImpl<PinManagerPinMessage> job)
        throws PinException
    {
        assert(job.getType()==PinManagerJobType.PIN);
        logger.info("pin pnfsId={} lifetime={} srmRequestId={}",
                    new Object[] { job.getPnfsId(), job.getLifetime(), job.getSrmRequestId() });

        if(getMaxPinDuration() != -1 && job.getLifetime() > getMaxPinDuration()) {
            job.setLifetime( getMaxPinDuration());
            logger.info("Pin lifetime exceeded maxPinDuration, " +
                        "new lifetime is set to {}", job.getLifetime());
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
            if (job.getMessage() != null) {
                job.getMessage().setPinId(pinRequest.getId());
            }
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
                             pinRequest.getExpirationTime(),
                             _poolStub);
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
                    StorageInfo storageInfo =
                        (job.getMessage() == null) ? null : job.getMessage().getStorageInfo();
                    allowedStates =
                        _checkStagePermission.canPerformStaging(job.getSubject(), storageInfo) ?
                        RequestContainerV5.allStates :
                        RequestContainerV5.allStatesExceptStage;
                } catch (PatternSyntaxException ex) {
                     logger.error("failed to get allowed pool manager states: " + ex);
                } catch (IOException ex) {
                     logger.error("failed to get allowed pool manager states: " + ex);
                }
                new Pinner(this, job, pin,
                           pinRequest.getId(), allowedStates,
                           _pnfsManagerStub, _poolManagerStub, _poolStub);
            } else {
                throw new PinException(1, "Pin returned is in the wrong state");
            }
        } catch (PinException e) {
            db.rollbackDBOperations();
            Long pinRequestIdLong = job.getPinRequestId();
            if(pinRequestIdLong != null) {
                pinRequestToJobMap.remove(pinRequestIdLong);
            }
            throw e;
        } finally {
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
                    PinManagerJob<?> job = pinRequestToJobMap.remove(pinRequest.getId());
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
    private void errorRepinRequests(Pin pin, Object error)
        throws PinDBException
    {
        for (Pin apin : db.allPinsByPnfsId(pin.getPnfsId())) {
            if (apin.getId() == pin.getId()) {
                // we do not
                continue;
            }
            if (apin.getState() == PinManagerPinState.REPINNING) {
                apin = db.getPinForUpdate(apin.getId());
                for (PinRequest pinRequest : apin.getRequests()) {
                    PinManagerJob<?> job = pinRequestToJobMap.remove(pinRequest.getId());
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
                PinManagerJob<?> job =
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
                PinManagerJob<PinManagerUnpinMessage> job =
                    new PinManagerJobImpl<PinManagerUnpinMessage>(PinManagerJobType.UNPIN,
                        pin.getPnfsId(),
                        0);
                new Unpinner(this,job,pin,false, _pnfsManagerStub, _poolStub);
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
                PinManagerJob<?> job =
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

    public <T extends Message>
                      void returnFailedResponse(Object reason, T request,
                                                MessageReply<T> reply)
    {
        logger.error("failResponse: "+reason);

        if (request == null) {
            logger.error("can not return failed response: pinManagerMessage is null ");
            return;
        }
        if (reason != null && !(reason instanceof java.io.Serializable)) {
            reason = reason.toString();
        }

        request.setFailed(1, reason);
        returnResponse(request,reply);
    }

    public <T extends Message>
                      void returnResponse(T request, MessageReply<T> reply)
    {
        if (request == null) {
            logger.error("can not return  response: pinManagerMessage is null");
            return;
        }
        reply.reply(request);
    }


    public MessageReply<PinManagerExtendPinMessage> extendLifetime(
            PinManagerExtendPinMessage extendLifetimeRequest)
        throws CacheException
    {
        long pinRequestId = extendLifetimeRequest.getPinId();
        PnfsId pnfsId = extendLifetimeRequest.getPnfsId();

        if (pnfsId == null ) {
            throw new InvalidMessageCacheException("PnfsId is null");
        }

        PinManagerJobImpl<PinManagerExtendPinMessage> job =
            new PinManagerJobImpl(PinManagerJobType.EXTEND_LIFETIME,
                extendLifetimeRequest,
                pnfsId,
                extendLifetimeRequest.getLifetime(),
                null);
        job.setPinRequestId(pinRequestId);

        extendLifetime(job);
        return job;
    }

    /**
     * This method MUST work with extendLifetimeRequest set to null as
     * it might be invoked by an admin command.
     */
    private void extendLifetime(PinManagerJobImpl<PinManagerExtendPinMessage> job)
            throws PinException
    {
        logger.info("extend lifetime pnfsId={} pinRequestId={} new lifetime={}",
                    new Object[] { job.getPnfsId(), job.getPinRequestId(), job.getLifetime() });
        if(getMaxPinDuration() !=-1 && job.getLifetime() > getMaxPinDuration()) {
            job.setLifetime(getMaxPinDuration());
            logger.info("Pin newLifetime exceeded maxPinDuration, " +
                        "newLifetime is set to {}", job.getLifetime());
        }
        db.initDBConnection();

        boolean  changedReplyRequired = false;
        try {
            Pin pin = db.getPinForUpdateByRequestId(job.getPinRequestId());
            if (pin == null) {
                throw new PinException(1, "Pin request " + job.getPinRequestId() + " not found");
            }

            Set<PinRequest> pinRequests = pin.getRequests();
            PinRequest pinRequest = null;
            for(PinRequest aPinRequest: pinRequests) {
                if(aPinRequest.getId() == job.getPinRequestId()) {
                    pinRequest = aPinRequest;
                    break;
                }
            }

            if (pinRequest == null) {
                throw new PinException(1, "Pin request " + job.getPinRequestId() + " not found");
            }
            if(!pin.getState().equals(PinManagerPinState.PINNED) &&
                pin.getState().equals(PinManagerPinState.INITIAL) &&
                pin.getState().equals(PinManagerPinState.PINNING ) ) {
                throw new PinException(1, "Pin request " + job.getPinRequestId() + " is not pinned anymore");
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
                         expiration, _poolStub);
        } catch (PinException e) {
            logger.error("extend lifetime: " + e);
            db.rollbackDBOperations();
            throw e;
        } finally {
            db.commitDBOperations();
        }
    }

    public void extendSucceeded(Pin pin,
                                PinRequest pinRequest,
                                PinManagerJob<?> extendJob,
                                long expiration)
        throws PinException
    {
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

    public void extendFailed(Pin pin ,PinRequest pinRequest,
                             PinManagerJob<?> extendJob,
                             Object reason) throws PinException
    {
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
    public void repin(PinManagerJob<?> job,Pin pin) throws PinException {
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

            PinManagerJobImpl<PinManagerPinMessage> newPinJob =
                new PinManagerJobImpl<PinManagerPinMessage>(PinManagerJobType.PIN,
                oldPin.getPnfsId(),lifetime);
            //save the extend job on pinRequestToJobMap
            // if the new pinning succeeds, the extend job will
            // receive a success notification
            // if it fails, it will cause the extend request to fail
            if(job != null && job.getPinRequestId() != null) {
                pinRequestToJobMap.put(job.getPinRequestId(),job);
            }
            new Pinner(this, newPinJob, newPin,0, RequestContainerV5.allStates,
                       _pnfsManagerStub, _poolManagerStub, _poolStub);

        } catch (PinDBException pdbe ) {
            logger.error("repinFile: "+pdbe.toString());
            db.rollbackDBOperations();
            return;
        }
        finally {
            db.commitDBOperations();
        }
    }

    public MessageReply<PinManagerUnpinMessage>
        unpin(PinManagerUnpinMessage unpinRequest)
        throws CacheException
    {
        PnfsId pnfsId = unpinRequest.getPnfsId();
        String requestId = unpinRequest.getRequestId();
        if (pnfsId == null ) {
            throw new InvalidMessageCacheException("PnfsId is null");
        }

        PinManagerJobImpl<PinManagerUnpinMessage> job =
            new PinManagerJobImpl(PinManagerJobType.UNPIN,
                                  unpinRequest,
                                  pnfsId,
                                  0,
                                  (requestId == null) ? null : Long.parseLong(requestId));
        if (unpinRequest.getPinId() != null){
            job.setPinRequestId(unpinRequest.getPinId());
        }

        if (job.getPinRequestId() == null && requestId == null) {
            unpinAllRequestForUser(job,false);
        } else {
            unpin(job,false);
        }

        return job;
    }

    public void unpinAllRequestForUser(PinManagerJob<PinManagerUnpinMessage> job, boolean force)
        throws PinException
    {
        logger.info("unpin all requests for pnfsId={}", job.getPnfsId());
        assert job.getPinId()==null && job.getSrmRequestId() ==0;

        db.initDBConnection();
        Long pinRequestIdLong = null;

        try {
            Pin pin = db.getAndLockActivePinWithRequestsByPnfsId(job.getPnfsId());
            if (pin == null) {
                throw new PinException(1, "Pin requests for "+job.getPnfsId()+
                                         " is not found");
            }

            if(!force &&  !pin.getState().equals(PinManagerPinState.PINNED)) {
                if (pin.getState().equals(PinManagerPinState.INITIAL) ||
                     pin.getState().equals(PinManagerPinState.PINNING)) {
                    throw new PinException(1, "Pin request for " + job.getPnfsId() + " is not pinned yet");
                } else  {
                    throw new PinException(1, "Pin request for " + job.getPnfsId() + " is not pinned, or is already being upinnned");
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

                    PinManagerJob<PinManagerUnpinMessage> unpinJob =
                        new PinManagerJobImpl<PinManagerUnpinMessage>(PinManagerJobType.UNPIN,
                            pin.getPnfsId(),
                            0);
                    new Unpinner(this,unpinJob,pin,false, _pnfsManagerStub, _poolStub);
                    return;
                }
            }

            if(!unpinedAtLeastOne) {
                throw new PinException(1, "Pin request for " + job.getPnfsId() + " cannot be unpinned, authorization failure");
            }
        } catch (PinException e) {
            logger.error("unpin: " + e.toString());
            db.rollbackDBOperations();
            if (pinRequestIdLong != null) {
                pinRequestToUnpinJobMap.remove(pinRequestIdLong);
            }
            throw e;
        } finally {
            db.commitDBOperations();
        }
    }


    /**
     * This method MUST work with unpinRequest and set to null as it
     * might be invoked by an admin command or by watchdog thread.
     */
    public void unpin(PinManagerJobImpl<PinManagerUnpinMessage> job, boolean force)
        throws PinException
    {
        logger.info("unpin pnfsId="+job.getPnfsId()+
                " pinRequestId="+job.getPinRequestId()+
                " srmRequestId="+job.getSrmRequestId());

        if(job.getPinRequestId() == null) {
            db.initDBConnection();

            try {
                long pinRequestId =
                    db.getPinRequestIdByByPnfsIdandSrmRequestId(job.getPnfsId(),
                                                                job.getSrmRequestId());
                if (job.getMessage() != null) {
                    job.getMessage().setPinId(pinRequestId);
                }
                job.setPinRequestId(pinRequestId);
            } catch (PinDBException e) {
                logger.error("unpin: " + e.toString());
                db.rollbackDBOperations();
                throw e;
            } finally {
                db.commitDBOperations();
            }
        }

        db.initDBConnection();
        Long pinRequestIdLong = null;
        try {
            Pin pin = db.getPinForUpdateByRequestId(job.getPinRequestId());
            if (pin == null) {
                throw new PinException(1, "Pin request " + job.getPinRequestId()+ " not found");
            }

            if(!force &&  ! pin.getState().equals(PinManagerPinState.PINNED)) {
                if (pin.getState().equals(PinManagerPinState.INITIAL) ||
                     pin.getState().equals(PinManagerPinState.PINNING)) {
                    throw new PinException(1, "Pin request " + job.getPinRequestId() + " is not pinned yet");
                } else {
                    throw new PinException(1, "Pin request " + job.getPinRequestId() + " is not pinned, or is already being upinnned");
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
                throw new PinException(1, "Pin request " + job.getPinRequestId() + " not found");
            }
            if(!force && !pinManagerPolicy.canUnpin(job.getAuthorizationRecord(),foundPinRequest)){
                throw new PinException(1, "Pin request " + job.getPinRequestId() + " cannot be unpinned, authorization failure");
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
            logger.info("starting unpinner for request {}", job.getPinRequestId());
            db.commitDBOperations();
            // we need to commit the new state before we start
            // processing
            // otherwise we might not see the request in database
            // if processing succeeds before the commit is executed
            // (a race condition )
            PinManagerJob<PinManagerUnpinMessage> unpinJob =
                new PinManagerJobImpl<PinManagerUnpinMessage>(PinManagerJobType.UNPIN,
                    pin.getPnfsId(),
                    0);
            new Unpinner(this,unpinJob,pin,false, _pnfsManagerStub, _poolStub);
            return;
        } catch (PinException e) {
            logger.error("unpin: " + e);
            db.rollbackDBOperations();
            if(job.getMessage()  != null) {
                if(pinRequestIdLong != null) {
                    pinRequestToUnpinJobMap.remove(pinRequestIdLong);
                }
            }
            throw e;
        } finally {
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
                PinManagerJob<?> job =
                        pinRequestToJobMap.remove(pinRequest.getId());
                if(job != null) {
                    job.returnFailedResponse("Pinning failed, unpin has suceeded");
                }
                // find all unpinAllRequestForUser messages and return success
                PinManagerJob<?> unpinjob =
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
                PinManagerJob<?> job =
                        pinRequestToJobMap.remove(pinRequest.getId());
                if(job != null) {
                    job.returnFailedResponse(
                                "Pinning failed, unpinning is in progress");
                }

                PinManagerJob<?> unpinjob =
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
           PinManagerJobImpl<PinManagerUnpinMessage> job =
                   new PinManagerJobImpl<PinManagerUnpinMessage>(PinManagerJobType.UNPIN,
                                                                 pinRequest.getPin().getPnfsId(),
                                                                 0,
                                                                 pinRequest.getAuthorizationRecord());
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

    public void removeFiles(PoolRemoveFilesMessage removeFile)
    {
        String[] pnfsIds = removeFile.getFiles();
        if(pnfsIds == null || pnfsIds.length == 0) {
            return;
        }

        for(String pnfsIdString: pnfsIds) {
            PnfsId pnfsId = null;
            try {
              pnfsId =  new PnfsId(pnfsIdString);
            } catch (IllegalArgumentException e) {
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
                            PinManagerJob<?> job =
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
        PinManagerMovePinMessage movePin)
        throws NoRouteToCellException
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
                _poolStub.send(new CellPath(srcPool), setStickyRequest);
            }
        }
    }

    public MessageReply<PinManagerMovePinMessage> movePin(PinManagerMovePinMessage movePin)
        throws CacheException
    {
        MessageReply<PinManagerMovePinMessage> reply =
            new MessageReply<PinManagerMovePinMessage>();
        PnfsId pnfsId = movePin.getPnfsId();
        String srcPool = movePin.getSourcePool();
        String dstPool = movePin.getTargetPool();

        if (pnfsId == null) {
            throw new InvalidMessageCacheException("PnfsId is null");
        }
        if (srcPool == null ) {
            throw new InvalidMessageCacheException("Source pool is not set");
        }
        if (dstPool == null ) {
            throw new InvalidMessageCacheException("Sestination pool is not set");
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
                    throw new PinException(1, "Pins for "+pnfsId+" in "+srcPool+ " in pinned state are not found");
                }
                if(pinsToMove.size() >1) {
                    logger.error("More than one pin found, which is not yet supported ");
                    throw new PinException(1, "More than one pin found, which is not yet supported");
                }

                for (Pin srcPin: pinsToMove) {
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
                                     reply,
                                     _poolStub);
                }
            } finally {
                db.commitDBOperations();
            }
        } catch (PinException e) {
            logger.error(e.toString());
            throw e;
        }
        return reply;
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
        MessageReply<PinManagerMovePinMessage> reply)
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
                returnResponse(movePin, reply);
                return false;
            }

            if(!srcPin.getState().equals(PinManagerPinState.PINNED)) {
                    pinMoveFailed(dstPin,movePin,reply,
                        "state of source pin has changed to "+
                        srcPin.getState() +
                        " by the time move succeeded");
                cleanMovedStickyFlag(dstPin);

                return false;
            }

            dstPin = db.getPinForUpdate(dstPin.getId());
            if(dstPin == null) {
                pinMoveFailed(dstPin,movePin,reply,
                    "dst pin has been removed by the time move succeeded");
                return false;
            }
            if(!dstPin.getState().equals(PinManagerPinState.MOVING)) {
                pinMoveFailed(dstPin,movePin,reply,
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
            pinMoveFailed(dstPin,movePin,reply,
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
        MessageReply<PinManagerMovePinMessage> reply)
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
                returnResponse(movePin, reply);
                return;
            }
            logger.debug("pinMoveSucceeded, deleting original pin");
            db.deletePin(srcPin.getId());
         } catch (PinDBException pdbe ) {
            logger.error("Exception in pinMoveSucceeded: "+pdbe);
            db.rollbackDBOperations();

            //return success anyway, as the pin was unpined in the source, but
            // db update failed, pin is in unpinning state in db
            returnResponse(movePin, reply);
            return;
        }
        finally {
            db.commitDBOperations();
        }
        //return success
        returnResponse(movePin, reply);
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
        PinManagerJob<PinManagerUnpinMessage> unpinJob =
            new PinManagerJobImpl<PinManagerUnpinMessage>(PinManagerJobType.UNPIN,
                dstPin.getPnfsId(),
                0);
        new Unpinner(this,unpinJob,dstPin,false, _pnfsManagerStub, _poolStub);
    }

    public void pinMoveFailed (
        Pin  srcPin ,
        Pin  dstPin ,
        String pool,
        long expiration,
        PinManagerMovePinMessage movePin,
        MessageReply<PinManagerMovePinMessage> reply,
        Object error) throws PinException {
        logger.error("pinMoveFailed, error="+error+" srcPin="+srcPin+
            " dstPin="+dstPin+
            " pool="+pool+
            " expiration="+expiration);

        db.initDBConnection();
        try {
            pinMoveFailed(dstPin,movePin,reply,error);
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
        MessageReply<PinManagerMovePinMessage> reply,
        Object error) throws PinException {
        returnFailedResponse(error,movePin,reply);
        db.deletePin(dstPin.getId());
        db.commitDBOperations();
    }

    private class PinManagerJobImpl<T extends Message>
        extends MessageReply<T> implements PinManagerJob<T>, InteractiveJob
    {
        private final PinManagerJobType type;
        private PinManagerJobState state = PinManagerJobState.ACTIVE;
        private Long pinId;
        private Long pinRequestId;
        private T pinManagerMessage;
        private final PnfsId pnfsId;
        private long lifetime;
        private final AuthorizationRecord authRecord;
        private long srmRequestId;
        private Object errorObject;
        private int errorCode;
        private SMCTask task;

        public PinManagerJobImpl(PinManagerJobType type,
                                 T message,
                                 PnfsId pnfsId,
                                 long lifetime,
                                 Long srmRequestId)
        {
            this(type, pnfsId, lifetime,
                 Subjects.getAuthorizationRecord(message.getSubject()));
            // do not even store the message, if the reply is not required
            if (message != null && message.getReplyRequired()) {
                this.pinManagerMessage = message;
            }
            if (srmRequestId != null) {
                this.srmRequestId = srmRequestId;
            }
        }

        public PinManagerJobImpl(PinManagerJobType type,
                                 PnfsId pnfsId,
                                 long lifetime)
        {
            this(type, pnfsId, lifetime, null);
        }

        public PinManagerJobImpl(PinManagerJobType type,
                                 PnfsId pnfsId,
                                 long lifetime,
                                 AuthorizationRecord authRecord)
        {
            this.type = type;
            this.pnfsId = pnfsId;
            this.lifetime = lifetime;
            this.authRecord = authRecord;
        }

        /**
         * @return the type
         */
        public PinManagerJobType getType() {
            return type;
        }

        /**
         * @return the pinManagerMessage
         */
        public T getMessage() {
            return pinManagerMessage;
        }

        /**
         * @return the pnfsId
         */
        public PnfsId getPnfsId() {
            return pnfsId;
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
            if (pinManagerMessage != null && pinManagerMessage.getReplyRequired()) {
                reply(pinManagerMessage);
                pinManagerMessage = null;
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
}

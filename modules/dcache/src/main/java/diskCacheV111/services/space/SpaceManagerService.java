// -*- c-basic-offset: 8; -*-
//______________________________________________________________________________
//
// Space Manager - cell that handles space reservation management in SRM
//                 essentially a layer on top of a database
//
// there are three essential tables:
//
//      +------------+  +--------+  +------------+
//      |srmlinkgroup|-<|srmspace|-<|srmspacefile|
//      +------------+  +--------+  +------------+
// srmlinkgroup contains field that caches sum(size-usedsize) of all space
// reservations belonging to the linkgroup. Field is called reservedspaceinbytes
//
// srmspace  contains fields that caches sum(size) of all files from srmspace
// that belong to this space reservation. Fields are usedspaceinbytes
//  (for files in state STORED) and allocatespaceinbytes
//  (for files in states RESERVED or TRANSFERRING)
//
// each time a space reservation is added/removed , reservedspaceinbytes in
// srmlinkgroup is updated
//
// each time a file is added/removed, usedspaceinbytes, allocatespaceinbytes and
// reservedspaceinbytes are updated depending on file state
//
//                                    Dmitry Litvintsev (litvinse@fnal.gov)
//______________________________________________________________________________
package diskCacheV111.services.space;

import com.google.common.base.Joiner;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.services.space.message.CancelUse;
import diskCacheV111.services.space.message.ExtendLifetime;
import diskCacheV111.services.space.message.GetFileSpaceTokensMessage;
import diskCacheV111.services.space.message.GetLinkGroupNamesMessage;
import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.services.space.message.GetSpaceMetaData;
import diskCacheV111.services.space.message.GetSpaceTokens;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.services.space.message.Release;
import diskCacheV111.services.space.message.Reserve;
import diskCacheV111.services.space.message.Use;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;
import diskCacheV111.vehicles.PoolAcceptFileMessage;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.PoolLinkGroupInfo;
import diskCacheV111.vehicles.PoolMgrSelectWritePoolMsg;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

import org.dcache.auth.FQAN;
import org.dcache.auth.Subjects;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.Utils;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;

public final class SpaceManagerService
        extends AbstractCellComponent
        implements CellCommandListener,
                   CellMessageReceiver,
                   Runnable
{
        private static final Logger LOGGER = LoggerFactory.getLogger(SpaceManagerService.class);

        private static final long EAGER_LINKGROUP_UPDATE_PERIOD = 1000;

        private final Object updateLinkGroupsSyncObject = new Object();

        private long updateLinkGroupsPeriod;
        private long currentUpdateLinkGroupsPeriod = EAGER_LINKGROUP_UPDATE_PERIOD;
        private long expireSpaceReservationsPeriod;

        private Thread updateLinkGroups;
        private Thread expireSpaceReservations;

        private AccessLatency defaultAccessLatency;
        private RetentionPolicy defaultRetentionPolicy;

        private boolean shouldDeleteStoredFileRecord;
        private boolean shouldReserveSpaceForNonSrmTransfers;
        private boolean shouldReturnFlushedSpaceToReservation;
        private boolean shouldCleanupExpiredSpaceFiles;
        private boolean isSpaceManagerEnabled;

        private CellPath poolManager;
        private PnfsHandler pnfs;

        private SpaceManagerAuthorizationPolicy authorizationPolicy;

        private Executor executor;

        private PoolMonitor poolMonitor;
        private SpaceManagerDatabase db;

        private java.io.File linkGroupAuthorizationFileName;
        private long latestLinkGroupUpdateTime = System.currentTimeMillis();
        private LinkGroupAuthorizationFile linkGroupAuthorizationFile;
        private long linkGroupAuthorizationFileLastUpdateTimestamp;

        @Required
        public void setPoolManager(CellPath poolManager)
        {
                this.poolManager = poolManager;
        }

        @Required
        public void setPnfsHandler(PnfsHandler pnfs)
        {
                this.pnfs = pnfs;
        }

        public void setPoolMonitor(PoolMonitor poolMonitor)
        {
                this.poolMonitor = poolMonitor;
        }

        @Required
        public void setSpaceManagerEnabled(boolean enabled)
        {
                this.isSpaceManagerEnabled = enabled;
        }

        @Required
        public void setUpdateLinkGroupsPeriod(long updateLinkGroupsPeriod)
        {
                this.updateLinkGroupsPeriod = updateLinkGroupsPeriod;
        }

        @Required
        public void setExpireSpaceReservationsPeriod(long expireSpaceReservationsPeriod)
        {
                this.expireSpaceReservationsPeriod = expireSpaceReservationsPeriod;
        }


        @Required
        public void setDefaultRetentionPolicy(RetentionPolicy defaultRetentionPolicy)
        {
                this.defaultRetentionPolicy = defaultRetentionPolicy;
        }

        @Required
        public void setDefaultAccessLatency(AccessLatency defaultAccessLatency)
        {
                this.defaultAccessLatency = defaultAccessLatency;
        }

        @Required
        public void setShouldReserveSpaceForNonSrmTransfers(boolean shouldReserveSpaceForNonSrmTransfers)
        {
                this.shouldReserveSpaceForNonSrmTransfers = shouldReserveSpaceForNonSrmTransfers;
        }

        @Required
        public void setShouldDeleteStoredFileRecord(boolean shouldDeleteStoredFileRecord)
        {
                this.shouldDeleteStoredFileRecord = shouldDeleteStoredFileRecord;
        }

        @Required
        public void setShouldCleanupExpiredSpaceFiles(boolean shouldCleanupExpiredSpaceFiles)
        {
                this.shouldCleanupExpiredSpaceFiles = shouldCleanupExpiredSpaceFiles;
        }

        @Required
        public void setShouldReturnFlushedSpaceToReservation(boolean shouldReturnFlushedSpaceToReservation)
        {
                this.shouldReturnFlushedSpaceToReservation = shouldReturnFlushedSpaceToReservation;
        }

        @Required
        public void setLinkGroupAuthorizationFileName(java.io.File linkGroupAuthorizationFileName)
        {
                this.linkGroupAuthorizationFileName = linkGroupAuthorizationFileName;
        }

        @Required
        public void setExecutor(ExecutorService executor)
        {
            this.executor = new CDCExecutorServiceDecorator(executor);
        }

        @Required
        public void setDatabase(SpaceManagerDatabase db)
        {
            this.db = db;
        }

        @Required
        public void setAuthorizationPolicy(SpaceManagerAuthorizationPolicy authorizationPolicy)
        {
                this.authorizationPolicy = authorizationPolicy;
        }

        public void start()
        {
                (updateLinkGroups = new Thread(this,"UpdateLinkGroups")).start();
                (expireSpaceReservations = new Thread(this,"ExpireThreadReservations")).start();
        }

        public void stop()
        {
                if (updateLinkGroups != null) {
                        updateLinkGroups.interrupt();
                }
                if (expireSpaceReservations != null) {
                        expireSpaceReservations.interrupt();
                }
        }


        @Override
        public void getInfo(PrintWriter printWriter) {
                printWriter.println("space.Manager "+getCellName());
                printWriter.println("isSpaceManagerEnabled="+ isSpaceManagerEnabled);
                printWriter.println("updateLinkGroupsPeriod="
                                    + updateLinkGroupsPeriod);
                printWriter.println("expireSpaceReservationsPeriod="
                                    + expireSpaceReservationsPeriod);
                printWriter.println("shouldDeleteStoredFileRecord="
                                    + shouldDeleteStoredFileRecord);
                printWriter.println("defaultLatencyForSpaceReservations="
                                    + defaultAccessLatency);
                printWriter.println("shouldReserveSpaceForNonSrmTransfers="
                                    + shouldReserveSpaceForNonSrmTransfers);
                printWriter.println("shouldReturnFlushedSpaceToReservation="
                                    + shouldReturnFlushedSpaceToReservation);
                printWriter.println("linkGroupAuthorizationFileName="
                                    + linkGroupAuthorizationFileName);
        }

        public static final String hh_release = " <spaceToken> [ <bytes> ] # release the space " +
                "reservation identified by <spaceToken>" ;
        public String ac_release_$_1_2(Args args) throws NumberFormatException, DataAccessException
        {
                long reservationId = Long.parseLong( args.argv(0));
                if (args.argc() == 1) {
                    Space space = db.updateSpace(reservationId,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 SpaceState.RELEASED);
                        return space.toString();
                }
                else {
                        return "partial release is not supported yet";
                }
        }

        private static long stringToSize(String s)
        {
                long size;
                int endIndex;
                int startIndex=0;
                if (s.endsWith("kB") || s.endsWith("KB")) {
                        endIndex=s.indexOf("KB");
                        if (endIndex==-1) {
                                endIndex=s.indexOf("kB");
                        }
                        String sSize = s.substring(startIndex,endIndex);
                        size    = sSize.isEmpty() ? 1000L : (long)(Double.parseDouble(sSize)*1.e+3+0.5);
                }
                else if (s.endsWith("KiB")) {
                        endIndex=s.indexOf("KiB");
                        String sSize = s.substring(startIndex,endIndex);
                        size    = sSize.isEmpty() ? 1024L : (long)(Double.parseDouble(sSize)*1024.+0.5);
                }
                else if (s.endsWith("MB")) {
                        endIndex=s.indexOf("MB");
                        String sSize = s.substring(startIndex,endIndex);
                        size    = sSize.isEmpty() ? 1000000L : (long)(Double.parseDouble(sSize)*1.e+6+0.5);
                }
                else if (s.endsWith("MiB")) {
                        endIndex=s.indexOf("MiB");
                        String sSize = s.substring(startIndex,endIndex);
                        size    = sSize.isEmpty() ? 1048576L : (long)(Double.parseDouble(sSize)*1048576.+0.5);
                }
                else if (s.endsWith("GB")) {
                        endIndex=s.indexOf("GB");
                        String sSize = s.substring(startIndex,endIndex);
                        size    = sSize.isEmpty() ? 1000000000L : (long)(Double.parseDouble(sSize)*1.e+9+0.5);
                }
                else if (s.endsWith("GiB")) {
                        endIndex=s.indexOf("GiB");
                        String sSize = s.substring(startIndex,endIndex);
                        size    = sSize.isEmpty() ? 1073741824L : (long)(Double.parseDouble(sSize)*1073741824.+0.5);
                }
                else if (s.endsWith("TB")) {
                        endIndex=s.indexOf("TB");
                        String sSize = s.substring(startIndex,endIndex);
                        size    = sSize.isEmpty() ? 1000000000000L : (long)(Double.parseDouble(sSize)*1.e+12+0.5);
                }
                else if (s.endsWith("TiB")) {
                        endIndex=s.indexOf("TiB");
                        String sSize = s.substring(startIndex,endIndex);
                        size    = sSize.isEmpty() ? 1099511627776L : (long)(Double.parseDouble(sSize)*1099511627776.+0.5);
                }
                else {
                        size = Long.parseLong(s);
                }
                if (size<0L) {
                        throw new IllegalArgumentException("size have to be non-negative");
                }
                return size;
        }

        public static final String hh_update_space_reservation = " [-size=<size>]  [-lifetime=<lifetime>] [-vog=<vogroup>] [-vor=<vorole>] <spaceToken> \n"+
                "                                                     # set new size and/or lifetime for the space token \n " +
                "                                                     # valid examples of size: 1000, 100kB, 100KB, 100KiB, 100MB, 100MiB, 100GB, 100GiB, 10.5TB, 100TiB \n" +
                "                                                     # see http://en.wikipedia.org/wiki/Gigabyte for explanation \n"+
                "                                                     # lifetime is in seconds (\"-1\" means infinity or permanent reservation";
        @Transactional
        public String ac_update_space_reservation_$_1(Args args) throws DataAccessException
        {
                long reservationId = Long.parseLong(args.argv(0));
                String sSize     = args.getOpt("size");
                String sLifetime = args.getOpt("lifetime");
                String voRole    = args.getOpt("vor");
                String voGroup   = args.getOpt("vog");
                if (sLifetime==null&&
                    sSize==null&&
                    voRole==null&&
                    voGroup==null) {
                        return "Need to specify at least one option \"-lifetime\", \"-size\" \"-vog\" or \"-vor\". If -lifetime=\"-1\"  then the reservation will not expire";
                }
                Long longLifetime = null;
                if (sLifetime != null) {
                        long lifetime = Long.parseLong(sLifetime);
                        longLifetime = (lifetime == -1) ? -1 : lifetime * 1000;
                }
                try {
                        if (voRole!=null || voGroup!=null) {
                                // check that linkgroup allows these role/group combination
                                Space space = db.getSpace(reservationId);
                                LinkGroup lg = db.getLinkGroup(space.getLinkGroupId());
                                boolean foundMatch = false;
                                // this will keep the same group/role
                                // if one of then is not specified:
                                if (voGroup==null) {
                                    voGroup = space.getVoGroup();
                                }
                                if (voRole==null) {
                                    voRole = space.getVoRole();
                                }
                                for (VOInfo info : lg.getVOs()) {
                                        if (info.match(voGroup,voRole)) {
                                                foundMatch = true;
                                                break;
                                        }
                                }
                                if (!foundMatch) {
                                        throw new IllegalArgumentException("cannot change voGroup:voRole to "+
                                                                           voGroup+ ':' +voRole+
                                                                           ". Supported vogroup:vorole pairs for this spacereservation\n"+
                                                                           Joiner.on('\n').join(lg.getVOs()));
                                }
                        }
                        Space space = db.updateSpace(reservationId,
                                                     voGroup,
                                                     voRole,
                                                     null,
                                                     null,
                                                     null,
                                                     (sSize != null ? stringToSize(sSize) : null),
                                                     longLifetime,
                                                     null,
                                                     null);
                        return space.toString();
                } catch (EmptyResultDataAccessException e) {
                        return e.toString();
                }
        }

        public static final String hh_update_link_groups = " #triggers update of the link groups";
        public String ac_update_link_groups_$_0(Args args)
        {
                synchronized(updateLinkGroupsSyncObject) {
                        updateLinkGroupsSyncObject.notify();
                }
                return "update started";
        }

        public static final String hh_ls = " [-lg=LinkGroupName] [-lgid=LinkGroupId] [-vog=vogroup] [-vor=vorole] [-desc=description] [-l] <id> # list space reservations";

        @Transactional(readOnly = true)
        public String ac_ls_$_0_1(Args args) throws DataAccessException
        {
                String lgName        = args.getOpt("lg");
                String lgid          = args.getOpt("lgid");
                String voGroup       = args.getOpt("vog");
                String voRole        = args.getOpt("vor");
                String description   = args.getOpt("desc");
                boolean isLongFormat = args.hasOption("l");
                String id = (args.argc() == 1) ? args.argv(0) : null;
                boolean isFilterNotSpecified = lgName == null && lgid == null && voGroup == null && description == null;
                if (description != null && id !=null ) {
                        return "Do not use -desc and -id simultaneously";
                }

                StringBuilder sb = new StringBuilder();
                if (isFilterNotSpecified) {
                        sb.append("Reservations:\n");
                }
                try {
                    listSpaceReservations(isLongFormat,
                                          id,
                                          lgName,
                                          lgid,
                                          description,
                                          voGroup,
                                          voRole,
                                          sb);
                } catch (EmptyResultDataAccessException e) {
                    return e.toString();
                }
                if (isFilterNotSpecified) {
                        sb.append("\n\nLinkGroups:\n");
                        listLinkGroups(isLongFormat,false,null,sb);
                }
                return sb.toString();
        }

        private void listSpaceReservations(boolean isLongFormat,
                                           String id,
                                           String linkGroupName,
                                           String linkGroupId,
                                           String description,
                                           String group,
                                           String role,
                                           StringBuilder sb) throws DataAccessException
        {
                List<Space> spaces;
                LinkGroup lg = null;
                if (linkGroupId!=null) {
                        lg = db.getLinkGroup(Long.parseLong(linkGroupId));
                        if (linkGroupName != null && !lg.getName().equals(linkGroupName)) {
                            sb.append("Cannot find LinkGroup with id=").
                                    append(linkGroupId).
                                    append(" and name=").
                                    append(linkGroupName);
                            return;
                        }
                } else if (linkGroupName!=null) {
                        lg = db.getLinkGroupByName(linkGroupName);
                }
                if (lg != null) {
                        sb.append("Found LinkGroup:\n");
                        lg.toStringBuilder(sb);
                        sb.append('\n');
                }

                if(id != null) {
                        Long longid = Long.valueOf(id);
                        Space space = db.getSpace(longid);
                        if (lg!=null) {
                                if (space.getLinkGroupId() != lg.getId()) {
                                        sb.append("LinkGroup with id=").
                                                append(lg.getId()).
                                                append(" and name=").
                                                append(lg.getName()).
                                                append(" does not contain space with id=").
                                                append(id);
                                }
                        }
                        else {
                                space.toStringBuilder(sb);
                        }
                        sb.append('\n');
                }
                if (linkGroupName==null&&linkGroupId==null&&description==null&&group==null&&role==null){
                        spaces = db.getReservedSpaces();
                        int count = spaces.size();
                        long totalReserved = 0;
                        for (Space space : spaces) {
                                totalReserved += space.getSizeInBytes();
                                space.toStringBuilder(sb);
                                sb.append('\n');
                        }
                        sb.append("total number of reservations: ").append(count).append('\n');
                        sb.append("total number of bytes reserved: ").append(totalReserved);
                        return;
                }
                if (description==null&&group==null&&role==null&&lg!=null) {
                        spaces = db.findSpaces(group, role, description, lg);
                        if (spaces.isEmpty()) {
                                sb.append("LinkGroup with id=").
                                        append(lg.getId()).
                                        append(" and name=").
                                        append(lg.getName()).
                                        append(" does not contain any space reservations\n");
                                return;
                        }
                        for (Space space : spaces) {
                                space.toStringBuilder(sb);
                                sb.append('\n');
                        }
                        return;
                }
                if (description!=null) {
                        spaces = db.findSpaces(group, role, description, lg);
                        if (spaces.isEmpty()) {
                                if (lg==null) {
                                        sb.append("Space with description ").
                                                append(description).
                                                append(" not found ");
                                }
                                else {
                                        sb.append("LinkGroup with id=").
                                                append(lg.getId()).
                                                append(" and name=").
                                                append(lg.getName()).
                                                append(" does not contain space with description ").
                                                append(description);
                                }
                                return;
                        }
                        for (Space space : spaces) {
                                space.toStringBuilder(sb);
                                sb.append('\n');
                        }
                        return;
                }
                if (role!=null&&group!=null) {
                        spaces = db.findSpaces(group, role, description, lg);
                        if (spaces.isEmpty()) {
                                if (lg==null) {
                                        sb.append("Space with vorole ").
                                                append(role).
                                                append(" and vogroup ").
                                                append(group).
                                                append(" not found ");
                                }
                                else {
                                        sb.append("LinkGroup with id=").
                                                append(lg.getId()).
                                                append(" and name=").
                                                append(lg.getName()).
                                                append(" does not contain space with vorole ").
                                                append(role).
                                                append(" and vogroup ").
                                                append(group);
                                }
                                return;
                        }
                        for (Space space : spaces) {
                                space.toStringBuilder(sb);
                                sb.append('\n');
                        }
                        return;
                }
                if (group!=null) {
                        spaces = db.findSpaces(group, role, description, lg);
                        if (spaces.isEmpty()) {
                                if (lg==null) {
                                        sb.append("Space with vogroup ").
                                                append(group).
                                                append(" not found ");
                                }
                                else {
                                        sb.append("LinkGroup with id=").
                                                append(lg.getId()).
                                                append(" and name=").
                                                append(
                                                " does not contain space with vogroup=").
                                                append(group);
                                }
                                return;
                        }
                        for (Space space : spaces) {
                                space.toStringBuilder(sb);
                                sb.append('\n');
                        }
                        return;
                }
                if (role!=null) {
                        spaces = db.findSpaces(group, role, description, lg);
                        if (spaces.isEmpty()) {
                                if (lg==null) {
                                        sb.append("Space with vorole ").
                                                append(role).
                                                append(" not found ");
                                }
                                else {
                                        sb.append("LinkGroup with id=").
                                                append(lg.getId()).
                                                append(" and name=").
                                                append(" does not contain space with vorole=").
                                                append(role);
                                }
                                return;
                        }
                        for (Space space : spaces) {
                                space.toStringBuilder(sb);
                                sb.append('\n');
                        }
                }
        }

        private void listLinkGroups(boolean isLongFormat,
                                    boolean all,
                                    String id,
                                    StringBuilder sb)
                throws DataAccessException
        {
                List<LinkGroup> groups;
                if (id != null) {
                        long longid = Long.parseLong(id);
                        try {
                                LinkGroup lg=db.getLinkGroup(longid);
                                lg.toStringBuilder(sb);
                                sb.append('\n');
                                return;
                        }
                        catch (EmptyResultDataAccessException e) {
                                sb.append("LinkGroup  with id=").
                                        append(id).
                                        append(" not found ");
                                return;
                        }
                }
                if (all) {
                        groups = db.getLinkGroups();
                }
                else {
                        groups = db.getLinkGroupsRefreshedAfter(latestLinkGroupUpdateTime);
                }
                int count = groups.size();
                long totalReservable = 0L;
                long totalReserved   = 0L;
                for (LinkGroup g : groups) {
                        totalReservable  += g.getAvailableSpaceInBytes();
                        totalReserved    += g.getReservedSpaceInBytes();
                        g.toStringBuilder(sb);
                        sb.append('\n');
                }
                sb.append("total number of linkGroups: ").
                        append(count).append('\n');
                sb.append("total number of bytes reservable: ").
                        append(totalReservable).append('\n');
                sb.append("total number of bytes reserved  : ").
                        append(totalReserved).append('\n');
                sb.append("last time all link groups were updated: ").
                        append((new Date(latestLinkGroupUpdateTime)).toString()).
                        append('(').append(latestLinkGroupUpdateTime).
                        append(')');
        }

        public static final String hh_ls_link_groups = " [-l] [-a]  <id> # list link groups";
        @Transactional(readOnly = true)
        public String ac_ls_link_groups_$_0_1(Args args) throws DataAccessException
        {
                boolean isLongFormat = args.hasOption("l");
                boolean all = args.hasOption("a");
                String id = null;
                if (args.argc() == 1) {
                        id = args.argv(0);
                }
                StringBuilder sb = new StringBuilder();
                sb.append("\n\nLinkGroups:\n");
                listLinkGroups(isLongFormat,all,id,sb);
                return sb.toString();
        }

        public static final String hh_ls_file_space_tokens = " <pnfsId>|<pnfsPath> # list space tokens " +
                "that contain a file";

        @Transactional(readOnly = true)
        public String ac_ls_file_space_tokens_$_1(Args args) throws DataAccessException
        {
                List<Long> tokens;
                try {
                    tokens = db.getSpaceTokensOfFile(new PnfsId(args.argv(0)), null);
                }
                catch (IllegalArgumentException e) {
                    tokens = db.getSpaceTokensOfFile(null, new FsPath(args.argv(0)));
                }
                if (!tokens.isEmpty()) {
                    return Joiner.on('\n').join(tokens);
                }
                else {
                    return "no space tokens found for file: " + args.argv(0);
                }
        }

        public final String hh_reserve = "  [-vog=voGroup] [-vor=voRole] " +
                "[-acclat=AccessLatency] [-retpol=RetentionPolicy] [-desc=Description] " +
                " [-lgid=LinkGroupId]" +
                " [-lg=LinkGroupName]" +
                " <sizeInBytes> <lifetimeInSecs (use quotes around negative one)> \n"+
                " default value for AccessLatency is "+defaultAccessLatency + '\n' +
                " default value for RetentionPolicy is "+defaultRetentionPolicy;

        @Transactional
        public String ac_reserve_$_2(Args args)
                throws IllegalArgumentException, DataAccessException
        {
                long sizeInBytes;
                try {
                        sizeInBytes = stringToSize(args.argv(0));
                }
                catch (IllegalArgumentException e) {
                        return "Cannot convert size specified ("
                               +args.argv(0)
                               +") to non-negative number. \n"
                               +"Valid definition of size:\n"+
                                "\t\t - a number of bytes (long integer less than 2^64) \n"+
                                "\t\t - 100kB, 100KB, 100KiB, 100MB, 100MiB, 100GB, 100GiB, 10.5TB, 100TiB \n"+
                                "see http://en.wikipedia.org/wiki/Gigabyte for explanation";
                }
                long lifetime = Long.parseLong(args.argv(1));
                if (lifetime > 0) {
                        lifetime *= 1000;
                }
                String voGroup       = args.getOpt("vog");
                String voRole        = args.getOpt("vor");
                String description   = args.getOpt("desc");
                String latencyString = args.getOpt("acclat");
                String policyString  = args.getOpt("retpol");

                AccessLatency latency = latencyString==null?
                    defaultAccessLatency:AccessLatency.getAccessLatency(latencyString);
                RetentionPolicy policy = policyString==null?
                    defaultRetentionPolicy:RetentionPolicy.getRetentionPolicy(policyString);

                String lgIdString = args.getOpt("lgid");
                String lgName     = args.getOpt("lg");
                if(lgIdString != null && lgName != null) {
                        return "Error: mutually exclusive options -lg and -lgid are specified";
                }
                List<Long> linkGroups = db.findLinkGroupIds(sizeInBytes,
                                                            voGroup,
                                                            voRole,
                                                            latency,
                                                            policy,
                                                            latestLinkGroupUpdateTime);
                long lgId;
                if(lgIdString == null && lgName == null) {
                        if (linkGroups.isEmpty()) {
                                LOGGER.warn("find LinkGroup Ids returned 0 linkGroups, no linkGroups found");
                                return "Failed to find linkgroup that can accommodate this space reservation. \n"+
                                        "Check that you have any link groups that satisfy the following criteria: \n"+
                                        "\t can fit the size you are requesting ("+sizeInBytes+")\n"+
                                        "\t vogroup,vorole you specified ("+
                                        voGroup+ ',' +voRole+
                                        ") are allowed, and \n"+
                                        "\t retention policy and access latency you specified ("+
                                        policyString+ ',' +latencyString+
                                        ") are allowed \n";
                        }
                        lgId = linkGroups.get(0);
                }
                else {
                        LinkGroup lg;
                        try {
                                if (lgIdString != null){
                                        lgId = Long.parseLong(lgIdString);
                                        lg   = db.getLinkGroup(lgId);
                                }
                                else {
                                        lg = db.getLinkGroupByName(lgName);
                                        lgId = lg.getId();
                                }
                        } catch (EmptyResultDataAccessException e) {
                                return e.getMessage();
                        }

                        if(linkGroups.isEmpty()) {
                                return "Link Group "+lg+" is found, but it cannot accommodate the reservation requested, \n"+
                                        "check that the link group satisfies the following criteria: \n"+
                                        "\t it can fit the size you are requesting ("+sizeInBytes+")\n"+
                                        "\t vogroup,vorole you specified ("+voGroup+ ',' +voRole+") are allowed, and \n"+
                                        "\t retention policy and access latency you specified ("+policyString+ ',' +latencyString+") are allowed \n";
                        }

                        boolean yes=false;
                        for (Long linkGroup : linkGroups) {
                            if (linkGroup == lgId) {
                                yes = true;
                                break;
                            }
                        }
                        if (!yes) {
                                return "Link Group "+lg+
                                       " is found, but it cannot accommodate the reservation requested, \n"+
                                        "check that the link group satisfies the following criteria: \n"+
                                        "\t it can fit the size you are requesting ("+sizeInBytes+")\n"+
                                        "\t vogroup,vorole you specified ("+voGroup+ ',' +voRole+") are allowed, and \n"+
                                        "\t retention policy and access latency you specified ("+policyString+ ',' +latencyString+") are allowed \n";
                        }
                }
                Space space = db.insertSpace(voGroup,
                                             voRole,
                                             policy,
                                             latency,
                                             lgId,
                                             sizeInBytes,
                                             lifetime,
                                             description,
                                             SpaceState.RESERVED,
                                             0,
                                             0);
                return space.toString();
        }

        public static final String hh_listInvalidSpaces = " [-e] [-r] [<n>]" +
                " # e=expired, r=released, default is both, n=number of rows to retrieve";
        public String ac_listInvalidSpaces_$_0_1(Args args)
                throws IllegalArgumentException, DataAccessException
        {
                int argCount = args.optc();
                boolean doExpired  = args.hasOption("e");
                boolean doReleased = args.hasOption("r");
                int nRows = args.argc() > 0 ? Integer.parseInt(args.argv(0)) : 1000;
                if (nRows < 0) {
                        return "number of rows must be non-negative";
                }
                Set<SpaceState> states = new HashSet<>();
                if (doExpired) {
                        --argCount;
                }
                if (doReleased){
                        --argCount;
                }
                if (argCount != 0) {
                        return "Unrecognized option.\nUsage: listInvalidSpaces" +
                                hh_listInvalidSpaces;
                }
                if (doExpired || !doReleased) {
                    states.add(SpaceState.EXPIRED);
                }
                if (doReleased || !doExpired) {
                    states.add(SpaceState.RELEASED);
                }
                List<Space> expiredSpaces = db.getSpaces(states, nRows);
                if (expiredSpaces.isEmpty()) {
                        return "There are no such spaces.";
                }
                return Joiner.on('\n').join(expiredSpaces);
        }


        public static final String hh_listFilesInSpace=" <space-id>";
        // @return a string containing a newline-separated list of the files in
        //         the space specified by <i>space-id</i>.

        public String ac_listFilesInSpace_$_1( Args args )
                throws DataAccessException, NumberFormatException
        {
                long spaceId = Long.parseLong( args.argv( 0 ) );
                // Get a list of the Invalid spaces
                List<File> filesInSpace = db.getFilesInSpace(spaceId);
                if (filesInSpace.isEmpty()) {
                        return "There are no files in this space.";
                }
                return Joiner.on('\n').join(filesInSpace);
        }

        public static final String hh_removeFilesFromSpace =
                " [-r] [-t] [-s] [-f] <Space Id>"+
                "# remove expired files from space, -r(reserved) -t(transferring) -s(stored) -f(flushed)";
        public String ac_removeFilesFromSpace_$_1_4(Args args)
                throws DataAccessException
        {
                long spaceId = Long.parseLong(args.argv(0));
                Set<FileState> states = new HashSet<>();
                if (args.hasOption("r")) {
                    states.add(FileState.RESERVED);
                }
                if (args.hasOption("t")) {
                    states.add(FileState.TRANSFERRING);
                }
                if (args.hasOption("s")) {
                    states.add(FileState.STORED);
                }
                if (args.hasOption("f")) {
                    states.add(FileState.FLUSHED);
                }
                StringBuilder sb = new StringBuilder();
                if (states.isEmpty()) {
                        sb.append("No option specified, will remove expired RESERVED and TRANSFERRING files.\n");
                        states.add(FileState.RESERVED);
                        states.add(FileState.TRANSFERRING);
                }
                db.removeExpiredFilesFromSpace(spaceId, states);
                return sb.toString();
        }

        public static final String hh_remove_file = " -id=<file id> | -pnfsId=<pnfsId>  " +
                "# remove file by spacefile id or pnfsid";

        @Transactional
        public String ac_remove_file(Args args)
                throws DataAccessException
        {
                String sid     = args.getOpt("id");
                String sPnfsId = args.getOpt("pnfsId");
                if (sid!=null&&sPnfsId!=null) {
                        return "do not handle \"-id\" and \"-pnfsId\" options simultaneously";
                }
                if (sid!=null) {
                        long id = Long.parseLong(sid);
                        db.removeFile(id);
                        return "removed file with id="+id;
                }
                if (sPnfsId!=null) {
                        PnfsId pnfsId = new PnfsId(sPnfsId);
                        File f = db.getFile(pnfsId);
                        db.removeFile(f.getId());
                        return "removed file with pnfsId="+pnfsId;
                }
                return "please specify  \"-id=\" or \"-pnfsId=\" option";
        }

        private void expireSpaceReservations() throws DataAccessException
        {
                LOGGER.trace("expireSpaceReservations()...");
                if (shouldCleanupExpiredSpaceFiles) {
                    for (File file : db.getExpiredFiles()) {
                                try {
                                        if (file.getPnfsId() != null) {
                                                try {
                                                        pnfs.deletePnfsEntry(file.getPnfsId(), file.getPnfsPath());
                                                } catch (FileNotFoundCacheException ignored) {
                                                }
                                        }
                                        db.removeFile(file.getId());
                                }
                                catch (DataAccessException e) {
                                        LOGGER.error("Failed to remove file {}: {}",
                                                     file, e.getMessage());
                                }
                                catch (CacheException e) {
                                        LOGGER.error("Failed to delete file {}: {}",
                                                     file.getPnfsId(), e.getMessage());
                                }
                        }
                }
                db.expireSpaces();
        }

        private void getValidSpaceTokens(GetSpaceTokensMessage msg) throws DataAccessException {
                List<Space> spaces;
                if(msg.getSpaceTokenId()!=null) {
                        spaces = Collections.singletonList(db.getSpace(msg.getSpaceTokenId()));
                }
                else {
                        spaces = db.getReservedSpaces();
                }
                msg.setSpaceTokenSet(spaces);
        }

        private void getLinkGroups(GetLinkGroupsMessage msg) throws DataAccessException {
                msg.setLinkGroups(db.getLinkGroups());
        }

        private void getLinkGroupNames(GetLinkGroupNamesMessage msg) throws DataAccessException {
                msg.setLinkGroupNames(newArrayList(transform(db.getLinkGroups(), LinkGroup.getName)));
        }

        @Nonnull
        private long[] getSpaceTokens(Subject subject, String description) throws DataAccessException
        {
                Set<Long> spaces = new HashSet<>();
                if (description==null) {
                    for (String s : Subjects.getFqans(subject)) {
                        if (s != null) {
                            FQAN fqan = new FQAN(s);
                            spaces.addAll(db.findSpaceTokensByVoGroupAndRole(fqan.getGroup(), fqan.getRole()));
                        }
                    }
                    spaces.addAll(db.findSpaceTokensByVoGroupAndRole(Subjects.getUserName(subject), ""));
                }
                else {
                    spaces.addAll(db.findSpaceTokensByDescription(description));
                }
                return Longs.toArray(spaces);
        }

        /** Returns true if message is of a type processed exclusively by SpaceManager */
        private boolean isSpaceManagerMessage(Message message)
        {
                return message instanceof Reserve
                        || message instanceof GetSpaceTokensMessage
                        || message instanceof GetLinkGroupsMessage
                        || message instanceof GetLinkGroupNamesMessage
                        || message instanceof Release
                        || message instanceof Use
                        || message instanceof CancelUse
                        || message instanceof GetSpaceMetaData
                        || message instanceof GetSpaceTokens
                        || message instanceof ExtendLifetime
                        || message instanceof GetFileSpaceTokensMessage;
        }

        /** Returns true if message is a notification to which SpaceManager subscribes */
        private boolean isNotificationMessage(Message message)
        {
                return message instanceof PoolFileFlushedMessage
                        || message instanceof PoolRemoveFilesMessage
                        || message instanceof PnfsDeleteEntryNotificationMessage;
        }

        /**
         * Returns true if message is of a type that needs processing by SpaceManager even if
         * SpaceManager is not the intended final destination.
         */
        private boolean isInterceptedMessage(Message message)
        {
                return (message instanceof PoolMgrSelectWritePoolMsg && ((PoolMgrSelectWritePoolMsg) message).getPnfsPath() != null && !message.isReply())
                       || message instanceof DoorTransferFinishedMessage
                       || (message instanceof PoolAcceptFileMessage && ((PoolAcceptFileMessage) message).getFileAttributes().getStorageInfo().getKey("LinkGroup") != null);
        }

        public void messageArrived(final CellMessage envelope,
                                   final Message message)
        {
            LOGGER.trace("messageArrived : type={} value={} from {}",
                         message.getClass().getName(), message, envelope.getSourcePath());

            if (!message.isReply()) {
                if (!isNotificationMessage(message) && !isSpaceManagerMessage(message)) {
                    messageToForward(envelope, message);
                } else if (isSpaceManagerEnabled) {
                    executor.execute(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            processMessage(message);
                            if (message.getReplyRequired()) {
                                try {
                                    envelope.revertDirection();
                                    sendMessage(envelope);
                                }
                                catch (NoRouteToCellException e) {
                                    LOGGER.error("Failed to send reply: {}", e.getMessage());
                                }
                            }
                        }
                    });
                } else if (message.getReplyRequired()) {
                    try {
                        message.setReply(1, "Space manager is disabled in configuration");
                        envelope.revertDirection();
                        sendMessage(envelope);
                    }
                    catch (NoRouteToCellException e) {
                        LOGGER.error("Failed to send reply: {}", e.getMessage());
                    }
                }
            }
        }

        public void messageToForward(final CellMessage envelope, final Message message)
        {
            LOGGER.trace("messageToForward: type={} value={} from {} going to {}",
                         message.getClass().getName(),
                         message,
                         envelope.getSourcePath(),
                         envelope.getDestinationPath());

            final boolean isEnRouteToDoor = message.isReply() || message instanceof DoorTransferFinishedMessage;
            if (!isEnRouteToDoor) {
                envelope.getDestinationPath().insert(poolManager);
            }

            if (envelope.nextDestination()) {
                if (isSpaceManagerEnabled && isInterceptedMessage(message)) {
                    executor.execute(new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            processMessage(message);

                            if (message.getReturnCode() != 0 && !isEnRouteToDoor) {
                                envelope.revertDirection();
                            }

                            try {
                                sendMessage(envelope);
                            } catch (NoRouteToCellException e) {
                                LOGGER.error("Failed to forward message: {}", e.getMessage());
                            }
                        }
                    });
                } else {
                    try {
                        sendMessage(envelope);
                    } catch (NoRouteToCellException e) {
                        LOGGER.error("Failed to forward message: {}", e.getMessage());
                    }
                }
            }
        }

        private void processMessage(Message message)
        {
            try {
                boolean isSuccessful = false;
                for (int attempts = 0; !isSuccessful; attempts++) {
                    try {
                        if (message instanceof PoolRemoveFilesMessage) {
                            // fileRemoved does its own transaction management
                            fileRemoved((PoolRemoveFilesMessage) message);
                        }
                        else {
                            processMessageTransactionally(message);
                        }
                        isSuccessful = true;
                    } catch (TransientDataAccessException | RecoverableDataAccessException e) {
                        if (attempts >= 3) {
                            throw e;
                        }
                        LOGGER.warn("Retriable data access error: {}", e.toString());
                    }
                }
            } catch (SpaceAuthorizationException e) {
                message.setFailedConditionally(CacheException.PERMISSION_DENIED, e.getMessage());
            } catch (NoFreeSpaceException e) {
                message.setFailedConditionally(CacheException.RESOURCE, e.getMessage());
            } catch (SpaceException e) {
                message.setFailedConditionally(CacheException.DEFAULT_ERROR_CODE, e.getMessage());
            } catch (IllegalArgumentException e) {
                LOGGER.error("Message processing failed: {}", e.getMessage(), e);
                message.setFailedConditionally(CacheException.INVALID_ARGS, e.getMessage());
            } catch (DataAccessException e) {
                LOGGER.error("Message processing failed: {}", e.toString());
                message.setFailedConditionally(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                               "Internal failure during space management");
            } catch (RuntimeException e) {
                LOGGER.error("Message processing failed: {}", e.getMessage(), e);
                message.setFailedConditionally(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                               "Internal failure during space management");
            }
        }

        @Transactional(rollbackFor = { SpaceException.class })
        private void processMessageTransactionally(Message message) throws SpaceException
        {
            if (message instanceof PoolMgrSelectWritePoolMsg) {
                selectPool((PoolMgrSelectWritePoolMsg) message);
            }
            else if (message instanceof PoolAcceptFileMessage) {
                PoolAcceptFileMessage poolRequest = (PoolAcceptFileMessage) message;
                if (message.isReply()) {
                    transferStarted(poolRequest.getPnfsId(), poolRequest.getReturnCode() == 0);
                }
                else {
                    transferStarting(poolRequest);
                }
            }
            else if (message instanceof DoorTransferFinishedMessage) {
                transferFinished((DoorTransferFinishedMessage) message);
            }
            else if (message instanceof Reserve) {
                reserveSpace((Reserve) message);
            }
            else if (message instanceof GetSpaceTokensMessage) {
                getValidSpaceTokens((GetSpaceTokensMessage) message);
            }
            else if (message instanceof GetLinkGroupsMessage) {
                getLinkGroups((GetLinkGroupsMessage) message);
            }
            else if (message instanceof GetLinkGroupNamesMessage) {
                getLinkGroupNames((GetLinkGroupNamesMessage) message);
            }
            else if (message instanceof Release) {
                releaseSpace((Release) message);
            }
            else if (message instanceof Use) {
                useSpace((Use) message);
            }
            else if (message instanceof CancelUse) {
                cancelUseSpace((CancelUse) message);
            }
            else if (message instanceof GetSpaceMetaData) {
                getSpaceMetaData((GetSpaceMetaData) message);
            }
            else if (message instanceof GetSpaceTokens) {
                getSpaceTokens((GetSpaceTokens) message);
            }
            else if (message instanceof ExtendLifetime) {
                extendLifetime((ExtendLifetime) message);
            }
            else if (message instanceof PoolFileFlushedMessage) {
                fileFlushed((PoolFileFlushedMessage) message);
            }
            else if (message instanceof GetFileSpaceTokensMessage) {
                getFileSpaceTokens((GetFileSpaceTokensMessage) message);
            }
            else if (message instanceof PnfsDeleteEntryNotificationMessage) {
                namespaceEntryDeleted((PnfsDeleteEntryNotificationMessage) message);
            }
            else {
                throw new RuntimeException(
                        "Unexpected " + message.getClass() + ": Please report this to support@dcache.org");
            }
        }

        @Override
        public void run(){
                if(Thread.currentThread() == expireSpaceReservations) {
                        while(true) {
                                expireSpaceReservations();
                                try{
                                        Thread.sleep(expireSpaceReservationsPeriod);
                                }
                                catch (InterruptedException ie) {
                                    LOGGER.trace("expire SpaceReservations thread has been interrupted");
                                    return;
                                }
                                catch (DataAccessException e) {
                                        LOGGER.error("expireSpaceReservations failed: {}", e.getMessage());
                                }
                                catch (Exception e) {
                                        LOGGER.error("expireSpaceReservations failed: {}", e.toString());
                                }
                        }
                }
                else if(Thread.currentThread() == updateLinkGroups) {
                        while(true) {
                                updateLinkGroups();
                                synchronized(updateLinkGroupsSyncObject) {
                                        try {
                                                updateLinkGroupsSyncObject.wait(currentUpdateLinkGroupsPeriod);
                                        }
                                        catch (InterruptedException ie) {
                                                LOGGER.trace("update LinkGroup thread has been interrupted");
                                                return;
                                        }
                                }
                        }
                }
        }

        private void updateLinkGroupAuthorizationFile() {
                java.io.File file = linkGroupAuthorizationFileName;
                if(file == null) {
                        return;
                }
                if(!file.exists()) {
                        linkGroupAuthorizationFile = null;
                }
                long lastModified = file.lastModified();
                if (linkGroupAuthorizationFile == null|| lastModified >= linkGroupAuthorizationFileLastUpdateTimestamp) {
                        linkGroupAuthorizationFileLastUpdateTimestamp = lastModified;
                        try {
                                linkGroupAuthorizationFile =
                                        new LinkGroupAuthorizationFile(file);
                        }
                        catch(Exception e) {
                                LOGGER.error("failed to parse LinkGroupAuthorizationFile: {}",
                                             e.getMessage());
                        }
                }
        }

        private void updateLinkGroups() {
                currentUpdateLinkGroupsPeriod = EAGER_LINKGROUP_UPDATE_PERIOD;
                long currentTime = System.currentTimeMillis();
                Collection<PoolLinkGroupInfo> linkGroupInfos = Utils.linkGroupInfos(poolMonitor.getPoolSelectionUnit(), poolMonitor.getCostModule()).values();
                if (linkGroupInfos.isEmpty()) {
                    return;
                }

                currentUpdateLinkGroupsPeriod = updateLinkGroupsPeriod;

                updateLinkGroupAuthorizationFile();
                for (PoolLinkGroupInfo info : linkGroupInfos) {
                        String linkGroupName = info.getName();
                        long avalSpaceInBytes = info.getAvailableSpaceInBytes();
                        VOInfo[] vos = null;
                        boolean onlineAllowed = info.isOnlineAllowed();
                        boolean nearlineAllowed = info.isNearlineAllowed();
                        boolean replicaAllowed = info.isReplicaAllowed();
                        boolean outputAllowed = info.isOutputAllowed();
                        boolean custodialAllowed = info.isCustodialAllowed();
                        if (linkGroupAuthorizationFile != null) {
                                LinkGroupAuthorizationRecord record =
                                        linkGroupAuthorizationFile
                                        .getLinkGroupAuthorizationRecord(linkGroupName);
                                if (record != null) {
                                        vos = record.getVOInfoArray();
                                }
                        }
                        try {
                                db.updateLinkGroup(linkGroupName,
                                                   avalSpaceInBytes,
                                                   currentTime,
                                                   onlineAllowed,
                                                   nearlineAllowed,
                                                   replicaAllowed,
                                                   outputAllowed,
                                                   custodialAllowed,
                                                   vos);
                        } catch (DataAccessException sqle) {
                                LOGGER.error("update of linkGroup {} failed: {}",
                                             linkGroupName, sqle.getMessage());
                        }
                }
                latestLinkGroupUpdateTime = currentTime;
        }

        private void releaseSpace(Release release)
                throws DataAccessException, SpaceException
        {
                LOGGER.trace("releaseSpace({})", release);

                long spaceToken = release.getSpaceToken();
                Long spaceToReleaseInBytes = release.getReleaseSizeInBytes();
                if (spaceToReleaseInBytes != null) {
                    throw new UnsupportedOperationException("partial release is not supported yet");
                }

                Space space = db.selectSpaceForUpdate(spaceToken);
                if (space.getState() == SpaceState.RELEASED) {
                    /* Stupid way to signal that it isn't found, but there is no other way at the moment. */
                    throw new EmptyResultDataAccessException("Space reservation " + spaceToken + " was already released.", 1);
                }
                Subject subject =  release.getSubject();
                authorizationPolicy.checkReleasePermission(subject, space);
                db.updateSpace(space,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               SpaceState.RELEASED);
        }

        private void reserveSpace(Reserve reserve)
                throws DataAccessException, SpaceException
        {
                if (reserve.getRetentionPolicy()==null) {
                        throw new IllegalArgumentException("reserveSpace : retentionPolicy=null is not supported");
                }

                Space space = reserveSpace(reserve.getSubject(),
                                           reserve.getSizeInBytes(),
                                           (reserve.getAccessLatency() == null ?
                                                   defaultAccessLatency : reserve.getAccessLatency()),
                                           reserve.getRetentionPolicy(),
                                           reserve.getLifetime(),
                                           reserve.getDescription(),
                                           null,
                                           null,
                                           null);
                reserve.setSpaceToken(space.getId());
        }

        private void useSpace(Use use)
                throws DataAccessException, SpaceException
        {
                LOGGER.trace("useSpace({})", use);
                long reservationId = use.getSpaceToken();
                Subject subject = use.getSubject();
                long sizeInBytes = use.getSizeInBytes();
                String pnfsPath = use.getPnfsName();
                PnfsId pnfsId = use.getPnfsId();
                long lifetime = use.getLifetime();
                long fileId = useSpace(reservationId,
                                       subject,
                                       sizeInBytes,
                                       lifetime,
                                       pnfsPath,
                                       pnfsId);
                use.setFileId(fileId);
        }

        private void transferStarting(PoolAcceptFileMessage message) throws DataAccessException, SpaceException
        {
            LOGGER.trace("transferStarting({})", message);
            PnfsId pnfsId = checkNotNull(message.getPnfsId());
            FileAttributes fileAttributes = message.getFileAttributes();
            Subject subject = message.getSubject();
            String linkGroupName = checkNotNull(fileAttributes.getStorageInfo().getKey("LinkGroup"));
            String spaceToken = fileAttributes.getStorageInfo().getKey("SpaceToken");
            String fileId = fileAttributes.getStorageInfo().setKey("SpaceFileId", null);
            if (fileId != null) {
                /* This takes care of records created by SRM before
                 * transfer has started
                 */
                File f = db.selectFileForUpdate(Long.parseLong(fileId));
                if (f.getPnfsId() != null) {
                    throw new DataIntegrityViolationException("File is already assigned a PNFS ID.");
                }
                db.updateFile(null, null, pnfsId, null, null, null, null, f);
            } else if (spaceToken != null) {
                LOGGER.trace("transferStarting: file is not " +
                                     "found, found default space " +
                                     "token, calling insertFile()");
                long lifetime = 1000 * 60 * 60;
                useSpace(Long.parseLong(spaceToken),
                        subject,
                        message.getPreallocated(),
                        lifetime,
                        null,
                        pnfsId);
            } else {
                LOGGER.trace("transferStarting: file is not found, no prior reservations for this file");

                long sizeInBytes = message.getPreallocated();
                long lifetime    = 1000*60*60;
                String description = null;
                LinkGroup linkGroup = db.getLinkGroupByName(linkGroupName);
                VOInfo voInfo = authorizationPolicy.checkReservePermission(subject, linkGroup);

                Space space = db.insertSpace(voInfo.getVoGroup(),
                                             voInfo.getVoRole(),
                                             fileAttributes.getRetentionPolicy(),
                                             fileAttributes.getAccessLatency(),
                                             linkGroup.getId(),
                                             sizeInBytes,
                                             lifetime,
                                             description,
                                             SpaceState.RESERVED,
                                             0,
                                             0);
                db.insertFile(space.getId(),
                              voInfo.getVoGroup(),
                              voInfo.getVoRole(),
                              sizeInBytes,
                              lifetime,
                              null,
                              pnfsId);

                /* One could inject SpaceToken and SpaceTokenDescription into storage
                 * info at this point, but since the space reservation is implicit and
                 * short lived, this information will not be of much use.
                 */
            }
        }

        private void transferStarted(PnfsId pnfsId,boolean success)
                throws DataAccessException
        {
            try {
                LOGGER.trace("transferStarted({},{})", pnfsId, success);
                File f = db.selectFileForUpdate(pnfsId);
                if (f.getState() == FileState.RESERVED) {
                    if(!success) {
                            if (f.getPnfsPath() != null) {
                                db.clearPnfsIdOfFile(f.getId());
                            } else {
                                /* This reservation was created by space manager
                                 * when the transfer started. Delete it.
                                 */
                                db.removeFile(f.getId());

                                /* TODO: If we also created the reservation, we should
                                 * release it at this point, but at the moment we cannot
                                 * know who created it. It will eventually expire
                                 * automatically.
                                 */
                            }
                    } else {
                            db.updateFile(null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          FileState.TRANSFERRING,
                                          null, f);
                    }
                }
            } catch (EmptyResultDataAccessException e) {
                LOGGER.trace("transferStarted failed: {}", e.getMessage());
            }
        }

        private void transferFinished(DoorTransferFinishedMessage finished)
                throws DataAccessException
        {
                boolean weDeleteStoredFileRecord = shouldDeleteStoredFileRecord;
                PnfsId pnfsId = finished.getPnfsId();
                long size = finished.getFileAttributes().getSize();
                boolean success = finished.getReturnCode() == 0;
                LOGGER.trace("transferFinished({},{})", pnfsId, success);
                File f;
                try {
                        f = db.selectFileForUpdate(pnfsId);
                }
                catch (EmptyResultDataAccessException e) {
                        LOGGER.trace("failed to find file {}: {}", pnfsId,
                                     e.getMessage());
                        return;
                }
                long spaceId = f.getSpaceId();
                if(f.getState() == FileState.RESERVED ||
                   f.getState() == FileState.TRANSFERRING) {
                        if(success) {
                                if(shouldReturnFlushedSpaceToReservation && weDeleteStoredFileRecord) {
                                        RetentionPolicy rp = db.getSpace(spaceId).getRetentionPolicy();
                                        if(rp.equals(RetentionPolicy.CUSTODIAL)) {
                                                //we do not delete it here, since the
                                                // file will get flushed and we will need
                                                // to account for that
                                                weDeleteStoredFileRecord = false;
                                        }
                                }
                                if(weDeleteStoredFileRecord) {
                                        LOGGER.trace("file transferred, deleting file record");
                                        db.removeFile(f.getId());
                                }
                                else {
                                        db.updateFile(null,
                                                      null,
                                                      null,
                                                      size,
                                                      null,
                                                      FileState.STORED,
                                                      null, f);
                                }
                        }
                        else {
                                if (f.getPnfsPath() != null) {
                                    db.removePnfsIdAndChangeStateOfFile(f.getId(), FileState.RESERVED);
                                } else {
                                    /* This reservation was created by space manager
                                     * when the transfer started. Delete it.
                                     */
                                    db.removeFile(f.getId());

                                    /* TODO: If we also created the reservation, we should
                                     * release it at this point, but at the moment we cannot
                                     * know who created it. It will eventually expire
                                     * automatically.
                                     */
                                }
                        }
                }
                else {
                        LOGGER.trace("transferFinished({}): file state={}",
                                     pnfsId, f.getState());
                }
        }

        private void  fileFlushed(PoolFileFlushedMessage fileFlushed) throws DataAccessException
        {
                if(!shouldReturnFlushedSpaceToReservation) {
                        return;
                }
                PnfsId pnfsId = fileFlushed.getPnfsId();
                LOGGER.trace("fileFlushed({})", pnfsId);
                FileAttributes fileAttributes = fileFlushed.getFileAttributes();
                AccessLatency ac = fileAttributes.getAccessLatency();
                if (ac.equals(AccessLatency.ONLINE)) {
                        LOGGER.trace("File Access latency is ONLINE " +
                                             "fileFlushed does nothing");
                        return;
                }
                long size = fileAttributes.getSize();
                try {
                        File f = db.selectFileForUpdate(pnfsId);
                        if(f.getState() == FileState.STORED) {
                                if(shouldDeleteStoredFileRecord) {
                                        LOGGER.trace("returnSpaceToReservation, " +
                                                             "deleting file record");
                                        db.removeFile(f.getId());
                                }
                                else {
                                        db.updateFile(null,
                                                      null,
                                                      null,
                                                      size,
                                                      null,
                                                      FileState.FLUSHED,
                                                      null, f);
                                }
                        }
                        else {
                                LOGGER.trace("returnSpaceToReservation({}): " +
                                                     "file state={}", pnfsId, f.getState());
                        }

                }
                catch (EmptyResultDataAccessException e) {
                    /* if this file is not in srmspacefile table, silently quit */
                }
        }

        private void fileRemoved(PoolRemoveFilesMessage fileRemoved)
        {
                for (String pnfsId : fileRemoved.getFiles()) {
                        try {
                                fileRemoved(pnfsId);
                        }
                        catch (IllegalArgumentException e) {
                                LOGGER.error("badly formed PNFS-ID: {}", pnfsId);
                        }
                        catch (DataAccessException sqle) {
                                LOGGER.trace("failed to remove file from space: {}",
                                             sqle.getMessage());
                                LOGGER.trace("fileRemoved({}): file not in a " +
                                                     "reservation, do nothing", pnfsId);
                        }
                }
        }

        @Transactional
        private void fileRemoved(String pnfsId)
        {
            LOGGER.trace("fileRemoved({})", pnfsId);
            File f = db.selectFileForUpdate(new PnfsId(pnfsId));
            if ((f.getState() != FileState.RESERVED && f.getState() != FileState.TRANSFERRING) || f.getPnfsPath() == null) {
                db.removeFile(f.getId());
            } else if (f.getState() == FileState.TRANSFERRING) {
                db.removePnfsIdAndChangeStateOfFile(f.getId(), FileState.RESERVED);
            }
        }

        private void cancelUseSpace(CancelUse cancelUse)
                throws DataAccessException, SpaceException
        {
                LOGGER.trace("cancelUseSpace({})", cancelUse);
                long reservationId = cancelUse.getSpaceToken();
                String pnfsPath    = cancelUse.getPnfsName();
                File f;
                try {
                        f = db.selectFileFromSpaceForUpdate(pnfsPath, reservationId);
                }
                catch (IncorrectResultSizeDataAccessException sqle) {
                        //
                        // this is not an error: we are here in two cases
                        //   1) no transient file found - OK
                        //   2) more than one transient file found, less OK, but
                        //      remaining transient files will be garbage collected after timeout
                        //
                        return;
                }
                if(f.getState() == FileState.RESERVED ||
                   f.getState() == FileState.TRANSFERRING) {
                        try {
                                if (f.getPnfsId() != null) {
                                        try {
                                        pnfs.deletePnfsEntry(f.getPnfsId(), pnfsPath);
                                        } catch (FileNotFoundCacheException ignored) {
                                        }
                                }
                                db.removeFile(f.getId());
                        } catch (CacheException e) {
                            throw new SpaceException("Failed to delete " + pnfsPath +
                                                     " while attempting to cancel its reservation in space " +
                                                     reservationId + ": " + e.getMessage(), e);
                        }
                }
        }

        private Space reserveSpace(Subject subject,
                                   long sizeInBytes,
                                   AccessLatency latency ,
                                   RetentionPolicy policy,
                                   long lifetime,
                                   String description,
                                   ProtocolInfo protocolInfo,
                                   FileAttributes fileAttributes,
                                   PnfsId pnfsId)
                throws DataAccessException, SpaceException
        {
                LOGGER.trace("reserveSpace( subject={}, sz={}, latency={}, " +
                                     "policy={}, lifetime={}, description={}", subject.getPrincipals(),
                             sizeInBytes, latency, policy, lifetime, description);
                List<LinkGroup> linkGroups = db.findLinkGroups(sizeInBytes, latency, policy, latestLinkGroupUpdateTime);
                if(linkGroups.isEmpty()) {
                        LOGGER.warn("failed to find matching linkgroup");
                        throw new NoFreeSpaceException(" no space available");
                }
                //
                // filter out groups we are not authorized to use
                //
                Map<String,VOInfo> linkGroupNameVoInfoMap = new HashMap<>();
                for (LinkGroup linkGroup : linkGroups) {
                        try {
                                VOInfo voInfo =
                                        authorizationPolicy.checkReservePermission(subject,
                                                                                   linkGroup);
                                linkGroupNameVoInfoMap.put(linkGroup.getName(),voInfo);
                        }
                        catch (SpaceAuthorizationException ignored) {
                        }
                }
                if(linkGroupNameVoInfoMap.isEmpty()) {
                        LOGGER.warn("failed to find linkgroup where user is " +
                                            "authorized to reserve space.");
                        throw new SpaceAuthorizationException("Failed to find LinkGroup where user is authorized to reserve space.");
                }
                List<String> linkGroupNames = new ArrayList<>(linkGroupNameVoInfoMap.keySet());
                LOGGER.trace("Found {} linkgroups protocolInfo={}, " +
                                     "storageInfo={}, pnfsId={}", linkGroups.size(),
                             protocolInfo, fileAttributes, pnfsId);
                if (linkGroupNameVoInfoMap.size()>1 &&
                    protocolInfo != null &&
                    fileAttributes != null) {
                        try {
                                linkGroupNames = findLinkGroupForWrite(protocolInfo, fileAttributes, linkGroupNames);
                                if(linkGroupNames.isEmpty()) {
                                        throw new SpaceAuthorizationException("PoolManagerSelectLinkGroupForWriteMessage: Failed to find LinkGroup where user is authorized to reserve space.");
                                }
                        }
                        catch (SpaceAuthorizationException e)  {
                                LOGGER.warn("authorization problem: {}",
                                            e.getMessage());
                                throw e;
                        }
                        catch(Exception e) {
                                throw new SpaceException("Internal error : Failed to get list of link group ids from Pool Manager "+e.getMessage());
                        }

                }
                String linkGroupName = linkGroupNames.get(0);
                VOInfo voInfo        = linkGroupNameVoInfoMap.get(linkGroupName);
                LinkGroup linkGroup  = null;
                for (LinkGroup lg : linkGroups) {
                        if (lg.getName().equals(linkGroupName) ) {
                                linkGroup = lg;
                                break;
                        }
                }
                LOGGER.trace("Chose linkgroup {}", linkGroup);
                return db.insertSpace(voInfo.getVoGroup(),
                                      voInfo.getVoRole(),
                                      policy,
                                      latency,
                                      linkGroup.getId(),
                                      sizeInBytes,
                                      lifetime,
                                      description,
                                      SpaceState.RESERVED,
                                      0,
                                      0);
        }

        private LinkGroup findLinkGroupForWrite(Subject subject, ProtocolInfo protocolInfo,
                                                FileAttributes fileAttributes, long size)
                throws DataAccessException
        {
            List<LinkGroup> linkGroups =
                    db.findLinkGroups(size, fileAttributes.getAccessLatency(), fileAttributes.getRetentionPolicy(), latestLinkGroupUpdateTime);
            List<String> linkGroupNames = new ArrayList<>();
            for (LinkGroup linkGroup : linkGroups) {
                try {
                    authorizationPolicy.checkReservePermission(subject, linkGroup);
                    linkGroupNames.add(linkGroup.getName());
                }
                catch (SpaceAuthorizationException ignored) {
                }
            }
            linkGroupNames = findLinkGroupForWrite(protocolInfo, fileAttributes, linkGroupNames);
            LOGGER.trace("Found {} linkgroups protocolInfo={}, fileAttributes={}",
                         linkGroups.size(), protocolInfo, fileAttributes);

            if (!linkGroupNames.isEmpty()) {
                String linkGroupName = linkGroupNames.get(0);
                for (LinkGroup lg : linkGroups) {
                    if (lg.getName().equals(linkGroupName) ) {
                        return lg;
                    }
                }
            }
            return null;
        }

        private List<String> findLinkGroupForWrite(ProtocolInfo protocolInfo, FileAttributes fileAttributes,
                                                   Collection<String> linkGroups)
        {
                String protocol = protocolInfo.getProtocol() + '/' + protocolInfo.getMajorVersion();
                String hostName =
                        (protocolInfo instanceof IpProtocolInfo)
                                ? ((IpProtocolInfo) protocolInfo).getSocketAddress().getAddress().getHostAddress()
                                : null;

                List<String> outputLinkGroups = new ArrayList<>(linkGroups.size());
                for (String linkGroup: linkGroups) {
                    PoolPreferenceLevel[] level =
                            poolMonitor.getPoolSelectionUnit().match(PoolSelectionUnit.DirectionType.WRITE,
                                    hostName,
                                    protocol,
                                    fileAttributes,
                                    linkGroup);
                    if (level.length > 0) {
                        outputLinkGroups.add(linkGroup);
                    }
                }
                return outputLinkGroups;
        }

        private long useSpace(long reservationId,
                              Subject subject,
                              long sizeInBytes,
                              long lifetime,
                              String pnfsPath,
                              PnfsId pnfsId)
                throws DataAccessException, SpaceException
        {
            String effectiveGroup;
            String effectiveRole;
            String primaryFqan = Subjects.getPrimaryFqan(subject);
            if (primaryFqan != null) {
                FQAN fqan = new FQAN(primaryFqan);
                effectiveGroup = fqan.getGroup();
                effectiveRole = fqan.getRole();
            } else {
                effectiveGroup = Subjects.getUserName(subject);
                effectiveRole = null;
            }
            return db.insertFile(reservationId,
                                 effectiveGroup,
                                 effectiveRole,
                                 sizeInBytes,
                                 lifetime,
                                 pnfsPath,
                                 pnfsId);
        }

        /**
         * Called upon intercepting PoolMgrSelectWritePoolMsg requests.
         *
         * Injects the link group name into the request message. Also adds SpaceToken, LinkGroup,
         * and SpaceFileId flags to StorageInfo. These are accessed when space manager intercepts
         * the subsequent PoolAcceptFileMessage.
         */
        private void selectPool(PoolMgrSelectWritePoolMsg selectWritePool) throws DataAccessException
        {
            LOGGER.trace("selectPool({})", selectWritePool);
            FileAttributes fileAttributes = selectWritePool.getFileAttributes();
            String defaultSpaceToken = fileAttributes.getStorageInfo().getMap().get("writeToken");
            Subject subject = selectWritePool.getSubject();
            boolean hasIdentity =
                    !Subjects.getFqans(subject).isEmpty() || Subjects.getUserName(subject) != null;

            String pnfsPath = new FsPath(checkNotNull(selectWritePool.getPnfsPath())).toString();
            File file = db.getUnboundFile(pnfsPath);
            if (file != null) {
                /*
                 * This takes care of records created by SRM before
                 * transfer has started
                 */
                Space space = db.getSpace(file.getSpaceId());
                LinkGroup linkGroup = db.getLinkGroup(space.getLinkGroupId());
                String linkGroupName = linkGroup.getName();
                selectWritePool.setLinkGroup(linkGroupName);

                StorageInfo storageInfo = fileAttributes.getStorageInfo();
                storageInfo.setKey("SpaceToken", Long.toString(space.getId()));
                storageInfo.setKey("LinkGroup", linkGroupName);
                fileAttributes.setAccessLatency(space.getAccessLatency());
                fileAttributes.setRetentionPolicy(space.getRetentionPolicy());

                if (fileAttributes.getSize() == 0 && file.getSizeInBytes() > 1) {
                    fileAttributes.setSize(file.getSizeInBytes());
                }
                if (space.getDescription() != null) {
                    storageInfo.setKey("SpaceTokenDescription", space.getDescription());
                }
                storageInfo.setKey("SpaceFileId", Long.toString(file.getId()));
                LOGGER.trace("selectPool: found linkGroup = {}, " +
                                     "forwarding message", linkGroupName);
            } else if (defaultSpaceToken != null) {
                LOGGER.trace("selectPool: file is not " +
                                     "found, found default space " +
                                     "token, calling insertFile()");
                Space space = db.getSpace(Long.parseLong(defaultSpaceToken));
                LinkGroup linkGroup = db.getLinkGroup(space.getLinkGroupId());
                String linkGroupName = linkGroup.getName();
                selectWritePool.setLinkGroup(linkGroupName);

                StorageInfo storageInfo = selectWritePool.getStorageInfo();
                storageInfo.setKey("SpaceToken", Long.toString(space.getId()));
                storageInfo.setKey("LinkGroup", linkGroupName);
                fileAttributes.setAccessLatency(space.getAccessLatency());
                fileAttributes.setRetentionPolicy(space.getRetentionPolicy());

                if (space.getDescription() != null) {
                    storageInfo.setKey("SpaceTokenDescription", space.getDescription());
                }
                LOGGER.trace("selectPool: found linkGroup = {}, " +
                                     "forwarding message", linkGroupName);
            } else if (shouldReserveSpaceForNonSrmTransfers && hasIdentity) {
                LOGGER.trace("selectPool: file is " +
                                     "not found, no prior " +
                                     "reservations for this file");

                LinkGroup linkGroup =
                        findLinkGroupForWrite(subject, selectWritePool
                                .getProtocolInfo(), fileAttributes, selectWritePool.getPreallocated());
                if (linkGroup != null) {
                    String linkGroupName = linkGroup.getName();
                    selectWritePool.setLinkGroup(linkGroupName);
                    fileAttributes.getStorageInfo().setKey("LinkGroup", linkGroupName);
                    LOGGER.trace("selectPool: found linkGroup = {}, " +
                                         "forwarding message", linkGroupName);
                } else {
                    LOGGER.trace("selectPool: did not find linkGroup that can " +
                                         "hold this file, processing file without space reservation.");
                }
            } else {
                LOGGER.trace("selectPool: file is " +
                                     "not found, no prior " +
                                     "reservations for this file " +
                                     "shouldReserveSpaceForNonSrmTransfers={} " +
                                     "subject={}",
                             shouldReserveSpaceForNonSrmTransfers,
                             subject.getPrincipals());
            }
        }

        private void namespaceEntryDeleted(PnfsDeleteEntryNotificationMessage msg) throws DataAccessException
        {
            try {
                File f = db.selectFileForUpdate(msg.getPnfsId());
                LOGGER.trace("Marking file as deleted {}", f);
                if ((f.getState() != FileState.RESERVED && f.getState() != FileState.TRANSFERRING) || f.getPnfsPath() == null) {
                    db.updateFile(null, null, null, null, null, null, true, f);
                } else if (f.getState() == FileState.TRANSFERRING) {
                    db.removePnfsIdAndChangeStateOfFile(f.getId(), FileState.RESERVED);
                }
            } catch (EmptyResultDataAccessException ignored) {
            }
        }

        private void getSpaceMetaData(GetSpaceMetaData gsmd) throws IllegalArgumentException {
                long[] tokens = gsmd.getSpaceTokens();
                if(tokens == null) {
                        throw new IllegalArgumentException("null space tokens");
                }
                Space[] spaces = new Space[tokens.length];
                for(int i=0;i<spaces.length; ++i){

                        Space space = null;
                        try {
                                space = db.getSpace(tokens[i]);
                                // Expiration of space reservations is a background activity and is not immediate.
                                // S2 tests however expect the state to be accurate at any point, hence we report
                                // the state as EXPIRED even when the actual state has not been updated in the
                                // database yet. See usecase.CheckGarbageSpaceCollector (S2).
                                if (space.getState().equals(SpaceState.RESERVED)) {
                                        long expirationTime = space.getExpirationTime();
                                        if (expirationTime > -1 && expirationTime - System.currentTimeMillis() <= 0) {
                                                space.setState(SpaceState.EXPIRED);
                                        }
                                }
                        }
                        catch(EmptyResultDataAccessException e) {
                                LOGGER.error("failed to find space {}: {}",
                                             tokens[i], e.getMessage());
                        }
                        spaces[i] = space;
                }
                gsmd.setSpaces(spaces);
        }

        private void getSpaceTokens(GetSpaceTokens gst) throws DataAccessException
        {
                String description = gst.getDescription();
                long [] tokens = getSpaceTokens(gst.getSubject(), description);
                gst.setSpaceToken(tokens);
        }

        private void getFileSpaceTokens(GetFileSpaceTokensMessage getFileTokens)
                throws DataAccessException
        {
                PnfsId pnfsId = getFileTokens.getPnfsId();
                FsPath pnfsPath = getFileTokens.getPnfsPath();
                getFileTokens.setSpaceToken(Longs.toArray(db.getSpaceTokensOfFile(pnfsId, pnfsPath)));
        }

        private void extendLifetime(ExtendLifetime extendLifetime) throws DataAccessException
        {
                long token            = extendLifetime.getSpaceToken();
                long newLifetime      = extendLifetime.getNewLifetime();
                Space space = db.selectSpaceForUpdate(token);
                if(SpaceState.isFinalState(space.getState())) {
                        throw new DataIntegrityViolationException("Space is already released");
                }
                long creationTime = space.getCreationTime();
                long lifetime = space.getLifetime();
                if(lifetime == -1) {
                        return;
                }
                if(newLifetime == -1) {
                        db.updateSpace(space, null, null, null, null, null, null, newLifetime, null, null);
                        return;
                }
                long currentTime = System.currentTimeMillis();
                long remainingLifetime = creationTime+lifetime-currentTime;
                if(remainingLifetime > newLifetime) {
                        return;
                }
                db.updateSpace(space, null, null, null, null, null, null, newLifetime, null, null);
        }
}

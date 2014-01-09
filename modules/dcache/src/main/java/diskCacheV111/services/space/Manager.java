// -*- c-basic-offset: 8; -*-
//______________________________________________________________________________
//
// Space Manager - cell that handles space reservation management in SRM
//                 essentially a layer on top of a database
// database schema is described in ManagerSchemaConstants
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
// $Id: Manager.java 9764 2008-07-07 17:48:24Z litvinse $
// $Author: litvinse $
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
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;
import javax.sql.DataSource;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getFirst;
import static java.util.Arrays.asList;

/**
 *   <pre> Space Manager dCache service provides ability
 *    \to reserve space in the pool linkGroups
 *
 *
 * @author  timur
 */
public final class Manager
        extends AbstractCellComponent
        implements CellCommandListener,
                   CellMessageReceiver,
                   Runnable {

        private static final long EAGER_LINKGROUP_UPDATE_PERIOD = 1000;

        private long updateLinkGroupsPeriod;
        private long currentUpdateLinkGroupsPeriod = EAGER_LINKGROUP_UPDATE_PERIOD;
        private long expireSpaceReservationsPeriod;

        private boolean deleteStoredFileRecord;

        private Thread updateLinkGroups;
        private Thread expireSpaceReservations;

        private AccessLatency     defaultAccessLatency;
        private RetentionPolicy defaultRetentionPolicy;

        private boolean reserveSpaceForNonSRMTransfers;
        private boolean returnFlushedSpaceToReservation;
        private boolean cleanupExpiredSpaceFiles;
        private String linkGroupAuthorizationFileName;
        private boolean spaceManagerEnabled;

        private CellPath poolManager;
        private PnfsHandler pnfs;

        private SpaceManagerAuthorizationPolicy authorizationPolicy;

        private Executor executor;

        private static final Logger logger = LoggerFactory.getLogger(Manager.class);
        private PoolMonitor poolMonitor;
        private JdbcTemplate jdbcTemplate;

        private final RowMapper<Space> spaceReservationMapper = new RowMapper<Space>()
        {
            @Override
            public Space mapRow(ResultSet set, int rowNum) throws SQLException
            {
                return new Space(set.getLong("id"),
                        set.getString("vogroup"),
                        set.getString("vorole"),
                        RetentionPolicy.getRetentionPolicy(set.getInt("retentionPolicy")),
                        AccessLatency.getAccessLatency(set.getInt("accessLatency")),
                        set.getLong("linkgroupid"),
                        set.getLong("sizeinbytes"),
                        set.getLong("creationtime"),
                        set.getLong("lifetime"),
                        set.getString("description"),
                        SpaceState.getState(set.getInt("state")),
                        set.getLong("usedspaceinbytes"),
                        set.getLong("allocatedspaceinbytes"));
            }
        };
        private final RowMapper<LinkGroup> linkGroupMapper = new RowMapper<LinkGroup>()
        {
            @Override
            public LinkGroup mapRow(ResultSet set, int rowNum) throws SQLException
            {
                LinkGroup lg = new LinkGroup();
                long id = set.getLong("id");
                lg.setId(id);
                lg.setName(set.getString("name"));
                lg.setFreeSpace(set.getLong("freeSpaceInBytes"));
                lg.setUpdateTime(set.getLong("lastUpdateTime"));
                lg.setOnlineAllowed(set.getBoolean("onlineAllowed"));
                lg.setNearlineAllowed(set.getBoolean("nearlineAllowed"));
                lg.setReplicaAllowed(set.getBoolean("replicaAllowed"));
                lg.setOutputAllowed(set.getBoolean("outputAllowed"));
                lg.setCustodialAllowed(set.getBoolean("custodialAllowed"));
                lg.setReservedSpaceInBytes(set.getLong("reservedspaceinbytes"));
                List<VOInfo> vos = jdbcTemplate.query(LinkGroupIO.SELECT_LINKGROUP_VO, voInfoMapper, id);
                lg.setVOs(vos.toArray(new VOInfo[vos.size()]));
                return lg;
            }
        };
        private final RowMapper<File> fileMapper = new RowMapper<File>()
        {
            @Override
            public File mapRow(ResultSet set, int rowNum) throws SQLException
            {
                String pnfsId = set.getString("pnfsId");
                return new File(set.getLong("id"),
                        set.getString("vogroup"),
                        set.getString("vorole"),
                        set.getLong("spacereservationid"),
                        set.getLong("sizeinbytes"),
                        set.getLong("creationtime"),
                        set.getLong("lifetime"),
                        set.getString("pnfspath"),
                        (pnfsId != null) ? new PnfsId(pnfsId) : null,
                        FileState.getState(set.getInt("state")),
                        (set.getObject("deleted") != null) ? set.getInt("deleted") : 0);
            }
        };
        private final RowMapper<VOInfo> voInfoMapper = new RowMapper<VOInfo>()
        {
            @Override
            public VOInfo mapRow(ResultSet rs, int rowNum) throws SQLException
            {
                return new VOInfo(rs.getString("vogroup"), rs.getString("vorole"));
            }
        };

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
                this.spaceManagerEnabled = enabled;
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
        public void setReserveSpaceForNonSRMTransfers(boolean reserveSpaceForNonSRMTransfers)
        {
                this.reserveSpaceForNonSRMTransfers = reserveSpaceForNonSRMTransfers;
        }

        @Required
        public void setDeleteStoredFileRecord(boolean  deleteStoredFileRecord)
        {
                this.deleteStoredFileRecord = deleteStoredFileRecord;
        }

        @Required
        public void setCleanupExpiredSpaceFiles(boolean cleanupExpiredSpaceFiles)
        {
                this.cleanupExpiredSpaceFiles = cleanupExpiredSpaceFiles;
        }

        @Required
        public void setReturnFlushedSpaceToReservation(boolean returnFlushedSpaceToReservation)
        {
                this.returnFlushedSpaceToReservation = returnFlushedSpaceToReservation;
        }

        @Required
        public void setLinkGroupAuthorizationFileName(String linkGroupAuthorizationFileName)
        {
                this.linkGroupAuthorizationFileName = linkGroupAuthorizationFileName;
        }

        @Required
        public void setExecutor(ExecutorService executor)
        {
            this.executor = new CDCExecutorServiceDecorator(executor);
        }

        @Required
        public void setDataSource(DataSource dataSource)
        {
            jdbcTemplate = new JdbcTemplate(dataSource);
        }

        @Required
        public void setAuthorizationPolicy(SpaceManagerAuthorizationPolicy authorizationPolicy)
        {
                this.authorizationPolicy = authorizationPolicy;
        }

        public void start()
        {
                dbinit();
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
                printWriter.println("spaceManagerEnabled="+spaceManagerEnabled);
                printWriter.println("updateLinkGroupsPeriod="
                                    + updateLinkGroupsPeriod);
                printWriter.println("expireSpaceReservationsPeriod="
                                    + expireSpaceReservationsPeriod);
                printWriter.println("deleteStoredFileRecord="
                                    + deleteStoredFileRecord);
                printWriter.println("defaultLatencyForSpaceReservations="
                                    + defaultAccessLatency);
                printWriter.println("reserveSpaceForNonSRMTransfers="
                                    + reserveSpaceForNonSRMTransfers);
                printWriter.println("returnFlushedSpaceToReservation="
                                    + returnFlushedSpaceToReservation);
                printWriter.println("linkGroupAuthorizationFileName="
                                    + linkGroupAuthorizationFileName);
        }

        public static final String hh_release = " <spaceToken> [ <bytes> ] # release the space " +
                "reservation identified by <spaceToken>" ;

        public String ac_release_$_1_2(Args args) throws NumberFormatException, DataAccessException
        {
                long reservationId = Long.parseLong( args.argv(0));
                if (args.argc() == 1) {
                        Space space = updateSpaceState(reservationId,SpaceState.RELEASED);
                        return space.toString();
                }
                else {
                        return "partial release is not supported yet";
                }
        }

        public static final String hh_update_space_reservation = " [-size=<size>]  [-lifetime=<lifetime>] [-vog=<vogroup>] [-vor=<vorole>] <spaceToken> \n"+
                "                                                     # set new size and/or lifetime for the space token \n " +
                "                                                     # valid examples of size: 1000, 100kB, 100KB, 100KiB, 100MB, 100MiB, 100GB, 100GiB, 10.5TB, 100TiB \n" +
                "                                                     # see http://en.wikipedia.org/wiki/Gigabyte for explanation \n"+
                "                                                     # lifetime is in seconds (\"-1\" means infinity or permanent reservation";

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
                                Space space = getSpace(reservationId);
                                LinkGroup lg = getLinkGroup(space.getLinkGroupId());
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
                        Space space = updateSpaceReservation(reservationId,
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
                long lgId = 0;
                LinkGroup lg = null;
                if (linkGroupId!=null) {
                        lgId = Long.parseLong(linkGroupId);
                        lg   = getLinkGroup(lgId);
                }
                if (linkGroupName!=null) {
                        lg = getLinkGroupByName(linkGroupName);
                        if (lgId!=0) {
                                if (lg.getId()!=lgId) {
                                        sb.append("Cannot find LinkGroup with id=").
                                                append(linkGroupId).
                                                append(" and name=").
                                                append(linkGroupName);
                                        return;
                                }
                        }
                }
                if (lg!=null) {
                        sb.append("Found LinkGroup:\n");
                        lg.toStringBuilder(sb);
                        sb.append('\n');
                }

                if(id != null) {
                        Long longid = Long.valueOf(id);
                        spaces = jdbcTemplate.query(SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_ID,
                                                    spaceReservationMapper, longid);
                        if (spaces.isEmpty()) {
                                if(lg==null) {
                                        sb.append("Space with id=").
                                                append(id).
                                                append(" not found ");
                                }
                                else {
                                        sb.append("LinkGroup with id=").
                                                append(lg.getId()).
                                                append(" and name=").
                                                append(lg.getName()).
                                                append(
                                                " does not contain space with id=").
                                                append(id);
                                }
                                return;
                        }
                        for (Space space : spaces ) {
                                if (lg!=null) {
                                        if (space.getLinkGroupId()!=lg.getId()) {
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
                        return;
                }
                if (linkGroupName==null&&linkGroupId==null&&description==null&&group==null&&role==null){
                        spaces = jdbcTemplate.query(SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS, spaceReservationMapper);
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
                        spaces = jdbcTemplate.query(SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_LINKGROUP_ID,
                                spaceReservationMapper, lg.getId());
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
                        if (lg==null) {
                                spaces = jdbcTemplate.query(SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_DESC,
                                        spaceReservationMapper, description);
                        }
                        else {
                                spaces = jdbcTemplate.query(SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_DESC_AND_LINKGROUP_ID,
                                        spaceReservationMapper, description, lg.getId());
                        }
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
                        if (lg==null) {
                                spaces = jdbcTemplate.query(SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_VOROLE,
                                        spaceReservationMapper, group, role);
                        }
                        else {
                            spaces = jdbcTemplate.query(SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_VOROLE_AND_LINKGROUP_ID,
                                    spaceReservationMapper, group, role, lg.getId());

                        }
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
                        if (lg==null) {
                                spaces = jdbcTemplate.query(
                                        SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP, spaceReservationMapper, group);
                        }
                        else {
                                spaces = jdbcTemplate.query(
                                        SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOGROUP_AND_LINKGROUP_ID, spaceReservationMapper,
                                        group, lg.getId());
                        }
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
                        if (lg==null) {
                                spaces = jdbcTemplate.query(
                                        SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOROLE, spaceReservationMapper, role);
                        }
                        else {
                                spaces = jdbcTemplate.query(
                                        SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_VOROLE_AND_LINKGROUP_ID, spaceReservationMapper,
                                        role, lg.getId());
                        }
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
                                LinkGroup lg=getLinkGroup(longid);
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
                        groups = jdbcTemplate.query(
                                LinkGroupIO.SELECT_ALL_LINKGROUPS, linkGroupMapper);
                }
                else {
                        groups = jdbcTemplate.query(
                                LinkGroupIO.SELECT_CURRENT_LINKGROUPS, linkGroupMapper, latestLinkGroupUpdateTime);
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
                long[] tokens;
                try {
                    tokens = getFileSpaceTokens(new PnfsId(args.argv(0)), null);
                }
                catch (IllegalArgumentException e) {
                    tokens = getFileSpaceTokens(null, args.argv(0));
                }
                if (tokens.length > 0) {
                    return Joiner.on('\n').join(Longs.asList(tokens));
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
                List<Long> linkGroups = findLinkGroupIds(sizeInBytes,
                                                     voGroup,
                                                     voRole,
                                                     latency,
                                                     policy);
                long lgId;
                if(lgIdString == null && lgName == null) {
                        if (linkGroups.isEmpty()) {
                                logger.warn("find LinkGroup Ids returned 0 linkGroups, no linkGroups found");
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
                                        lg   = getLinkGroup(lgId);
                                }
                                else {
                                        lg = getLinkGroupByName(lgName);
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
                Space space = reserveSpaceInLinkGroup(lgId,
                                                      voGroup,
                                                      voRole,
                                                      sizeInBytes,
                                                      latency,
                                                      policy,
                                                      lifetime,
                                                      description);
                return space.toString();
        }

        public static final String hh_listInvalidSpaces = " [-e] [-r] [<n>]" +
                " # e=expired, r=released, default is both, n=number of rows to retrieve";

        private static final int RELEASED = 1;
        private static final int EXPIRED  = 2;

        private static final String[] badSpaceType= { "released",
                                                     "expired",
                                                     "released or expired" };
        public String ac_listInvalidSpaces_$_0_1( Args args )
                throws IllegalArgumentException, DataAccessException
        {
                int argCount       = args.optc();
                boolean doExpired  = args.hasOption( "e" );
                boolean doReleased = args.hasOption( "r" );
                int nRows = 1000;
                if (args.argc()>0) {
                        nRows = Integer.parseInt(args.argv(0));
                }
                if (nRows < 0 ) {
                        return "number of rows must be non-negative";
                }
                int listOptions = RELEASED | EXPIRED;
                if ( doExpired || doReleased ) {
                        listOptions = 0;
                        if ( doExpired ) {
                                listOptions = EXPIRED;
                                --argCount;
                        }
                        if ( doReleased ) {
                                listOptions |= RELEASED;
                                --argCount;
                        }
                }
                if ( argCount != 0 ) {
                        return "Unrecognized option.\nUsage: listInvalidSpaces" +
                                hh_listInvalidSpaces;
                }
                List< Space > expiredSpaces = listInvalidSpaces( listOptions , nRows );
                if ( expiredSpaces.isEmpty() ) {
                        return "There are no " + badSpaceType[ listOptions-1 ] + " spaces.";
                }
                return Joiner.on('\n').join(expiredSpaces);
        }

        private static final String SELECT_INVALID_SPACES=
                "SELECT * FROM "+ManagerSchemaConstants.SPACE_TABLE_NAME +
                " WHERE state = ";

        @Transactional(readOnly = true)
        private List<Space> listInvalidSpaces(int spaceTypes, int nRows)
                throws DataAccessException
        {
                String query;
                switch ( spaceTypes ) {
                case EXPIRED: // do just expired
                        query = SELECT_INVALID_SPACES + SpaceState.EXPIRED.getStateId();
                        break;
                case RELEASED: // do just released
                        query = SELECT_INVALID_SPACES + SpaceState.RELEASED.getStateId();
                        break;
                case RELEASED | EXPIRED: // do both
                        query = SELECT_INVALID_SPACES + SpaceState.EXPIRED.getStateId() +
                                " OR state = " + SpaceState.RELEASED.getStateId();
                        break;
                default: // something is broken
                    throw new IllegalArgumentException("listInvalidSpaces: got invalid space type "
                            + spaceTypes);
                }
                query += " LIMIT " + nRows;
                return jdbcTemplate.query(query, spaceReservationMapper);
        }


        public static final String hh_listFilesInSpace=" <space-id>";
        // @return a string containing a newline-separated list of the files in
        //         the space specified by <i>space-id</i>.

        public String ac_listFilesInSpace_$_1( Args args )
                throws DataAccessException, NumberFormatException
        {
                long spaceId = Long.parseLong( args.argv( 0 ) );
                // Get a list of the Invalid spaces
                List<File> filesInSpace = getFilesInSpace(spaceId);
                if (filesInSpace.isEmpty()) {
                        return "There are no files in this space.";
                }
                return Joiner.on('\n').join(filesInSpace);
        }

        // This method returns an array of all the files in the specified space.
        @Transactional(readOnly = true)
        private List<File> getFilesInSpace(long spaceId)
                throws DataAccessException
        {
                return jdbcTemplate.query(FileIO.SELECT_BY_SPACERESERVATION_ID, fileMapper, spaceId);
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
                removeExpiredFilesFromSpace(spaceId, states);
                return sb.toString();
        }

        @Transactional
        private void removeExpiredFilesFromSpace(long spaceId, Set<FileState> states)
                throws DataAccessException
        {
            List<File> files = jdbcTemplate.query(FileIO.SELECT_EXPIRED_SPACEFILES1, fileMapper,
                                                  System.currentTimeMillis(),
                                                  spaceId);
            for (File file : files) {
                if (states.contains(file.getState())) {
                    removeFileFromSpace(file.getId());
                }
            }
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
                        removeFileFromSpace(id);
                        return "removed file with id="+id;
                }
                if (sPnfsId!=null) {
                        PnfsId pnfsId = new PnfsId(sPnfsId);
                        File f = getFile(pnfsId);
                        removeFileFromSpace(f.getId());
                        return "removed file with pnfsId="+pnfsId;
                }
                return "please specify  \"-id=\" or \"-pnfsId=\" option";
        }

        @Transactional
        private void dbinit() throws DataAccessException {
                insertRetentionPolicies();
                insertAccessLatencies();
        }

        private static final String countPolicies=
                "SELECT count(*) from "+
                ManagerSchemaConstants.RETENTION_POLICY_TABLE_NAME;

        private static final String insertPolicy = "INSERT INTO "+
                ManagerSchemaConstants.RETENTION_POLICY_TABLE_NAME +
                " (id, name) VALUES (?,?)" ;

        private void insertRetentionPolicies() throws DataAccessException
        {
                RetentionPolicy[] policies = RetentionPolicy.getAllPolicies();
                Long cnt = jdbcTemplate.queryForObject(countPolicies, Long.class);
                if (cnt == policies.length) {
                        return;
                }
                for (RetentionPolicy policy : policies) {
                    try {
                        jdbcTemplate.update(insertPolicy, policy.getId(), policy.toString());
                    } catch (DataAccessException sqle) {
                        logger.error("insert retention policy {} failed: {}",
                                policy, sqle.getMessage());
                    }
                }
        }

        private static final String countLatencies =
                "SELECT count(*) from "+ManagerSchemaConstants.ACCESS_LATENCY_TABLE_NAME;

        private static final String insertLatency = "INSERT INTO "+
                ManagerSchemaConstants.ACCESS_LATENCY_TABLE_NAME +
                " (id, name) VALUES (?,?)";

        private void insertAccessLatencies() throws DataAccessException
        {
                AccessLatency[] latencies = AccessLatency.getAllLatencies();
                Long cnt = jdbcTemplate.queryForObject(countLatencies, Long.class);
                if (cnt == latencies.length) {
                        return;
                }
                for (AccessLatency latency : latencies) {
                    try {
                        jdbcTemplate.update(insertLatency, latency.getId(), latency.toString());
                    } catch (DataAccessException sqle) {
                        logger.error("insert access latency {} failed: {}",
                                latency, sqle.getMessage());
                    }
                }
        }

        private static final String selectLinkGroupVOs =
                "SELECT VOGroup,VORole FROM "+ManagerSchemaConstants.LINK_GROUP_VOS_TABLE_NAME +
                " WHERE linkGroupId=?";

        private static final String onlineSelectionCondition =
                "lg.onlineallowed = 1 ";
        private static final String nearlineSelectionCondition =
                "lg.nearlineallowed = 1 ";
        private static final String replicaSelectionCondition =
                "lg.replicaallowed = 1 ";
        private static final String outputSelectionCondition =
                "lg.outputallowed = 1 ";
        private static final String custodialSelectionCondition =
                "lg.custodialAllowed = 1 ";

        private static final String voGroupSelectionCondition =
                " ( lgvo.VOGroup = ? OR lgvo.VOGroup = '*' ) ";
        private static final String voRoleSelectionCondition =
                " ( lgvo.VORole = ? OR lgvo.VORole = '*' ) ";

        private static final String spaceCondition=
                " lg.freespaceinbytes-lg.reservedspaceinbytes >= ? ";
        private static final String orderBy=
                " order by lg.freespaceinbytes-lg.reservedspaceinbytes desc ";

        private static final String selectLinkGroupIdPart1 =
                "SELECT lg.id FROM srmlinkgroup lg, srmlinkgroupvos lgvo "+
                "WHERE lg.id = lgvo.linkGroupId  and  lg.lastUpdateTime >= ? ";

        private static final String selectLinkGroupInfoPart1 =
                "SELECT lg.* FROM srmlinkgroup lg "+
                "WHERE lg.lastUpdateTime >= ? ";

        private static final String selectOnlineReplicaLinkGroup =
                selectLinkGroupIdPart1 +" and "+
                onlineSelectionCondition + " and "+
                replicaSelectionCondition + " and "+
                voGroupSelectionCondition + " and "+
                voRoleSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        private static final String selectOnlineOutputLinkGroup  =
                selectLinkGroupIdPart1 +" and "+
                onlineSelectionCondition + " and "+
                outputSelectionCondition + " and "+
                voGroupSelectionCondition + " and "+
                voRoleSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        private static final String selectOnlineCustodialLinkGroup  =
                selectLinkGroupIdPart1 +" and "+
                onlineSelectionCondition + " and "+
                custodialSelectionCondition + " and "+
                voGroupSelectionCondition + " and "+
                voRoleSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        private static final String selectNearlineReplicaLinkGroup  =
                selectLinkGroupIdPart1 +" and "+
                nearlineSelectionCondition + " and "+
                replicaSelectionCondition + " and "+
                voGroupSelectionCondition + " and "+
                voRoleSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        private static final String selectNearlineOutputLinkGroup =
                selectLinkGroupIdPart1 +" and "+
                nearlineSelectionCondition + " and "+
                outputSelectionCondition + " and "+
                voGroupSelectionCondition + " and "+
                voRoleSelectionCondition + " and "+
                spaceCondition +
                orderBy;


        private static final String selectNearlineCustodialLinkGroup =
                selectLinkGroupIdPart1 +" and "+
                nearlineSelectionCondition + " and "+
                custodialSelectionCondition + " and "+
                voGroupSelectionCondition + " and "+
                voRoleSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        private static final String selectAllOnlineReplicaLinkGroup =
                selectLinkGroupInfoPart1+" and "+
                onlineSelectionCondition + " and "+
                replicaSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        private static final String selectAllOnlineOutputLinkGroup  =
                selectLinkGroupInfoPart1+" and "+
                onlineSelectionCondition + " and "+
                outputSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        private static final String selectAllOnlineCustodialLinkGroup  =
                selectLinkGroupInfoPart1+" and "+
                onlineSelectionCondition + " and "+
                custodialSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        private static final String selectAllNearlineReplicaLinkGroup  =
                selectLinkGroupInfoPart1+" and "+
                nearlineSelectionCondition + " and "+
                replicaSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        private static final String selectAllNearlineOutputLinkGroup =
                selectLinkGroupInfoPart1+" and "+
                nearlineSelectionCondition + " and "+
                outputSelectionCondition + " and "+
                spaceCondition +
                orderBy;


        private static final String selectAllNearlineCustodialLinkGroup =
                selectLinkGroupInfoPart1+" and "+
                nearlineSelectionCondition + " and "+
                custodialSelectionCondition + " and "+
                spaceCondition +
                orderBy;

        //
        // the function below returns list of linkgroup ids that correspond
        // to linkgroups that satisfy retention policy/access latency criteria,
        // voGroup/voRoles criteria and have sufficient space to accommodate new
        // space reservation. Sufficient space is defined as lg.freespaceinbytes-lg.reservedspaceinbytes
        // we do not use select for update here as we do not want to lock many
        // rows.

        private List<Long> findLinkGroupIds(long sizeInBytes,
                                            String voGroup,
                                            String voRole,
                                            AccessLatency al,
                                            RetentionPolicy rp)
                throws DataAccessException
        {
                logger.trace("findLinkGroupIds(sizeInBytes={}, " +
                        "voGroup={} voRole={}, AccessLatency={}, " +
                        "RetentionPolicy={})", sizeInBytes, voGroup,
                        voRole, al, rp);
                String select;
                if (al.equals(AccessLatency.ONLINE)) {
                        if(rp.equals(RetentionPolicy.REPLICA)) {
                                select = selectOnlineReplicaLinkGroup;
                        }
                        else if ( rp.equals(RetentionPolicy.OUTPUT)) {
                                select = selectOnlineOutputLinkGroup;
                        }
                        else {
                                select = selectOnlineCustodialLinkGroup;
                        }
                }
                else {
                        if (rp.equals(RetentionPolicy.REPLICA)) {
                                select = selectNearlineReplicaLinkGroup;
                        }
                        else if ( rp.equals(RetentionPolicy.OUTPUT)) {
                                select = selectNearlineOutputLinkGroup;
                        }
                        else {
                                select = selectNearlineCustodialLinkGroup;
                        }
                }
                return jdbcTemplate.queryForList(select, Long.class,
                                                 latestLinkGroupUpdateTime,
                                                 voGroup,
                                                 voRole,
                                                 sizeInBytes);
        }

        private List<LinkGroup> findLinkGroupIds(long sizeInBytes,
                                                 AccessLatency al,
                                                 RetentionPolicy rp)
                throws DataAccessException
        {
                logger.trace("findLinkGroupIds(sizeInBytes={}, " +
                        "AccessLatency={}, RetentionPolicy={})",
                        sizeInBytes, al, rp);
                String select;
                if(al.equals(AccessLatency.ONLINE)) {
                        if(rp.equals(RetentionPolicy.REPLICA)) {
                                select = selectAllOnlineReplicaLinkGroup;
                        }
                        else
                                if ( rp.equals(RetentionPolicy.OUTPUT)) {
                                        select = selectAllOnlineOutputLinkGroup;
                                }
                                else {
                                        select = selectAllOnlineCustodialLinkGroup;
                                }

                }
                else {
                        if(rp.equals(RetentionPolicy.REPLICA)) {
                                select = selectAllNearlineReplicaLinkGroup;
                        }
                        else
                                if ( rp.equals(RetentionPolicy.OUTPUT)) {
                                        select = selectAllNearlineOutputLinkGroup;
                                }
                                else {
                                        select = selectAllNearlineCustodialLinkGroup;
                                }
                }
                return  jdbcTemplate.query(select, linkGroupMapper,
                                           latestLinkGroupUpdateTime,
                                           sizeInBytes);
        }

        private Space getSpace(long id) throws DataAccessException {
            try {
                return jdbcTemplate.queryForObject(
                        SpaceReservationIO.SELECT_SPACE_RESERVATION_BY_ID, spaceReservationMapper, id);
            } catch (EmptyResultDataAccessException e) {
                throw new EmptyResultDataAccessException("No such space reservation: " + id, 1, e);
            }
        }

        private LinkGroup getLinkGroup(long id) throws DataAccessException {
            try {
                return jdbcTemplate.queryForObject(
                        LinkGroupIO.SELECT_LINKGROUP_BY_ID, linkGroupMapper, id);
            } catch (EmptyResultDataAccessException e) {
                throw new EmptyResultDataAccessException("No such link group: " + id, 1, e);
            }
        }

        private LinkGroup getLinkGroupByName(String name) throws DataAccessException {
            try {
                return jdbcTemplate.queryForObject(
                        LinkGroupIO.SELECT_LINKGROUP_BY_NAME, linkGroupMapper, name);
            } catch (EmptyResultDataAccessException e) {
                throw new EmptyResultDataAccessException("No such link group: " + name, 1, e);
            }
        }

//------------------------------------------------------------------------------
// select for update functions
//------------------------------------------------------------------------------
        @Nonnull
        private Space selectSpaceForUpdate(long id, long sizeInBytes) throws DataAccessException
        {
                try {
                        return jdbcTemplate.queryForObject(
                                SpaceReservationIO.SELECT_FOR_UPDATE_BY_ID_AND_SIZE, spaceReservationMapper,
                                id, sizeInBytes);
                }
                catch (EmptyResultDataAccessException e) {
                    throw new EmptyResultDataAccessException("No space reservation with id " + id + " and " + sizeInBytes + " bytes available.", 1, e);
                }
        }

        @Nonnull
        private Space selectSpaceForUpdate(long id)  throws DataAccessException
        {
                try {
                        return jdbcTemplate.queryForObject(
                                SpaceReservationIO.SELECT_FOR_UPDATE_BY_ID, spaceReservationMapper, id);
                }
                catch (EmptyResultDataAccessException e) {
                    throw new EmptyResultDataAccessException("No such space reservation: " + id, 1, e);
                }
        }

        @Nonnull
        private File selectFileForUpdate(PnfsId pnfsId) throws DataAccessException
        {
                try {
                        return jdbcTemplate.queryForObject(
                                FileIO.SELECT_FOR_UPDATE_BY_PNFSID, fileMapper, pnfsId.toString());
                }
                catch (EmptyResultDataAccessException e) {
                    throw new EmptyResultDataAccessException("No file with PNFS ID: " + pnfsId, 1, e);
                }
        }

        @Nonnull
        private File selectFileForUpdate(long id) throws DataAccessException
        {
                try {
                        return jdbcTemplate.queryForObject(
                                FileIO.SELECT_FOR_UPDATE_BY_ID, fileMapper, id);
                }
                catch (EmptyResultDataAccessException e){
                    throw new EmptyResultDataAccessException("No such file id: " + id, 1, e);
                }
        }

        @Nonnull
        private File selectFileFromSpaceForUpdate(String pnfsPath,
                                                  long reservationId)
                throws DataAccessException
        {
                try {
                        return jdbcTemplate.queryForObject(
                                FileIO.SELECT_TRANSIENT_FILES_BY_PNFSPATH_AND_RESERVATIONID,
                                fileMapper,
                                pnfsPath,
                                reservationId);
                }
                catch (EmptyResultDataAccessException e){
                    throw new EmptyResultDataAccessException("No such transient file in space " + reservationId + ": " + pnfsPath, 1, e);
                }
        }

        private void removeFileFromSpace(long fileId) throws DataAccessException
        {
                int rc = jdbcTemplate.update(FileIO.DELETE, fileId);
                if (rc > 1) {
                    throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("delete returned row count = " + rc, 1, rc);
                }
        }


//------------------------------------------------------------------------------
        private Space updateSpaceState(long id, SpaceState spaceState) throws DataAccessException
        {
                return updateSpaceReservation(id,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              null,
                                              spaceState);
        }

        @Transactional
        private Space updateSpaceReservation(long id,
                                             String voGroup,
                                             String voRole,
                                             RetentionPolicy retentionPolicy,
                                             AccessLatency accessLatency,
                                             Long linkGroupId,
                                             Long sizeInBytes,
                                             Long lifetime,
                                             String description,
                                             SpaceState state)
                throws DataAccessException
        {
                Space space = selectSpaceForUpdate(id);
                if (voGroup !=null) {
                    space.setVoGroup(voGroup);
                }
                if (voRole !=null) {
                    space.setVoRole(voRole);
                }
                if (retentionPolicy !=null) {
                    space.setRetentionPolicy(retentionPolicy);
                }
                if (accessLatency !=null) {
                    space.setAccessLatency(accessLatency);
                }
                if (sizeInBytes != null)  {
                        long usedSpace = space.getUsedSizeInBytes()+ space.getAllocatedSpaceInBytes();
                        if (sizeInBytes < usedSpace) {
                                throw new DataIntegrityViolationException("Cannot downsize space reservation below "+usedSpace+"bytes, remove files first ");
                        }
                        space.setSizeInBytes(sizeInBytes);
                }
                if (lifetime !=null) {
                    space.setLifetime(lifetime);
                }
                if (description != null) {
                    space.setDescription(description);
                }
                SpaceState oldState = space.getState();
                if (state != null)  {
                        if (SpaceState.isFinalState(oldState)) {
                                throw new DataIntegrityViolationException("change from "+oldState+" to "+ state +" is not allowed");
                        }
                        space.setState(state);
                }
                jdbcTemplate.update(SpaceReservationIO.UPDATE,
                                    space.getVoGroup(),
                                    space.getVoRole(),
                                    space.getRetentionPolicy().getId(),
                                    space.getAccessLatency().getId(),
                                    space.getLinkGroupId(),
                                    space.getSizeInBytes(),
                                    space.getCreationTime(),
                                    space.getLifetime(),
                                    space.getDescription(),
                                    space.getState().getStateId(),
                                    space.getId());
                return space;
        }

        private void expireSpaceReservations() throws DataAccessException
        {
                logger.trace("expireSpaceReservations()...");
                if (cleanupExpiredSpaceFiles) {
                        List<File> files = jdbcTemplate.query(FileIO.SELECT_EXPIRED_SPACEFILES,
                                                              fileMapper,
                                                              System.currentTimeMillis());
                        for (File file : files) {
                                try {
                                        if (file.getPnfsId() != null) {
                                                try {
                                                        pnfs.deletePnfsEntry(file.getPnfsId(), file.getPnfsPath());
                                                } catch (FileNotFoundCacheException ignored) {
                                                }
                                        }
                                        removeFileFromSpace(file.getId());
                                }
                                catch (DataAccessException e) {
                                        logger.error("Failed to remove file {}: {}",
                                                file, e.getMessage());
                                }
                                catch (CacheException e) {
                                        logger.error("Failed to delete file {}: {}",
                                                file.getPnfsId(), e.getMessage());
                                }
                        }
                }
                List<Space> spaces = jdbcTemplate.query(
                        SpaceReservationIO.SELECT_EXPIRED_SPACE_RESERVATIONS1, spaceReservationMapper,
                        System.currentTimeMillis());
                for (Space space : spaces) {
                        try {
                                updateSpaceReservation(space.getId(),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       SpaceState.EXPIRED);
                        }
                        catch (DataAccessException e) {
                                logger.error("failed to remove expired reservation {}: {}", space, e.getMessage());
                        }
                }
        }

        private Space insertSpaceReservation(String voGroup,
                                             String voRole,
                                             RetentionPolicy retentionPolicy,
                                             AccessLatency accessLatency,
                                             long linkGroupId,
                                             long sizeInBytes,
                                             long lifetime,
                                             String description,
                                             SpaceState state,
                                             long used,
                                             long allocated)
                throws DataAccessException
        {
                long creationTime = System.currentTimeMillis();
                KeyHolder keyHolder = new GeneratedKeyHolder();
                int rc = jdbcTemplate.update(
                            SpaceReservationIO.insert(voGroup,
                                                      voRole,
                                                      retentionPolicy,
                                                      accessLatency,
                                                      linkGroupId,
                                                      sizeInBytes,
                                                      creationTime,
                                                      lifetime,
                                                      description,
                                                      state,
                                                      used,
                                                      allocated),
                            keyHolder);
                if (rc!=1) {
                        throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("insert returned row count =" + rc, 1, rc);
                }
                return new Space((Long) keyHolder.getKeys().get("id"),
                                 voGroup,
                                 voRole,
                                 retentionPolicy,
                                 accessLatency,
                                 linkGroupId,
                                 sizeInBytes,
                                 creationTime,
                                 lifetime,
                                 description,
                                 state,
                                 used,
                                 allocated);
        }

        //
        // functions for infoProvider
        //

        private void getValidSpaceTokens(GetSpaceTokensMessage msg) throws DataAccessException {
                List<Space> spaces;
                if(msg.getSpaceTokenId()!=null) {
                        spaces = Collections.singletonList(getSpace(msg.getSpaceTokenId()));
                }
                else {
                        spaces = jdbcTemplate.query(SpaceReservationIO.SELECT_CURRENT_SPACE_RESERVATIONS,
                                                    spaceReservationMapper);
                }
                msg.setSpaceTokenSet(spaces);
        }

        private void getLinkGroups(GetLinkGroupsMessage msg) throws DataAccessException {
                List<LinkGroup> groups = jdbcTemplate.query(LinkGroupIO.SELECT_ALL_LINKGROUPS, linkGroupMapper);
                msg.setLinkGroups(groups);
        }

        private void getLinkGroupNames(GetLinkGroupNamesMessage msg) throws DataAccessException {
                List<String> names = jdbcTemplate.queryForList("SELECT name FROM " + LinkGroupIO.LINKGROUP_TABLE, String.class);
                msg.setLinkGroupNames(names);
        }

        private static final String SELECT_SPACE_TOKENS_BY_DESCRIPTION =
                "SELECT id FROM "+ManagerSchemaConstants.SPACE_TABLE_NAME +
                " WHERE  state = ? AND description = ?";

        private static final String SELECT_SPACE_TOKENS_BY_VOGROUP =
                "SELECT id FROM "+ManagerSchemaConstants.SPACE_TABLE_NAME +
                " WHERE  state = ? AND voGroup = ?";

        private static final String SELECT_SPACE_TOKENS_BY_VOROLE =
                "SELECT id FROM "+ManagerSchemaConstants.SPACE_TABLE_NAME +
                " WHERE  state = ? AND  voRole = ?";

        private static final String SELECT_SPACE_TOKENS_BY_VOGROUP_AND_VOROLE =
                "SELECT id FROM "+ManagerSchemaConstants.SPACE_TABLE_NAME +
                " WHERE  state = ? AND voGroup = ? AND voRole = ?";

        @Nonnull
        private List<Long> findSpacesByVoGroupAndRole(String voGroup, String voRole)
                throws DataAccessException
        {
                if (!isNullOrEmpty(voGroup) && !isNullOrEmpty(voRole)) {
                        return jdbcTemplate.queryForList(SELECT_SPACE_TOKENS_BY_VOGROUP_AND_VOROLE, Long.class,
                                                         SpaceState.RESERVED.getStateId(),
                                                         voGroup,
                                                         voRole);
                }
                if (!isNullOrEmpty(voGroup)) {
                        return jdbcTemplate.queryForList(SELECT_SPACE_TOKENS_BY_VOGROUP, Long.class,
                                                         SpaceState.RESERVED.getStateId(),
                                                         voGroup);
                }
                if (!isNullOrEmpty(voRole)) {
                        return jdbcTemplate.queryForList(SELECT_SPACE_TOKENS_BY_VOROLE, Long.class,
                                                         SpaceState.RESERVED.getStateId(),
                                                         voRole);
                }
                return Collections.emptyList();
        }

        @Nonnull
        private long[] getSpaceTokens(Subject subject, String description) throws DataAccessException
        {
                Set<Long> spaces = new HashSet<>();
                if (description==null) {
                    for (String s : Subjects.getFqans(subject)) {
                        if (s != null) {
                            FQAN fqan = new FQAN(s);
                            spaces.addAll(findSpacesByVoGroupAndRole(fqan.getGroup(), fqan.getRole()));
                        }
                    }
                    spaces.addAll(findSpacesByVoGroupAndRole(Subjects.getUserName(subject), ""));
                }
                else {
                        List<Long> foundSpaces = jdbcTemplate.queryForList(SELECT_SPACE_TOKENS_BY_DESCRIPTION,
                                                                           Long.class,
                                                                           SpaceState.RESERVED.getStateId(),
                                                                           description);
                        spaces.addAll(foundSpaces);
                }
                return Longs.toArray(spaces);
        }

        private static final String SELECT_SPACE_FILE_BY_PNFSID =
                "SELECT spacereservationid FROM "+ManagerSchemaConstants.SPACE_FILE_TABLE_NAME +
                " WHERE pnfsId = ? ";

        private static final String SELECT_SPACE_FILE_BY_PNFSPATH =
                "SELECT spacereservationid FROM "+ManagerSchemaConstants.SPACE_FILE_TABLE_NAME +
                " WHERE pnfsPath = ? ";

        private static final String SELECT_SPACE_FILE_BY_PNFSID_AND_PNFSPATH =
                "SELECT spacereservationid FROM "+ManagerSchemaConstants.SPACE_FILE_TABLE_NAME +
                " WHERE pnfsId = ? AND pnfsPath = ?";

        @Nonnull
        private long[] getFileSpaceTokens(PnfsId pnfsId, String pnfsPath)  throws DataAccessException
        {
                List<Long> files;
                if (pnfsId != null && pnfsPath != null) {
                        files = jdbcTemplate.queryForList(SELECT_SPACE_FILE_BY_PNFSID_AND_PNFSPATH, Long.class,
                                                          pnfsId.toString(),
                                                          new FsPath(pnfsPath).toString());
                }
                else if (pnfsId != null) {
                        files = jdbcTemplate.queryForList(SELECT_SPACE_FILE_BY_PNFSID, Long.class,
                                                          pnfsId.toString());
                }
                else if (pnfsPath != null) {
                        files = jdbcTemplate.queryForList(SELECT_SPACE_FILE_BY_PNFSPATH, Long.class,
                                                          new FsPath(pnfsPath).toString());
                }
                else {
                        throw new IllegalArgumentException("getFileSpaceTokens: all arguments are nulls, not supported");
                }
                return Longs.toArray(files);
        }

        private void updateSpaceFile(String voGroup,
                                     String voRole,
                                     PnfsId pnfsId,
                                     Long sizeInBytes,
                                     Long lifetime,
                                     FileState state,
                                     File f)
                throws DataAccessException
        {
                if (voGroup!=null) {
                        f.setVoGroup(voGroup);
                }
                if (voRole!=null) {
                        f.setVoRole(voRole);
                }
                if (sizeInBytes != null) {
                    f.setSizeInBytes(sizeInBytes);
                }
                if (lifetime!=null) {
                    f.setLifetime(lifetime);
                }
                if (state!=null)   {
                        f.setState(state);
                }
                if (pnfsId!=null ) {
                    f.setPnfsId(pnfsId);
                }
                int rc = jdbcTemplate.update(FileIO.UPDATE,
                                             f.getVoGroup(),
                                             f.getVoRole(),
                                             f.getSizeInBytes(),
                                             f.getLifetime(),
                                             Objects.toString(f.getPnfsId()),
                                             f.getState().getStateId(),
                                             f.getId());
                if (rc!=1) {
                        throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("Update failed, row count=" + rc, 1, rc);
                }
        }

        @Transactional
        private void setPnfsIdOfFileInSpace(long id, PnfsId pnfsId) throws DataAccessException
        {
                File f = selectFileForUpdate(id);
                if (f.getPnfsId() != null) {
                    throw new DataIntegrityViolationException("File is already assigned a PNFS ID.");
                }
                updateSpaceFile(null, null, pnfsId, null, null, null, f);
        }

        private void removePnfsIdOfFileInSpace(long id)
                throws DataAccessException
        {
            int rc = jdbcTemplate.update(FileIO.REMOVE_PNFSID_ON_SPACEFILE, id);
            if (rc != 1) {
                throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("Update failed, row count=" + rc, 1, rc);
            }
        }

        private void removePnfsIdAndChangeStateOfFileInSpace(long id, FileState state)
                throws DataAccessException
        {
            int rc = jdbcTemplate.update(FileIO.REMOVE_PNFSID_AND_CHANGE_STATE_SPACEFILE, state.getStateId(), id);
            if (rc != 1) {
                throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("Update failed, row count=" + rc, 1, rc);
            }
        }

        private File getFile(PnfsId pnfsId) throws DataAccessException
        {
                try {
                        return jdbcTemplate.queryForObject(FileIO.SELECT_BY_PNFSID, fileMapper, pnfsId.toString());
                } catch (EmptyResultDataAccessException e) {
                        throw new EmptyResultDataAccessException("file with pnfsId=" + pnfsId + " is not found", 1, e);
                } catch (IncorrectResultSizeDataAccessException e) {
                        throw new IncorrectResultSizeDataAccessException("found more than one record with pnfsId="+ pnfsId, 1, e.getActualSize(), e);
                }
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
            logger.trace("messageArrived : type={} value={} from {}",
                         message.getClass().getName(), message, envelope.getSourcePath());

            if (!message.isReply()) {
                if (!isNotificationMessage(message) && !isSpaceManagerMessage(message)) {
                    messageToForward(envelope, message);
                } else if (spaceManagerEnabled) {
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
                                    logger.error("Failed to send reply: {}", e.getMessage());
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
                        logger.error("Failed to send reply: {}", e.getMessage());
                    }
                }
            }
        }

        public void messageToForward(final CellMessage envelope, final Message message)
        {
            logger.trace("messageToForward: type={} value={} from {} going to {}",
                         message.getClass().getName(),
                         message,
                         envelope.getSourcePath(),
                         envelope.getDestinationPath());

            final boolean isEnRouteToDoor = message.isReply() || message instanceof DoorTransferFinishedMessage;
            if (!isEnRouteToDoor) {
                envelope.getDestinationPath().insert(poolManager);
            }

            if (envelope.nextDestination()) {
                if (spaceManagerEnabled && isInterceptedMessage(message)) {
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
                                logger.error("Failed to forward message: {}", e.getMessage());
                            }
                        }
                    });
                } else {
                    try {
                        sendMessage(envelope);
                    } catch (NoRouteToCellException e) {
                        logger.error("Failed to forward message: {}", e.getMessage());
                    }
                }
            }
        }

        private void processMessage(Message message)
        {
            try {
                if (message instanceof PoolRemoveFilesMessage) {
                    // fileRemoved does its own transaction management
                    fileRemoved((PoolRemoveFilesMessage) message);
                }
                else {
                    processMessageTransactionally(message);
                }
            } catch (SpaceAuthorizationException e) {
                message.setFailedConditionally(CacheException.PERMISSION_DENIED, e.getMessage());
            } catch (NoFreeSpaceException e) {
                message.setFailedConditionally(CacheException.RESOURCE, e.getMessage());
            } catch (SpaceException e) {
                message.setFailedConditionally(CacheException.DEFAULT_ERROR_CODE, e.getMessage());
            } catch (IllegalArgumentException e) {
                logger.error("forwarding msg failed: {}", e.getMessage(), e);
                message.setFailedConditionally(CacheException.INVALID_ARGS, e.getMessage());
            } catch (RuntimeException e) {
                logger.error("forwarding msg failed: {}", e.getMessage(), e);
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

        private final Object updateLinkGroupsSyncObject = new Object();
        @Override
        public void run(){
                if(Thread.currentThread() == expireSpaceReservations) {
                        while(true) {
                                expireSpaceReservations();
                                try{
                                        Thread.sleep(expireSpaceReservationsPeriod);
                                }
                                catch (InterruptedException ie) {
                                    logger.trace("expire SpaceReservations thread has been interrupted");
                                    return;
                                }
                                catch (DataAccessException e) {
                                        logger.error("expireSpaceReservations failed: {}", e.getMessage());
                                }
                                catch (Exception e) {
                                        logger.error("expireSpaceReservations failed: {}", e.toString());
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
                                                logger.trace("update LinkGroup thread has been interrupted");
                                                return;
                                        }
                                }
                        }
                }
        }

        private long latestLinkGroupUpdateTime =System.currentTimeMillis();
        private LinkGroupAuthorizationFile linkGroupAuthorizationFile;
        private long linkGroupAuthorizationFileLastUpdateTimestampt;

        private void updateLinkGroupAuthorizationFile() {
                if(linkGroupAuthorizationFileName == null) {
                        return;
                }
                java.io.File f = new java.io.File (linkGroupAuthorizationFileName);
                if(!f.exists()) {
                        linkGroupAuthorizationFile = null;
                }
                long lastModified = f.lastModified();
                if (linkGroupAuthorizationFile==null||
                    lastModified>=linkGroupAuthorizationFileLastUpdateTimestampt) {
                        linkGroupAuthorizationFileLastUpdateTimestampt = lastModified;
                        try {
                                linkGroupAuthorizationFile =
                                        new LinkGroupAuthorizationFile(linkGroupAuthorizationFileName);
                        }
                        catch(Exception e) {
                                logger.error("failed to parse LinkGroup" +
                                        "AuthorizationFile: {}",
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
                                updateLinkGroup(linkGroupName,
                                                avalSpaceInBytes,
                                                currentTime,
                                                onlineAllowed,
                                                nearlineAllowed,
                                                replicaAllowed,
                                                outputAllowed,
                                                custodialAllowed,
                                                vos);
                        } catch (DataAccessException sqle) {
                                logger.error("update of linkGroup {} failed: {}",
                                             linkGroupName, sqle.getMessage());
                        }
                }
                latestLinkGroupUpdateTime = currentTime;
        }

        private static final String INSERT_LINKGROUP_VO =
                "INSERT INTO "+ManagerSchemaConstants.LINK_GROUP_VOS_TABLE_NAME +
                " ( VOGroup, VORole, linkGroupId ) VALUES ( ? , ? , ? )";

        private static final String DELETE_LINKGROUP_VO =
                "DELETE FROM "+ManagerSchemaConstants.LINK_GROUP_VOS_TABLE_NAME +
                " WHERE VOGroup  = ? AND VORole = ? AND linkGroupId = ? ";

        @Transactional
        private long updateLinkGroup(String linkGroupName,
                                     long freeSpace,
                                     long updateTime,
                                     boolean onlineAllowed,
                                     boolean nearlineAllowed,
                                     boolean replicaAllowed,
                                     boolean outputAllowed,
                                     boolean custodialAllowed,
                                     VOInfo[] linkGroupVOs) throws DataAccessException
        {
                long id;
                try {
                        LinkGroup group =
                                jdbcTemplate.queryForObject(LinkGroupIO.SELECT_LINKGROUP_FOR_UPDATE_BY_NAME,
                                                            linkGroupMapper,
                                                            linkGroupName);
                        id = group.getId();
                        jdbcTemplate.update(LinkGroupIO.UPDATE,
                                            freeSpace,
                                            updateTime,
                                            (onlineAllowed ?1:0),
                                            (nearlineAllowed ?1:0),
                                            (replicaAllowed ?1:0),
                                            (outputAllowed ?1:0),
                                            (custodialAllowed ?1:0),
                                            id);
                }
                catch (EmptyResultDataAccessException e) {
                        try {
                            KeyHolder keyHolder = new GeneratedKeyHolder();
                            jdbcTemplate.update(
                                    LinkGroupIO.insert(linkGroupName,
                                                       freeSpace,
                                                       updateTime,
                                                       onlineAllowed,
                                                       nearlineAllowed,
                                                       replicaAllowed,
                                                       outputAllowed,
                                                       custodialAllowed,
                                                       0),
                                    keyHolder);
                            id = (Long) keyHolder.getKeys().get("id");
                        }
                        catch (DataAccessException e1) {
                                logger.error("failed to insert linkgroup {}: {}",
                                         linkGroupName, e.getMessage());
                                throw e1;
                        }
                }

                final Set<VOInfo> deleteVOs = new HashSet<>();
                final Set<VOInfo> insertVOs = new HashSet<>();
                if (linkGroupVOs != null) {
                    insertVOs.addAll(asList(linkGroupVOs));
                }

                jdbcTemplate.query(selectLinkGroupVOs, new RowCallbackHandler()
                {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException
                    {
                        String nextVOGroup = rs.getString(1);
                        String nextVORole = rs.getString(2);
                        VOInfo nextVO = new VOInfo(nextVOGroup, nextVORole);
                        if (!insertVOs.remove(nextVO)) {
                            deleteVOs.add(nextVO);
                        }
                    }
                }, id);

                for (VOInfo nextVo : insertVOs) {
                        jdbcTemplate.update(INSERT_LINKGROUP_VO,
                                            nextVo.getVoGroup(),
                                            nextVo.getVoRole(),
                                            id);
                }
                for (VOInfo nextVo : deleteVOs) {
                        jdbcTemplate.update(DELETE_LINKGROUP_VO,
                                            nextVo.getVoGroup(),
                                            nextVo.getVoRole(),
                                            id);
                }
                return id;
        }

        private void releaseSpace(Release release)
                throws DataAccessException, SpaceException
        {
                logger.trace("releaseSpace({})", release);

                long spaceToken = release.getSpaceToken();
                Long spaceToReleaseInBytes = release.getReleaseSizeInBytes();
                if (spaceToReleaseInBytes != null) {
                    throw new UnsupportedOperationException("partial release is not supported yet");
                }

                Space space = getSpace(spaceToken);
                if (space.getState() == SpaceState.RELEASED) {
                    /* Stupid way to signal that it isn't found, but there is no other way at the moment. */
                    throw new EmptyResultDataAccessException("Space reservation " + spaceToken + " was already released.", 1);
                }
                Subject subject =  release.getSubject();
                authorizationPolicy.checkReleasePermission(subject, space);
                updateSpaceState(spaceToken, SpaceState.RELEASED);
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
                logger.trace("useSpace({})", use);
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
            logger.trace("transferStarting({})", message);
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
                setPnfsIdOfFileInSpace(Long.parseLong(fileId), pnfsId);
            } else if (spaceToken != null) {
                logger.trace("transferStarting: file is not " +
                        "found, found default space " +
                        "token, calling useSpace()");
                long lifetime = 1000 * 60 * 60;
                useSpace(Long.parseLong(spaceToken),
                        subject,
                        message.getPreallocated(),
                        lifetime,
                        null,
                        pnfsId);
            } else {
                logger.trace("transferStarting: file is not found, no prior reservations for this file");

                long sizeInBytes = message.getPreallocated();
                long lifetime    = 1000*60*60;
                String description = null;
                LinkGroup linkGroup = getLinkGroupByName(linkGroupName);
                VOInfo voInfo = authorizationPolicy.checkReservePermission(subject, linkGroup);

                Space space = reserveSpaceInLinkGroup(linkGroup.getId(),
                                                      voInfo.getVoGroup(),
                                                      voInfo.getVoRole(),
                                                      sizeInBytes,
                                                      fileAttributes.getAccessLatency(),
                                                      fileAttributes.getRetentionPolicy(),
                                                      lifetime,
                                                      description);
                useSpace(space.getId(),
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
                logger.trace("transferStarted({},{})", pnfsId, success);
                File f = selectFileForUpdate(pnfsId);
                if (f.getState() == FileState.RESERVED) {
                    if(!success) {
                            if (f.getPnfsPath() != null) {
                                removePnfsIdOfFileInSpace(f.getId());
                            } else {
                                /* This reservation was created by space manager
                                 * when the transfer started. Delete it.
                                 */
                                removeFileFromSpace(f.getId());

                                /* TODO: If we also created the reservation, we should
                                 * release it at this point, but at the moment we cannot
                                 * know who created it. It will eventually expire
                                 * automatically.
                                 */
                            }
                    } else {
                            updateSpaceFile(null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            FileState.TRANSFERRING,
                                            f);
                    }
                }
            } catch (EmptyResultDataAccessException e) {
                logger.trace("transferStarted failed: {}", e.getMessage());
            }
        }

        private void transferFinished(DoorTransferFinishedMessage finished)
                throws DataAccessException
        {
                boolean weDeleteStoredFileRecord = deleteStoredFileRecord;
                PnfsId pnfsId = finished.getPnfsId();
                long size = finished.getFileAttributes().getSize();
                boolean success = finished.getReturnCode() == 0;
                logger.trace("transferFinished({},{})", pnfsId, success);
                File f;
                try {
                        f = selectFileForUpdate(pnfsId);
                }
                catch (EmptyResultDataAccessException e) {
                        logger.trace("failed to find file {}: {}", pnfsId,
                                     e.getMessage());
                        return;
                }
                long spaceId = f.getSpaceId();
                if(f.getState() == FileState.RESERVED ||
                   f.getState() == FileState.TRANSFERRING) {
                        if(success) {
                                if(returnFlushedSpaceToReservation && weDeleteStoredFileRecord) {
                                        RetentionPolicy rp = getSpace(spaceId).getRetentionPolicy();
                                        if(rp.equals(RetentionPolicy.CUSTODIAL)) {
                                                //we do not delete it here, since the
                                                // file will get flushed and we will need
                                                // to account for that
                                                weDeleteStoredFileRecord = false;
                                        }
                                }
                                if(weDeleteStoredFileRecord) {
                                        logger.trace("file transferred, deleting file record");
                                        removeFileFromSpace(f.getId());
                                }
                                else {
                                        updateSpaceFile(null,
                                                        null,
                                                        null,
                                                        size,
                                                        null,
                                                        FileState.STORED,
                                                        f);
                                }
                        }
                        else {
                                if (f.getPnfsPath() != null) {
                                    removePnfsIdAndChangeStateOfFileInSpace(f.getId(), FileState.RESERVED);
                                } else {
                                    /* This reservation was created by space manager
                                     * when the transfer started. Delete it.
                                     */
                                    removeFileFromSpace(f.getId());

                                    /* TODO: If we also created the reservation, we should
                                     * release it at this point, but at the moment we cannot
                                     * know who created it. It will eventually expire
                                     * automatically.
                                     */
                                }
                        }
                }
                else {
                        logger.trace("transferFinished({}): file state={}",
                                pnfsId, f.getState());
                }
        }

        private void  fileFlushed(PoolFileFlushedMessage fileFlushed) throws DataAccessException
        {
                if(!returnFlushedSpaceToReservation) {
                        return;
                }
                PnfsId pnfsId = fileFlushed.getPnfsId();
                logger.trace("fileFlushed({})", pnfsId);
                FileAttributes fileAttributes = fileFlushed.getFileAttributes();
                AccessLatency ac = fileAttributes.getAccessLatency();
                if (ac.equals(AccessLatency.ONLINE)) {
                        logger.trace("File Access latency is ONLINE " +
                                "fileFlushed does nothing");
                        return;
                }
                long size = fileAttributes.getSize();
                try {
                        File f = selectFileForUpdate(pnfsId);
                        if(f.getState() == FileState.STORED) {
                                if(deleteStoredFileRecord) {
                                        logger.trace("returnSpaceToReservation, " +
                                                "deleting file record");
                                        removeFileFromSpace(f.getId());
                                }
                                else {
                                        updateSpaceFile(null,
                                                        null,
                                                        null,
                                                        size,
                                                        null,
                                                        FileState.FLUSHED,
                                                        f);
                                }
                        }
                        else {
                                logger.trace("returnSpaceToReservation({}): " +
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
                                logger.error("badly formed PNFS-ID: {}", pnfsId);
                        }
                        catch (DataAccessException sqle) {
                                logger.trace("failed to remove file from space: {}",
                                        sqle.getMessage());
                                logger.trace("fileRemoved({}): file not in a " +
                                        "reservation, do nothing", pnfsId);
                        }
                }
        }

        @Transactional
        private void fileRemoved(String pnfsId)
        {
            logger.trace("fileRemoved({})", pnfsId);
            File f = selectFileForUpdate(new PnfsId(pnfsId));
            if ((f.getState() != FileState.RESERVED && f.getState() != FileState.TRANSFERRING) || f.getPnfsPath() == null) {
                removeFileFromSpace(f.getId());
            } else if (f.getState() == FileState.TRANSFERRING) {
                removePnfsIdAndChangeStateOfFileInSpace(f.getId(), FileState.RESERVED);
            }
        }

        private void cancelUseSpace(CancelUse cancelUse)
                throws DataAccessException, SpaceException
        {
                logger.trace("cancelUseSpace({})", cancelUse);
                long reservationId = cancelUse.getSpaceToken();
                String pnfsPath    = cancelUse.getPnfsName();
                File f;
                try {
                        f=selectFileFromSpaceForUpdate(pnfsPath,reservationId);
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
                                removeFileFromSpace(f.getId());
                        } catch (CacheException e) {
                            throw new SpaceException("Failed to delete " + pnfsPath +
                                                     " while attempting to cancel its reservation in space " +
                                                     reservationId + ": " + e.getMessage(), e);
                        }
                }
        }

        @Transactional(rollbackFor = { SpaceException.class })
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
                logger.trace("reserveSpace( subject={}, sz={}, latency={}, " +
                        "policy={}, lifetime={}, description={}", subject.getPrincipals(),
                        sizeInBytes, latency, policy, lifetime, description);
                List<LinkGroup> linkGroups = findLinkGroupIds(sizeInBytes,
                                                              latency,
                                                              policy);
                if(linkGroups.isEmpty()) {
                        logger.warn("failed to find matching linkgroup");
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
                        logger.warn("failed to find linkgroup where user is " +
                                "authorized to reserve space.");
                        throw new SpaceAuthorizationException("Failed to find LinkGroup where user is authorized to reserve space.");
                }
                List<String> linkGroupNames = new ArrayList<>(linkGroupNameVoInfoMap.keySet());
                logger.trace("Found {} linkgroups protocolInfo={}, " +
                        "storageInfo={}, pnfsId={}", linkGroups.size(),
                        protocolInfo, fileAttributes, pnfsId);
                if (linkGroupNameVoInfoMap.size()>1 &&
                    protocolInfo != null &&
                    fileAttributes != null) {
                        try {
                                linkGroupNames = selectLinkGroupForWrite(protocolInfo, fileAttributes, linkGroupNames);
                                if(linkGroupNames.isEmpty()) {
                                        throw new SpaceAuthorizationException("PoolManagerSelectLinkGroupForWriteMessage: Failed to find LinkGroup where user is authorized to reserve space.");
                                }
                        }
                        catch (SpaceAuthorizationException e)  {
                                logger.warn("authorization problem: {}",
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
                logger.trace("Chose linkgroup {}",linkGroup);
                return reserveSpaceInLinkGroup(linkGroup.getId(),
                                               voInfo.getVoGroup(),
                                               voInfo.getVoRole(),
                                               sizeInBytes,
                                               latency,
                                               policy,
                                               lifetime,
                                               description);
        }

        private LinkGroup selectLinkGroupForWrite(Subject subject, ProtocolInfo protocolInfo, FileAttributes fileAttributes, long size)
                throws DataAccessException
        {
            List<LinkGroup> linkGroups =
                    findLinkGroupIds(size, fileAttributes.getAccessLatency(), fileAttributes.getRetentionPolicy());
            List<String> linkGroupNames = new ArrayList<>();
            for (LinkGroup linkGroup : linkGroups) {
                try {
                    authorizationPolicy.checkReservePermission(subject, linkGroup);
                    linkGroupNames.add(linkGroup.getName());
                }
                catch (SpaceAuthorizationException ignored) {
                }
            }
            linkGroupNames = selectLinkGroupForWrite(protocolInfo, fileAttributes, linkGroupNames);
            logger.trace("Found {} linkgroups protocolInfo={}, fileAttributes={}",
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

        private List<String> selectLinkGroupForWrite(ProtocolInfo protocolInfo, FileAttributes fileAttributes,
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

        private Space reserveSpaceInLinkGroup(long linkGroupId,
                                              String voGroup,
                                              String voRole,
                                              long sizeInBytes,
                                              AccessLatency latency,
                                              RetentionPolicy policy,
                                              long lifetime,
                                              String description)
                throws DataAccessException
        {
                logger.trace("reserveSpaceInLinkGroup(linkGroupId={}, " +
                        "group={}, role={}, sz={}, latency={}, policy={}, " +
                        "lifetime={}, description={})", linkGroupId, voGroup,
                        voRole, sizeInBytes, latency, policy, lifetime,
                        description);
                return insertSpaceReservation(voGroup,
                                              voRole,
                                              policy,
                                              latency,
                                              linkGroupId,
                                              sizeInBytes,
                                              lifetime,
                                              description,
                                              SpaceState.RESERVED,
                                              0,
                                              0);
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
            return useSpace(reservationId,
                    effectiveGroup,
                    effectiveRole,
                    sizeInBytes,
                    lifetime,
                    pnfsPath,
                    pnfsId);
        }

        @Transactional(rollbackFor = { SpaceException.class })
        private long useSpace(long reservationId,
                              String voGroup,
                              String voRole,
                              long sizeInBytes,
                              long lifetime,
                              String pnfsPath,
                              PnfsId pnfsId)
                throws DataAccessException, SpaceException
        {
                //
                // check that there is no such file already being transferred
                //
                FsPath path;
                if (pnfsPath != null) {
                    path = new FsPath(pnfsPath);
                    List<File> files = jdbcTemplate.query(FileIO.SELECT_TRANSFERRING_OR_RESERVED_BY_PNFSPATH,
                                                          fileMapper, path.toString());
                    if (!files.isEmpty()) {
                        throw new DataIntegrityViolationException("Already have "+files.size()+" record(s) with pnfsPath="+pnfsPath);
                    }
                } else {
                    path = null;
                }
                long creationTime = System.currentTimeMillis();
                Space space = selectSpaceForUpdate(reservationId,0L); // "0L" is a hack needed to get a better error code from comparison below
                long currentTime = System.currentTimeMillis();
                if(space.getLifetime() != -1 && space.getCreationTime()+space.getLifetime()  < currentTime) {
                        throw new SpaceExpiredException("space with id="+
                                                                reservationId +
                                                        " has expired");
                }
                if (space.getState() == SpaceState.EXPIRED) {
                        throw new SpaceExpiredException("space with id="+
                                                                reservationId +
                                                        " has expired");
                }
                if (space.getState() == SpaceState.RELEASED) {
                        throw new SpaceReleasedException("space with id="+
                                                                 reservationId +
                                                         " was released");
                }
                if (space.getAvailableSpaceInBytes()< sizeInBytes) {
                        throw new NoFreeSpaceException("space with id="+
                                                               reservationId +
                                                       " does not have enough space");
                }

                KeyHolder keyHolder = new GeneratedKeyHolder();

                int rc = jdbcTemplate.update(FileIO.insert(reservationId,
                                                           voGroup,
                                                           voRole,
                                                           sizeInBytes,
                                                           creationTime,
                                                           lifetime,
                                                           path,
                                                           pnfsId,
                                                           FileState.RESERVED),
                                             keyHolder);
                if (rc != 1) {
                    throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("insert returned row count =" + rc, 1, rc);
                }
                return (Long) keyHolder.getKeys().get("id");
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
            logger.trace("selectPool({})", selectWritePool);
            FileAttributes fileAttributes = selectWritePool.getFileAttributes();
            String defaultSpaceToken = fileAttributes.getStorageInfo().getMap().get("writeToken");
            Subject subject = selectWritePool.getSubject();
            boolean hasIdentity =
                    !Subjects.getFqans(subject).isEmpty() || Subjects.getUserName(subject) != null;

            String pnfsPath = new FsPath(checkNotNull(selectWritePool.getPnfsPath())).toString();
            List<File> files = jdbcTemplate.query(
                    "SELECT * FROM " + FileIO.SRM_SPACEFILE_TABLE + " WHERE pnfspath=? and pnfsid is null and deleted != 1",
                    fileMapper,
                    pnfsPath);
            File file = getFirst(files, null);
            if (file != null) {
                /*
                 * This takes care of records created by SRM before
                 * transfer has started
                 */
                Space space = getSpace(file.getSpaceId());
                LinkGroup linkGroup = getLinkGroup(space.getLinkGroupId());
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
                logger.trace("selectPool: found linkGroup = {}, " +
                        "forwarding message", linkGroupName);
            } else if (defaultSpaceToken != null) {
                logger.trace("selectPool: file is not " +
                        "found, found default space " +
                        "token, calling useSpace()");
                Space space = getSpace(Long.parseLong(defaultSpaceToken));
                LinkGroup linkGroup = getLinkGroup(space.getLinkGroupId());
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
                logger.trace("selectPool: found linkGroup = {}, " +
                        "forwarding message", linkGroupName);
            } else if (reserveSpaceForNonSRMTransfers && hasIdentity) {
                logger.trace("selectPool: file is " +
                        "not found, no prior " +
                        "reservations for this file");

                LinkGroup linkGroup =
                        selectLinkGroupForWrite(subject, selectWritePool
                                .getProtocolInfo(), fileAttributes, selectWritePool.getPreallocated());
                if (linkGroup != null) {
                    String linkGroupName = linkGroup.getName();
                    selectWritePool.setLinkGroup(linkGroupName);
                    fileAttributes.getStorageInfo().setKey("LinkGroup", linkGroupName);
                    logger.trace("selectPool: found linkGroup = {}, " +
                            "forwarding message", linkGroupName);
                } else {
                    logger.trace("selectPool: did not find linkGroup that can " +
                            "hold this file, processing file without space reservation.");
                }
            } else {
                logger.trace("selectPool: file is " +
                        "not found, no prior " +
                        "reservations for this file " +
                        "reserveSpaceForNonSRMTransfers={} " +
                        "subject={}",
                        reserveSpaceForNonSRMTransfers,
                        subject.getPrincipals());
            }
        }

        private void namespaceEntryDeleted(PnfsDeleteEntryNotificationMessage msg) throws DataAccessException
        {
                File file = getFile(msg.getPnfsId());
                logger.trace("Marking file as deleted {}", file);
                int rc;
                File f = selectFileForUpdate(file.getId());
                if ((f.getState() != FileState.RESERVED && f.getState() != FileState.TRANSFERRING) || f.getPnfsPath() == null) {
                    rc = jdbcTemplate.update(FileIO.UPDATE_DELETED_FLAG, 1, f.getId());
                    if (rc != 1) {
                        throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("Update failed, row count=" + rc, 1, rc);
                    }
                } else if (f.getState() == FileState.TRANSFERRING) {
                    removePnfsIdAndChangeStateOfFileInSpace(f.getId(), FileState.RESERVED);
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
                                space = getSpace(tokens[i]);
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
                                logger.error("failed to find space {}: {}",
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
                String pnfsPath = getFileTokens.getPnfsPath();
                getFileTokens.setSpaceToken(getFileSpaceTokens(pnfsId, pnfsPath));
        }

        private void extendLifetime(ExtendLifetime extendLifetime) throws DataAccessException
        {
                long token            = extendLifetime.getSpaceToken();
                long newLifetime      = extendLifetime.getNewLifetime();
                Space space = selectSpaceForUpdate(token);
                if(SpaceState.isFinalState(space.getState())) {
                        throw new DataIntegrityViolationException("Space is already released");
                }
                long creationTime = space.getCreationTime();
                long lifetime = space.getLifetime();
                if(lifetime == -1) {
                        return;
                }
                if(newLifetime == -1) {
                        jdbcTemplate.update(SpaceReservationIO.UPDATE_LIFETIME,
                                            newLifetime,
                                            token);
                        return;
                }
                long currentTime = System.currentTimeMillis();
                long remainingLifetime = creationTime+lifetime-currentTime;
                if(remainingLifetime > newLifetime) {
                        return;
                }
                jdbcTemplate.update(SpaceReservationIO.UPDATE_LIFETIME,
                                    newLifetime,
                                    token);
        }
}

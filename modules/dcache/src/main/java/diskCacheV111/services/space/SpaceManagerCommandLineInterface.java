package diskCacheV111.services.space;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.transaction.annotation.Transactional;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;

import dmg.cells.nucleus.CellCommandListener;
import dmg.util.CommandSyntaxException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;
import dmg.util.command.Option;

import org.dcache.auth.FQAN;
import org.dcache.util.ColumnWriter;
import org.dcache.util.Glob;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.primitives.Longs.tryParse;

public class SpaceManagerCommandLineInterface implements CellCommandListener
{
    private SpaceManagerDatabase db;
    private PnfsHandler pnfs;
    private AccessLatency defaultAccessLatency;
    private RetentionPolicy defaultRetentionPolicy;
    private LinkGroupLoader linkGroupLoader;
    private Executor executor;

    @Required
    public void setExecutor(Executor executor)
    {
        this.executor = executor;
    }

    @Required
    public void setDatabase(SpaceManagerDatabase db)
    {
        this.db = db;
    }

    @Required
    public void setPnfs(PnfsHandler pnfs)
    {
        this.pnfs = pnfs;
    }

    @Required
    public void setDefaultAccessLatency(AccessLatency defaultAccessLatency)
    {
        this.defaultAccessLatency = defaultAccessLatency;
    }

    @Required
    public void setDefaultRetentionPolicy(RetentionPolicy defaultRetentionPolicy)
    {
        this.defaultRetentionPolicy = defaultRetentionPolicy;
    }

    @Required
    public void setLinkGroupLoader(LinkGroupLoader linkGroupLoader)
    {
        this.linkGroupLoader = linkGroupLoader;
    }

    @Transactional(rollbackFor = { Exception.class })
    private <T extends Serializable> T callInTransaction(Callable<T> callable) throws Exception
    {
        return callable.call();
    }

    /**
     * Base class for asynchronous commands.
     *
     * Executes all commands in a database transaction.
     */
    private abstract class AsyncCommand extends DelayedCommand<String>
    {
        public AsyncCommand()
        {
            super(executor);
        }

        @Override
        protected final String execute()
                throws SpaceException, CacheException, CommandSyntaxException, DataAccessException, IllegalArgumentException
        {
            try {
                return callInTransaction(new Callable<String>()
                {
                    @Override
                    public String call() throws Exception
                    {
                        return executeInTransaction();
                    }
                });
            } catch (EmptyResultDataAccessException e) {
                // These are usually a result of the user querying for an object that doesn't exist.
                return e.getMessage();
            } catch (Exception e) {
                Throwables.propagateIfInstanceOf(e, CommandSyntaxException.class);
                Throwables.propagateIfInstanceOf(e, SpaceException.class);
                Throwables.propagateIfInstanceOf(e, CacheException.class);
                throw Throwables.propagate(e);
            }
        }

        protected abstract String executeInTransaction()
                throws SpaceException, CacheException, CommandSyntaxException, DataAccessException, IllegalArgumentException;
    }

    @Command(name = "release space", hint = "release reservation",
             description = "Releases a space reservation. The files in the reservation are not deleted " +
                     "from dCache, but the space occupied by those files is no longer accounted " +
                     "for by the space manager. Such files will continue to appear as used space in " +
                     "their link group.")
    public class ReleaseSpaceCommand extends AsyncCommand
    {
        @Argument(usage = "Space token of reservation to release.")
        long token;

        @Override
        public String executeInTransaction() throws DataAccessException
        {
            Space space = db.selectSpaceForUpdate(token);
            space.setState(SpaceState.RELEASED);
            db.updateSpace(space);
            return space.toString();
        }
    }

    @Command(name = "update space", hint = "modify space reservation parameters")
    public class UpdateSpaceCommand extends AsyncCommand
    {
        @Option(name = "size",
                usage = "Size in bytes, with optional byte unit suffix using either SI or IEEE 1541 prefixes.",
                metaVar="bytes")
        String size;

        @Option(name = "owner", usage = "User name or FQAN.", valueSpec="USER|FQAN")
        String owner;

        @Option(name = "lifetime",
                usage = "Lifetime in seconds from now.")
        Long lifetime;

        @Option(name = "eternal",
                usage = "Space reservation will never expire.")
        boolean eternal;

        @Option(name = "desc",
                usage = "Space token description.")
        String description;

        @Argument(metaVar="spacetoken", usage = "Token of space reservation to update.")
        Long token;

        @Override
        public String executeInTransaction() throws DataAccessException, SpaceReleasedException, SpaceExpiredException
        {
            Space space = db.selectSpaceForUpdate(token);

            if (space.getState() == SpaceState.RELEASED) {
                throw new SpaceReleasedException("Space reservation has been released and cannot be updated.");
            }
            if (space.getState() == SpaceState.EXPIRED) {
                throw new SpaceExpiredException("Space reservation has expired and cannot be updated.");
            }

            if (owner != null) {
                String group;
                String role;

                // check that linkgroup allows this owner combination
                LinkGroup lg = db.getLinkGroup(space.getLinkGroupId());

                FQAN fqan = new FQAN(owner);
                group = fqan.getGroup();
                role = emptyToNull(fqan.getRole());

                boolean foundMatch = false;
                for (VOInfo info : lg.getVOs()) {
                    if (info.match(group, role)) {
                        foundMatch = true;
                        break;
                    }
                }
                if (!foundMatch) {
                    return "Cannot change owner to " + owner + ". " +
                            "Authorized for this link group are:\n"+
                            Joiner.on('\n').join(lg.getVOs());
                }
                space.setVoGroup(group);
                space.setVoRole(role);
            }

            if (eternal) {
                if (lifetime != null) {
                    throw new IllegalArgumentException("Eternal reservations cannot have a lifetime.");
                }
                space.setExpirationTime(null);
            } else if (lifetime != null) {
                space.setExpirationTime(System.currentTimeMillis() + lifetime * 1000);
            }

            if (size != null) {
                space.setSizeInBytes(Unit.parseByteQuantity(size));
            }
            if (description != null) {
                space.setDescription(description);
            }
            db.updateSpace(space);
            return space.toString();
        }
    }

    @Command(name = "ls link groups", hint = "list link groups",
             description = "If an argument is given, the command displays all link groups with a name " +
                     "matching the pattern. If no argument is given, all link groups are " +
                     "displayed. The list can be further restricted using the options.\n\n" +

                     "For each link group the following information is displayed left to right: " +
                     "File types allowed in this link group (output(o), replica(r), custodial(c), " +
                     "nearline (n), online(o)), number of reservations in link group, reserved " +
                     "space, unreserved space, last refresh timestamp, and the link group name.\n\n" +

                     "Link groups are periodically imported from pool manager. The last refresh time " +
                     "indicates when the information was last updated.\n\n" +

                     "Link groups don't have a size. Only the current amount of free space in online " +
                     "pools accessible by the link group is known. Part of that free space is reserved " +
                     "(but not used) by space reservations. This is reported as reserved space. The " +
                     "remaining free space is reported as unreserved space. Unreserved space can be " +
                     "reserved by creating new space reservations or by enlarging existing reservations. " +
                     "Since pools may go offline, unreserved space can become negative. In this case " +
                     "the link group is overallocated and the reserved space is no longer guaranteed.")
    public class ListLinkGroupsCommand extends AsyncCommand
    {
        @Option(name = "a", usage = "Include link groups that have not been refreshed recently.")
        boolean all;

        @Option(name = "l", usage = "Include additional details.")
        boolean verbose;

        @Option(name = "al", usage = "Only show link groups that allow this access latency.",
                values = { "online", "nearline" })
        AccessLatency al;

        @Option(name = "rp", usage = "Only show link groups that allow this retention policy.",
                values = { "output", "replica", "custodial"})
        RetentionPolicy rp;

        @Option(name = "h",
                usage = "Use unit suffixes Byte, Kilobyte, Megabyte, Gigabyte, Terabyte and " +
                        "Petabyte in order to reduce the number of digits to three or less " +
                        "using base 10 for sizes.")
        boolean humanReadable;

        @Argument(required = false)
        Glob name;

        @Override
        public String executeInTransaction() throws DataAccessException
        {
            SpaceManagerDatabase.LinkGroupCriterion linkgroups = db.linkGroups();
            if (!all) {
                linkgroups.whereUpdateTimeAfter(linkGroupLoader.getLatestUpdateTime());
            }
            if (al != null) {
                linkgroups.allowsAccessLatency(al);
            }
            if (rp != null) {
                linkgroups.allowsRetentionPolicy(rp);
            }
            if (name != null) {
                linkgroups.whereNameMatches(name);
            }

            ColumnWriter writer = new ColumnWriter()
                    .abbreviateBytes(humanReadable)
                    .header("FLAGS").fixed("-").left("output").left("replica").left("custodial").fixed(":").left("nearline").left("online")
                    .space().header("CNT").right("spaces")
                    .space().header("RESVD").bytes("reserved")
                    .fixed(" + ")
                    .header("AVAIL").bytes("available")
                    .fixed(" = ")
                    .header("FREE").bytes("free")
                    .space().header("UPDATED").date("updated")
                    .space().header("NAME").left("name");

            for (LinkGroup group : db.get(linkgroups)) {
                writer.row()
                        .value("output", group.isOutputAllowed() ? 'o' : '-')
                        .value("replica", group.isReplicaAllowed() ? 'r' : '-')
                        .value("custodial", group.isCustodialAllowed() ? 'c' : '-')
                        .value("nearline", group.isNearlineAllowed() ? 'n' : '-')
                        .value("online", group.isOnlineAllowed() ? 'o' : '-')
                        .value("spaces", db.count(db.spaces().whereLinkGroupIs(group.getId())))
                        .value("reserved", group.getReservedSpace())
                        .value("available", group.getAvailableSpace())
                        .value("free", group.getFreeSpace())
                        .value("updated", group.getUpdateTime())
                        .value("name", group.getName());
                if (verbose) {
                    for (VOInfo voInfo : group.getVOs()) {
                        writer.row("    " + voInfo);
                    }
                }
            }
            return writer.toString();
        }
    }

    @Command(name = "ls spaces", hint = "list space reservations",
             description = "If an argument is given, the command displays space reservations for which the " +
                     "space description matches the pattern. If the argument is an integer, the argument " +
                     "is interpreted as a space token and a matching space reservation is displayed." +
                     "If no argument is given, all space reservations are displayed. The list can be " +
                     "further restricted using the options.\n\n" +

                     "For each space reservation the following information may be displayed left to right: " +
                     "Space token, reservation state (reserved(-), released(r), expired(e)), default " +
                     "retention policy, default access latency, number of files in space reservation, owner, " +
                     "allocated bytes, used bytes, unused bytes, size of space, creation time, expiration time, " +
                     "and space description.\n\n" +

                     "Space reservations have a size. This size can be partitioned into space that is " +
                     "used by files stored in the space reservation, space that is allocated for named " +
                     "files that have not yet been uploaded, and free space. The latter two make up the " +
                     "reserved space of the link group within which the space exists. Note that ones " +
                     "a file is uploaded to a space reservation, the space consumed by the file is " +
                     "obviously not free anymore and will thus not appear in the link group statistics.")
    public class ListSpacesCommand extends AsyncCommand
    {
        @Option(name = "a", usage = "Include ephemeral, expired and released space reservations.")
        boolean all;

        @Option(name = "l", usage = "Include additional details.")
        boolean verbose;

        @Option(name = "e", usage = "Include ephemeral space reservations.")
        boolean ephemeral;

        @Option(name = "owner",
                usage = "Only show reservations whose owner matches this pattern.",
                valueSpec="USER|FQAN")
        String owner;

        @Option(name = "al",
                usage = "Only show reservations with this default access latency.",
                values = { "online", "nearline" })
        AccessLatency al;

        @Option(name = "rp",
                usage = "Only show reservations with this default retention policy.",
                values = { "replica", "custodial" })
        RetentionPolicy rp;

        @Option(name = "state",
                values = { "reserved", "released", "expired" },
                usage = "Only show reservations in one of these states.")
        SpaceState[] states;

        @Option(name = "lg",
                usage = "Only show reservations in the named link group.")
        String linkGroup;

        @Option(name = "h",
                usage = "Use unit suffixes Byte, Kilobyte, Megabyte, Gigabyte, Terabyte and " +
                        "Petabyte in order to reduce the number of digits to three or less " +
                        "using base 10 for sizes.")
        boolean humanReadable;

        @Option(name = "limit",
                usage = "Limit output to this many space reservations.",
                metaVar = "rows")
        Integer limit = 10000;

        @Argument(required = false,
                  usage = "Only show reservations with this token or a description matching this pattern.",
                  valueSpec = "TOKEN|PATTERN")
        Glob pattern;

        @Override
        public String executeInTransaction() throws DataAccessException
        {
            ColumnWriter writer = new ColumnWriter()
                    .abbreviateBytes(humanReadable)
                    .header("TOKEN").right("token");
            if (all || verbose || states != null && states.length > 0 || pattern != null) {
                writer.space().header("S").left("status");
            }
            if (verbose || linkGroup == null) {
                writer.space().header("LINKGROUP").left("linkgroup");
            }
            writer
                    .space().header("RETENTION").left("rp")
                    .space().header("LATENCY").left("al");
            if (verbose || owner != null) {
                writer.space().header("OWNER").left("owner");
            }
            writer.space().header("ALLO").bytes("allocated")
                    .fixed(" + ").header("USED").bytes("used")
                    .fixed(" + ").header("FREE").bytes("free")
                    .fixed(" = ").header("SIZE").bytes("size");
            if (verbose) {
                writer.space().header("CREATED").date("created");
            }
            if (ephemeral || all || verbose || pattern != null) {
                writer.space().header("EXPIRES").date("expires");
            }
            writer.space().header("DESCRIPTION").left("description");

            Iterable<Space> spaces;
            if (pattern == null) {
                spaces = db.get(spacesWhereOptionsMatch(), limit);
            } else {
                spaces = db.get(spacesWhereOptionsMatch().whereDescriptionMatches(pattern), limit);
                Long token = tryParse(pattern.toString());
                if (token != null) {
                    List<Space> moreSpaces =
                            db.get(spacesWhereOptionsMatch().whereTokenIs(token), limit);
                    spaces = concat(moreSpaces, spaces);
                }
            }

            Map<Long,String> linkGroups =
                    Maps.transformValues(Maps.uniqueIndex(db.get(db.linkGroups()), LinkGroup.getId), LinkGroup.getName);

            for (Space space : spaces) {
                char status;
                switch (space.getState()) {
                case EXPIRED:
                    status = 'e';
                    break;
                case RELEASED:
                    status = 'r';
                    break;
                default:
                    status = '-';
                    break;
                }
                writer.row()
                        .value("token", space.getId())
                        .value("status", status)
                        .value("linkgroup", linkGroups.get(space.getLinkGroupId()))
                        .value("rp", space.getRetentionPolicy())
                        .value("al", space.getAccessLatency())
                        .value("allocated", space.getAllocatedSpaceInBytes())
                        .value("used", space.getUsedSizeInBytes())
                        .value("free", space.getAvailableSpaceInBytes())
                        .value("size", space.getSizeInBytes())
                        .value("created", space.getCreationTime())
                        .value("expires", space.getExpirationTime())
                        .value("description", space.getDescription())
                        .value("owner", toOwner(space.getVoGroup(), space.getVoRole()));
            }
            return writer.toString();
        }

        private SpaceManagerDatabase.SpaceCriterion spacesWhereOptionsMatch()
        {
            SpaceManagerDatabase.SpaceCriterion spaces = db.spaces();
            if (owner != null) {
                FQAN fqan = new FQAN(owner);
                spaces.whereGroupMatches(new Glob(fqan.getGroup()));
                if (fqan.hasRole()) {
                    spaces.whereRoleMatches(new Glob(fqan.getRole()));
                }
            }
            if (linkGroup != null) {
                spaces.whereLinkGroupIs(db.getLinkGroupByName(linkGroup).getId());
            }
            if (al != null) {
                spaces.whereAccessLatencyIs(al);
            }
            if (rp != null) {
                spaces.whereRetentionPolicyIs(rp);
            }
            if (states != null && states.length > 0) {
                spaces.whereStateIsIn(states);
            } else if (!all && pattern == null) {
                spaces.whereStateIsIn(SpaceState.RESERVED);
            }
            if (!ephemeral && !all && pattern == null) {
                spaces.thatNeverExpire();
            }
            return spaces;
        }
    }

    private String toOwner(String voGroup, String voRole)
    {
        if (voGroup.charAt(0) != '/' || voRole == null || voRole.equals("*")) {
            return voGroup;
        } else {
            return voGroup + "/Role=" + voRole;
        }
    }

    @Command(name = "ls files", hint = "list file reservations",
             description = "If an argument is given, the command displays reserved files for which the " +
                     "PNFS ID or path matches the argument. If no argument is " +
                     "given, all file reservations in a transient state are displayed. The list can be " +
                     "further expanded or restricted using the options.\n\n" +

                     "For each file reservation the following information may be displayed left to right: " +
                     "current state [transferring(t), stored(s), flushed(f)], space token, owner, size in " +
                     "bytes, creation time, expiration time, PNFS ID, and path.\n\n" +

                     "A space reservation contains file reservations that consume the reserved space. " +
                     "Each file reservation is in one of three states: TRANSFERRING, STORED, or FLUSHED.\n\n" +

                     "TRANSFERRING files are in the process of being uploaded. Such files have " +
                     "a PNFS ID associated with them.\n\n" +

                     "STORED files have finished uploading.\n\n" +

                     "FLUSHED files have been flushed to tape and no longer consume space in the " +
                     "space reservation.")
    public class ListFilesCommand extends AsyncCommand
    {
        @Option(name = "owner",
                usage = "Only show files whose owner matches this pattern.",
                valueSpec="USER|FQAN")
        String owner;

        @Option(name = "token",
                usage = "Only show files in space reservation with this token.")
        Long token;

        @Option(name = "a",
                usage = "Include stored and flushed files.")
        boolean all;

        @Option(name = "p",
                usage = "Lookup file system path from PNFS ID. This may slow down listing " +
                        "considerably.")
        boolean lookup;

        @Option(name = "limit",
                usage = "Limit output to this many space reservations.",
                metaVar = "rows")
        Integer limit = 10000;

        @Option(name = "h",
                usage = "Use unit suffixes Byte, Kilobyte, Megabyte, Gigabyte, Terabyte and " +
                        "Petabyte in order to reduce the number of digits to three or less " +
                        "using base 10 for sizes.")
        boolean humanReadable;

        @Option(name = "state",
                values = { "transferring", "stored", "flushed" },
                usage = "Only show files in one of these states.")
        FileState[] states;

        @Argument(required = false,
                  usage = "Only show files with this PNFSID or path.",
                  valueSpec = "PNFSID|PATH")
        Glob pattern;

        @Override
        public String executeInTransaction() throws DataAccessException, CacheException
        {
            ColumnWriter writer = new ColumnWriter()
                    .abbreviateBytes(humanReadable)
                    .header("STATE").left("state")
                    .space().header("TOKEN").right("token")
                    .space().header("OWNER").left("owner")
                    .space().header("SIZE").bytes("size")
                    .space().header("CREATED").date("created");
            writer.space().header("PNFSID").left("pnfsid");
            if (lookup) {
                writer.space().header("PATH").left("path");
            }

            Iterable<File> files;
            if (pattern != null) {
                PnfsId pnfsId = pnfs.getPnfsIdByPath(pattern.toString());
                files = db.get(filesWhereOptionsMatch().wherePnfsIdIs(pnfsId), limit);
                try {
                    List<File> moreFiles = db.get(filesWhereOptionsMatch().wherePnfsIdIs(new PnfsId(pattern.toString())), limit);
                    files = concat(moreFiles, files);
                } catch (IllegalArgumentException ignored) {
                }
            } else {
                files = db.get(filesWhereOptionsMatch(), limit);
            }

            for (File file : files) {
                char state;
                switch (file.getState()) {
                case TRANSFERRING:
                    state = 't';
                    break;
                case STORED:
                    state = 's';
                    break;
                case FLUSHED:
                    state = 'f';
                    break;
                default:
                    state = '-';
                    break;
                }
                PnfsId pnfsId = file.getPnfsId();
                FsPath path;
                try {
                    path = lookup ? pnfs.getPathByPnfsId(pnfsId) : null;
                } catch (FileNotFoundCacheException e) {
                    path = null;
                }
                writer.row()
                        .value("owner", toOwner(file.getVoGroup(), file.getVoRole()))
                        .value("created", file.getCreationTime())
                        .value("size", file.getSizeInBytes())
                        .value("pnfsid", pnfsId)
                        .value("path", path)
                        .value("token", file.getSpaceId())
                        .value("state", state);
            }

            return writer.toString();
        }

        private SpaceManagerDatabase.FileCriterion filesWhereOptionsMatch()
        {
            SpaceManagerDatabase.FileCriterion files = db.files();
            if (owner != null) {
                FQAN fqan = new FQAN(owner);
                files.whereGroupMatches(new Glob(fqan.getGroup()));
                if (fqan.hasRole()) {
                    files.whereRoleMatches(new Glob(fqan.getRole()));
                }
            }
            if (token != null) {
                files.whereSpaceTokenIs(token);
            }
            if (states != null && states.length > 0) {
                files.whereStateIsIn(states);
            } else if (!all && pattern == null) {
                files.whereStateIsIn(FileState.TRANSFERRING);
            }
            return files;
        }
    }

    @Command(name = "reserve space", hint = "create space reservation",
             description = "A space reservation has a size, an access latency, a retention policy, " +
                     "and an owner. It may have a description, and a lifetime. If the lifetime " +
                     "is exceeded, the reservation expires and the files in it are released. " +
                     "The owner is only used to authorize creation of the reservation in the " +
                     "link group, and to authorize the release of the reservation - it is " +
                     "not used to authorize uploads to the reservation.\n\n" +

                     "Space reservations are created in link groups. The link group authorizes " +
                     "reservations. The owner of the reservation as well as its file type " +
                     "(retention policy and access latency) must be allowed in the link group " +
                     "within which the reservation is created.\n\n" +

                     "The size argument accepts an optional byte unit suffix using either SI or " +
                     "IEEE 1541 prefixes.")
    public class ReserveSpaceCommand extends AsyncCommand
    {
        @Option(name = "owner", usage = "User name or FQAN.", valueSpec="USER|FQAN")
        String owner;

        @Option(name = "al", usage = "Access latency.",
                values = { "online", "nearline" })
        AccessLatency al = defaultAccessLatency;

        @Option(name = "rp", usage = "Retention policy.",
                values = { "replica", "custodial"})
        RetentionPolicy rp  = defaultRetentionPolicy;

        @Option(name = "desc")
        String description;

        @Option(name = "lg", required = true, metaVar = "name",
                usage = "Link group within which to create the space reservation.")
        String lg;

        @Option(name = "lifetime", metaVar = "seconds",
                usage = "Lifetime in seconds. If no lifetime is given, the reservation will " +
                        "never expire.")
        Long lifetime;

        @Argument(
                usage = "Size of reservation in bytes. Accepts an optional byte unit suffix using " +
                        "either SI or IEEE 1541 prefixes.")
        String size;

        @Override
        public String executeInTransaction() throws DataAccessException
        {
            long sizeInBytes = Unit.parseByteQuantity(size);

            LinkGroup linkGroup = db.getLinkGroupByName(lg);
            if (linkGroup.getUpdateTime() < linkGroupLoader.getLatestUpdateTime()) {
                return "Link group " + lg + " has existed, but it is no longer published by pool manager.";
            }


            String group = null;
            String role = null;
            if (owner != null) {
                FQAN fqan = new FQAN(owner);
                group = fqan.getGroup();
                role = emptyToNull(fqan.getRole());
            }
            List<Long> linkGroups = db.findLinkGroupIds(sizeInBytes,
                                                        group,
                                                        role,
                                                        al,
                                                        rp,
                                                        linkGroupLoader.getLatestUpdateTime());

            if (!linkGroups.contains(linkGroup.getId())) {
                return "Link group " + lg + " cannot accommodate the reservation requested, \n"+
                        "check that the link group satisfies the following criteria: \n"+
                        "\t it can fit the size you are requesting ("+sizeInBytes+"),\n"+
                        "\t owner you specified (" + owner +") is authorized, and \n"+
                        "\t retention policy and access latency you specified (" + rp +  ',' + al + ") are allowed.";
            }

            Space space = db.insertSpace(group,
                                         role,
                                         rp,
                                         al,
                                         linkGroup.getId(),
                                         sizeInBytes,
                                         (lifetime == null || lifetime == -1) ? -1 : lifetime * 1000,
                                         description,
                                         SpaceState.RESERVED,
                                         0,
                                         0);
            return space.toString();
        }
    }

    @Command(name = "purge file", hint = "purge file from space",
             description = "Removes a file from its space reservation without deleting the file from " +
                     "dCache. The space in the reservation that was set aside for the file will be " +
                     "available to other files, assuming the link group has enough free space.\n\n" +

                     "This command is the file level equivalent to releasing the entire space " +
                     "reservation.")
    public class PurgeFileCommand extends AsyncCommand
    {
        @Argument(metaVar = "pnfsid", usage = "PNFS ID of file.")
        PnfsId pnfsId;

        @Override
        public String executeInTransaction() throws DataAccessException, CommandSyntaxException
        {
            File f = db.findFile(pnfsId);
            if (f == null) {
                return "No such file reservation: " + pnfsId;
            }
            db.removeFile(f.getId());
            return "Purged " + f;
        }
    }

    @Command(name = "purge spaces", hint = "remove perished space reservations",
             description = "Space reservations that are expired or released are said to have perished. " +
                     "Perished space is no longer considered reserved, but it is kept in the database " +
                     "until purged. Until a space reservation is purged, the files it contained are " +
                     "still tracked in the database and can be inspected using the 'ls files' command.\n\n" +

                     "Purging a space reservation does not delete the files in dCache. They remain in " +
                     "the linkgroup in which they were stored, however space manager no longer tracks " +
                     "the files.\n\n" +

                     "Space reservations that have an expiration date are purged automatically after " +
                     "a configurable amount of time after they expire. Space reservations without an " +
                     "expiration data have to be purged explicitly.")
    public class PurgeSpacesCommand extends AsyncCommand
    {
        @Override
        protected String executeInTransaction() throws DataAccessException
        {
            db.remove(db.files().in(
                    db.spaces()
                            .whereStateIsIn(SpaceState.EXPIRED, SpaceState.RELEASED)));
            int spaces =
                    db.remove(db.spaces()
                                      .whereStateIsIn(SpaceState.EXPIRED, SpaceState.RELEASED)
                                      .thatHaveNoFiles());
            return (spaces == 1) ? "One space reservation purged." : (spaces + " space reservations purged.");
        }
    }

    private enum Unit
    {
        K(1000L),
        KB(1000L),
        KIB(1024L),
        M(1000L * 1000),
        MB(1000L * 1000),
        MIB(1024L * 1024),
        G(1000L * 1000 * 1000),
        GB(1000L * 1000 * 1000),
        GIB(1024L * 1024 * 1024),
        T(1000L * 1000 * 1000 * 1000),
        TB(1000L * 1000 * 1000 * 1000),
        TIB(1024L * 1024 * 1024 * 1024),
        P(1000L * 1000 * 1000 * 1000 * 1000),
        PB(1000L * 1000 * 1000 * 1000 * 1000),
        PIB(1024L * 1024 * 1024 * 1024 * 1024);

        long factor;

        Unit(long factor)
        {
            this.factor = factor;
        }

        private static long parseByteQuantity(String s)
        {
            try {
                s = s.toUpperCase();
                for (Unit unit : Unit.values()) {
                    if (s.endsWith(unit.name())) {
                        String sSize = s.substring(0, s.length() - unit.name().length());
                        long size = (long) (Double.parseDouble(sSize) * unit.factor + 0.5);
                        return checkNonNegative(size);
                    }
                }
                return checkNonNegative(Long.parseLong(s));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Cannot convert size specified (" + s + ") to non-negative number. \n"
                                                           + "Valid definitions of size:\n"
                                                           + "\t\t - a number of bytes (long integer less than 2^64) \n"
                                                           + "\t\t - 100k, 100kB, 100KB, 100KiB, 100M, 100MB, 100MiB, 100G, 100GB, \n"
                                                           + "\t\t   100GiB, 10T, 10.5TB, 100TiB, 2P, 2.3PB, 1PiB\n"
                                                           + "see http://en.wikipedia.org/wiki/Gigabyte for an explanation.");
            }
        }

        private static long checkNonNegative(long size)
        {
            if (size < 0L) {
                throw new IllegalArgumentException("Size must be non-negative.");
            }
            return size;
        }
    }
}

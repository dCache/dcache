package diskCacheV111.services.space;

import org.springframework.dao.DataAccessException;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.List;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;

import org.dcache.util.Glob;

@ParametersAreNonnullByDefault
public interface SpaceManagerDatabase
{
    File selectFileForUpdate(PnfsId pnfsId) throws DataAccessException;

    File selectFileForUpdate(long id) throws DataAccessException;

    File selectFileFromSpaceForUpdate(String pnfsPath, long reservationId)
            throws DataAccessException;

    void removeFile(long fileId) throws DataAccessException;

    void clearPnfsIdOfFile(long id)
            throws DataAccessException;

    void removePnfsIdAndChangeStateOfFile(long id, FileState state)
            throws DataAccessException;


    void updateFile(@Nullable String voGroup,
                    @Nullable String voRole,
                    @Nullable PnfsId pnfsId,
                    @Nullable Long sizeInBytes,
                    @Nullable Long lifetime,
                    @Nullable FileState state,
                    @Nullable Boolean deleted,
                    File f)
            throws DataAccessException;

    List<File> getExpiredFiles();

    File getUnboundFile(String pnfsPath);

    long insertFile(long reservationId,
                    @Nullable String voGroup,
                    @Nullable String voRole,
                    long sizeInBytes,
                    long lifetime,
                    @Nullable String pnfsPath,
                    @Nullable PnfsId pnfsId)
            throws DataAccessException, SpaceException;

    List<Long> getSpaceTokensOfFile(@Nullable PnfsId pnfsId, @Nullable FsPath pnfsPath) throws DataAccessException;

    List<Long> findSpaceTokensByDescription(String description);

    void expireSpaces();

    List<Space> getReservedSpaces();

    Space insertSpace(@Nullable String voGroup,
                      @Nullable String voRole,
                      RetentionPolicy retentionPolicy,
                      AccessLatency accessLatency,
                      long linkGroupId,
                      long sizeInBytes,
                      long lifetime,
                      @Nullable String description,
                      SpaceState state,
                      long used,
                      long allocated)
            throws DataAccessException;

    Space selectSpaceForUpdate(long id, long sizeInBytes) throws DataAccessException;

    Space selectSpaceForUpdate(long id) throws DataAccessException;

    Space updateSpace(long id,
                      @Nullable String voGroup,
                      @Nullable String voRole,
                      @Nullable RetentionPolicy retentionPolicy,
                      @Nullable AccessLatency accessLatency,
                      @Nullable Long linkGroupId,
                      @Nullable Long sizeInBytes,
                      @Nullable Long lifetime,
                      @Nullable String description,
                      @Nullable SpaceState state)
            throws DataAccessException;

    Space updateSpace(Space space,
                      @Nullable String voGroup,
                      @Nullable String voRole,
                      @Nullable RetentionPolicy retentionPolicy,
                      @Nullable AccessLatency accessLatency,
                      @Nullable Long linkGroupId,
                      @Nullable Long sizeInBytes,
                      @Nullable Long lifetime,
                      @Nullable String description,
                      @Nullable SpaceState state)
            throws DataAccessException;

    List<Long> findSpaceTokensByVoGroupAndRole(@Nullable String voGroup, @Nullable String voRole)
            throws DataAccessException;

    Space getSpace(long id) throws DataAccessException;

    long updateLinkGroup(String linkGroupName,
                         long freeSpace,
                         long updateTime,
                         boolean onlineAllowed,
                         boolean nearlineAllowed,
                         boolean replicaAllowed,
                         boolean outputAllowed,
                         boolean custodialAllowed,
                         VOInfo[] linkGroupVOs) throws DataAccessException;

    LinkGroup getLinkGroup(long id) throws DataAccessException;

    List<LinkGroup> getLinkGroups();

    LinkGroup getLinkGroupByName(String name) throws DataAccessException;

    List<Long> findLinkGroupIds(long sizeInBytes,
                                String voGroup,
                                String voRole,
                                AccessLatency al,
                                RetentionPolicy rp,
                                long lastUpdateTime)
            throws DataAccessException;

    List<LinkGroup> findLinkGroups(long sizeInBytes,
                                   AccessLatency al,
                                   RetentionPolicy rp,
                                   long lastUpdateTime)
            throws DataAccessException;

    File getFile(PnfsId pnfsId) throws DataAccessException;

    /** Return a new link group criterion. */
    LinkGroupCriterion linkGroupCriterion();

    /** Return link groups matching a criterion. */
    List<LinkGroup> getLinkGroups(LinkGroupCriterion criterion);

    /** Return a new space reservation criterion. */
    SpaceCriterion spaceCriterion();

    /** Return space reservations matching a criterion. */
    List<Space> getSpaces(SpaceCriterion criterion, Integer limit);

    int getCountOfSpaces(SpaceCriterion criterion);

    /** Return a new file criterion. */
    FileCriterion fileCriterion();

    /** Get files matching the criterion. */
    List<File> getFiles(FileCriterion criterion, Integer limit);

    /** Return the number of files matching the criterion. */
    int getCountOfFiles(FileCriterion criterion);

    /** Selection criterion for link groups. */
    public interface LinkGroupCriterion
    {
        LinkGroupCriterion whereUpdateTimeAfter(long latestLinkGroupUpdateTime);

        LinkGroupCriterion allowsAccessLatency(AccessLatency al);

        LinkGroupCriterion allowsRetentionPolicy(RetentionPolicy rp);

        LinkGroupCriterion whereNameMatches(Glob name);
    }

    /** Selection criterion for space reservations. */
    public interface SpaceCriterion
    {
        SpaceCriterion whereStateIsIn(SpaceState... state);

        SpaceCriterion whereRetentionPolicyIs(RetentionPolicy rp);

        SpaceCriterion whereAccessLatencyIs(AccessLatency al);

        SpaceCriterion whereDescriptionMatches(Glob desc);

        SpaceCriterion whereRoleMatches(Glob role);

        SpaceCriterion whereGroupMatches(Glob group);

        SpaceCriterion whereTokenIs(long token);

        SpaceCriterion whereLifetimeIs(int i);

        SpaceCriterion whereLinkGroupIs(long id);
    }

    /** Selection criterion for file reservations. */
    public interface FileCriterion
    {
        FileCriterion whereGroupMatches(Glob group);

        FileCriterion whereRoleMatches(Glob role);

        FileCriterion whereSpaceTokenIs(Long token);

        FileCriterion whereStateIsIn(FileState... states);

        FileCriterion whereDeletedIs(boolean b);

        FileCriterion wherePathMatches(Glob pattern);

        FileCriterion wherePnfsIdIs(PnfsId pnfsId);
    }
}

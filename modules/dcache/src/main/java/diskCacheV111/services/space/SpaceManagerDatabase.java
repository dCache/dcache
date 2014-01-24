package diskCacheV111.services.space;

import org.springframework.dao.DataAccessException;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.List;

import diskCacheV111.util.AccessLatency;
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

    void expireSpaces();

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

    Space selectSpaceForUpdate(long id) throws DataAccessException;

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
    LinkGroupCriterion linkGroups();

    /** Return link groups matching criterion. */
    List<LinkGroup> get(LinkGroupCriterion criterion);



    /** Return a new space reservation criterion. */
    SpaceCriterion spaces();

    /** Return space reservations matching criterion. */
    List<Space> get(SpaceCriterion criterion, Integer limit);

    /** Return space tokens of spaces matchin criterion. */
    List<Long> getSpaceTokensOf(SpaceCriterion criterion);

    /** Return the number of space reservations matching criterion. */
    int count(SpaceCriterion criterion);


    /** Return a new file criterion. */
    FileCriterion files();

    /** Get files matching the criterion. */
    List<File> get(FileCriterion criterion, Integer limit);

    /** Return the number of files matching the criterion. */
    int count(FileCriterion criterion);

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

        SpaceCriterion whereGroupIs(String group);

        SpaceCriterion whereRoleIs(String role);

        SpaceCriterion whereDescriptionIs(String description);
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

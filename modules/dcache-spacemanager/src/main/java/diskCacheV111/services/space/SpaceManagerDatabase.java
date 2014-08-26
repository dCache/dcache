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

    void removeFile(long fileId) throws DataAccessException;

    void updateFile(File f)
            throws DataAccessException;

    long insertFile(long reservationId,
                    @Nullable String voGroup,
                    @Nullable String voRole,
                    long sizeInBytes,
                    @Nullable PnfsId pnfsId,
                    FileState state)
            throws DataAccessException, SpaceException;

    void expire(SpaceCriterion criterion);

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

    Space updateSpace(Space space)
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

    /**
     *  Returns the file reservation bound to the pnfsid, or null
     *  if such a reservation is not found.
     */
    File findFile(PnfsId pnfsId) throws DataAccessException;

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

    /** Delete all spaces matching criterion. */
    int remove(SpaceCriterion spaceCriterion);

    /** Return a new file criterion. */
    FileCriterion files();

    /** Get files matching the criterion. */
    List<File> get(FileCriterion criterion, Integer limit);

    /** Return the number of files matching the criterion. */
    int count(FileCriterion criterion);

    /** Delete all files matching criterion. */
    int remove(FileCriterion criterion);

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

        SpaceCriterion thatNeverExpire();

        SpaceCriterion whereLinkGroupIs(long id);

        SpaceCriterion whereGroupIs(String group);

        SpaceCriterion whereRoleIs(String role);

        SpaceCriterion whereDescriptionIs(String description);

        SpaceCriterion thatExpireBefore(long millis);

        SpaceCriterion thatHaveNoFiles();
    }

    /** Selection criterion for file reservations. */
    public interface FileCriterion
    {
        FileCriterion whereGroupMatches(Glob group);

        FileCriterion whereRoleMatches(Glob role);

        FileCriterion whereSpaceTokenIs(Long token);

        FileCriterion whereStateIsIn(FileState... states);

        FileCriterion wherePnfsIdIs(PnfsId pnfsId);

        FileCriterion in(SpaceCriterion spaceCriterion);

        FileCriterion whereCreationTimeIsBefore(long millis);
    }
}

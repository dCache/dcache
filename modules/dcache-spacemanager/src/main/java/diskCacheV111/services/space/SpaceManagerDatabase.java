package diskCacheV111.services.space;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.dcache.util.SqlGlob;
import org.springframework.dao.DataAccessException;

@ParametersAreNonnullByDefault
public interface SpaceManagerDatabase {

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
          @Nullable VOInfo[] linkGroupVOs) throws DataAccessException;

    LinkGroup getLinkGroup(long id) throws DataAccessException;

    LinkGroup getLinkGroupByName(String name) throws DataAccessException;

    /**
     * Returns the file reservation bound to the pnfsid, or null if such a reservation is not
     * found.
     */
    File findFile(PnfsId pnfsId) throws DataAccessException;

    /**
     * Return a new link group criterion.
     */
    LinkGroupCriterion linkGroups();

    /**
     * Return link groups matching criterion.
     */
    List<LinkGroup> get(LinkGroupCriterion criterion);


    /**
     * Return a new space reservation criterion.
     */
    SpaceCriterion spaces();

    /**
     * Return space reservations matching criterion.
     */
    List<Space> get(SpaceCriterion criterion, @Nullable Integer limit);

    /**
     * Return space tokens of spaces matchin criterion.
     */
    List<Long> getSpaceTokensOf(SpaceCriterion criterion);

    /**
     * Return the number of space reservations matching criterion.
     */
    int count(SpaceCriterion criterion);

    /**
     * Delete all spaces matching criterion.
     */
    int remove(SpaceCriterion spaceCriterion);

    /**
     * Return a new file criterion.
     */
    FileCriterion files();

    /**
     * Get files matching the criterion.
     */
    List<File> get(FileCriterion criterion, @Nullable Integer limit);

    /**
     * Return the number of files matching the criterion.
     */
    int count(FileCriterion criterion);

    /**
     * Delete all files matching criterion.
     */
    int remove(FileCriterion criterion);

    /**
     * Selection criterion for link groups.
     */
    interface LinkGroupCriterion {

        LinkGroupCriterion whereUpdateTimeAfter(long latestLinkGroupUpdateTime);

        LinkGroupCriterion allowsAccessLatency(@Nullable AccessLatency al);

        LinkGroupCriterion allowsRetentionPolicy(@Nullable RetentionPolicy rp);

        LinkGroupCriterion whereNameMatches(SqlGlob name);

        LinkGroupCriterion hasAvailable(long sizeInBytes);
    }

    /**
     * Selection criterion for space reservations.
     */
    interface SpaceCriterion {

        SpaceCriterion whereStateIsIn(SpaceState... state);

        SpaceCriterion whereRetentionPolicyIs(RetentionPolicy rp);

        SpaceCriterion whereAccessLatencyIs(AccessLatency al);

        SpaceCriterion whereDescriptionMatches(SqlGlob desc);

        SpaceCriterion whereRoleMatches(SqlGlob role);

        SpaceCriterion whereGroupMatches(SqlGlob group);

        SpaceCriterion whereTokenIs(long token);

        SpaceCriterion thatNeverExpire();

        SpaceCriterion whereLinkGroupIs(long id);

        SpaceCriterion whereGroupIs(String group);

        SpaceCriterion whereRoleIs(String role);

        SpaceCriterion whereDescriptionIs(String description);

        SpaceCriterion thatExpireBefore(long millis);

        SpaceCriterion thatHaveNoFiles();
    }

    /**
     * Selection criterion for file reservations.
     */
    interface FileCriterion {

        FileCriterion whereGroupMatches(SqlGlob group);

        FileCriterion whereRoleMatches(SqlGlob role);

        FileCriterion whereSpaceTokenIs(Long token);

        FileCriterion whereStateIsIn(FileState... states);

        FileCriterion wherePnfsIdIs(PnfsId pnfsId);

        FileCriterion in(SpaceCriterion spaceCriterion);

        FileCriterion whereCreationTimeIsBefore(long millis);
    }
}

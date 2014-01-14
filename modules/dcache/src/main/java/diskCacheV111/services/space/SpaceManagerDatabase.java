package diskCacheV111.services.space;

import org.springframework.dao.DataAccessException;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.List;
import java.util.Set;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.VOInfo;

@ParametersAreNonnullByDefault
public interface SpaceManagerDatabase
{
    File selectFileForUpdate(PnfsId pnfsId) throws DataAccessException;

    File selectFileForUpdate(long id) throws DataAccessException;

    File selectFileFromSpaceForUpdate(String pnfsPath, long reservationId)
            throws DataAccessException;

    List<File> getFilesInSpace(long spaceId)
            throws DataAccessException;

    void removeExpiredFilesFromSpace(long spaceId, Set<FileState> states)
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

    List<Space> findSpaces(@Nullable String group, @Nullable String role, @Nullable String description,
                           @Nullable LinkGroup lg);

    List<Space> getSpaces(Set<SpaceState> states, int nRows)
            throws DataAccessException;

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

    List<LinkGroup> getLinkGroupsRefreshedAfter(long lastUpdateTime);

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
}

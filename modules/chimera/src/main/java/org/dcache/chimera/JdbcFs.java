/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.dcache.acl.enums.AceFlags.DIRECTORY_INHERIT_ACE;
import static org.dcache.acl.enums.AceFlags.FILE_INHERIT_ACE;
import static org.dcache.acl.enums.AceFlags.INHERIT_ONLY_ACE;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.NO_STAT;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.STAT;
import static org.dcache.util.ByteUnit.EiB;
import static org.dcache.util.SqlHelper.tryToClose;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import diskCacheV111.util.RetentionPolicy;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.dcache.acl.ACE;
import org.dcache.acl.enums.RsType;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.quota.QuotaHandler;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.util.Checksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.NonTransientDataAccessResourceException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * JDBC-FS is THE building block of Chimera. It's an abstraction layer, which allows to build
 * filesystem on top of a RDBMS.
 *
 * @Immutable
 * @Threadsafe
 */
public class JdbcFs implements FileSystemProvider, LeaderLatchListener {

    /**
     * common error message for unimplemented
     */
    private static final String NOT_IMPL =
          "this operation is unsupported for this "
                + "file system; please install a dCache-aware "
                + "implementation of the file system interface";

    /**
     * logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcFs.class);
    /**
     * the number of pnfs levels. Level zero associated with file real content, which is not our
     * regular case.
     */
    public static final int LEVELS_NUMBER = 7;

    /**
     * SQL query engine
     */
    private final FsSqlDriver _sqlDriver;

    /**
     * Database connection pool
     */
    private final DataSource _dbConnectionsPool;

    private final PlatformTransactionManager _tx;

    private final TransactionDefinition _txDefinition = new DefaultTransactionDefinition();

    /*
     * A dummy constant key force bay cache interface. the value doesn't
     * matter - only that it's the same value every time
     */
    private static final Integer DUMMY_KEY = 0;
    /**
     * Cache value of FsStat
     */
    private final Executor _fsStatUpdateExecutor =
          Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                      .setNameFormat("fsstat-updater-thread-%d")
                      .build()
          );

    private final LoadingCache<Object, FsStat> _fsStatCache
          = CacheBuilder.newBuilder()
          .refreshAfterWrite(100, TimeUnit.MILLISECONDS)
          .build(
                CacheLoader.asyncReloading(new CacheLoader<Object, FsStat>() {

                                               @Override
                                               public FsStat load(Object k) throws Exception {
                                                   return JdbcFs.this.getFsStat0();
                                               }
                                           }
                      , _fsStatUpdateExecutor));

    /* The PNFS ID to inode number mapping will never change while dCache is running.
     */
    protected final Cache<String, Long> _inoCache =
          CacheBuilder.newBuilder()
                .maximumSize(100000)
                .build();

    /* The inode number to PNFS ID mapping will never change while dCache is running.
     */
    protected final Cache<Long, String> _idCache =
          CacheBuilder.newBuilder()
                .maximumSize(100000)
                .build();

    private QuotaHandler _quota;

    /**
     * current fs id
     */
    private final int _fsId;
    /**
     * available space
     */
    static final long AVAILABLE_SPACE = EiB.toBytes(1L);
    /**
     * total files
     */
    static final long TOTAL_FILES = 62914560L;

    /**
     * maximal length of an object name in a directory.
     */
    private static final int MAX_NAME_LEN = 255;


    /**
     * switch quota check on/off
     */
    private boolean _quotaEnabled;

    /**
     * default retention policy
     */
    private RetentionPolicy _defaultRetentionPolicy;

    /**
     * File attribute consistency policy.
     */
    private final String _attributeConsistency;

    private final ScheduledExecutorService maintenanceTaskExecutor = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
                .setNameFormat("chimera-maintenance-executor-%d")
                .build()
    );

    private ScheduledFuture<?> maintenanceTask;


    public JdbcFs(DataSource dataSource, PlatformTransactionManager txManager, String consistency)
          throws SQLException, ChimeraFsException {
        this(dataSource, txManager, 0, consistency);
    }

    public JdbcFs(DataSource dataSource, PlatformTransactionManager txManager, int id, String consistency)
          throws SQLException, ChimeraFsException {
        _dbConnectionsPool = dataSource;
        _fsId = id;

        _tx = txManager;

        _attributeConsistency = consistency;
        // try to get database dialect specific query engine
        _sqlDriver = FsSqlDriver.getDriverInstance(dataSource, _attributeConsistency);
    }

    public void setQuota(QuotaHandler quota) {
        _quota = quota;
    }

    public void setQuotaEnabled(boolean enabled) {
        _quotaEnabled = enabled;
        if (_quotaEnabled) {
            _quota.scheduleRefreshQuota();
        }
    }

    public QuotaHandler getQuota() {
        return _quota;
    }

    public boolean isQuotaEnabled() {
        return _quotaEnabled;
    }

    public void setDefaultRetentionPolicy(RetentionPolicy rp) {
        _defaultRetentionPolicy = rp;
    }

    private FsInode getWormID() throws ChimeraFsException {

        return this.path2inode("/admin/etc/config");
    }

    private <T> T inTransaction(FallibleTransactionCallback<T> callback)
          throws ChimeraFsException {
        TransactionStatus status = _tx.getTransaction(_txDefinition);
        T result;
        try {
            result = callback.doInTransaction(status);
            _tx.commit(status);
        } catch (ChimeraFsException e) {
            rollbackOnException(status, e);
            throw e;
        } catch (NonTransientDataAccessResourceException e) {
            rollbackOnException(status, e);
            throw new BackEndErrorChimeraFsException(e.getMessage(), e);
        } catch (DataAccessException e) {
            rollbackOnException(status, e);
            throw new ChimeraFsException(e.getMessage(), e);
        } catch (Exception e) {
            rollbackOnException(status, e);
            throw e;
        }
        return result;
    }

    /**
     * Perform a rollback, handling rollback exceptions properly.
     *
     * @param status object representing the transaction
     * @param ex     the thrown application exception or error
     * @throws TransactionException in case of a rollback error
     */
    private void rollbackOnException(TransactionStatus status, Throwable ex)
          throws TransactionException {
        LOGGER.debug("Initiating transaction rollback on application exception", ex);
        try {
            _tx.rollback(status);
        } catch (TransactionSystemException e) {
            LOGGER.error("Application exception overridden by rollback exception", ex);
            e.initApplicationException(ex);
            throw e;
        } catch (RuntimeException e) {
            LOGGER.error("Application exception overridden by rollback exception", ex);
            throw e;
        } catch (Error err) {
            LOGGER.error("Application exception overridden by rollback error", ex);
            throw err;
        }
    }

    //////////////////////////////////////////////////////////
    ////
    ////
    ////      Fs operations
    ////
    /////////////////////////////////////////////////////////
    @Override
    public FsInode createLink(String src, String dest) throws ChimeraFsException {
        File file = new File(src);
        return inTransaction(
              status -> createLink(path2inode(file.getParent()), file.getName(), dest));
    }

    @Override
    public FsInode createLink(FsInode parent, String name, String dest) throws ChimeraFsException {
        return inTransaction(status -> createLink(parent, name, 0, 0, 0777,
              dest.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public FsInode createLink(FsInode parent, String name, int uid, int gid, int mode, byte[] dest)
          throws ChimeraFsException {

        checkNameLength(name);

        return inTransaction(status -> {
            FsInode inode;
            try {
                Stat stat = parent.statCache();
                int group = (stat.getMode() & UnixPermission.S_ISGID) != 0 ? stat.getGid() : gid;
                inode = _sqlDriver.createFile(parent, name, uid, group, mode,
                      UnixPermission.S_IFLNK);
                // link is a regular file where content is a reference
                _sqlDriver.setInodeIo(inode, true);
                _sqlDriver.write(inode, 0, 0, dest, 0, dest.length);
                _sqlDriver.copyAcl(parent, inode, RsType.FILE,
                      EnumSet.of(INHERIT_ONLY_ACE, DIRECTORY_INHERIT_ACE, FILE_INHERIT_ACE),
                      EnumSet.of(FILE_INHERIT_ACE));
                fillIdCaches(inode);
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
            return inode;
        });
    }

    /**
     * create a hard link
     *
     * @param parent inode of directory where to create
     * @param inode
     * @param name
     * @return
     * @throws ChimeraFsException
     */
    @Override
    public FsInode createHLink(FsInode parent, FsInode inode, String name)
          throws ChimeraFsException {

        checkNameLength(name);

        return inTransaction(status -> {
            try {


                Stat parentStat = parent.stat();
                if (parentStat == null) {
                    throw FileNotFoundChimeraFsException.of(parent);
                }

                if ((parentStat.getMode() & UnixPermission.F_TYPE) != UnixPermission.S_IFDIR) {
                    throw new NotDirChimeraException(parent);
                }

                Stat inodeStat = inode.stat();
                if (inodeStat == null) {
                    throw FileNotFoundChimeraFsException.of(inode);
                }

                if ((inodeStat.getMode() & UnixPermission.F_TYPE) == UnixPermission.S_IFDIR) {
                    throw new PermissionDeniedChimeraFsException("hard link not allowed for directory");
                }

                _sqlDriver.createEntryInParent(parent, name, inode);
                _sqlDriver.incNlinkForFile(inode);
                _sqlDriver.incNlinkForDir(parent, 0);
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
            return inode;
        });
    }

    @Override
    public FsInode createFile(String path) throws ChimeraFsException {
        File file = new File(path);
        return inTransaction(status -> createFile(path2inode(file.getParent()), file.getName()));
    }

    @Override
    public FsInode createFile(FsInode parent, String name) throws ChimeraFsException {
        return inTransaction(status -> createFile(parent, name, 0, 0, 0644));
    }

    @Override
    public FsInode createFileLevel(FsInode inode, int level) throws ChimeraFsException {
        return inTransaction(
              status -> _sqlDriver.createLevel(inode, 0, 0, 0644 | UnixPermission.S_IFREG, level));
    }

    @Override
    public FsInode createFile(FsInode parent, String name, int owner, int group, int mode)
          throws ChimeraFsException {
        return createFile(parent, name, owner, group, mode, UnixPermission.S_IFREG);
    }

    @Override
    public FsInode createFile(FsInode parent, String name, int owner, int group, int mode, int type)
          throws ChimeraFsException {
        if (name.startsWith(".(")) { // special files only
            String[] cmd = PnfsCommandProcessor.process(name);

            if (name.startsWith(".(tag)(") && (cmd.length == 2)) {
                this.createTag(parent, cmd[1], owner, group, 0644);
                return new FsInode_TAG(this, parent.ino(), cmd[1]);
            }

            if (name.startsWith(".(pset)(") || name.startsWith(".(fset)(")
                  || name.startsWith(".(suri)(")) {
                /**
                 * This is not 100% correct, as we throw exist even if
                 * someone tries to set attribute for a file which does not exist.
                 */
                throw new FileExistsChimeraFsException(name);
            }

            if (name.startsWith(".(use)(") && (cmd.length == 3)) {
                int level = Integer.parseInt(cmd[1]);
                return inTransaction(status -> {
                    FsInode useInode = _sqlDriver.inodeOf(parent, cmd[2], STAT);
                    if (useInode == null) {
                        throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, cmd[2]);
                    }
                    try {
                        Stat stat = useInode.statCache();
                        return _sqlDriver.createLevel(useInode, stat.getUid(),
                              stat.getGid(),
                              stat.getMode(), level);
                    } catch (DuplicateKeyException e) {
                        throw new FileExistsChimeraFsException(name, e);
                    }
                });
            }

            if (name.startsWith(".(access)(") && (cmd.length == 3)) {
                int accessLevel = Integer.parseInt(cmd[2]);
                if (accessLevel == 0) {
                    return id2inode(cmd[1], NO_STAT);
                }
                return inTransaction(status -> {
                    try {
                        FsInode accessInode = id2inode(cmd[1], STAT);
                        Stat stat = accessInode.statCache();
                        return _sqlDriver.createLevel(accessInode,
                              stat.getUid(), stat.getGid(),
                              stat.getMode(), accessLevel);
                    } catch (DuplicateKeyException e) {
                        throw new FileExistsChimeraFsException(name, e);
                    }
                });
            }

            return null;
        }

        checkNameLength(name);
        checkArgument(UnixPermission.getType(type) != UnixPermission.S_IFDIR);

        return inTransaction(status -> {
            try {
                Stat parentStat = parent.statCache();
                if (parentStat == null) {
                    throw FileNotFoundChimeraFsException.of(parent);
                }

                if ((parentStat.getMode() & UnixPermission.F_TYPE) != UnixPermission.S_IFDIR) {
                    throw new NotDirChimeraException(parent);
                }

                int gid = (parentStat.getMode() & UnixPermission.S_ISGID) != 0 ? parentStat.getGid()
                      : group;
                if (_quotaEnabled) {
                    RetentionPolicy rp = getRetentionPolicyFromParentTag(parent);
                    checkQuota(owner, gid, rp);
                }
                FsInode inode = _sqlDriver.createFile(parent, name, owner, gid, mode, type);
                _sqlDriver.copyAcl(parent, inode, RsType.FILE,
                      EnumSet.of(INHERIT_ONLY_ACE, DIRECTORY_INHERIT_ACE, FILE_INHERIT_ACE),
                      EnumSet.of(FILE_INHERIT_ACE));
                fillIdCaches(inode);
                return inode;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
        });
    }

    /**
     * Create a new entry with given inode id.
     *
     * @param parent
     * @param id
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @param type
     * @throws ChimeraFsException
     */
    @Override
    public void createFileWithId(FsInode parent, String id, String name, int owner, int group,
          int mode, int type) throws ChimeraFsException {

        checkNameLength(name);
        checkArgument((type & UnixPermission.S_IFDIR) == 0);

        inTransaction(status -> {
            try {
                if (!parent.exists()) {
                    throw FileNotFoundChimeraFsException.of(parent);
                }
                if (!parent.isDirectory()) {
                    throw new NotDirChimeraException(parent);
                }
                Stat stat = parent.statCache();
                int gid = (stat.getMode() & UnixPermission.S_ISGID) != 0 ? stat.getGid() : group;
                if (_quotaEnabled) {
                    RetentionPolicy rp = getRetentionPolicyFromParentTag(parent);
                    checkQuota(owner, gid, rp);
                }
                FsInode inode = _sqlDriver.createFileWithId(parent, id, name, owner, gid, mode,
                      type);
                _sqlDriver.copyAcl(parent, inode, RsType.FILE,
                      EnumSet.of(INHERIT_ONLY_ACE, DIRECTORY_INHERIT_ACE, FILE_INHERIT_ACE),
                      EnumSet.of(FILE_INHERIT_ACE));
                fillIdCaches(inode);
                return null;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
        });
    }

    @Override
    public DirectoryStreamB<ChimeraDirectoryEntry> newDirectoryStream(FsInode dir)
          throws ChimeraFsException {
        if(dir.type() == FsInodeType.LABELS){
            return _sqlDriver.labelsDirectoryStream(dir);
        }else if ((dir.type() == FsInodeType.LABEL)) {
            return _sqlDriver.virtualDirectoryStream(dir, _sqlDriver.getLabelById(dir.ino()));
        } else {
            return _sqlDriver.newDirectoryStream(dir);
        }
    }

    @Override
    public DirectoryStreamB<ChimeraDirectoryEntry> virtualDirectoryStream(FsInode dir,
          String labelname) throws ChimeraFsException {
        return _sqlDriver.virtualDirectoryStream(dir, labelname);
    }


    @Override
    public DirectoryStreamB<ChimeraDirectoryEntry> listLabelsStream(FsInode dir) throws ChimeraFsException {
        return _sqlDriver.labelsDirectoryStream(dir);
    }



    @Override
    public void remove(String path) throws ChimeraFsException {

        File filePath = new File(path);

        String parentPath = filePath.getParent();
        if (parentPath == null) {
            throw new InvalidArgumentChimeraException("Cannot delete file system root.");
        }

        inTransaction(status -> {
            FsInode parent = path2inode(parentPath);
            String name = filePath.getName();
            FsInode inode = _sqlDriver.inodeOf(parent, name, STAT);
            if (inode == null || !_sqlDriver.remove(parent, name, inode)) {
                throw FileNotFoundChimeraFsException.ofPath(path);
            }
            return null;
        });
    }

    @Override
    public void remove(FsInode directory, String name, FsInode inode) throws ChimeraFsException {
        inTransaction(status -> {
            if (!_sqlDriver.remove(directory, name, inode)) {
                throw FileNotFoundChimeraFsException.ofFileInDirectory(directory, name);
            }
            return null;
        });
    }

    @Override
    public void remove(FsInode inode) throws ChimeraFsException {
        inTransaction(status -> {
            if (inode.type() != FsInodeType.INODE) {
                // now allowed
                throw new InvalidArgumentChimeraException("Not a file.");
            }
            if (inode.ino() == _sqlDriver.getRootInumber()) {
                throw new InvalidArgumentChimeraException("Cannot delete file system root.");
            }
            if (!inode.exists()) {
                throw FileNotFoundChimeraFsException.of(inode);
            }
            if (inode.isDirectory() && inode.statCache().getNlink() > 2) {
                throw new DirNotEmptyChimeraFsException("Directory is not empty");
            }
            _sqlDriver.remove(inode);
            return null;
        });
    }

    @Override
    public Stat stat(String path) throws ChimeraFsException {
        return stat(path2inode(path));
    }

    @Override
    public Stat stat(FsInode inode) throws ChimeraFsException {
        return stat(inode, 0);
    }

    @Override
    public Stat stat(FsInode inode, int level) throws ChimeraFsException {
        Stat stat = _sqlDriver.stat(inode, level);
        if (stat == null) {
            throw FileNotFoundChimeraFsException.of(inode);
        }
        if (level == 0 ){
            _inoCache.put(stat.getId(), stat.getIno());
            _idCache.put(stat.getIno(), stat.getId());
        }
        return stat;
    }

    @Override
    public FsInode mkdir(String path) throws ChimeraFsException {
        int li = path.lastIndexOf('/');
        String file = path.substring(li + 1);
        String dir = (li > 1) ? path.substring(0, li) : "/";
        return inTransaction(status -> mkdir(path2inode(dir), file));
    }

    @Override
    public FsInode mkdir(FsInode parent, String name) throws ChimeraFsException {
        return mkdir(parent, name, 0, 0, 0755);
    }

    @Override
    public FsInode mkdir(FsInode parent, String name, int owner, int group, int mode)
          throws ChimeraFsException {
        checkNameLength(name);

        return inTransaction(status -> {
            try {
                if (!parent.isDirectory()) {
                    throw new NotDirChimeraException(parent);
                }

                Stat stat = parent.statCache();
                int gid, perm;
                if ((stat.getMode() & UnixPermission.S_ISGID) != 0) {
                    gid = stat.getGid();
                    perm = mode | UnixPermission.S_ISGID;
                } else {
                    gid = group;
                    perm = mode;
                }

                // ensure that t_inodes and t_tags_inodes update in the same order as
                // in removeDir
                FsInode inode = _sqlDriver.mkdir(parent, name, owner, gid, perm);
                _sqlDriver.copyTags(parent, inode);
                _sqlDriver.copyAcl(parent, inode, RsType.DIR, EnumSet.of(INHERIT_ONLY_ACE),
                      EnumSet.of(FILE_INHERIT_ACE, DIRECTORY_INHERIT_ACE));
                fillIdCaches(inode);
                return inode;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(name, e);
            }
        });
    }

    private void fillIdCaches(FsInode inode) {
        Stat stat = inode.getStatCache();
        if (stat != null) {
            _inoCache.put(stat.getId(), stat.getIno());
            _idCache.put(stat.getIno(), stat.getId());
        }
    }

    @Override
    public FsInode mkdir(FsInode parent, String name, int owner, int group, int mode,
          List<ACE> acl, Map<String, byte[]> tags)
          throws ChimeraFsException {
        checkNameLength(name);

        return inTransaction(status -> {
            try {
                if (!parent.isDirectory()) {
                    throw new NotDirChimeraException(parent);
                }
                Stat stat = parent.statCache();
                int gid, perm;
                if ((stat.getMode() & UnixPermission.S_ISGID) != 0) {
                    gid = stat.getGid();
                    perm = mode | UnixPermission.S_ISGID;
                } else {
                    gid = group;
                    perm = mode;
                }
                FsInode inode = _sqlDriver.mkdir(parent, name, owner, gid, perm);
                _sqlDriver.createTags(inode, owner, gid, perm & 0666, tags);
                _sqlDriver.writeAcl(inode, RsType.DIR, acl);
                fillIdCaches(inode);
                return inode;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(name, e);
            }
        });
    }

    @Override
    public FsInode path2inode(String path) throws ChimeraFsException {
        return path2inode(path, new RootInode(this, _sqlDriver.getRootInumber()));
    }

    @Override
    public FsInode path2inode(String path, FsInode startFrom) throws ChimeraFsException {
        //TODO check and add pnfsid Creation to FsInode_LABEL
        if (path.equals("/.(collection)")) {
            Stat stat = _sqlDriver.statLabelsParent();
            FsInode labelInode = new FsInode_LABELS(this, 0L, stat);
            return labelInode;
        }

        if (path.startsWith("/.(collection)")) {
            Long labelId;
            try {
               String tempLabelId = path.substring("/.(collection)".length() + 1, path.length());
                if (tempLabelId.contains("/")) {
                    String pathParent = _sqlDriver.inode2path(Long.parseLong(tempLabelId.substring(tempLabelId.lastIndexOf('-') + 1)), 0L);
                    String result = tempLabelId.split("/")[1];
                    String correctPath = pathParent + "/" + result;
                    FsInode inode = _sqlDriver.path2inode(startFrom, correctPath.substring(0, correctPath.lastIndexOf('-'))
                    );
                    return inode;
                } else {
                    labelId = _sqlDriver.getLabel(
                            path.substring("/.(collection)".length() + 1, path.length()));               }

            } catch (NoLabelChimeraException e) {
                throw FileNotFoundChimeraFsException.ofPath(path);
            }
            String pnfsIdSecond = "FFFF0000000000000000";
            String tempId = String.format("%016x", labelId);
            String pnfsID = pnfsIdSecond + tempId;
            Stat stat = _sqlDriver.stat(pnfsID);

            FsInode labelInode = new FsInode_LABEL(this, labelId, stat);
            return labelInode;
        }
        FsInode inode = _sqlDriver.path2inode(startFrom, path);
        if (inode == null) {
            throw FileNotFoundChimeraFsException.ofPath(path);
        }
        fillIdCaches(inode);
        return inode;
    }


    @Override
    public String inode2id(FsInode inode) throws ChimeraFsException {
        try {
            return _idCache.get(inode.ino(), () -> {
                String id = _sqlDriver.getId(inode);
                if (id == null) {
                    throw FileNotFoundChimeraFsException.of(inode);
                }
                return id;
            });
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), ChimeraFsException.class);
            Throwables.throwIfInstanceOf(e.getCause(), DataAccessException.class);
            Throwables.throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }
    @Override
    public FsInode id2inode(String id, StatCacheOption option) throws ChimeraFsException {
        if (option == NO_STAT) {
            try {
                return new FsInode(this, _inoCache.get(id, () -> {
                    Long ino = _sqlDriver.getInumber(id);
                    if (ino == null) {
                        throw FileNotFoundChimeraFsException.ofPnfsId(id);
                    }
                    return ino;
                }));
            } catch (ExecutionException e) {
                Throwables.throwIfInstanceOf(e.getCause(), ChimeraFsException.class);
                Throwables.throwIfInstanceOf(e.getCause(), DataAccessException.class);
                Throwables.throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        } else {
            Stat stat = _sqlDriver.stat(id);
            if (stat == null) {
                throw FileNotFoundChimeraFsException.ofPnfsId(id);
            }
            _inoCache.put(stat.getId(), stat.getIno());
            _idCache.put(stat.getIno(), stat.getId());
            if (id.startsWith("FFFF")) {
                return new FsInode_LABEL(this, stat.getIno(), stat);
            } else if (id.startsWith("FFFFF")) {
                return new FsInode(this, stat.getIno(), FsInodeType.LABELS, 0, stat);
            } else {
                return new FsInode(this, stat.getIno(), FsInodeType.INODE, 0, stat);
            }
        }
    }

    @Override
    public List<FsInode> path2inodes(String path) throws ChimeraFsException {
        return path2inodes(path, new RootInode(this, _sqlDriver.getRootInumber()));
    }

    @Override
    public List<FsInode> path2inodes(String path, FsInode startFrom)
          throws ChimeraFsException {
        List<FsInode> inodes = _sqlDriver.path2inodes(startFrom, path);
        if (inodes.isEmpty()) {
            throw FileNotFoundChimeraFsException.ofPath(path);
        }
        fillIdCaches(inodes.get(inodes.size() - 1));
        return inodes;
    }

    @Override
    public FsInode inodeOf(FsInode parent, String name, StatCacheOption cacheOption)
          throws ChimeraFsException {
        // only if it's PNFS command
        //TODO this should be checked there is inconsistency in unit test and system test
        // not sure if this is correct, should be discussed
        if (parent.type() == FsInodeType.LABELS) {
            FsInode labelInode = new FsInode_LABEL(this, _sqlDriver.getLabel(name));
            if (!labelInode.exists()) {
                throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
            }
            return labelInode;
        }
        if (name.startsWith(".(")) {

            if (name.startsWith(".(id)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                FsInode inode = _sqlDriver.inodeOf(parent, cmd[1], NO_STAT);
                if (inode == null) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, cmd[1]);
                }
                return new FsInode_ID(this, inode.ino());
            }

            if (name.startsWith(".(use)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 3) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                try {
                    int level = Integer.parseInt(cmd[1]);

                    FsInode inode = _sqlDriver.inodeOf(parent, cmd[2], NO_STAT);
                    if (inode == null) {
                        throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, cmd[2]);
                    }
                    if (level <= LEVELS_NUMBER) {
                        stat(inode, level);
                        return new FsInode(this, inode.ino(), level);
                    } else {
                        // is it error or a real file?
                    }
                } catch (NumberFormatException nfe) {
                    //	possible wrong format...ignore
                }
            }

            if (name.startsWith(".(access)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if ((cmd.length < 2) || (cmd.length > 3)) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                try {
                    int level = cmd.length == 2 ? 0 : Integer.parseInt(cmd[2]);

                    FsInode useInode = id2inode(cmd[1], STAT);

                    if (level <= LEVELS_NUMBER) {
                        return new FsInode(this, useInode.ino(), level);
                    } else {
                        // is it error or a real file?
                    }
                } catch (NumberFormatException nfe) {
                    //	possible wrong format...ignore
                } catch (FileNotFoundChimeraFsException e) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }

            }

            if (name.startsWith(".(nameof)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                FsInode nameofInode = new FsInode_NAMEOF(this, id2inode(cmd[1], NO_STAT).ino());
                if (!nameofInode.exists()) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                return nameofInode;
            }

            if (name.equals(".(collection)")) {
                Stat stat = _sqlDriver.statLabelsParent();
                FsInode labelInode = new FsInode_LABELS(this, 0, stat);
                if (!labelInode.exists()) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                return labelInode;
            }

            if (name.startsWith(".(const)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                FsInode constInode = new FsInode_CONST(this, parent.ino());
                if (!constInode.exists()) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                return constInode;
            }

            if (name.startsWith(".(parent)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                FsInode parentInode = new FsInode_PARENT(this, id2inode(cmd[1], NO_STAT).ino());
                if (!parentInode.exists()) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                return parentInode;
            }

            if (name.startsWith(".(pathof)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                FsInode pathofInode = new FsInode_PATHOF(this, id2inode(cmd[1], NO_STAT).ino());
                if (!pathofInode.exists()) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                return pathofInode;
            }

            if (name.startsWith(".(tag)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                FsInode tagInode = new FsInode_TAG(this, parent.ino(), cmd[1]);
                if (!tagInode.exists()) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                return tagInode;
            }

            if (name.startsWith(".(suri)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                FsInode inode = _sqlDriver.inodeOf(parent, cmd[1], NO_STAT);
                if (inode == null) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, cmd[1]);
                }
                return new FsInode_SURI(this, inode.ino());
            }

            if (name.equals(".(checksums)()")) {
                return new FsInode_CKSTYP(this, parent.ino());
            }

            if (name.equals(".(tags)()")) {
                return new FsInode_TAGS(this, parent.ino());
            }

            if (name.startsWith(".(pset)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length < 3) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                String[] args = new String[cmd.length - 2];
                System.arraycopy(cmd, 2, args, 0, args.length);
                FsInode psetInode = new FsInode_PSET(this, id2inode(cmd[1], NO_STAT).ino(), args);
                if (!psetInode.exists()) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                return psetInode;
            }

            if (name.equals(".(get)(cursor)")) {
                FsInode pgetInode = new FsInode_PCUR(this, parent.ino());
                if (!pgetInode.exists()) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                return pgetInode;
            }

            if (name.startsWith(".(get)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length < 3) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }

                FsInode inode = _sqlDriver.inodeOf(parent, cmd[1], NO_STAT);
                if (inode == null) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, cmd[1]);
                }
                switch (cmd[2]) {
                    case "locality":
                        return new FsInode_PLOC(this, inode.ino());
                    case "checksum":
                    case "checksums":
                        return new FsInode_PCRC(this, inode.ino());
                    case "pins":
                        return new FsInode_PINS(this, inode.ino());
                    default:
                        throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, cmd[2]);
                }
            }

            if (name.equals(".(config)")) {
                return getWormID();
            }

            if (name.startsWith(".(config)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }
                FsInode inode = _sqlDriver.inodeOf(getWormID(), cmd[1], NO_STAT);
                if (inode == null) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, cmd[1]);
                }
                return inode;
            }

            if (name.startsWith(".(fset)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length < 3) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
                }

                FsInode inode = _sqlDriver.inodeOf(parent, cmd[1], NO_STAT);
                if (inode == null) {
                    throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, cmd[1]);
                }

                String[] args = new String[cmd.length - 2];
                System.arraycopy(cmd, 2, args, 0, args.length);

                return new FsInode_PSET(this, inode.ino(), args);
            }
        }

        FsInode inode = _sqlDriver.inodeOf(parent, name, cacheOption);
        if (inode == null) {
            throw FileNotFoundChimeraFsException.ofFileInDirectory(parent, name);
        }
        fillIdCaches(inode);
        inode.setParent(parent);
        return inode;
    }

    @Override
    public String inode2path(FsInode inode) throws ChimeraFsException {
        return inode2path(inode, new RootInode(this, _sqlDriver.getRootInumber()));
    }

    /**
     * @param inode
     * @param startFrom
     * @return path of inode starting from startFrom
     * @throws ChimeraFsException
     */
    @Override
    public String inode2path(FsInode inode, FsInode startFrom) throws ChimeraFsException {
        return _sqlDriver.inode2path(inode, startFrom);
    }

    @Override
    public boolean removeFileMetadata(String path, int level) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.removeInodeLevel(path2inode(path), level));
    }

    @Override
    public Collection<Link> find(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.find(inode);
    }

    @Override
    public void setInodeAttributes(FsInode inode, int level, Stat stat) throws ChimeraFsException {
        inTransaction(status -> {
            switch (inode.type()) {
                case INODE:
                case PSET:
                    boolean applied = _sqlDriver.setInodeAttributes(inode, level, stat);
                    if (!applied) {
                        /**
                         * there are two cases why update can fail: 1. inode
                         * does not exists 2. we try to set a size on a non file
                         * object
                         */
                        Stat s = _sqlDriver.stat(inode);
                        if (s == null) {
                            throw FileNotFoundChimeraFsException.ofLevel(inode, level);
                        }
                        if ((s.getMode() & UnixPermission.F_TYPE) == UnixPermission.S_IFDIR) {
                            throw new IsDirChimeraException(inode);
                        }
                        throw new InvalidArgumentChimeraException();
                    }
                    break;
                case TAG:
                    if (stat.isDefined(Stat.StatAttributes.MODE)) {
                        _sqlDriver.setTagMode((FsInode_TAG) inode, stat.getMode());
                    }
                    if (stat.isDefined(Stat.StatAttributes.UID)) {
                        _sqlDriver.setTagOwner((FsInode_TAG) inode, stat.getUid());
                    }
                    if (stat.isDefined(Stat.StatAttributes.GID)) {
                        _sqlDriver.setTagOwnerGroup((FsInode_TAG) inode, stat.getGid());
                    }
                    break;
            }
            return null;
        });
    }

    @Override
    public boolean isIoEnabled(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.isIoEnabled(inode);
    }

    @Override
    public void setInodeIo(FsInode inode, boolean enable) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.setInodeIo(inode, enable);
            return null;
        });
    }

    public int write(FsInode inode, long beginIndex, byte[] data, int offset, int len)
          throws ChimeraFsException {
        return this.write(inode, 0, beginIndex, data, offset, len);
    }

    @Override
    public int write(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len)
          throws ChimeraFsException {
        return inTransaction(status -> {
            try {
                if (level == 0 && !inode.isIoEnabled()) {
                    LOGGER.debug("{}: IO (write) not allowed", inode);
                    return -1;
                }
                return _sqlDriver.write(inode, level, beginIndex, data, offset, len);
            } catch (ForeignKeyViolationException e) {
                throw FileNotFoundChimeraFsException.ofLevel(inode, level, e);
            }
        });
    }

    public int read(FsInode inode, long beginIndex, byte[] data, int offset, int len)
          throws ChimeraFsException {
        return this.read(inode, 0, beginIndex, data, offset, len);
    }

    @Override
    public int read(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len)
          throws ChimeraFsException {
        if (level == 0 && !inode.isIoEnabled()) {
            LOGGER.debug("{}: IO(read) not allowed", inode);
            return -1;
        }
        return _sqlDriver.read(inode, level, beginIndex, data, offset, len);
    }

    @Override
    public byte[] readLink(String path) throws ChimeraFsException {
        return readLink(path2inode(path));
    }

    @Override
    public byte[] readLink(FsInode inode) throws ChimeraFsException {
        int len = (int) inode.statCache().getSize();
        byte[] b = new byte[len];
        int n = read(inode, 0, b, 0, b.length);
        return (n >= 0) ? b : new byte[0];
    }

    @Override
    public boolean rename(FsInode inode, FsInode srcDir, String source, FsInode destDir,
          String dest) throws ChimeraFsException {
        checkNameLength(dest);

        return inTransaction(status -> {
            if (!destDir.isDirectory()) {
                throw new NotDirChimeraException(destDir);
            }

            FsInode destInode = _sqlDriver.inodeOf(destDir, dest, STAT);

            if (destInode != null) {
                if (destInode.equals(inode)) {
                    // according to POSIX, we are done
                    return false;
                }

                /* Renaming into existing is only allowed for the same type of entry.
                 */
                if (inode.isDirectory() != destInode.isDirectory()) {
                    throw new FileExistsChimeraFsException(dest);
                }

                if (!_sqlDriver.remove(destDir, dest, destInode)) {
                    // Concurrent modification - retry
                    return rename(inode, srcDir, source, destDir, dest);
                }
            }

            if (!_sqlDriver.rename(inode, srcDir, source, destDir, dest)) {
                throw FileNotFoundChimeraFsException.ofPath(source);
            }
            return true;
        });
    }

    /////////////////////////////////////////////////////////////////////
    ////
    ////   Location info
    ////
    ////////////////////////////////////////////////////////////////////
    @Override
    public List<StorageLocatable> getInodeLocations(FsInode inode, int type)
          throws ChimeraFsException {
        return _sqlDriver.getInodeLocations(inode, type);
    }

    @Override
    public List<StorageLocatable> getInodeLocations(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.getInodeLocations(inode);
    }

    @Override
    public void addInodeLocation(FsInode inode, int type, String location)
          throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.addInodeLocation(inode, type, location);
            } catch (ForeignKeyViolationException e) {
                throw FileNotFoundChimeraFsException.of(inode, e);
            }
            return null;
        });
    }

    @Override
    public void clearInodeLocation(FsInode inode, int type, String location)
          throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.clearInodeLocation(inode, type, location);
            return null;
        });
    }

    @Override
    public void clearTapeLocations(FsInode inode) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.clearTapeLocations(inode);
            return null;
        });
    }

    /////////////////////////////////////////////////////////////////////
    ////
    ////   Directory tags handling
    ////
    ////////////////////////////////////////////////////////////////////
    @Override
    public String[] tags(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.tags(inode);
    }

    @Override
    public Map<String, byte[]> getAllTags(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.getAllTags(inode);
    }

    @Override
    public void createTag(FsInode inode, String name) throws ChimeraFsException {
        this.createTag(inode, name, 0, 0, 0644);
    }

    @Override
    public void createTag(FsInode inode, String name, int uid, int gid, int mode)
          throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.createTag(inode, name, uid, gid, mode);
                return null;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException();
            }
        });
    }

    @Override
    public int pushTag(FsInode inode, String tagName) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.pushTag(inode, tagName));
    }

    @Override
    public int setTag(FsInode inode, String tagName, byte[] data, int offset, int len)
          throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.setTag(inode, tagName, data, offset, len));
    }

    @Override
    public void removeTag(FsInode dir, String tagName) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.removeTag(dir, tagName);
            return null;
        });
    }

    @Override
    public int getTag(FsInode inode, String tagName, byte[] data, int offset, int len)
          throws ChimeraFsException {
        return _sqlDriver.getTag(inode, tagName, data, offset, len);
    }

    @Override
    public Stat statTag(FsInode dir, String name) throws ChimeraFsException {
        return _sqlDriver.statTag(dir, name);
    }

    @Override
    public Stat statLabelsParent(FsInode dir) throws ChimeraFsException {
        return _sqlDriver.statLabelsParent();
    }

    @Override
    public void setTagOwner(FsInode_TAG tagInode, String name, int owner)
          throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.setTagOwner(tagInode, owner);
            return null;
        });
    }

    @Override
    public void setTagOwnerGroup(FsInode_TAG tagInode, String name, int owner)
          throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.setTagOwnerGroup(tagInode, owner);
            return null;
        });
    }

    @Override
    public void setTagMode(FsInode_TAG tagInode, String name, int mode) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.setTagMode(tagInode, mode);
            return null;
        });
    }

    @Override
    public List<OriginTag> findTags(String tagName) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.findTags(tagName));
    }


    ///////////////////////////////////////////////////////////////
    //
    // Id and Co.
    //
    @Override
    public int getFsId() {
        return _fsId;
    }

    /*
     * Storage Information
     *
     * currently it's not allowed to modify it
     */
    @Override
    public void setStorageInfo(FsInode inode, InodeStorageInformation storageInfo)
          throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.setStorageInfo(inode, storageInfo);
            } catch (ForeignKeyViolationException e) {
                throw FileNotFoundChimeraFsException.of(inode, e);
            }
            return null;
        });
    }

    @Override
    public InodeStorageInformation getStorageInfo(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.getStorageInfo(inode);
    }

    /*
     * inode checksum handling
     */
    @Override
    public void setInodeChecksum(FsInode inode, int type, String checksum)
          throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.setInodeChecksum(inode, type, checksum);
            } catch (ForeignKeyViolationException e) {
                throw FileNotFoundChimeraFsException.of(inode, e);
            }
            return null;
        });
    }

    @Override
    public void removeInodeChecksum(FsInode inode, int type) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.removeInodeChecksum(inode, type);
            return null;
        });
    }

    @Override
    public Set<Checksum> getInodeChecksums(FsInode inode) throws ChimeraFsException {
        return new HashSet<>(_sqlDriver.getInodeChecksums(inode));
    }

    /**
     * Get inode's Access Control List. An empty list is returned if there are no ACL assigned to
     * the <code>inode</code>.
     *
     * @param inode
     * @return acl
     */
    @Override
    public List<ACE> getACL(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.readAcl(inode);
    }

    /**
     * Set inode's Access Control List. The existing ACL will be replaced.
     *
     * @param inode
     * @param acl
     */
    @Override
    public void setACL(FsInode inode, List<ACE> acl) throws ChimeraFsException {
        inTransaction(status -> {
            boolean modified = _sqlDriver.deleteAcl(inode);
            if (!acl.isEmpty()) {
                _sqlDriver.writeAcl(inode, inode.isDirectory() ? RsType.DIR : RsType.FILE, acl);
                modified = true;
            }
            if (modified) {
                // empty stat will update ctime
                _sqlDriver.setInodeAttributes(inode, 0, new Stat());
            }
            return null;
        });
    }

    private static void checkNameLength(String name) throws InvalidNameChimeraException {
        if (name.length() > MAX_NAME_LEN) {
            throw new InvalidNameChimeraException("Name too long");
        }
    }

    @Override
    public void updateFsStat() throws ChimeraFsException {
        _sqlDriver.updateFsStat();
    }

    public FsStat getFsStat0() throws ChimeraFsException {
        return _sqlDriver.getFsStat();
    }

    @Override
    public FsStat getFsStat() throws ChimeraFsException {
        try {
            return _fsStatCache.get(DUMMY_KEY);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            Throwables.propagateIfPossible(t, ChimeraFsException.class);
            throw new ChimeraFsException(t.getMessage(), t);
        }
    }

    ///////////////////////////////////////////////////////////////
    //
    //  Some information
    //
    @Override
    public String getInfo() {

        String databaseProductName = "Unknown";
        String databaseProductVersion = "Unknown";
        Connection dbConnection = null;
        try {
            dbConnection = _dbConnectionsPool.getConnection();
            if (dbConnection != null) {
                databaseProductName = dbConnection.getMetaData().getDatabaseProductName();
                databaseProductVersion = dbConnection.getMetaData().getDatabaseProductVersion();
            }
        } catch (SQLException se) {
            // ignored
        } finally {
            tryToClose(dbConnection);
        }

        StringBuilder sb = new StringBuilder();

        sb.append("DB        : ").append(_dbConnectionsPool).append('\n');
        sb.append("DB Engine : ").append(databaseProductName).append(' ')
              .append(databaseProductVersion).append('\n');
        try {
            sb.append("rootID    : ")
                  .append(inode2id(new FsInode(this, _sqlDriver.getRootInumber()))).append('\n');
        } catch (ChimeraFsException e) {
            sb.append("rootID    : ").append(e.getMessage()).append('\n');
        }
        sb.append("FsId      : ").append(_fsId).append('\n');
        sb.append("Wcc       : ").append(_attributeConsistency).append("\n");
        return sb.toString();
    }


    /*
     * (non-Javadoc)
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        maintenanceTaskExecutor.shutdown();
        _sqlDriver.close();
    }

    @Override
    public String getFileLocality(FsInode_PLOC node) throws ChimeraFsException {
        throw new ChimeraFsException(NOT_IMPL);
    }

    /**
     * To maintain the abstraction level, we relegate the actual callout to the subclass.
     */
    @Override
    public void pin(FsInode pnfsid, long lifetime) throws ChimeraFsException {
        throw new ChimeraFsException(NOT_IMPL);
    }

    /**
     * To maintain the abstraction level, we relegate the actual callout to the subclass.
     */
    @Override
    public void unpin(FsInode pnfsid) throws ChimeraFsException {
        throw new ChimeraFsException(NOT_IMPL);
    }


    @Override
    public byte[] getXattr(FsInode inode, String attr) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.getXattr(inode, attr));
    }

    @Override
    public void setXattr(FsInode inode, String attr, byte[] value, SetXattrMode mode)
          throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.setXattr(inode, attr, value, mode);
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
            return null;
        });
    }

    @Override
    public Set<String> listXattrs(FsInode inode) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.listXattrs(inode));
    }


    @Override
    public void addLabel(FsInode inode, String labelname) throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.addLabel(inode, labelname);
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
            return null;
        });
    }
    @Override
    public Set<String> getLabels(FsInode inode) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.getLabels(inode));
    }

    @Override
    public String getLabelById(long ino) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.getLabelById(ino));
    }

    @Override
    public void removeLabel(FsInode inode, String labelname) throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.removeLabel(inode, labelname);
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
            return null;
        });
    }


    @Override
    public void removeXattr(FsInode inode, String attr) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.removeXattr(inode, attr);
            return null;
        });
    }

    @Override
    public List<PinInfo> listPins(FsInode pnfsid) throws ChimeraFsException {
        throw new ChimeraFsException(NOT_IMPL);
    }

    @Override
    public String resolvePath(String path) throws ChimeraFsException {
        try {
            return _sqlDriver.resolvePath(new RootInode(this, _sqlDriver.getRootInumber()), path);
        } catch (SQLException e) {
            throw new ChimeraFsException(e.getMessage(), e);
        }
    }

    @Override
    public String qosPolicyIdToName(Integer id) throws ChimeraFsException {
        try {
            return _sqlDriver.getQoSPolicyName(id);
        } catch (SQLException e) {
            throw new ChimeraFsException(e.getMessage(), e);
        }
    }

    @Override
    public Integer qosPolicyNameToId(String name) throws ChimeraFsException {
        try {
            return _sqlDriver.getQoSPolicyId(name);
        } catch (SQLException e) {
            throw new ChimeraFsException(e.getMessage(), e);
        }
    }

    private interface FallibleTransactionCallback<T> {

        T doInTransaction(TransactionStatus status) throws ChimeraFsException;
    }

    private static class RootInode extends FsInode {

        public RootInode(FileSystemProvider fs, long ino) {
            super(fs, ino);
        }

        @Override
        public boolean exists() throws ChimeraFsException {
            return true;
        }

        @Override
        public boolean isDirectory() {
            return true;
        }

        @Override
        public boolean isLink() {
            return false;
        }

        @Override
        public FsInode getParent() {
            return null;
        }
    }

    // REVISIT: this method violates DRY by duplicating functionality from
    // ChimeraHsmStorageInfoExtractor#getRetentionPolicy.
    private RetentionPolicy getRetentionPolicyFromParentTag(FsInode parent) {
        String firstLine;

        try {
            Stat tagStat = statTag(parent, "RetentionPolicy");
            byte[] data = new byte[(int) tagStat.getSize()];
            getTag(parent, "RetentionPolicy", data, 0, data.length);
            firstLine = ByteSource.wrap(data).asCharSource(UTF_8).readFirstLine();
        } catch (ChimeraFsException e) {
            return _defaultRetentionPolicy;
        } catch (IOException e) {
            // This should not happen.
            throw new RuntimeException("Unexpected IOException found", e);
        }

        try {
            return RetentionPolicy.valueOf(firstLine);
        } catch (IllegalArgumentException e) {
            LOGGER.error("Badly formatted RetentionPolicy tag in {}: {}", parent,
                  e.getMessage());
            return _defaultRetentionPolicy;
        }
    }

    private void checkQuota(int uid, int gid, RetentionPolicy rp)
          throws QuotaChimeraFsException {
        LOGGER.debug("Checking quota for {} {} {}", uid, gid, rp);
        if (!_quota.checkGroupQuota(gid, rp)) {
            throw new QuotaChimeraFsException(
                  String.format("%s group quota exceeded for gid=%d", rp, gid));
        }
        if (!_quota.checkUserQuota(uid, rp)) {
            throw new QuotaChimeraFsException(
                  String.format("%s user quota exceeded for uid=%d", rp, uid));
        }
    }

    private synchronized void enableMaintenanceTask() {
        if (maintenanceTask == null) {
            maintenanceTask = maintenanceTaskExecutor.scheduleWithFixedDelay(
                  () -> _sqlDriver.performMaintenanceTask(), 10, 20, TimeUnit.SECONDS
            );
        }
    }

    private synchronized void disableMaintenanceTask() {
        if (maintenanceTask != null) {
            maintenanceTask.cancel(false);
            maintenanceTask = null;
        }
    }

    @Override
    public void isLeader() {
        enableMaintenanceTask();
    }

    @Override
    public void notLeader() {
        disableMaintenanceTask();
    }

}

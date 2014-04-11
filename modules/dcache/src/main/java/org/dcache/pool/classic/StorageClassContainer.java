package org.dcache.pool.classic;

import com.google.common.base.Function;

import java.io.PrintWriter;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import diskCacheV111.pools.StorageClassFlushInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import dmg.util.Formats;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;

import org.dcache.pool.nearline.NearlineStorageHandler;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.Repository;

import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;

/**
 * Manages tape flush queues.
 *
 * A flush queue is created for each storage class and HSM pair. Queues can be explicitly
 * defined or created implicitly when a tape file for a particular storage class is first
 * encountered.
 *
 * Each queue is represented by a StorageClassInfo object.
 */
public class StorageClassContainer
    extends AbstractCellComponent
    implements CellCommandListener
{
    private final Map<String, StorageClassInfo> _storageClasses = new HashMap<>();
    private final Map<PnfsId, StorageClassInfo> _pnfsIds = new HashMap<>();
    private final Repository _repository;
    private final NearlineStorageHandler _storageHandler;
    private boolean  _poolStatusInfoChanged = true;

    public StorageClassContainer(Repository repository, NearlineStorageHandler storageHandler)
    {
        _repository = repository;
        _storageHandler = storageHandler;
    }

    public synchronized Collection<StorageClassInfo> getStorageClassInfos()
    {
        return new ArrayList<>(_storageClasses.values());
    }

    public synchronized StorageClassFlushInfo[] getFlushInfos()
    {
        return toArray(transform(
                _storageClasses.values(),
                new Function<StorageClassInfo, StorageClassFlushInfo>()
                {
                    @Override
                    public StorageClassFlushInfo apply(StorageClassInfo storageClassInfo)
                    {
                        return storageClassInfo.getFlushInfo();
                    }
                }), StorageClassFlushInfo.class);
    }

    public synchronized boolean poolStatusChanged()
    {
        boolean result = _poolStatusInfoChanged;
        _poolStatusInfoChanged = false;
        return result;
    }

    public synchronized int getStorageClassCount()
    {
        return _storageClasses.size();
    }

    public synchronized int getRequestCount()
    {
        return _pnfsIds.size();
    }

    public synchronized StorageClassInfo getStorageClassInfo(String hsmName, String storageClass)
    {
        return _storageClasses.get(storageClass + "@" + hsmName.toLowerCase());
    }

    public synchronized StorageClassInfo getStorageClassInfo(PnfsId pnfsId)
    {
        return _pnfsIds.get(pnfsId);
    }

    private synchronized
        StorageClassInfo defineStorageClass(String hsmName, String storageClass)
    {
        StorageClassInfo info =
                getStorageClassInfo(hsmName, storageClass);
        if (info == null) {
            info = new StorageClassInfo(_storageHandler, hsmName, storageClass);
        }
        info.setDefined(true);
        _storageClasses.put(info.getFullName(), info);
        return info;
    }

    private synchronized
        void removeStorageClass(String hsmName, String storageClass)
    {
        StorageClassInfo info = getStorageClassInfo(hsmName, storageClass);
        if (info != null) {
            if (info.size() > 0) {
                throw new IllegalArgumentException("Class not empty");
            }
            _storageClasses.remove(info.getFullName());
        }
    }

    private synchronized
        void suspendStorageClass(String hsmName, String storageClass, boolean suspend)
    {
        StorageClassInfo info = getStorageClassInfo(hsmName, storageClass);
        if (info == null) {
            throw new IllegalArgumentException("Storage class not found : " + storageClass + "@" + hsmName);
        }
        info.setSuspended(suspend);
    }

    private synchronized void suspendStorageClasses(boolean suspend)
    {
        for (StorageClassInfo info : _storageClasses.values()) {
            info.setSuspended(suspend);
        }
    }

    /**
     * Removes an entry from the list of HSM storage requests.
     *
     * @returns true if the entry was found and removed, false otherwise.
     */
    public synchronized boolean
        removeCacheEntry(PnfsId pnfsId)
    {
        StorageClassInfo info = _pnfsIds.remove(pnfsId);
        if (info == null) {
            return false;
        }
        boolean removed = info.remove(pnfsId);
        if (info.size() == 0 && ! info.isDefined()) {
            _storageClasses.remove(info.getFullName());
        }
        return removed;
    }

    /**
     *  adds a CacheEntry to the list of HSM storage requests.
     */
    public synchronized boolean addCacheEntry(PnfsId id)
        throws CacheException, InterruptedException
    {
        CacheEntry entry = _repository.getEntry(id);
        StorageInfo storageInfo = entry.getFileAttributes().getStorageInfo();
        String storageClass = storageInfo.getStorageClass();
        String hsmName = storageInfo.getHsm().toLowerCase();

        String composedName = storageClass + "@" + hsmName;
        StorageClassInfo classInfo = _storageClasses.get(composedName);

        if (classInfo == null) {
            classInfo = new StorageClassInfo(_storageHandler, hsmName, storageClass);
            //
            // in case we find a template, we take the
            // 'pending', 'expire' and 'total' parameter from it.
            //
            StorageClassInfo tmpInfo =
                    _storageClasses.get("*@" + hsmName);
            if (tmpInfo != null) {
                classInfo.setExpiration(tmpInfo.getExpiration());
                classInfo.setPending(tmpInfo.getPending());
                classInfo.setMaxSize(tmpInfo.getMaxSize());
            }
            _storageClasses.put(composedName, classInfo);
        }

        classInfo.add(entry);
        _pnfsIds.put(entry.getPnfsId(), classInfo);
        return classInfo.size() >= classInfo.getPending();
    }

    public void flush(PnfsId pnfsId, CompletionHandler<Void,PnfsId> callback)
            throws CacheException, InterruptedException
    {
        CacheEntry entry = _repository.getEntry(pnfsId);
        StorageInfo storageInfo = entry.getFileAttributes().getStorageInfo();
        String hsm = storageInfo.getHsm().toLowerCase();
        _storageHandler.flush(hsm, Collections.singleton(pnfsId), callback);
    }

    public void flushAll(int maxActive, long retryDelayOnError)
    {
        long now = System.currentTimeMillis();
        int active = 0;
        for (StorageClassInfo info: getStorageClassInfos()) {
            if (active >= maxActive) {
                break;
            }
            if (info.getActiveCount() > 0) {
                active++;
            } else if (info.isTriggered() && ((now - info.getLastSubmitted()) > retryDelayOnError)) {
                info.flush(Integer.MAX_VALUE, null);
                active++;
            }
        }
    }

    @Override
    public synchronized void printSetup(PrintWriter pw)
    {
        for (StorageClassInfo classInfo : _storageClasses.values()) {
            if (classInfo.isDefined()) {
                pw.println("queue define class " + classInfo.getHsm() +
                        " " + classInfo.getStorageClass() +
                        " -pending=" + classInfo.getPending() +
                        " -total=" + classInfo.getMaxSize() +
                        " -expire=" + TimeUnit.MILLISECONDS.toSeconds(classInfo.getExpiration()));
            }
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("   Classes  : " + getStorageClassCount());
        pw.println("   Requests : " + getRequestCount());
    }

    //////////////////////////////////////////////////////////////////////////////////
    //
    //   the interpreter
    //
    private void dumpClassInfo(StringBuilder sb, StorageClassInfo classInfo)
    {
        sb.append("              Class@Hsm : ").append(classInfo.getFullName()).append("\n");
        sb.append(" Exiration rest/defined : ").append(classInfo.expiresIn()).
            append(" / ").
            append(TimeUnit.MILLISECONDS.toSeconds(classInfo.getExpiration())).
            append("   seconds\n");
        sb.append(" Pending   rest/defined : ").append(classInfo.size()).
            append(" / ").
            append(classInfo.getPending()).
            append("\n");
        sb.append(" Size      rest/defined : ").append(classInfo.getTotalSize()).
            append(" / ").
            append(classInfo.getMaxSize()).
            append("\n");
        sb.append(" Active Store Procs.    : ").
            append(classInfo.getActiveCount()).
            append(classInfo.isSuspended() ? "  SUSPENDED" : "").
            append("\n");
    }

    @Command(name = "queue activate",
            description = "Move a file from FAILED to ACTIVE.")
    class ActivateFileCommand implements Callable<String>
    {
        @Argument
        PnfsId pnfsId;

        @Override
        public String call() throws IllegalArgumentException, CacheException
        {
            StorageClassInfo info = getStorageClassInfo(pnfsId);
            if (info == null) {
                throw new IllegalArgumentException("Not found : " + pnfsId);
            }
            info.activate(pnfsId);
            return "";
        }
    }

    @Command(name = "queue activate class",
            description = "Move files of a storage class from FAILED to ACTIVE.")
    class ActivateClassCommand implements Callable<String>
    {
        @Argument(valueSpec = "<storageClass>@<hsm>")
        String className;

        @Override
        public String call() throws IllegalArgumentException, NoSuchElementException
        {
            int pos = className.indexOf("@");
            if (pos <= 0 || pos + 1 == className.length()) {
                throw new IllegalArgumentException("Illegal storage class syntax : class@hsm");
            }
            StorageClassInfo classInfo =
                    getStorageClassInfo(
                            className.substring(pos + 1),
                            className.substring(0, pos));
            if (classInfo == null) {
                throw new IllegalArgumentException("No such storage class: " + className);
            }
            classInfo.activateAll();
            return "";
        }
    }

    @Command(name = "queue deactivate",
            description = "Move a file from ACTIVE to FAILED.")
    class DeactivateFileCommand implements Callable<String>
    {
        @Argument
        PnfsId pnfsId;

        @Override
        public String call() throws IllegalArgumentException, CacheException
        {
            StorageClassInfo info = getStorageClassInfo(pnfsId);
            if (info == null) {
                throw new IllegalArgumentException("Not found : " + pnfsId);
            }
            info.deactivate(pnfsId);
            return "";
        }
    }

    @Command(name = "queue ls classes",
            description = "List flush queues.")
    class LsClassesCommand implements Callable<String>
    {
        @Option(name = "l")
        boolean verbose;

        @Override
        public String call() throws Exception
        {
            StringBuilder sb = new StringBuilder();
            for (StorageClassInfo classInfo : getStorageClassInfos()) {
                if (verbose) {
                    dumpClassInfo(sb, classInfo);
                } else {
                    sb.append(classInfo.getStorageClass()).append("@").append(classInfo.getHsm());
                    sb.append("  active=").append(classInfo.getActiveCount());
                    sb.append("\n");
                }

            }
            return sb.toString();
        }
    }

    @Command(name = "queue ls queue",
            description = "List content of flush queues.")
    class LsQueueCommand implements Callable<String>
    {
        @Option(name = "l", usage = "Verbose listing")
        boolean verbose;

        @Override
        public String call() throws CacheException, InterruptedException
        {
            StringBuilder sb = new StringBuilder();
            for (StorageClassInfo classInfo : getStorageClassInfos()) {
                boolean suspended = classInfo.isSuspended();
                if (verbose) {
                    dumpClassInfo(sb, classInfo);
                } else {
                    sb.append(" Class@Hsm : ").
                            append(classInfo.getStorageClass()).
                            append("@").
                            append(classInfo.getHsm()).
                            append(suspended?"  SUSPENDED":"").
                            append("\n");
                }
                for (PnfsId id : classInfo.getRequests()) {
                    try {
                        CacheEntry info = _repository.getEntry(id);
                        long        time   = info.getLastAccessTime();
                        StorageInfo sinfo  = info.getFileAttributes().getStorageInfo();
                        String      sclass = sinfo.getStorageClass();
                        String      hsm    = sinfo.getHsm();
                        String      cclass = sinfo.getCacheClass();
                        sb.append("  ").append(id).append("  ").
                                append(Formats.field(hsm,8,Formats.LEFT)).
                                append(Formats.field(sclass==null?"-":sclass,20,Formats.LEFT)).
                                append(Formats.field(cclass==null?"-":cclass,20,Formats.LEFT)).
                                append("\n");
                    } catch (FileNotInCacheException e) {
                        /* Temporary inconsistency because the entry
                         * was deleted after generating the list.
                         */
                    }
                }
                boolean headerDone = false;

                for (PnfsId id : classInfo.getFailedRequests()) {
                    try {
                        if (! headerDone) {
                            headerDone = true;
                            sb.append("\n Deactivated Requests\n\n");
                        }
                        CacheEntry info = _repository.getEntry(id);
                        long        time   = info.getLastAccessTime();
                        StorageInfo sinfo  = info.getFileAttributes().getStorageInfo();
                        String      sclass = sinfo.getStorageClass();
                        String      hsm    = sinfo.getHsm();
                        String      cclass = sinfo.getCacheClass();
                        sb.append("  ").append(id).append("  ").
                                append(Formats.field(hsm,8,Formats.LEFT)).
                                append(Formats.field(sclass==null?"-":sclass,20,Formats.LEFT)).
                                append(Formats.field(cclass==null?"-":cclass,20,Formats.LEFT)).
                                append("\n");
                    } catch (FileNotInCacheException e) {
                        /* Temporary inconsistency because the entry
                         * was deleted after generating the list.
                         */
                    }
                }
                sb.append("\n");
            }

            return sb.toString();
        }
    }

    @Command(name = "queue remove class",
            description = "Delete a flush queue")
    class RemoveQueueCommand implements Callable<String>
    {
        @Argument(index = 0, usage = "Name of HSM system")
        String hsm;

        @Argument(index = 1, usage = "Name of storage class")
        String storageClass;

        @Override
        public String call()
        {
            removeStorageClass(hsm.toLowerCase(), storageClass);
            return "";
        }
    }

    @Command(name = "queue suspend class",
            description = "Disable a flush queue.")
    class SuspendQueueCommand implements Callable<String>
    {
        @Argument(index = 0, usage = "Name of HSM system", valueSpec = "<hsm>|*")
        String hsm;

        @Argument(index = 1, usage = "Name of storage class", required = false)
        String storageClass;

        @Override
        public String call()
        {
            if (hsm.equals("*")) {
                suspendStorageClasses(true);
            } else {
                suspendStorageClass(hsm.toLowerCase(), storageClass, true);
            }
            return "";
        }
    }

    @Command(name = "queue resume class",
            description = "Enable a previously suspended flush queue.")
    class ResumeQueueCommand implements Callable<String>
    {
        @Argument(index = 0, usage = "Name of HSM system", valueSpec = "<hsm>|*")
        String hsm;

        @Argument(index = 1, usage = "Name of storage class", required = false)
        String storageClass;

        @Override
        public String call()
        {
            if (hsm.equals("*")) {
                suspendStorageClasses(false);
            } else {
                suspendStorageClass(hsm.toLowerCase(), storageClass, false);
            }
            return "";
        }
    }

    @Command(name = "queue define class",
            description = "Create a new flush queue.")
    class DefineQueueCommand implements Callable<String>
    {
        @Argument(index = 0, usage = "Name of HSM system")
        String hsm;

        @Argument(index = 1, usage = "Name of storage class")
        String storageClass;

        @Option(name = "expire", valueSpec="<seconds>")
        Integer expirationTime;

        @Option(name = "total", valueSpec="<bytes>")
        Long maxTotalSize;

        @Option(name = "pending", valueSpec="<files>")
        Integer maxPending;

        @Override
        public String call()
        {
            StorageClassInfo info = defineStorageClass(hsm.toLowerCase(), storageClass);
            if (expirationTime != null) {
                info.setExpiration(TimeUnit.SECONDS.toMillis(expirationTime));
            }
            if (maxPending != null) {
                info.setPending(maxPending);
            }
            if (maxTotalSize != null) {
                info.setMaxSize(maxTotalSize);
            }
            _poolStatusInfoChanged = true;
            return info.toString();
        }
    }

    @Command(name = "queue remove pnfsid",
            description = "Remove a file from the flush queue. WARNING: The file will no longer flushed to tape!")
    class RemoveFileCommand implements Callable<String>
    {
        @Argument
        PnfsId pnfsId;

        @Override
        public String call()
        {
            if (!removeCacheEntry(pnfsId)) {
                throw new IllegalArgumentException("Not found : " + pnfsId);
            }
            return "Removed : " + pnfsId;
        }
    }
}

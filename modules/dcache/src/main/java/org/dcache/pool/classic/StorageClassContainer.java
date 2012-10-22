// $Id$

package org.dcache.pool.classic;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.Repository;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.AbstractCellComponent;

import dmg.util.*;
import dmg.cells.nucleus.CellInfo;

import java.io.*;
import java.util.*;

public class StorageClassContainer
    extends AbstractCellComponent
    implements CellCommandListener
{
    private Repository _repository;
    private final Object _storageClassLock = new Object();
    private final String   _poolName;
    private final Map<String, StorageClassInfo> _storageClasses =
        new HashMap();
    private final Map<PnfsId, StorageClassInfo> _pnfsIds =
        new HashMap();
    private boolean  _poolStatusInfoChanged = true;

    public StorageClassContainer(Repository repository, String poolName)
    {
        _repository = repository;
        _poolName = poolName;
    }

    public synchronized Collection<StorageClassInfo> getStorageClassInfos()
    {
        return new ArrayList<>(_storageClasses.values());
    }

    public synchronized boolean poolStatusChanged()
    {
        boolean result = _poolStatusInfoChanged;
        _poolStatusInfoChanged = false;
        return result;
    }

    public int size()
    {
        return _storageClasses.size();
    }

    public int getStorageClassCount()
    {
        return _storageClasses.size();
    }

    public int getRequestCount()
    {
        return _pnfsIds.size();
    }

    public StorageClassInfo
        getStorageClassInfoByName(String hsmName, String storageClass)
        throws NoSuchElementException
    {
        String composedName = storageClass+"@"+hsmName.toLowerCase();
        StorageClassInfo info =
                _storageClasses.get(composedName);

        if (info == null) {
            throw new NoSuchElementException(composedName);
        }

        return info;
    }

    /**
     *  defines a storageClass
     */
    public synchronized
        StorageClassInfo defineStorageClass(String hsmName, String storageClass)
    {
        String composedName = storageClass+"@"+hsmName.toLowerCase();
        synchronized (_storageClassLock) {
            StorageClassInfo info =
                    _storageClasses.get(composedName);

            if (info == null) {
                info = new StorageClassInfo(hsmName, storageClass);
            }

            info.setDefined(true);
            _storageClasses.put(composedName, info);
            return info;
        }
    }

    public synchronized
        StorageClassInfo removeStorageClass(String hsmName, String storageClass)
    {
        synchronized (_storageClassLock) {
            String composedName = storageClass+"@"+hsmName.toLowerCase();
            StorageClassInfo info = _storageClasses.get(composedName);
            if (info.size() > 0) {
                throw new IllegalArgumentException("Class not empty");
            }

            return _storageClasses.remove(composedName);
        }
    }

    public synchronized
        void suspendStorageClass(String hsmName, String storageClass, boolean suspend)
    {
        synchronized (_storageClassLock) {
            String composedName = storageClass+"@"+hsmName.toLowerCase();
            StorageClassInfo info = _storageClasses.get(composedName);
            if (info == null) {
                throw new
                        IllegalArgumentException("class not found : " + composedName);
            }

            info.setSuspended(suspend);
        }
    }

    public synchronized
        void suspendStorageClasses(boolean suspend)
    {
        synchronized (_storageClassLock) {
            for (StorageClassInfo info : _storageClasses.values()) {
                info.setSuspended(suspend);
            }
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
        synchronized (_storageClassLock) {
            if ((info.size() == 0) && ! info.isDefined()) {
                _storageClasses.remove(info.getName() + "@" + info.getHsm());
            }
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
        String storageClass = entry.getStorageInfo().getStorageClass();
        String hsmName      = entry.getStorageInfo().getHsm().toLowerCase();

        String composedName = storageClass+"@"+hsmName;
        synchronized (_storageClassLock) {
            StorageClassInfo classInfo =
                    _storageClasses.get(composedName);

            if (classInfo == null) {
                classInfo =  new StorageClassInfo(hsmName,storageClass);
                //
                // in case we find a template, we take the
                // 'pending', 'expire' and 'total' parameter from it.
                //
                StorageClassInfo tmpInfo =
                        _storageClasses.get("*@"+hsmName);
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
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        for (StorageClassInfo classInfo : _storageClasses.values()) {
            if (classInfo.isDefined()) {
                pw.println("queue define class " + classInfo.getHsm() +
                        " " + classInfo.getStorageClass() +
                        " -pending=" + classInfo.getPending() +
                        " -total=" + classInfo.getMaxSize() +
                        " -expire=" + classInfo.getExpiration());
            }
        }
    }

    //////////////////////////////////////////////////////////////////////////////////
    //
    //   the interpreter
    //
    private void dumpClassInfo(StringBuilder sb, StorageClassInfo classInfo)
    {
        sb.append("                   Name : ").append(classInfo.getName()).append("\n");
        sb.append("              Class@Hsm : ").append(classInfo.getStorageClass()).
            append("@").
            append(classInfo.getHsm()).append("\n");
        sb.append(" Exiration rest/defined : ").append(classInfo.expiresIn()).
            append(" / ").
            append(classInfo.getExpiration()).
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

    public String hh_queue_activate =
        "<pnfsId>| class <storageClass>@<hsm>  # move pnfsid from <failed> to active";
    public String ac_queue_activate_$_1_2(Args args) throws CacheException
    {
        if (args.argc() == 1) {
            PnfsId pnfsId = new PnfsId(args.argv(0));
            StorageClassInfo info = _pnfsIds.get(pnfsId);
            if (info == null) {
                throw new
                        IllegalArgumentException("Not found : " + pnfsId);
            }

            info.activate(pnfsId);

            return "";
        } else {
            if (!args.argv(0).equals("class")) {
                throw new
                        IllegalArgumentException("queue activate class <storageClass>");
            }

            String className = args.argv(1);
            int pos = className.indexOf("@");
            if ((pos <= 0) || ((pos+1) == className.length())) {
                throw new
                        IllegalArgumentException("Illegal storage class syntax : class@hsm");
            }

            StorageClassInfo classInfo =
                getStorageClassInfoByName(
                                          className.substring(pos+1),
                                          className.substring(0,pos));


            classInfo.activateAll();

            return "";
        }
    }

    public String hh_queue_deactivate = "<pnfsId>  # move pnfsid from <active> to <failed>";
    public String ac_queue_deactivate_$_1(Args args) throws CacheException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        StorageClassInfo info = _pnfsIds.get(pnfsId);
        if (info == null) {
            throw new IllegalArgumentException("Not found : " + pnfsId);
        }

        info.deactivate(pnfsId);

        return "";
    }

    public String hh_queue_ls_classes = " [-l}";
    public String ac_queue_ls_classes(Args args)
    {
        StringBuilder sb = new StringBuilder();
        boolean l = args.hasOption("l");
        for (StorageClassInfo classInfo : getStorageClassInfos()) {
            if (l) {
                dumpClassInfo(sb, classInfo);
            } else {
                sb.append(classInfo.getStorageClass()).append("@").append(classInfo.getHsm());
                /*
                  sb.append("  readpref=").append(classInfo.getReadPreference());
                  sb.append("  writepref=").append(classInfo.getWritePreference());
                */
                sb.append("  active=").append(classInfo.getActiveCount());
                sb.append("\n");
            }

        }
        return sb.toString();
    }

    public String hh_queue_ls_queue = " [-l]";
    public String ac_queue_ls_queue(Args args)
        throws CacheException, InterruptedException
    {
        StringBuilder sb = new StringBuilder();
        boolean l = args.hasOption("l");
        try {
            for (StorageClassInfo classInfo : getStorageClassInfos()) {
                boolean suspended = classInfo.isSuspended();
                if (l) {
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
                        StorageInfo sinfo  = info.getStorageInfo();
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
                        StorageInfo sinfo  = info.getStorageInfo();
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
        } catch (NullPointerException ee) {
            ee.printStackTrace();
            throw ee;
        }
    }

    public String hh_queue_remove_class = "<hsm> <storageClass>";
    public String ac_queue_remove_class_$_2(Args args)
    {
        String hsmName   = args.argv(0);
        String className = args.argv(1);
        removeStorageClass(hsmName.toLowerCase(), className);
        return "";
    }

    public String hh_queue_suspend_class = "<hsm> <storageClass> | *";
    public String ac_queue_suspend_class_$_1_2(Args args)
    {
        if (args.argv(0).equals("*")) {
            suspendStorageClasses(true);
        } else {
            String hsmName   = args.argv(0);
            String className = args.argv(1);
            suspendStorageClass(hsmName.toLowerCase(), className, true);
        }
        return "";
    }

    public String hh_queue_resume_class = "<hsm> <storageClass> | *";
    public String ac_queue_resume_class_$_1_2(Args args)
    {
        if (args.argv(0).equals("*")) {
            suspendStorageClasses(false);
        } else {
            String hsmName   = args.argv(0);
            String className = args.argv(1);
            suspendStorageClass(hsmName.toLowerCase(), className, false);
        }
        return "";
    }

    public String hh_define_class = "DEPRICATED";
    public String ac_define_class_$_2(Args args)
    {
        return ac_queue_define_class_$_2(args);
    }

    public String hh_queue_define_class = "<hsm> <storageClass> " +
        "[-expire=<expirationTime/sec>] " +
        "[-total=<maxTotalSize/bytes>] " +
        "[-pending=<maxPending>] ";
    public String ac_queue_define_class_$_2(Args args)
    {
        String hsmName   = args.argv(0);
        String className = args.argv(1);
        StorageClassInfo info =
            defineStorageClass(hsmName.toLowerCase(), className);

        String tmp;
        if ((tmp = args.getOpt("expire")) != null) {
            info.setExpiration(Integer.parseInt(tmp));
        }
        if ((tmp = args.getOpt("pending")) != null) {
            info.setPending(Integer.parseInt(tmp));
        }
        if ((tmp = args.getOpt("total")) != null) {
            info.setMaxSize(Long.parseLong(tmp));
        }

        _poolStatusInfoChanged = true;
        return info.toString();
    }

    public String hh_queue_remove_pnfsid = "<pnfsId> # !!!! DANGEROUS";
    public String ac_queue_remove_pnfsid_$_1(Args args)
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        if (!removeCacheEntry(pnfsId)) {
            throw new IllegalArgumentException("Not found : " + pnfsId);
        }

        return "Removed : " + pnfsId;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println("    Version : $Id$");
        pw.println("   Classes  : " + getStorageClassCount());
        pw.println("   Requests : " + getRequestCount());
    }
}

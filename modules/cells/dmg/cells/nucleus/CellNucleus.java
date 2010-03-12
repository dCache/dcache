package dmg.cells.nucleus;

import dmg.util.Args;
import dmg.util.PinboardAppender;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.RejectedExecutionException;
import java.lang.reflect.*;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.MDC;
import org.apache.log4j.NDC;

/**
 *
 *
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 */
public class CellNucleus implements ThreadFactory
{
    private static final  int    INITIAL  =  0;
    private static final  int    ACTIVE   =  1;
    private static final  int    REMOVING =  2;
    private static final  int    DEAD     =  3;

    private static CellGlue __cellGlue  = null;
    private final  String    _cellName;
    private final  String    _cellType;
    private        ThreadGroup _threads      = null;
    private final  Cell      _cell;
    private final  Date      _creationTime   = new Date();
    private        int       _state          = INITIAL;
    private        int       _printoutLevel  = 0;

    private final static Logger _logMessages =
        Logger.getLogger("logger.org.dcache.cells.messages");
    private final static Logger _logNucleus =
        Logger.getLogger(CellNucleus.class);
    private final Logger _logCell;

    //  have to be synchronized map
    private final  Map<UOID, CellLock> _waitHash = new HashMap<UOID, CellLock>();
    private String _cellClass;

    private volatile ExecutorService _callbackExecutor;
    private volatile ExecutorService _messageExecutor;

    private boolean _isPrivateCallbackExecutor = true;
    private boolean _isPrivateMessageExecutor = true;

    public CellNucleus(Cell cell, String name) {

        this(cell, name, "Generic");
    }
    public CellNucleus(Cell cell, String name, String type) {

        _logCell = Logger.getLogger(cell.getClass());

        if (__cellGlue == null) {
            //
            // the cell gluon hasn't yet been created
            // (we insist in creating a SystemCell first.)
            //
            if (cell instanceof dmg.cells.nucleus.SystemCell) {
                __cellGlue = new CellGlue(name);
                _cellName    = "System";
                _cellType    = "System";
                __cellGlue.setSystemNucleus(this);
            } else {
                throw new
                    IllegalArgumentException("System must be first Cell");
            }

        } else {
            //
            // we don't accept more then one System.cells
            //
            if (cell instanceof dmg.cells.nucleus.SystemCell) {
                throw new
                    IllegalArgumentException("System already exists");
            } else {
                String cellName    = name.replace('@', '+');

                if ((cellName == null) ||
                   (cellName.equals("")))cellName = "*";
                if (cellName.charAt(cellName.length() - 1) == '*') {
                    if (cellName.length() == 1) {
                	cellName = "$-"+getUnique();
                    } else {
                	cellName = cellName.substring(0,cellName.length()-1)+
                            "-"+getUnique();
                    }
                }

                _cellName = cellName;
                _cellType    = type;

            }
        }

        _cell = cell;
        _cellClass = _cell.getClass().getName();

        //
        // for the use in restricted sandboxes
        //
        try {

            _threads = new ThreadGroup(__cellGlue.getMasterThreadGroup(),
                                       _cellName+"-threads");

        } catch(SecurityException se) {
            _threads = null;
        }

        _callbackExecutor = Executors.newSingleThreadExecutor(this);
        _messageExecutor = Executors.newSingleThreadExecutor(this);

        nsay("Created : "+name);
        _state = ACTIVE;

        //
        // make ourself known to the world
        //
        _printoutLevel = __cellGlue.getDefaultPrintoutLevel();
        __cellGlue.addCell(_cellName, this);

    }
    void setSystemNucleus(CellNucleus nucleus) {
        __cellGlue.setSystemNucleus(nucleus);
    }

    public String getCellName() { return _cellName; }
    public String getCellType() { return _cellType; }

    public String getCellClass()
    {
        return _cellClass;
    }

    public void setCellClass(String cellClass)
    {
        _cellClass = cellClass;
    }

    public CellAddressCore getThisAddress() {
        return new CellAddressCore(_cellName, __cellGlue.getCellDomainName());
    }
    public CellDomainInfo getCellDomainInfo() {
        return __cellGlue.getCellDomainInfo();
    }
    public String getCellDomainName() {
        return __cellGlue.getCellDomainName();
    }
    public String [] getCellNames() { return __cellGlue.getCellNames(); }
    public CellInfo getCellInfo(String name) {
        return __cellGlue.getCellInfo(name);
    }
    public CellInfo getCellInfo() {
        return __cellGlue.getCellInfo(getCellName());
    }

    public Map<String, Object> getDomainContext()
    {
        return __cellGlue.getCellContext();
    }

    public Reader getDomainContextReader(String contextName)
        throws FileNotFoundException  {
        Object o = __cellGlue.getCellContext(contextName);
        if (o == null)
            throw new
                FileNotFoundException("Context not found : "+contextName);
        return new StringReader(o.toString());
    }
    public void   setDomainContext(String contextName, Object context) {
        __cellGlue.getCellContext().put(contextName, context);
    }
    public Object getDomainContext(String str) {
        return __cellGlue.getCellContext(str);
    }
    public String [] [] getClassProviders() {
        return __cellGlue.getClassProviders();
    }
    public synchronized void setClassProvider(String selection, String provider) {
        __cellGlue.setClassProvider(selection, provider);
    }
    Cell getThisCell() { return _cell; }

    CellInfo _getCellInfo() {
        CellInfo info = new CellInfo();
        info.setCellName(getCellName());
        info.setDomainName(getCellDomainName());
        info.setCellType(getCellType());
        info.setCreationTime(_creationTime);
        try {
            info.setCellVersion(getCellVersionByObject(_cell));
        } catch(Exception e) {}
        try {
            info.setPrivateInfo(_cell.getInfo());
        } catch(Exception e) {
            info.setPrivateInfo("Not yet/No more available\n");
        }
        try {
            info.setShortInfo(_cell.toString());
        } catch(Exception e) {
            info.setShortInfo("Not yet/No more available");
        }
        info.setCellClass(_cellClass);
        try {
            info.setEventQueueSize(getEventQueueSize());
            info.setState(_state);
            info.setThreadCount(_threads.activeCount());
        } catch(Exception e) {
            info.setEventQueueSize(0);
            info.setState(0);
            info.setThreadCount(0);
        }
        return info;
    }
    public void   setPrintoutLevel(int level) { _printoutLevel = level; }
    public int    getPrintoutLevel() { return _printoutLevel; }
    public void   setPrintoutLevel(String cellName, int level) {
        __cellGlue.setPrintoutLevel(cellName, level);
    }
    public int    getPrintoutLevel(String cellName) {
        return __cellGlue.getPrintoutLevel(cellName);
    }

    public synchronized void setAsyncCallback(boolean asyncCallback)
    {
        if (asyncCallback) {
            setCallbackExecutor(Executors.newCachedThreadPool(this));
        } else {
            setCallbackExecutor(Executors.newSingleThreadExecutor(this));
        }
        _isPrivateCallbackExecutor = true;
    }

    /**
     * Executor used for message callbacks.
     */
    public synchronized void setCallbackExecutor(ExecutorService executor)
    {
        if (executor == null) {
            throw new IllegalArgumentException("null is not allowed");
        }
        if (_isPrivateCallbackExecutor) {
            _callbackExecutor.shutdown();
        }
        _callbackExecutor = executor;
        _isPrivateCallbackExecutor = false;
    }

    /**
     * Executor used for incoming message delivery.
     */
    public synchronized void setMessageExecutor(ExecutorService executor)
    {
        if (executor == null) {
            throw new IllegalArgumentException("null is not allowed");
        }
        if (_isPrivateMessageExecutor) {
            _messageExecutor.shutdown();
        }
        _messageExecutor = executor;
        _isPrivateMessageExecutor = false;
    }

    public void   sendMessage(CellMessage msg)
        throws SerializationException,
               NoRouteToCellException    {

        sendMessage(msg, true, true);

    }
    public void   resendMessage(CellMessage msg)
        throws SerializationException,
               NoRouteToCellException    {

        sendMessage(msg, false, true);

    }
    public void   sendMessage(CellMessage msg,
                              boolean locally,
                              boolean remotely)
        throws SerializationException,
               NoRouteToCellException    {

        if (!msg.isStreamMode()) {
            msg.touch();
        }

        EventLogger.sendBegin(this, msg, "async");
        try {
            __cellGlue.sendMessage(this, msg, locally, remotely);
        } finally {
            EventLogger.sendEnd(msg);
        }
    }
    public CellMessage   sendAndWait(CellMessage msg, long timeout)
        throws SerializationException,
               NoRouteToCellException,
               InterruptedException      {
        return sendAndWait(msg, true, true, timeout);
    }

    public CellMessage sendAndWait(CellMessage msg,
                                   boolean local,
                                   boolean remote,
                                   long    timeout)
        throws SerializationException,
               NoRouteToCellException,
               InterruptedException
    {
        if (!msg.isStreamMode()) {
            msg.touch();
        }

        msg.setTtl(timeout);

        EventLogger.sendBegin(this, msg, "blocking");
        UOID uoid = msg.getUOID();
        try {
            CellLock lock = new CellLock();
            synchronized (_waitHash) {
                _waitHash.put(uoid, lock);
            }
            nsay("sendAndWait : adding to hash : " + uoid);

            __cellGlue.sendMessage(this, msg, local, remote);

            //
            // because of a linux native thread problem with
            // wait(n > 0), we have to use a interruptedFlag
            // and the time messurement.
            //
            synchronized (lock) {
                long start = System.currentTimeMillis();
                while (lock.getObject() == null && timeout > 0) {
                    lock.wait(timeout);
                    timeout -= (System.currentTimeMillis() - start);
                }
            }
            CellMessage answer = (CellMessage)lock.getObject();
            if (answer == null) {
                return null;
            }
            answer = new CellMessage(answer);

            Object obj = answer.getMessageObject();
            if (obj instanceof NoRouteToCellException) {
                throw (NoRouteToCellException) obj;
            } else if (obj instanceof SerializationException) {
                throw (SerializationException) obj;
            }
            return answer;
        } finally {
            synchronized (_waitHash) {
                _waitHash.remove(uoid);
            }
            EventLogger.sendEnd(msg);
        }
    }

    public Map<UOID,CellLock > getWaitQueue() {

        Map<UOID,CellLock > hash = new HashMap<UOID,CellLock >();
        synchronized (_waitHash) {
            hash.putAll(_waitHash);
        }
        return hash;
    }

    public int updateWaitQueue()
    {
        Collection<CellLock> expired = new ArrayList<CellLock>();
        long now  = System.currentTimeMillis();
        int size;

        synchronized (_waitHash) {
            Iterator<CellLock> i = _waitHash.values().iterator();
            while (i.hasNext()) {
                CellLock lock =  i.next();
                if (lock != null && !lock.isSync() && lock.getTimeout() < now) {
                    expired.add(lock);
                    i.remove();
                }
            }
            size = _waitHash.size();
        }

        //
        // _waitHash can't be used here. Otherwise
        // we will end up in a deadlock (NO LOCKS WHILE CALLING CALLBACKS)
        //
        for (CellLock lock: expired) {
            CellMessage envelope = lock.getMessage();
            EventLogger.sendEnd(envelope);
            lock.getCallback().answerTimedOut(envelope);
        }

        return size;
    }

    public void sendMessage(CellMessage msg,
                            boolean local,
                            boolean remote,
                            CellMessageAnswerable callback,
                            long timeout)
        throws SerializationException
    {
        if (!msg.isStreamMode()) {
            msg.touch();
        }

        msg.setTtl(timeout);

        EventLogger.sendBegin(this, msg, "callback");
        UOID uoid = msg.getUOID();
        boolean success = false;
        try {
            CellLock lock = new CellLock(msg, callback, timeout);
            synchronized (_waitHash) {
                _waitHash.put(uoid, lock);
            }

            __cellGlue.sendMessage(this, msg, local, remote);
            success = true;
        } catch (NoRouteToCellException e) {
            if (callback != null)
                callback.exceptionArrived(msg, e);
        } finally {
            if (!success) {
                synchronized (_waitHash) {
                    _waitHash.remove(uoid);
                }
                EventLogger.sendEnd(msg);
            }
        }
    }
    public void addCellEventListener(CellEventListener listener) {
        __cellGlue.addCellEventListener(this, listener);

    }
    public void export() { __cellGlue.export(this);  }
    /**
     *
     * The kill method schedules the specified cell for deletion.
     * The actual remove operation will run in a different
     * thread. So on return of this method the cell may
     * or may not be alive.
     */
    public void kill() {   __cellGlue.kill(this);  }
    /**
     *
     * The kill method schedules this Cell for deletion.
     * The actual remove operation will run in a different
     * thread. So on return of this method the cell may
     * or may not be alive.
     */
    public void kill(String cellName) throws IllegalArgumentException {
        __cellGlue.kill(this, cellName);
    }


    /**
     * Blocks until the given cell is dead.
     *
     * @param timeout the maximum time to wait in milliseconds.
     * @throws InterruptedException if another thread interrupted the
     * current thread before or while the current thread was waiting
     * for a notification. The interrupted status of the current
     * thread is cleared when this exception is thrown.
     * @return True if the cell died, false in case of a timeout.
     */
    public boolean join(String cellName, long timeout)
        throws InterruptedException
    {
        return __cellGlue.join(cellName, timeout);
    }

    /**
     * Returns the non-daemon threads of a thread group.
     */
    private Collection<Thread> getNonDaemonThreads(ThreadGroup group)
    {
        Thread[] threads = new Thread[group.activeCount()];
        int count = group.enumerate(threads);
        Collection<Thread> nonDaemonThreads = new ArrayList(count);
        for (int i = 0; i < count; i++) {
            Thread thread = threads[i];
            if (!thread.isDaemon()) {
                nonDaemonThreads.add(thread);
            }
        }
        return nonDaemonThreads;
    }

    /**
     * Waits for at most timeout milliseconds for the termination of a
     * set of threads.
     *
     * @return true if all threads terminated, false otherwise
     */
    private boolean joinThreads(Collection<Thread> threads, long timeout)
        throws InterruptedException
    {
        long deadline = System.currentTimeMillis() + timeout;
        for (Thread thread: threads) {
            if (thread.isAlive()) {
                long wait = deadline - System.currentTimeMillis();
                if (wait <= 0) {
                    return false;
                }
                thread.join(wait);
                if (thread.isAlive()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Interrupts a set of threads.
     */
    private void killThreads(Collection<Thread> threads)
        throws InterruptedException
    {
        for (Thread thread: threads) {
            if (thread.isAlive()) {
                nsay("killerThread : interrupting " + thread.getName());
                thread.interrupt();
            }
        }
    }

    private Runnable wrapLoggingContext(final Runnable runnable)
    {
        final Stack stack = NDC.cloneStack();
        return new Runnable() {
            public void run() {
                CDC.setCellsContext(CellNucleus.this);
                NDC.inherit(stack);
                try {
                    runnable.run();
                } finally {
                    NDC.remove();
                }
            }
        };
    }

    public Thread newThread(Runnable target)
    {
        return new Thread(_threads, wrapLoggingContext(target));
    }

    public Thread newThread(Runnable target, String name)
    {
        return new Thread(_threads, wrapLoggingContext(target), name);
    }

    //
    //  package
    //
    Thread [] getThreads(String cellName) {
        return __cellGlue.getThreads(cellName);
    }
    public ThreadGroup getThreadGroup() { return _threads; }
    Thread [] getThreads() {
        if (_threads == null)return new Thread[0];

        int threadCount = _threads.activeCount();
        Thread [] list  = new Thread[threadCount];
        int rc = _threads.enumerate(list);
        if (rc == list.length)return list;
        Thread [] ret = new Thread[rc];
        System.arraycopy(list, 0, ret, 0, rc);
        return ret;
    }

    int  getUnique() { return __cellGlue.getUnique(); }

    int getEventQueueSize()
    {
        if (_messageExecutor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor executor =
                (ThreadPoolExecutor) _messageExecutor;
            return executor.getQueue().size();
        }
        return 0;
    }

    void addToEventQueue(CellEvent ce) {
        //
        //
        if (ce instanceof RoutedMessageEvent) {
            if (_cell instanceof CellTunnel) {
                //
                // nothing to do (no transformation needed)
                //
            } else {
                //
                // originally this case has not been forseen,
                // but it appeared rather useful. It allows alias
                // cells which serves several different cells names.
                // mainly useful for debuggin purposes (see alias
                // package.
                //
                ce = new MessageEvent(((RoutedMessageEvent)ce).getMessage());
            }
        }

        try {
            if (ce instanceof MessageEvent) {
                //
                // we have to cover 3 cases :
                //   - absolutely asynchronous request
                //   - asynchronous, but we have a callback to call
                //   - synchronous
                //
                final CellMessage msg = ((MessageEvent) ce).getMessage();
                if (msg != null) {
                    nsay("addToEventQueue : message arrived : "+msg);
                    CellLock lock;

                    synchronized (_waitHash) {
                        lock = _waitHash.remove(msg.getLastUOID());
                    }

                    if (lock != null) {
                        //
                        // we were waiting for you (sync or async)
                        //
                        nsay("addToEventQueue : lock found for : "+msg);
                        if (lock.isSync()) {
                            nsay("addToEventQueue : is synchronous : "+msg);
                            synchronized (lock) {
                                lock.setObject(msg);
                                lock.notifyAll();
                            }
                            nsay("addToEventQueue : dest. was triggered : "+msg);
                        } else {
                            nsay("addToEventQueue : is asynchronous : "+msg);
                            _callbackExecutor.execute(new CallbackTask(lock, msg));
                        }
                        return;
                    }
                }     // end of : msg != null
            }        // end of : ce instanceof MessageEvent

            _messageExecutor.execute(new DeliverMessageTask(ce));
        } catch (RejectedExecutionException e) {
            _logNucleus.error("Message queue overflow. Dropping " + ce);
        }
    }

    void sendKillEvent(KillEvent ce)
    {
        nsay("sendKillEvent : received "+ce);
        Thread thread = new KillerThread(ce);
        thread.start();
        nsay("sendKillEvent : " + thread.getName()+" started on group "+
             thread.getThreadGroup().getName());
    }

    //
    // helper to get version string from arbitrary object
    //
    public static CellVersion getCellVersionByObject(Object obj) throws Exception {
        Class<?> c =  obj.getClass();

        Method m = c.getMethod("getCellVersion", (Class<?> [])null);

        return (CellVersion)m.invoke(obj, (Object [])null);
    }
    public static CellVersion getCellVersionByClass(Class<?> c) throws Exception {

        Method m = c.getMethod("getCellVersion", (Class [])null);

        return (CellVersion)m.invoke((Object)null, (Object [])null);
    }
    ////////////////////////////////////////////////////////////
    //
    //   create new cell by different arguments
    //   String, String [], Socket
    //   can choose between systemLoader only or
    //   Domain loader.
    //
    public Cell createNewCell(String cellClass,
                              String cellName,
                              String cellArgs,
                              boolean systemOnly)
        throws ClassNotFoundException,
               NoSuchMethodException,
               SecurityException,
               InstantiationException,
               InvocationTargetException,
               IllegalAccessException,
               ClassCastException
    {
        try {
            Object [] args = new Object[1];
            args[0] = cellArgs;
            return (Cell)__cellGlue._newInstance(cellClass,
                                                 cellName,
                                                 args,
                                                 systemOnly);
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw e;
        }
    }

    public Class<?> loadClass(String className) throws ClassNotFoundException {
        return __cellGlue.loadClass(className);
    }
    /*
      public Cell createNewCell(String cellClass,
      String cellName,
      String [] cellArgs)
      throws ClassNotFoundException,
      NoSuchMethodException,
      SecurityException,
      InstantiationException,
      InvocationTargetException,
      IllegalAccessException,
      ClassCastException          {

      Object [] args = new Object[1];
      args[0] = cellArgs;

      return (Cell)__cellGlue._newInstance(cellClass,
      cellName,
      args,
      true);
      }
    */
    public Object  createNewCell(String className,
                                 String cellName,
                                 String [] argsClassNames,
                                 Object [] args)
        throws ClassNotFoundException,
               NoSuchMethodException,
               InstantiationException,
               IllegalAccessException,
               InvocationTargetException,
               ClassCastException                       {

        if (argsClassNames == null)
            return __cellGlue._newInstance(
                                           className, cellName, args, false);
        else
            return __cellGlue._newInstance(
                                           className, cellName, argsClassNames, args, false);
    }
    public Cell createNewCell(String cellClass,
                              String cellName,
                              Socket socket,
                              boolean systemOnly)
        throws ClassNotFoundException,
               NoSuchMethodException,
               SecurityException,
               InstantiationException,
               InvocationTargetException,
               IllegalAccessException,
               ClassCastException          {

        Object [] args = new Object[1];
        args[0] = socket;

        return (Cell)__cellGlue._newInstance(cellClass,
                                             cellName,
                                             args,
                                             systemOnly);
    }
    ////////////////////////////////////////////////////////////
    //
    //
    // the routing stuff
    //
    public void routeAdd(CellRoute  route) throws IllegalArgumentException {
        __cellGlue.routeAdd(route);
    }
    public void routeDelete(CellRoute  route) throws IllegalArgumentException {
        __cellGlue.routeDelete(route);
    }
    CellRoute routeFind(CellAddressCore addr) {
        return __cellGlue.getRoutingTable().find(addr);
    }
    CellRoutingTable getRoutingTable() { return __cellGlue.getRoutingTable(); }
    CellRoute [] getRoutingList() { return __cellGlue.getRoutingList(); }
    //
    CellTunnelInfo [] getCellTunnelInfos() { return __cellGlue.getCellTunnelInfos(); }
    //

    public static final int  PRINT_CELL          =    1;
    public static final int  PRINT_ERROR_CELL    =    2;
    public static final int  PRINT_NUCLEUS       =    4;
    public static final int  PRINT_ERROR_NUCLEUS =    8;
    public static final int  PRINT_FATAL         = 0x10;
    public static final int  PRINT_ERRORS        =
        PRINT_ERROR_CELL|PRINT_ERROR_NUCLEUS;
    public static final int  PRINT_EVERYTHING    =
        PRINT_CELL|PRINT_ERROR_CELL|PRINT_NUCLEUS|PRINT_ERROR_NUCLEUS|PRINT_FATAL;

    /* Log a message to a logger. Calls the initLoggingContext method
     * to ensure that the 'cell' and 'domain' MDCs are set. These
     * should have been set for the calling thread already, but many
     * cells don't create the thread in the proper context.
     */
    private void log(Logger logger, Level level, String message)
    {
        Object cell = MDC.get(CDC.MDC_CELL);
        Object domain = MDC.get(CDC.MDC_DOMAIN);
        CDC.setCellsContext(this);
        try {
            logger.log(level, message);
        } finally {
            if (cell == null) {
                MDC.remove(CDC.MDC_CELL);
            } else {
                MDC.put(CDC.MDC_CELL, cell);
            }
            if (domain == null) {
                MDC.remove(CDC.MDC_DOMAIN);
            } else {
                MDC.put(CDC.MDC_DOMAIN, domain);
            }
        }
    }

    /* Log exception to a logger. Calls the initLoggingContext method
     * to ensure that the 'cell' and 'domain' MDCs are set. These
     * should have been set for the calling thread already, but many
     * cells don't create the thread in the proper context.
     */
    private void log(Logger logger, Level level, Throwable t)
    {
        Object cell = MDC.get(CDC.MDC_CELL);
        Object domain = MDC.get(CDC.MDC_DOMAIN);
        CDC.setCellsContext(this);
        try {
            logger.log(level, t, t);
        } finally {
            if (cell == null) {
                MDC.remove(CDC.MDC_CELL);
            } else {
                MDC.put(CDC.MDC_CELL, cell);
            }
            if (domain == null) {
                MDC.remove(CDC.MDC_DOMAIN);
            } else {
                MDC.put(CDC.MDC_DOMAIN, domain);
            }
        }
    }

    public void say(int level, String str)
    {
        if ((level & PRINT_FATAL) > 0) {
            fsay(str);
        } else if ((level & PRINT_ERROR_CELL) > 0) {
            esay(str);
        } else if ((level & PRINT_ERROR_NUCLEUS) > 0) {
            nesay(str);
        } else if ((level & PRINT_CELL) > 0 ) {
            say(str);
        } else if ((level & PRINT_NUCLEUS) > 0) {
            nsay(str);
        }
    }

    public void say(String str)
    {
        if ((_printoutLevel & PRINT_CELL) > 0) {
            log(_logCell, Level.WARN, str);
        } else {
            log(_logCell, Level.INFO, str);
        }
    }

    public void esay(String str)
    {
        if ((_printoutLevel & PRINT_ERROR_CELL) > 0) {
            log(_logCell, Level.ERROR, str);
        } else {
            log(_logCell, Level.INFO, str);
        }
    }

    public void fsay(String str)
    {
        log(_logCell, Level.FATAL, str);
    }

    private void nsay(String str)
    {
        if ((_printoutLevel & PRINT_NUCLEUS) > 0) {
            log(_logNucleus, Level.WARN, str);
        } else {
            log(_logNucleus, Level.INFO, str);
        }
    }

    private void nesay(String str)
    {
        if ((_printoutLevel & PRINT_ERROR_NUCLEUS) > 0) {
            log(_logNucleus, Level.ERROR, str);
        } else {
            log(_logNucleus, Level.INFO, str);
        }
    }

    public void esay(Throwable t)
    {
        if ((_printoutLevel & PRINT_ERROR_CELL) > 0) {
            log(_logCell, Level.ERROR, t);
        } else {
            log(_logCell, Level.INFO, t);
        }
    }

    private void nesay(Throwable t)
    {
        if ((_printoutLevel & PRINT_ERROR_NUCLEUS) > 0) {
            log(_logNucleus, Level.ERROR, t);
        } else {
            log(_logNucleus, Level.INFO, t);
        }
    }

    private class KillerThread extends Thread
    {
        private final KillEvent _event;

        public KillerThread(KillEvent event)
        {
            super(__cellGlue.getKillerThreadGroup(), "killer-" + _cellName);
            _event = event;
        }

        public void run()
        {
            nsay("killerThread : started");
            _state = REMOVING;
            addToEventQueue(new LastMessageEvent());
            try {
                _cell.prepareRemoval(_event);
            } catch (Throwable e) {
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
            }

            synchronized (this) {
                if (_isPrivateCallbackExecutor) {
                    _callbackExecutor.shutdown();
                }
                if (_isPrivateMessageExecutor) {
                    _messageExecutor.shutdown();
                }
            }

            nsay("killerThread : waiting for all threads in "+_threads+" to finish");

            try {
                Collection<Thread> threads = getNonDaemonThreads(_threads);

                /* Some threads shut down asynchronously. Give them
                 * one second before we start to kill them.
                 */
                while (!joinThreads(threads, 1000)) {
                    killThreads(threads);
                }
                _threads.destroy();
            } catch (IllegalThreadStateException e) {
                _threads.setDaemon(true);
            } catch (InterruptedException e) {
                nesay("killerThread : Interrupted while waiting for threads");
            }
            __cellGlue.destroy(CellNucleus.this);
            _state = DEAD;
            nsay("killerThread : stopped");

            PinboardAppender.removePinboard(_cellName);
        }
    }

    private abstract class AbstractNucleusTask implements Runnable
    {
        protected abstract void innerRun();

        public void run ()
        {
            CDC cdc = new CDC();
            try {
                CDC.setCellsContext(CellNucleus.this);
                innerRun();
            } catch (Throwable e) {
                Thread t = Thread.currentThread();
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
            } finally {
                cdc.apply();
            }
        }
    }

    private class CallbackTask extends AbstractNucleusTask
    {
        private final CellLock _lock;
        private final CellMessage _message;

        public CallbackTask(CellLock lock, CellMessage message)
        {
            _lock = lock;
            _message = message;
        }

        public void innerRun()
        {
            CellMessageAnswerable callback =
                _lock.getCallback();

            CellMessage answer;
            Object obj;
            try {
                answer = new CellMessage(_message);
                CDC.setMessageContext(answer);
                obj = answer.getMessageObject();
            } catch (SerializationException e) {
                nesay(e.getMessage());
                obj = e;
                answer = null;
            }

            EventLogger.sendEnd(_lock.getMessage());
            if (obj instanceof Exception) {
                callback.
                    exceptionArrived(_lock.getMessage(), (Exception) obj);
            } else {
                callback.
                    answerArrived(_lock.getMessage(), answer);
            }
            nsay("addToEventQueue : callback done for : " + _message);
        }
    }

    private class DeliverMessageTask extends AbstractNucleusTask
    {
        private final CellEvent _event;

        public DeliverMessageTask(CellEvent event)
        {
            _event = event;
            EventLogger.queueBegin(_event);
        }

        public void innerRun()
        {
            EventLogger.queueEnd(_event);

            if (_event instanceof LastMessageEvent) {
                nsay("messageThread : LastMessageEvent arrived");
                _cell.messageArrived((MessageEvent) _event);
            } else if (_event instanceof RoutedMessageEvent) {
                nsay("messageThread : RoutedMessageEvent arrived");
                _cell.messageArrived((RoutedMessageEvent) _event);
            } else if (_event instanceof MessageEvent) {
                MessageEvent msgEvent = (MessageEvent) _event;
                nsay("messageThread : MessageEvent arrived");
                CellMessage msg;
                try {
                    msg = new CellMessage(msgEvent.getMessage());
                } catch (SerializationException e) {
                    CellMessage envelope = msgEvent.getMessage();
                    _logCell.fatal(String.format("Discarding a malformed message from %s with UOID %s and session [%s]: %s",
                                                 envelope.getSourcePath(),
                                                 envelope.getUOID(),
                                                 envelope.getSession(),
                                                 e.getMessage()), e);
                    return;
                }

                CDC.setMessageContext(msg);
                try {
                    //
                    // deserialize the message
                    //
                    if (_logMessages.isDebugEnabled()) {
                        String messageObject = msg.getMessageObject() == null? "NULL" : msg.getMessageObject().getClass().getName();
                        _logMessages.debug("nucleusMessageArrived src=" + msg.getSourceAddress() +
                                           " dest=" + msg.getDestinationAddress() + " [" + messageObject + "] UOID=" + msg.getUOID().toString());
                    }
                    //
                    // and deliver it
                    //
                    nsay("messageThread : delivering message : "+msg);
                    _cell.messageArrived(new MessageEvent(msg));
                    nsay("messageThread : delivering message done : "+msg);
                } catch (RuntimeException e) {
                    if (!msg.isReply()) {
                        try {
                            msg.revertDirection();
                            msg.setMessageObject(e);
                            sendMessage(msg);
                        } catch (NoRouteToCellException f) {
                            esay("PANIC : Problem returning answer : " + f);
                        }
                    }
                    throw e;
                } finally {
                    CDC.clearMessageContext();
                }
            }
        }
    }
}

package dmg.cells.nucleus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

import dmg.cells.network.PingMessage;
import dmg.cells.services.RoutingManager;
import dmg.util.Authorizable;
import dmg.util.AuthorizedArgs;
import dmg.util.AuthorizedString;
import dmg.util.CommandAclException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import dmg.util.Gate;
import dmg.util.Pinboard;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import dmg.util.logback.FilterShell;

import org.dcache.util.Args;
import org.dcache.util.Version;

/**
 *
 *
 *
 * The CellAdapter builds the basic implementation of a Cell.
 * The CellAdapter implements all required methods of the
 * Cell interface and performs some basic actions as long
 * as those methods are not overloaded by a subclass of
 * CellAdapter. CellAdapter introduces a new set of methods
 * which provide a similiar functionallity as the original
 * Cell callbacks, but are much easier to use.
 * Additionally CellAdapter offers a subset of the CellNucleus
 * methods to be called directly without storing the
 * corresponding handle to the CellNucleus.
 * CellAdapter has a buildin command interpreter
 * <code>see CommandInterpreter</code> which is automatically
 * invokes for each packet containing a plain String object.
 * The result is returned to the sender.
 * CellAdapter forwards and answers ping requests without
 * informing the subclass of CellAdapter as long as this
 * capabitity is not switched of explicitly.
 *
 * @author Patrick Fuhrmann
 * @version 0.2.11, 10/22/1998
 */

public class   CellAdapter extends CommandInterpreter
    implements Cell, CellEventListener, CellEndpoint
{
    private final static Logger _log =
        LoggerFactory.getLogger(CellAdapter.class);

    private final CellVersion _version = new CellVersion(Version.of(this));

    private final static ThreadLocal<CellMessage> CURRENT_MESSAGE = new ThreadLocal<>();

    private final CellNucleus _nucleus;
    private final Gate        _readyGate = new Gate(false);
    private final Gate        _startGate = new Gate(false);
    private final Gate        _shutdownGate = new Gate(false);
    private final Args        _args;
    private boolean     _useInterpreter = true;
    private boolean     _returnCommandException = true;
    private boolean     _answerPing     = true;
    private String      _autoSetup;
    private String      _definedSetup;

    /**
     * Creates a Cell and the corresponding CellNucleus with the
     * specified name. An extra boolean argument 'startNow'
     * allows to delay the arrival of messages until the
     * CellAdapter.start() method is called.
     *
     * @param cellName is the name of the newly created cell. The name
     *                 has to be unique within the context of this CellDomain.
     * @param args an arbitrary argument string with can be obtained
     *        by getArgs later on.
     * @param startNow the arrival of messages is enabled.
     * @exception IllegalArgumentException is thrown if the name is
     *            not unique within this CellDomain.
     */
    public CellAdapter(String cellName,
                       String args,
                       boolean startNow) {

        this(cellName,  new Args(args == null ? "" : args), startNow);

    }
    public CellAdapter(String cellName,
                       String cellType,
                       String args,
                       boolean startNow) {

        this(cellName,  cellType, new Args(args == null ? "" : args), startNow);

    }
    public CellAdapter(String  cellName,
                       Args    args,
                       boolean startNow) {
        this(cellName, "Generic", args, startNow);
    }
    public CellAdapter(String  cellName,
                       String  cellType,
                       Args    args,
                       boolean startNow) {
        _args      = args;
        _nucleus   = new CellNucleus(this, cellName, cellType);
        _autoSetup = cellName + "Setup";

        if ((_args.argc() > 0) &&
            ((_definedSetup = _args.argv(0)).length() > 1) &&
            (_definedSetup.startsWith("!"))) {

            _definedSetup = _definedSetup.substring(1);
            _args.shift();

        } else {
            _definedSetup = null;
        }

        if (_args.hasOption("export") && (_args.getOption("export").isEmpty() || Boolean.parseBoolean(_args.getOption("export")))) {
            export();
        }

        if (_args.hasOption("replyObject") && _args.getOpt("replyObject").equals("false")) {
            setCommandExceptionEnabled(false);
        }

        addCommandListener(new FilterShell(_nucleus.getLoggingThresholds()));
        addCommandListener(new HelpCommands());

        if (startNow) {
            start();
        }
    }

    /**
     *  starts the delivery of messages to this cell and
     *  executes the auto and defined Setup context.
     *  (&lt;cellName&gt;Setup and "!&lt;setupContextName&gt;)
     *  This method has to be called if the
     *  contructor has been used with the startNow
     *  argument set to 'false'.
     *
     */
    public void start() {
        executeSetupContext();
        _startGate.open();
    }

    @Override
    public Serializable command(Args args) throws CommandException {
        if (args.argc() == 0) {
            return "";
        }

        //
        // check for the NOOP command.
        //
        if (args.argc() > 0 && args.argv(0).equals("xyzzy")) {
            return "Nothing happens.";
        }

        return super.command(args);
    }

    /**
     *  Executes the ContextVariable :
     *  &lt;cellName&gt;Setup and "!&lt;setupContextName&gt;"
     *
     */
    public void executeSetupContext()
    {
        if (_autoSetup != null) {
            executeDomainContext(_autoSetup);
        }
        _autoSetup = null;
        if (_definedSetup != null) {
            executeDomainContext(_definedSetup);
        }
        _definedSetup = null;
    }

    protected void executeDomainContext(String name)
    {
        if (name != null) {
            try {
                try (Reader in = _nucleus.getDomainContextReader(name)) {
                    CellShell shell = new CellShell(this);
                    shell.execute("context:" + name, in, new Args(""));
                }

            } catch (FileNotFoundException e) {
                // Ignored: Context variable is not defined
            } catch (CommandExitException | IOException e) {
                _log.warn(e.getMessage());
            }
        }
    }

    /**
     * Creates a Cell and the corresponding CellNucleus with the
     * specified name.
     *
     * @param cellName is the name of the newly created cell. The name
     *                 has to be unique within the context of this CellDomain.
     * @exception IllegalArgumentException is thrown if the name is
     *            not unique within this CellDomain.
     */
    public CellAdapter(String cellName) {
        this(cellName, "", true);
    }
    /**
     * Creates a Cell and the corresponding CellNucleus with the
     * specified name and a set of arguments.
     *
     * @param cellName is the name of the newly created cell. The name
     *                 has to be unique within the context of this CellDomain.
     * @exception IllegalArgumentException is thrown if the name is
     *            not unique within this CellDomain.
     */
    public CellAdapter(String cellName, String args) {
        this(cellName, args, true);
    }
    //
    // adapter to the nucleus
    //

    /**
     *  Adds a CellEventListener to the current CellNucleus.
     * @param cel has to be an object which implements CellEventListener.
     * @see CellEventListener
     */
    public void addCellEventListener(CellEventListener cel) {
        _nucleus.addCellEventListener(cel);
    }
    /**
     *  Declares this Cell to be a CellEventListener.
     *  All methods are implemented by the CellAdapter but
     *  don't perform any actions. The subclass has to
     *  overwrite all those methods, it is interested in.
     *
     * @see CellEventListener
     */
    public void addCellEventListener() {
        _nucleus.addCellEventListener(this);
    }
    /**
     *  returns an Args object created from the second
     *  argument of the constructor : this(String name, String args).
     *
     * @return a handle to an dmg.util.Args object.
     *
     * @see Args
     */
    @Override
    public Args getArgs() { return _args; }
    /**
     *  enables or disables the return type of the buildin command interpreter.
     *
     * @param use enables the return of CommandExceptions.
     */
    public void setCommandExceptionEnabled(boolean use) {
        _returnCommandException = use;
    }
    /**
     *  enables or disables the buildin command interpreter.
     *  The default behaviour is to use the interpreter.
     * @param use enables the interpreter if set to 'true' otherwise
     *            the interpreter  is disabled.
     */
    public void useInterpreter(boolean use) { _useInterpreter = use; }
    /**
     *  enables or disables the ability to answer or to forward
     *  a ping request without calling 'messageArrived' or
     *  'messageToForward'.
     *  The default behaviour is to answer or to forward a ping.
     * @param ping instructs the CellAdapter to answer or forward ping requests.
     */
    public void setAnswerPing(boolean ping) { _answerPing = ping; }
    /**
     *  returns the CellNucleus assigned to this cell. This handle
     *  might be usefull to have access to the full nucleus functionallity.
     *
     * @return a handle to the CellNucleus connected to this cell.
     */
    public CellNucleus getNucleus() { return _nucleus; }

    /**
     * Setup the logging context of the calling thread. Threads
     * created from the calling thread automatically inherit this
     * information.
     */
    public void initLoggingContext() { CDC.reset(_nucleus); }

    /**
     *  informs the CellCore to remove this cell.
     *  The cell kernel will start the kill sequence as soon as
     *  possible.
     */
    protected void kill() { _nucleus.kill(); }
    /**
     *  returns the name of this cell.
     * @return the name of this cell.
     */
    public String getCellName() { return _nucleus.getCellName(); }
    /**
     *  returns the name of the domain this cell resides in.
     * @return the name of this domain.
     */
    public String getCellDomainName() { return _nucleus.getCellDomainName(); }
    /**
     * marks this cell to be exportable. This call triggers an
     * CellExported event to be delivered to all CellEventListeners.
     * The call should only be used for cells with a
     * wellknown name because this name is distributed to
     * all relevent domains as soon as a
     * RoutingManager is
     * running.
     *
     * @see RoutingManager
     */
    public void   export() { _nucleus.export(); }
    /**
     * Defines a pinboard for this CellAdapter.
     *
     * @param size maximum number of lines kept by the pinboard.
     *
     */
    public void createPinboard(int size)
    {
        _nucleus.setPinboard(new Pinboard(size <= 0 ? 200 : size));
    }

    /**
     *
     *
     * @param className Name of the cellClass which should be created
     * @param cellName  Name of the cell instance
     * @param args      An array of Objects which are passed to the
     *                  constructor of the specified cellClass.
     *
     */
    public Object  createNewCell(String className,
                                 String cellName,
                                 String [] argsClassNames,
                                 Object [] args)
        throws ClassNotFoundException,
               NoSuchMethodException,
               SecurityException,
               InstantiationException,
               InvocationTargetException,
               IllegalAccessException,
               ClassCastException                       {

        return _nucleus.createNewCell(className, cellName,
                                      argsClassNames, args);

    }

    @Override
    public Map<String,Object> getDomainContext()
    {
        return _nucleus.getDomainContext();
    }

    /**
     *
     * Returns a reader of the specified context Object.
     * The method allows to read throw a 'context object'
     * as if it was a file.
     *
     * @param contextName Name of the context Object.
     *
     */
    public Reader getDomainContextReader(String contextName)
        throws FileNotFoundException {
        return _nucleus.getDomainContextReader(contextName);
    }

    protected <T> Future<T> invokeOnMessageThread(Callable<T> task)
    {
        return _nucleus.invokeOnMessageThread(task);
    }

    /**
     *  sends a <code>CellMessage</code> along the specified path.
     *
     * @param msg the message to be sent.
     * @exception SerializationException if the payload object of this
     *            message is not Serializable.
     * @exception NoRouteToCellException if the destination <code>CellPath</code>
     *            couldn't be reached.
     *
     */
    @Override
    public void sendMessage(CellMessage msg)
        throws SerializationException, NoRouteToCellException
    {
        getNucleus().sendMessage(msg, true, true);
    }

    @Override
    public void sendMessageWithRetryOnNoRouteToCell(CellMessage msg,
                                                    CellMessageAnswerable callback,
                                                    Executor executor,
                                                    long timeout)
        throws SerializationException
    {
        CellMessageAnswerable retryingCallback =
                new RetryingCellMessageAnswerable(msg, callback, executor, timeout);
        sendMessage(msg, retryingCallback, executor, timeout);
    }

    @Override
    public void sendMessage(CellMessage msg,
                            CellMessageAnswerable callback,
                            Executor executor,
                            long timeout)
        throws SerializationException
    {
        getNucleus().sendMessage(msg, true, true, callback, executor, timeout);
    }

    /**
     *  Returns the message object which caused a
     *  Command Interpreter client method to trigger.
     *  The result object is only 'non-zero' inside
     *  a ac_xxx method.
     */
    public final static CellMessage getThisMessage() {
        return CURRENT_MESSAGE.get();
    }
    //
    // methods which may be overwriten
    //

    /**
     * should be overwrite to provide a more specific
     * one line information about this cell.
     *
     * @return a one line information String.
     */
    public String toString() {  return _nucleus.getCellName();  }
    /**
     * should be overwrite to provide more specific
     * information about this cell.
     *
     * @param printWriter the printWriter which has to be used to
     *                    write the information to.
     *
     */
    public void getInfo(PrintWriter printWriter) {
        printWriter.println(" CellName  : "+_nucleus.getCellName());
        printWriter.println(" CellClass : "+this.getClass().getName());
        printWriter.println(" Arguments : "+_args);
    }

    @Override
    public CellVersion getCellVersion()
    {
        return _version;
    }

    @Override
    public CellInfo getCellInfo() { return _nucleus.getCellInfo(); }
    /**
     * has to be overwritten to receive arriving messages.
     * The LastMessageEvent is filtered out and starts the
     * kill sequence which calls 'cleanUp' at the end of the
     * sequence. If the CommandInterpreter facility is enabled,
     * all string messages are send to the command interpreter
     * and answered without intervention of the callback.
     * If the command could not be found by the CommandInterpreter,
     * <link>dmg.cells.nucleus.CellAdapter#commandArrived(CellMessage)</link>
     * is called if it is overwritten
     * by one of the CellAdapters subclasses.
     * This callback is only used to inform about messages of which
     * the current cell is the final destination.
     * Other messages are delivered throu <code>messageToForward</code>.
     *
     * @param msg the reference to message arrived.
     */
    public void messageArrived(CellMessage msg) {
        _log.info(" CellMessage From   : " + msg.getSourcePath());
        _log.info(" CellMessage To     : " + msg.getDestinationPath());
        _log.info(" CellMessage Object : " + msg.getMessageObject());

    }
    /**
     * has to be overwritten to receive arriving messages which
     * are not directly addressed to this cell. The default behaviour
     * is to select the next destination and to resend the message.
     *
     * @param msg the reference to message arrived.
     *
     */
    public void messageToForward(CellMessage msg) {
        msg.nextDestination();
        try {
            _nucleus.sendMessage(msg, true, true);
        } catch (NoRouteToCellException nrtc) {
            _log.warn("CellAdapter : NoRouteToCell in messageToForward : "+nrtc);
        } catch (Exception eee) {
            _log.warn("CellAdapter : Exception in messageToForward : "+eee);
        }
    }
    public Class<?> loadClass(String className) throws ClassNotFoundException {
        return _nucleus.loadClass(className);
    }

    /**
     *
     *  If overwritten this method delivers commands which
     *  produced a syntax error which intereted by the
     *  CommandInterpreter. The original message string
     *  is provides together with a help text offered
     *  by the interpreter.
     *  If not overwritten this helptext is send back to the
     *  caller.
     *
     * @param str is the orginal command string.
     * @param cse is the syntax error exception thrown by the
     *            command interpreter. cse.getHelpText offers
     *            the possible help text.
     * @return the object which is send back to the caller.
     *             If <code>null</code> nothing is send back.
     */
    public Serializable commandArrived(String str, CommandSyntaxException cse) {
        StringBuilder sb = new StringBuilder();
        sb.append("Syntax Error : ").append(cse.getMessage()).append("\n");
        String help  = cse.getHelpText();
        if (help != null) {
            sb.append("Help : \n");
            sb.append(help);
        }
        return sb.toString();
    }

    protected void awaitStart()
    {
        _startGate.check();
    }

    /**
     * has to be overwritten to perform any actions before this
     * cell is destroyed. 'cleanUp' is called after the last
     * message has arrived. The default behaviour is to do nothing.
     *
     */
    public void cleanUp() {  }
    //
    // methods from the cellEventListener Interface
    //
    /**
     *   belongs to the CellEventListener Interface
     */
    @Override
    public void cellCreated(CellEvent ce) {}
    /**
     *   belongs to the CellEventListener Interface
     */
    @Override
    public void cellDied(CellEvent ce) {}
    /**
     *   belongs to the CellEventListener Interface
     */
    @Override
    public void cellExported(CellEvent ce) {}
    /**
     *   belongs to the CellEventListener Interface
     */
    @Override
    public void routeAdded(CellEvent ce) {}
    /**
     *   belongs to the CellEventListener Interface
     */
    @Override
    public void routeDeleted(CellEvent ce) {}

    @Command(name = "xgetcellinfo")
    public class GetCellInfoCommand implements Callable<CellInfo>
    {
        @Override
        public CellInfo call()
        {
            return getCellInfo();
        }
    }

    @Command(name = "info")
    public class InfoCommand implements Callable<String>
    {
        @Option(name = "a", usage = "Display content of unanswered message requests.")
        boolean full;

        @Option(name = "l", usage = "Display unanswered message requests.")
        boolean lng;

        @Override
        public String call()
        {
            if (lng || full) {
                StringBuilder sb = new StringBuilder();
                sb.append(getInfo()).append("\n");
                Map<UOID,CellLock > map = _nucleus.getWaitQueue();
                if (! map.isEmpty()) {
                    sb.append("\nWe are waiting for the following messages\n");
                }
                for (Map.Entry<UOID,CellLock > entry : map.entrySet()) {
                    Object    key   = entry.getKey();
                    CellLock  lock  = entry.getValue();
                    sb.append(key.toString()).append(" r=");
                    long res = lock.getTimeout() - System.currentTimeMillis();
                    sb.append(res/1000).append(" sec;");
                    CellMessage msg = lock.getMessage();
                    if (msg == null) {
                        sb.append("msg=none");
                    } else {
                        Object obj = msg.getMessageObject();
                        if (obj != null) {
                            sb.append("msg=").append(obj.getClass().getName());
                            if (full) {
                                sb.append("/").append(obj.toString());
                            }
                        }
                    }
                    sb.append("\n");
                }
                return sb.toString();
            } else {
                return getInfo();
            }
        }
    }

    @Command(name = "show pinboard",
             hint = "display the most recent pinboard messages",
             description = "The pinboard always stores the most recent log messages.  It has " +
                     "a fixed capacity: once full appending a new message will eject the oldest " +
                     "stored message.  See also the 'log set' command.")
    public class ShowPinboardCommand implements Callable<String>
    {
        @Argument(required = false, metaVar = "lines",
                  usage = "How many pinboard entries to display.")
        int lines = 20;

        @Override
        public String call()
        {
            Pinboard pinboard = _nucleus.getPinboard();
            if (pinboard == null) {
                return "No pinboard defined";
            }
            StringBuilder sb = new StringBuilder();
            pinboard.dump(sb, lines);
            return sb.toString();
        }
    }

    @Command(name = "dump pinboard", hint = "write pinboard to file",
             description = "Writes the pinboard log to FILE on the local file system of the service.")
    public class DumpPinboardCommand implements Callable<String>
    {
        @Argument(metaVar = "file")
        File file;

        @Override
        public String call() throws IOException
        {
            Pinboard pinboard = _nucleus.getPinboard();
            if (pinboard == null) {
                return "No pinboard defined.";
            }
            pinboard.dump(file);
            return "Pinboard dumped to " + file;
        }
    }

    /**
     *   belongs to the Cell Interface.
     *   If this method is overwritten, the 'cleanUp'
     *   method won't be called.
     */
    @Override
    public void prepareRemoval(KillEvent ce)
    {
        _log.info("CellAdapter : prepareRemoval : waiting for gate to open");
        _readyGate.check();
        _shutdownGate.open();
        cleanUp();
        dumpPinboard();
        _log.info("CellAdapter : prepareRemoval : done");
    }

    //
    // package private (we need it in CellShell)
    //
    void dumpPinboard()
    {
        Pinboard pinboard = _nucleus.getPinboard();
        try {
            Map<String,Object> context = getDomainContext();
            String dumpDir = (String) context.get("dumpDirectory");
            if (dumpDir == null) {
                _log.info("Pinboard not dumped (dumpDirectory not sp.)");
                return;
            }
            File dir = new File(dumpDir);
            if (!dir.isDirectory()) {
                _log.info("Pinboard not dumped (dumpDirectory {} not found)",
                          dumpDir);
                return;
            }
            if (pinboard == null) {
                _log.info("Pinboard not dumped (no pinboard defined)");
                return;
            }

            File dump = new File(dir,
                                 getCellDomainName()+"-"+
                                 getCellName()+"-"+
                                 Long.toHexString(System.currentTimeMillis()));
            pinboard.dump(dump);
        } catch (IOException e) {
            _log.error("Dumping pinboard failed : {}", e.toString());
        }
    }
    /**
     *   belongs to the Cell Interface.
     *   Is never called.
     */
    @Override
    public void   exceptionArrived(ExceptionEvent ce) {
        _log.info(" exceptionArrived "+ce);
    }
    /**
     *   belongs to the Cell Interface.
     *   If this method is overwritten, the getInfo(PrintWriter pw)
     *   is never called.
     */
    @Override
    public String getInfo() {
        StringWriter stringWriter = new StringWriter();
        PrintWriter   printWriter = new PrintWriter(stringWriter);

        getInfo(printWriter);
        printWriter.flush();
        return stringWriter.getBuffer().toString();
    }
    /**
     *   belongs to the Cell Interface.
     *   If this method is overwritten, the messageArrived(CellMessage cm)
     *   and the messageToForward(CellMessage) methods
     *   are never called.
     */
    @Override
    public void   messageArrived(MessageEvent me) {
        _startGate.check();
        if (me instanceof LastMessageEvent) {
            _log.info("messageArrived : LastMessageEvent (opening gate)");
            _readyGate.open();
        } else {
            CellMessage msg = me.getMessage();
            Serializable obj = msg.getMessageObject();

            if (msg.isFinalDestination()) {
                if (!msg.isReply() && msg.getLocalAge() > msg.getAdjustedTtl()) {
                    _log.warn("Discarding " + obj.getClass().getSimpleName() +
                                      " because its time to live has been exceeded.");
                    return;
                }

                if (_useInterpreter && (! msg.isReply()) &&
                    ((obj instanceof String) ||
                     (obj instanceof AuthorizedString))) {

                    Serializable o;
                    UOID uoid = msg.getUOID();
                    EventLogger.deliverBegin(msg);
                    try {
                        CURRENT_MESSAGE.set(msg);
                        o =  executeLocalCommand(obj);
                        if (o == null) {
                            return;
                        }
                    } catch (CommandThrowableException e) {
                        o = e.getCause();
                    } catch (CommandException ce) {
                        o = ce;
                    } finally {
                        EventLogger.deliverEnd(msg.getSession(), uoid);
                        CURRENT_MESSAGE.remove();
                    }

                    try {
                        msg.revertDirection();
                        if (o instanceof Reply) {
                            Reply reply = (Reply)o;
                            reply.deliver(this, msg);
                        } else {
                            msg.setMessageObject(o);
                            _nucleus.sendMessage(msg, true, true);
                        }
                    } catch (NoRouteToCellException e) {
                        _log.warn("PANIC : Problem returning answer : " + e);
                    }
                } else if ((obj instanceof PingMessage) && _answerPing) {
                    PingMessage ping = (PingMessage)obj;
                    if (ping.isWayBack()) {
                        messageArrived(msg);
                        return;
                    }
                    ping.setWayBack();
                    msg.revertDirection();
                    try {
                        _nucleus.sendMessage(msg, true, true);
                    } catch (NoRouteToCellException ee) {
                        _log.warn("Couldn't revert PingMessage : "+ee);
                    }
                } else {
                    UOID uoid = msg.getUOID();
                    EventLogger.deliverBegin(msg);
                    try {
                        messageArrived(msg);
                    } finally {
                        EventLogger.deliverEnd(msg.getSession(), uoid);
                    }
                }
            } else if (obj instanceof PingMessage) {
                msg.nextDestination();
                try {
                    _nucleus.sendMessage(msg, true, true);
                } catch (NoRouteToCellException ee) {
                    _log.warn("Couldn't forward PingMessage : " + ee);
                }

             } else {
                UOID uoid = msg.getUOID();
                EventLogger.deliverBegin(msg);
                try {
                    messageToForward(msg);
                } finally {
                    EventLogger.deliverEnd(msg.getSession(), uoid);
                }
            }
        }

    }
    private Serializable executeLocalCommand(Object command)
        throws CommandException  {

        if (command instanceof Authorizable) {

            if (_returnCommandException) {
                AuthorizedArgs args = new AuthorizedArgs((Authorizable)command);
                return command(args);
            } else {
                return autoCommand(command);
            }

        } else if (command instanceof String) {

            if (_returnCommandException) {
                Args args = new Args((String)command);
                return command(args);
            } else {
                return autoCommand(command);
            }
        } else {
            throw new
                    CommandPanicException("Illegal CommandClass detected",
                    new Exception("PANIC"));
        }


    }
    private Serializable autoCommand(Object command) {

        try {
            if (command instanceof String) {
                Args args = new Args((String) command);
                return command(new Args((String) command));
            } else if (command instanceof AuthorizedString) {
                AuthorizedArgs args = new AuthorizedArgs((AuthorizedString) command);
                return command(args);
            } else {
                return "Panic : internal server error 14345";
            }
        } catch (CommandSyntaxException cse) {
            return commandArrived(command.toString(), cse);
        } catch (CommandExitException cee) {
            return "Sorry, can't exit";
        } catch (CommandThrowableException cte) {
            StringBuilder sb = new StringBuilder();
            sb.append(cte.getMessage()).append("\n");
            Throwable t = cte.getTargetException();
            sb.append(t.getClass().getName()).append(" : ")
                    .append(t.getMessage()).append("\n");
            return sb.toString();
        } catch (CommandPanicException cpe) {
            StringBuilder sb = new StringBuilder();
            sb.append("Panic : ").append(cpe.getMessage()).append("\n");
            Throwable t = cpe.getTargetException();
            sb.append(t.getClass().getName()).append(" : ")
                    .append(t.getMessage()).append("\n");
            return sb.toString();
        } catch (Exception e) {
            return "??? : "+e.toString();
        }
    }

    @Override
    protected Serializable doExecute(CommandEntry entry, Args args,
            String[] acls) throws CommandException
    {
        if (args instanceof Authorizable) {
            checkAclPermission((Authorizable) args, args, acls);
        }

        return super.doExecute(entry, args, acls);
    }

    private CellPath _aclPath    = new CellPath("acm");
    private long     _aclTimeout = 10000L;
    protected void checkAclPermission(Authorizable auth, Object command, String [] acls) throws CommandException {

        String user = auth.getAuthorizedPrincipal();

        if (user.equals("admin") || acls.length == 0) {
            return;
        }

        CommandException recentException = null;

        for (String acl : acls) {
            try {
                checkAclPermission(user, command, acl);
                return;
            } catch (CommandAclException ce) {
                recentException = ce;
            }
        }
        throw recentException;
    }
    protected void checkAclPermission(String user, Object command,  String acl) throws CommandException {

        Object [] request = new Object[5];

        request[0] = "request";
        request[1] = "<nobody>";
        request[2] = "check-permission";
        request[3] = user;
        request[4] = acl;

        CellMessage reply;

        try {
            reply = _nucleus.sendAndWait(new CellMessage(_aclPath, request), _aclTimeout);
            if (reply == null) {
                throw new CommandException("Error in acl handling : Acl Request timed out (" + _aclPath + ")");
            }

        } catch (NoRouteToCellException | ExecutionException | InterruptedException e) {
            throw new CommandException("Error in acl handling: " + e.getMessage(), e);
        }

        Object r = reply.getMessageObject();
        if ((r == null) ||
            (! (r instanceof Object [])) ||
            (((Object [])r).length < 6) ||
            (! (((Object [])r)[5] instanceof Boolean))) {
            throw new CommandException("Error in acl handling: illegal reply arrived");
        }

        if (! ((Boolean) ((Object[]) r)[5])) {
            throw new CommandAclException(user, acl);
        }
    }

    private class RetryingCellMessageAnswerable implements CellMessageAnswerable, Runnable
    {
        private final long deadline;
        private final CellMessageAnswerable callback;
        private final CellMessage msg;
        private final Executor executor;

        public RetryingCellMessageAnswerable(CellMessage msg, CellMessageAnswerable callback, Executor executor, long timeout)
        {
            this.callback = callback;
            this.msg = msg;
            this.executor = executor;
            deadline = System.currentTimeMillis() + timeout;
        }

        @Override
        public void answerArrived(CellMessage request, CellMessage answer)
        {
            callback.answerArrived(request, answer);
        }

        @Override
        public void exceptionArrived(final CellMessage request, Exception exception)
        {
            if (!(exception instanceof NoRouteToCellException)) {
                callback.exceptionArrived(request, exception);
            } else if (deadline > System.currentTimeMillis()) {
                _nucleus.invokeLater(this);
            } else {
                callback.answerTimedOut(request);
            }
        }

        @Override
        public void answerTimedOut(CellMessage request)
        {
            callback.answerTimedOut(request);
        }

        @Override
        public void run() {
            long timeout = deadline - System.currentTimeMillis();
            if (timeout > 0) {
                sendMessage(msg, this, executor, timeout);
            } else {
                callback.answerTimedOut(msg);
            }
        }
    }
}

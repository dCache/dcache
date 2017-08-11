package dmg.cells.nucleus;

import com.google.common.base.Ascii;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;

import dmg.cells.network.PingMessage;
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
import dmg.util.Pinboard;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import dmg.util.logback.FilterShell;

import org.dcache.util.Args;
import org.dcache.util.Version;
import org.dcache.util.cli.CommandExecutor;

import static org.dcache.util.MathUtils.addWithInfinity;
import static org.dcache.util.MathUtils.subWithInfinity;

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

public class CellAdapter
    implements Cell, CellEventListener, CellEndpoint
{
    private static final Logger _log =
            LoggerFactory.getLogger(CellAdapter.class);
    public static final String MAX_MESSAGE_THREADS = "cell.max-message-threads";
    public static final String MAX_MESSAGES_QUEUED = "cell.max-messages-queued";

    private final CellVersion _version = new CellVersion(Version.of(this));

    private final LongAdder _routeAddedCounter = new LongAdder();

    private final CellNucleus _nucleus;
    private final Args _args;
    private boolean _useInterpreter = true;
    private boolean _returnCommandException = true;
    private boolean _answerPing = true;
    private String _autoSetup;
    private String _definedSetup;

    private CommandInterpreter _commandInterpreter = new CommandInterpreter(this)
    {
        @Override
        protected Serializable doExecute(CommandEntry entry, Args args,
                                         String[] acls) throws CommandException
        {
            if (args instanceof Authorizable) {
                checkAclPermission((Authorizable) args, args, acls);
            }
            return CellAdapter.this.executeCommand(entry.getCommand(), args);
        }

        @Override
        public Serializable command(Args args) throws CommandException
        {
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
    };

    /**
     * Creates a Cell and the corresponding CellNucleus with the
     * specified name. An extra boolean argument 'startNow'
     * allows to delay the arrival of messages until the
     * CellAdapter.start() method is called.
     *
     * @param cellName is the name of the newly created cell. The name
     *                 has to be unique within the context of this CellDomain.
     * @param args     an arbitrary argument string with can be obtained
     *                 by getArgs later on.
     * @throws IllegalArgumentException is thrown if the name is
     *                                  not unique within this CellDomain.
     */
    public CellAdapter(String cellName, String args)
    {
        this(cellName, new Args(args == null ? "" : args));
    }

    public CellAdapter(String cellName,
                       String cellType,
                       String args)
    {
        this(cellName, cellType, new Args(args == null ? "" : args));
    }

    public CellAdapter(String cellName,
                       Args args)
    {
        this(cellName, "Generic", args);
    }

    public CellAdapter(String cellName,
                       String cellType,
                       Args args)
    {
        this(cellName, cellType, args, null);
    }

    public CellAdapter(String cellName,
                       String cellType,
                       Args args,
                       Executor executor)
    {
        _args = args;
        _autoSetup = cellName + "Setup";

        if ((_args.argc() > 0) &&
                ((_definedSetup = _args.argv(0)).length() > 1) &&
                (_definedSetup.startsWith("!"))) {

            _definedSetup = _definedSetup.substring(1);
            _args.shift();

        } else {
            _definedSetup = null;
        }

        if (!_args.getBooleanOption("replyObject", true)) {
            setCommandExceptionEnabled(false);
        }

        _nucleus = new CellNucleus(this, cellName, cellType, executor);
        if (!Strings.isNullOrEmpty(_args.getOption(MAX_MESSAGE_THREADS))) {
            _nucleus.setMaximumPoolSize(_args.getIntOption(MAX_MESSAGE_THREADS));
        }
        if (!Strings.isNullOrEmpty(_args.getOption(MAX_MESSAGES_QUEUED))) {
            _nucleus.setMaximumPoolSize(_args.getIntOption(MAX_MESSAGES_QUEUED));
        }

        addCommandListener(new FilterShell(_nucleus.getLoggingThresholds()));
        addCommandListener(_commandInterpreter.new HelpCommands());
        addCellEventListener(this);
    }

    /**
     * starts the delivery of messages to this cell and
     * executes the auto and defined Setup context.
     * (&lt;cellName&gt;Setup and "!&lt;setupContextName&gt;)
     * Failures to start the cell due to external influences are indicated by
     * a CommandException; all other exceptions are treated as bugs.
     */
    public ListenableFuture<Void> start()
    {
        return _nucleus.start();
    }

    public void addCommandListener(Object commandListener)
    {
        _commandInterpreter.addCommandListener(commandListener);
    }

    public String command(String command) throws CommandExitException
    {
        return _commandInterpreter.command(command);
    }

    public Serializable command(Args args) throws CommandException
    {
        return _commandInterpreter.command(args);
    }

    /**
     * Called to execute admin shell commands. Subclasses may override this to intercept
     * command execution. Implementations should call CommandExecutor#execute to execute
     * the command.
     *
     * @param command The command to execute
     * @param args Arguments of the command
     * @return The return value of the command
     * @throws CommandException If the command fails
     */
    protected Serializable executeCommand(CommandExecutor command, Args args) throws CommandException
    {
        return command.execute(args);
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
                    CellShell shell = new CellShell(_nucleus, _commandInterpreter);
                    shell.execute("context:" + name, in, new Args(""));
                }

            } catch (FileNotFoundException e) {
                // Ignored: Context variable is not defined
            } catch (CommandExitException | IOException e) {
                _log.warn(e.getMessage());
            }
        }
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
     *  returns an Args object created from the second
     *  argument of the constructor : this(String name, String args).
     *
     * @return a handle to an dmg.util.Args object.
     *
     * @see Args
     */
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
     * Returns a Apache Curator instance for this cell. The Curator maintains
     * the CDC of the caller and any callbacks without an explicit executor are
     * delivered on the cell message executor.
     *
     * Note that killing a cell does not remove ephemeral nodes created by the
     * cell. Such nodes are bound to the lifetime of the domain and thus a cell
     * should remove such nodes when it is killed.
     */
    public CuratorFramework getCuratorFramework()
    {
        return _nucleus.getCuratorFramework();
    }

    /**
     * Let's this cell consume messages from a particular named queue. Messages posted to
     * the queue will be delivered to the cell like any other message.
     */
    public void consume(String queue)
    {
        _nucleus.consume(queue);
    }

    /**
     * Subscribes this cell to a specific publish-subscribe topic. Messages posted to
     * the topic will be delivered to the cell like any other message.
     */
    public void subscribe(String topic)
    {
        _nucleus.subscribe(topic);
    }

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

    protected Future<?> invokeOnMessageThread(Runnable task)
    {
        return _nucleus.invokeOnMessageThread(task);
    }

    protected <T> Future<T> invokeOnMessageThread(Callable<T> task)
    {
        return _nucleus.invokeOnMessageThread(task);
    }

    protected void invokeLater(Runnable task)
    {
        _nucleus.invokeLater(() -> invokeOnMessageThread(task));
    }

    @Override
    public void sendMessage(CellMessage msg, SendFlag... flags)
        throws SerializationException
    {
        boolean shouldAddSource = true;
        for (SendFlag flag : flags) {
            switch (flag) {
            case RETRY_ON_NO_ROUTE_TO_CELL:
                throw new IllegalArgumentException("RETRY_ON_NO_ROUTE_TO_CELL can only be used with a callback.");
            case PASS_THROUGH:
                shouldAddSource = false;
                break;
            }
        }

        getNucleus().sendMessage(msg, true, true, shouldAddSource);
    }

    @Override
    public void sendMessage(CellMessage msg,
                            CellMessageAnswerable callback,
                            Executor executor,
                            long timeout,
                            SendFlag... flags)
        throws SerializationException
    {
        boolean shouldAddSource = true;
        for (SendFlag flag : flags) {
            switch (flag) {
            case RETRY_ON_NO_ROUTE_TO_CELL:
                callback = new RetryingCellMessageAnswerable(msg, callback, executor, timeout, _routeAddedCounter.longValue());
                break;
            case PASS_THROUGH:
                shouldAddSource = false;
                break;
            }
        }

        getNucleus().sendMessage(msg, true, true, shouldAddSource, callback, executor, timeout);
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
        _log.info(" CellMessage From   : {}", msg.getSourcePath());
        _log.info(" CellMessage To     : {}", msg.getDestinationPath());
        _log.info(" CellMessage Object : {}", msg.getMessageObject());

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
        try {
            _nucleus.sendMessage(msg, true, true, true);
        } catch (RuntimeException e) {
            _log.warn("CellAdapter : Exception in messageToForward : {}", e.toString());
        }
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
        sb.append("Syntax Error : ").append(cse.getMessage()).append('\n');
        String help  = cse.getHelpText();
        if (help != null) {
            sb.append("Help : \n");
            sb.append(help);
        }
        return sb.toString();
    }

    /**
     * Has to be overridden to perform any actions before this
     * cell is started. {@code starting} is called before the first
     * message arrives.
     *
     * If an exception is thrown, the cell will not be started and
     * stopped will be invoked.
     */
    protected void starting() throws Exception {}

    /**
     * Has to be overridden to perform any actions after this
     * cell is started. At this point message delivery has begun
     * and the cell can receive requests and replies from other cells.
     */
    protected void started() {}

    /**
     * Has to be overridden to perform any action before this cell is
     * destroyed. {@code stopping} is called before the cell is
     * unpublished.
     */
    protected void stopping() {}

    /**
     * Has to be overridden to perform any actions before this
     * cell is destroyed. {@code stopped} is called after the last
     * message has arrived.
     */
    protected void stopped() { }

    @Override
    public void routeAdded(CellEvent ce)
    {
        _routeAddedCounter.increment();
        _nucleus.runDeferredTasksNow();
    }

    @Command(name = "xgetcellinfo", hint = "get this cell information",
            description = "Returns CellInfo object of this cell. This contains " +
                    "the name of the cell, the current state (where, I: " +
                    "Initial, A: Active, R: Removing, D: Dead and U: " +
                    "Unknown), the number of messages queued for this " +
                    "cell, thread count, the class name and a short " +
                    "description of the cell itself.")
    public class GetCellInfoCommand implements Callable<CellInfo>
    {
        @Override
        public CellInfo call()
        {
            return getCellInfo();
        }
    }

    @Command(name = "info", hint = "get detailed cell information",
            description = "Shows detailed information about this cell. " +
                    "The returned information can contain the performance " +
                    "statistics, past and current activities of the cell. " +
                    "This usually depends on the type of cell.")
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
                sb.append(getInfo()).append('\n');
                Map<UOID,CellLock > map = _nucleus.getWaitQueue();
                if (! map.isEmpty()) {
                    sb.append("\nWe are waiting for the following messages\n");
                }
                for (Map.Entry<UOID,CellLock > entry : map.entrySet()) {
                    Object    key   = entry.getKey();
                    CellLock  lock  = entry.getValue();
                    sb.append(key).append(" r=");
                    long res = subWithInfinity(lock.getTimeout(), System.currentTimeMillis());
                    sb.append(res/1000).append(" sec;");
                    CellMessage msg = lock.getMessage();
                    if (msg == null) {
                        sb.append("msg=none");
                    } else {
                        Object obj = msg.getMessageObject();
                        if (obj != null) {
                            sb.append("msg=").append(obj.getClass().getName());
                            if (full) {
                                sb.append('/').append(obj);
                            }
                        }
                    }
                    sb.append('\n');
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

    @Override
    public void prepareStartup(StartEvent event) throws Exception
    {
        try {
            EventLogger.startingBegin(getCellName());
            starting();
        } finally {
            EventLogger.startingEnd(getCellName());
        }
        executeSetupContext();
    }

    @Override
    public void postStartup(StartEvent event)
    {
        for (String queue : Splitter.on(",").omitEmptyStrings().split(_args.getOption("consume", ""))) {
            consume(queue);
        }
        for (String topic : Splitter.on(",").omitEmptyStrings().split(_args.getOption("subscribe", ""))) {
            subscribe(topic);
        }

        try {
            EventLogger.startedBegin(getCellName());
            started();
        } finally {
            EventLogger.startedEnd(getCellName());
        }
    }

    @Override
    public void prepareRemoval(KillEvent killEvent)
    {
        try {
            EventLogger.stoppingBegin(getCellName());
            stopping();
        } finally {
            EventLogger.stoppingEnd(getCellName());
        }
    }

    @Override
    public void postRemoval(KillEvent ce)
    {
        _log.info("CellAdapter : prepareRemoval : waiting for gate to open");
        try {
            EventLogger.stoppedBegin(getCellName());
            stopped();
        } finally {
            EventLogger.stoppedEnd(getCellName());
            dumpPinboard();
        }
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
                                 getCellDomainName() + '-' +
                                 getCellName() + '-' +
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
        _log.info(" exceptionArrived {}", ce.toString());
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
        CellMessage msg = me.getMessage();
        Serializable obj = msg.getMessageObject();

        if (msg.isFinalDestination()) {
            if (!msg.isReply() && msg.getLocalAge() > msg.getAdjustedTtl()) {
                _log.warn("Discarding {} because its age of {} ms exceeds its time to live of {} ms.",
                          (obj instanceof CharSequence) ? '\'' + Ascii.truncate((CharSequence) obj, 50, "...") + '\'' : obj.getClass().getSimpleName(),
                          msg.getLocalAge(), msg.getAdjustedTtl());
                return;
            }

            if (_useInterpreter && (! msg.isReply()) &&
                ((obj instanceof String) ||
                 (obj instanceof AuthorizedString))) {

                Serializable o;
                UOID uoid = msg.getUOID();
                EventLogger.deliverBegin(msg);
                try {
                    o =  executeLocalCommand(obj);
                    if (o == null) {
                        return;
                    }
                } catch (CommandPanicException e) {
                    o = e;
                    _log.error("Command failed due to a bug, please contact support@dcache.org.", e);
                } catch (CommandException ce) {
                    o = ce;
                } finally {
                    EventLogger.deliverEnd(msg.getSession(), uoid);
                }

                if (o instanceof Reply) {
                    Reply reply = (Reply) o;
                    reply.deliver(this, msg);
                } else {
                    msg.revertDirection();
                    msg.setMessageObject(o);
                    _nucleus.sendMessage(msg, true, true, true);
                }
            } else if ((obj instanceof PingMessage) && _answerPing) {
                PingMessage ping = (PingMessage)obj;
                if (ping.isWayBack()) {
                    messageArrived(msg);
                    return;
                }
                ping.setWayBack();
                ping.setOutboundPath(msg.getSourcePath());
                msg.revertDirection();
                _nucleus.sendMessage(msg, true, true, true);
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
            _nucleus.sendMessage(msg, true, true, true);
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
            throw new CommandPanicException("Illegal CommandClass detected");
        }


    }
    private Serializable autoCommand(Object command) {

        try {
            if (command instanceof String) {
                return command(new Args((String) command));
            } else if (command instanceof AuthorizedString) {
                return command(new AuthorizedArgs((AuthorizedString) command));
            } else {
                return "Panic : internal server error 14345";
            }
        } catch (CommandSyntaxException cse) {
            return commandArrived(command.toString(), cse);
        } catch (CommandExitException cee) {
            return "Sorry, can't exit";
        } catch (CommandThrowableException cte) {
            StringBuilder sb = new StringBuilder();
            sb.append(cte.getMessage()).append('\n');
            Throwable t = cte.getTargetException();
            sb.append(t.getClass().getName()).append(" : ")
                    .append(t.getMessage()).append('\n');
            return sb.toString();
        } catch (CommandPanicException cpe) {
            StringBuilder sb = new StringBuilder();
            sb.append("Panic : ").append(cpe.getMessage()).append('\n');
            Throwable t = cpe.getTargetException();
            sb.append(t.getClass().getName()).append(" : ")
                    .append(t.getMessage()).append('\n');
            return sb.toString();
        } catch (Exception e) {
            return "??? : " + e;
        }
    }

    private CellPath _aclPath    = new CellPath("acm");
    private static final long ACL_TIMEOUT = 10_000L;
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
            reply = _nucleus.sendAndWait(new CellMessage(_aclPath, request), ACL_TIMEOUT);
            if (reply == null) {
                throw new CommandException("Error in acl handling : Acl Request timed out (" + _aclPath + ')');
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

        private long cnt;

        private final CellMessageAnswerable callback;
        private final CellMessage msg;
        private final Executor executor;

        public RetryingCellMessageAnswerable(CellMessage msg, CellMessageAnswerable callback, Executor executor,
                                             long timeout, long cnt)
        {
            this.callback = callback;
            this.msg = msg;
            this.executor = executor;
            this.cnt = cnt;
            deadline = addWithInfinity(System.currentTimeMillis(), timeout);
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
            } else if (deadline <= System.currentTimeMillis()) {
                callback.answerTimedOut(request);
            } else {
                long currentCnt = _routeAddedCounter.longValue();
                if (cnt == currentCnt) {
                    _nucleus.invokeLater(this);
                } else {
                    cnt = currentCnt;
                    _nucleus.sendMessage(msg, true, true, false, this, executor,
                                         subWithInfinity(deadline, System.currentTimeMillis()));
                }
            }
        }

        @Override
        public void answerTimedOut(CellMessage request)
        {
            callback.answerTimedOut(request);
        }

        @Override
        public void run() {
            long timeout = subWithInfinity(deadline, System.currentTimeMillis());
            if (timeout > 0) {
                _nucleus.sendMessage(msg, true, true, false, this, executor, timeout);
            } else {
                callback.answerTimedOut(msg);
            }
        }
    }
}

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

import dmg.cells.network.PingMessage;
import dmg.cells.services.RoutingManager;
import dmg.util.Args;
import dmg.util.Authorizable;
import dmg.util.AuthorizedArgs;
import dmg.util.AuthorizedString;
import dmg.util.CommandAclException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandPanicException;
import dmg.util.CommandRequestable;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import dmg.util.Gate;
import dmg.util.Pinboard;
import org.dcache.util.Version;
import dmg.util.logback.FilterShell;

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

public class   CellAdapter
    extends CommandInterpreter
    implements Cell, CellEventListener, CellEndpoint
{
    private final static Logger _log =
        LoggerFactory.getLogger(CellAdapter.class);

    /**
     * Retry period for cell communication failures.
     */
    private final static long RETRY_PERIOD = 30000; // 30 seconds

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

        String async = _args.getOpt("callback");
        if (async == null) {
            async = (String) _nucleus.getDomainContext("callback");
        }
        if (async != null) {
            switch (async) {
            case "async":
                setAsyncCallback(true);
                _log.info("Callback set to async");
                break;
            case "sync":
                setAsyncCallback(false);
                _log.info("Callback set to sync");
                break;
            default:
                _log.warn("Illegal value for 'callback' option : " + async);
                break;
            }
        }
        if (_args.hasOption("replyObject") && _args.getOpt("replyObject").equals("false")) {
            setCommandExceptionEnabled(false);
        }

        addCommandListener(new FilterShell(_nucleus.getLoggingThresholds()));

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

    public void setAsyncCallback(boolean async) {
        _nucleus.setAsyncCallback(async);
    }

    public final static String hh_exec_context = "<var> [<arg> ...]";
    public final static String fh_exec_context =
        "Executes the batch script in the context variable.";
    public String ac_exec_context_$_1_99(Args args)
        throws IOException, CommandExitException
    {
        StringWriter out = new StringWriter();
        String var = args.argv(0);
        try (Reader in = _nucleus.getDomainContextReader(var)) {
            args.shift();
            CellShell shell = new CellShell(this);
            shell.execute("context:" + var, in, out, out, args);
        }

        return out.toString();
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
        throws SerializationException,
               NoRouteToCellException    {
        _nucleus.sendMessage(msg);
    }
    /**
     *  sends a <code>CellMessage</code> along the specified path.
     *  Two additional boolean arguments allow to specify whether
     *  the message should only be delivered locally, remotely or
     *  both. The callback arguments (which has to be non-null
     *  allows to specify a Class which is informed as soon as
     *  an answer arrived or if the timeout has expired.
     *
     * @param msg the message to be sent.
     * @param locally if set to 'false' the message is not delivered
     *                locally.
     * @param remotely if set to 'false' the message is not delivered
     *                 remotely.
     * @param callback specifies a class which will be informed as
     *                 soon as the message arrives.
     * @param timeout  is the timeout interval in msec.
     *
     * @exception SerializationException if the payload object of this
     *            message is not Serializable.
     *
     */
    public void sendMessage(CellMessage msg,
                            boolean locally,
                            boolean remotely,
                            CellMessageAnswerable callback,
                            long    timeout)
        throws SerializationException {
        _nucleus.sendMessage(msg, locally, remotely, callback, timeout);
    }
    @Override
    public void sendMessage(CellMessage msg,
                            CellMessageAnswerable callback,
                            long    timeout)
        throws SerializationException {
        _nucleus.sendMessage(msg, true, true, callback, timeout);
    }
    /**
     *  sends a <code>CellMessage</code> along the specified path.
     *  Two additional boolean arguments allow to specify whether
     *  the message should only be delivered locally, remotely or
     *  both.
     *
     * @param msg the message to be sent.
     * @param locally if set to 'false' the message is not delivered
     *                locally.
     * @param remotely if set to 'false' the message is not delivered
     *                 remotely.
     * @exception SerializationException if the payload object of this
     *            message is not Serializable.
     * @exception NoRouteToCellException if the destination <code>CellPath</code>
     *            couldn't be reached.
     *
     */
    public void sendMessage(CellMessage msg, boolean locally,
                            boolean remotely)
        throws SerializationException,
               NoRouteToCellException    {
        _nucleus.sendMessage(msg, locally, remotely);
    }
    /**
     *  sends a <code>CellMessage</code> along the specified path,
     *  and waits <code>millisecs</code> for an answer to arrive.
     *  The answer will bypass the ordinary queuing mechanism and
     *  will be delivered before any other asynchronous message.
     *  The answer need to have the getLastUOID set to the
     *  UOID of the message send with sendAndWait. If the answer
     *  doesn't arrive withing the specified time intervall,
     *  the method returns 'null' and the answer will be handled
     *  as if it was an ordinary asynchronous message.
     *
     * @param msg the message to be sent.
     * @param local if 'false' the destination is not looked up locally.
     * @param remote if 'false' the destination is not looked up remotely.
     * @param millisecs milliseconds to wait for an answer.
     * @return the answer CellMessage or 'null' if intervall timed out.
     * @exception SerializationException if the payload object of this
     *            message is not Serializable.
     * @exception NoRouteToCellException if the destination <code>CellPath</code>
     *            couldn't be reached.
     *
     */
    public CellMessage sendAndWait(CellMessage msg,
                                   boolean local,
                                   boolean remote,
                                   long millisecs)
        throws SerializationException,
               NoRouteToCellException,
               InterruptedException        {
        return _nucleus.sendAndWait(msg, local, remote, millisecs);
    }
    /**
     *
     * convenience method : identical to <br>
     *  sendAndWait(msg, millisecs, true, true);
     *
     * @param msg the message to be sent.
     * @param millisecs milliseconds to wait.
     * @return the answer CellMessage or 'null' if intervall timed out.
     * @exception SerializationException if the payload object of this
     *            message is not Serializable.
     * @exception NoRouteToCellException if the destination <code>CellPath</code>
     *            couldn't be reached.
     *
     * @see CellNucleus#sendAndWait(CellMessage,long,boolean,boolean)
     */
    @Override
    public CellMessage sendAndWait(CellMessage msg,
                                   long millisecs)
        throws SerializationException,
               NoRouteToCellException,
               InterruptedException        {
        return _nucleus.sendAndWait(msg, true, true, millisecs);
    }

    private long timeUntil(long time)
    {
        return time - System.currentTimeMillis();
    }

    /**
     * @see CellEndpoint.sendAndWaitToPermanent
     */
    @Override
    public CellMessage sendAndWaitToPermanent(CellMessage envelope,
                                              long timeout)
        throws SerializationException,
               InterruptedException
    {
        long deadline = System.currentTimeMillis() + timeout;
        try {
            return sendAndWait(envelope, timeUntil(deadline));
        } catch (NoRouteToCellException e) {
            _log.warn("{}", e.toString());

            while (timeUntil(deadline) > RETRY_PERIOD) {
                if (_shutdownGate.await(RETRY_PERIOD)) {
                    break;
                }
                try {
                    return sendAndWait(envelope, timeUntil(deadline));
                } catch (NoRouteToCellException ignored) {
                }
            }
            return null;
        }
    }



    /**
     *  sends a <code>CellMessage</code> along the specified path.
     *  <strong>resendMessage does not resolve the local cell
     *  Namespace, only the routes are inspected.</strong>
     *
     *
     * @param msg the message to be sent.
     * @exception SerializationException if the payload object of this
     *            message is not Serializable.
     * @exception NoRouteToCellException if the destination <code>CellPath</code>
     *            couldn't be reached.
     *
     */
    public void resendMessage(CellMessage msg)
        throws SerializationException,
               NoRouteToCellException    {
        _nucleus.resendMessage(msg);
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
     * @param printWrite the printWrite which has to be used to
     *                   write the information to.
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
     * @see CellAdapter#commandArrived(CellMessage)
     *
     */
    public void messageArrived(CellMessage msg) {
        _log.info(" CellMessage From   : "+msg.getSourcePath());
        _log.info(" CellMessage To     : "+msg.getDestinationPath());
        _log.info(" CellMessage Object : "+msg.getMessageObject());

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
            _nucleus.sendMessage(msg);
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
    //
    // methods which are automatically scanned by
    // the CommandInterpreterFacility
    //
    public String ac_say_$_1(Args args) {
        _log.info(args.argv(0));
        return "";
    }
    public Object ac_xgetcellinfo(Args args) {
        return getCellInfo();
    }
    public static final String hh_info = "[-l|-a]";
    public String ac_info(Args args)
    {
        boolean full = args.hasOption("a");
        boolean lng  = full || args.hasOption("l");
        if (lng) {
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
    public static final String hh_show_pinboard =
        "[<lines>] # dumps the last <lines> to the terminal";
    public String ac_show_pinboard_$_0_1(Args args)
    {
        Pinboard pinboard = _nucleus.getPinboard();
        if (pinboard == null) {
            return "No Pinboard defined";
        }
        StringBuffer sb = new StringBuffer();
        if (args.argc() > 0) {
            pinboard.dump(sb, Integer.parseInt(args.argv(0)));
        } else {
            pinboard.dump(sb, 20);
        }

        return sb.toString();
    }

    public static final String hh_dump_pinboard =
        "<filename> # dumps the full pinboard to <filename>";
    public String ac_dump_pinboard_$_1(Args args)
    {
        Pinboard pinboard = _nucleus.getPinboard();
        if (pinboard == null) {
            return "No Pinboard defined";
        }

        try {
            pinboard.dump(new File(args.argv(0)));
        } catch (IOException e) {
            return "Dump Failed : "+e;
        }
        return "Pinboard dumped to "+args.argv(0);
    }

    /**
     *   belongs to the Cell Interface.
     *   If this method is overwritten, the 'cleanUp'
     *   method won't becalled.
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
                if (_useInterpreter && (! msg.isReply()) &&
                    ((obj instanceof String) ||
                     (obj instanceof AuthorizedString) ||
                     (obj instanceof CommandRequestable))) {

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
                            _nucleus.sendMessage(msg);
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
                        _nucleus.sendMessage(msg);
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
                    _nucleus.sendMessage(msg);
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
                return command(new AuthorizedArgs((Authorizable)command));
            } else {
                return autoCommand(command);
            }

        } else if (command instanceof String) {

            if (_returnCommandException) {
                return command(new Args((String)command));
            } else {
                return autoCommand(command);
            }

        } else if (command instanceof CommandRequestable) {
            return command((CommandRequestable)command);
        } else {
            throw new
                    CommandPanicException("Illegal CommandClass detected",
                    new Exception("PANIC"));
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
    private CellPath _aclPath    = new CellPath("acm");
    private long     _aclTimeout = 10000L;
    @Override
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
            reply = _nucleus.sendAndWait(
                                         new CellMessage(_aclPath, request),
                                         _aclTimeout);

            if (reply == null) {
                throw new
                        CommandException("Error in acl handling : Acl Request timed out (" + _aclPath + ")");
            }

        } catch (NoRouteToCellException | SerializationException | InterruptedException ee) {
            throw new
                CommandException("Error in acl handling : "+ee.getMessage());
        }

        Object r = reply.getMessageObject();
        if ((r == null) ||
            (! (r instanceof Object [])) ||
            (((Object [])r).length < 6) ||
            (! (((Object [])r)[5] instanceof Boolean))) {
            throw new
                    CommandException("Error in acl handling : illegal reply arrived");
        }

        if (! ((Boolean) ((Object[]) r)[5])) {
            throw new
                    CommandAclException(user, acl);
        }

    }
}

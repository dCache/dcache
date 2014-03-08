package dmg.cells.services.login;

import jline.ANSIBuffer;
import jline.Completor;
import jline.ConsoleReader;
import jline.History;
import jline.UnixTerminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import dmg.cells.applets.login.DomainObjectFrame;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.CommandExitException;
import dmg.util.CommandSyntaxException;
import dmg.util.StreamEngine;

import org.dcache.auth.Subjects;
import org.dcache.util.Args;

import static com.google.common.base.Objects.firstNonNull;

public class StreamObjectCell
    extends CellAdapter
    implements Runnable
{
    private static final Logger _log = LoggerFactory.getLogger(StreamObjectCell.class);

    private static final int HISTORY_SIZE = 50;
    private static final String CONTROL_C_ANSWER =
        "Got interrupt. Please issue \'logoff\' from within the admin cell to end this session.\n";

    private static final Class<?>[][] CONST_SIGNATURE = {
        { String.class, CellEndpoint.class, Args.class },
        { String.class, CellNucleus.class, Args.class },
        { CellNucleus.class, Args.class },
        { CellNucleus.class },
        { Args.class },
        {}
    };
    private static final Class<?>[][] COM_SIGNATURE = {
        { Object.class },
        { String.class },
        { String.class, Object.class  },
        { String.class, String.class  }
    };

    private StreamEngine _engine;
    private Subject _subject;
    private Thread _workerThread;
    private CellNucleus _nucleus;
    private File _historyFile;
    private boolean _useColors;

    private Object _commandObject;
    private Method[] _commandMethod = new Method[COM_SIGNATURE.length];
    private Method _promptMethod;
    private Method _helloMethod;

    public StreamObjectCell(String name, StreamEngine engine, Args args)
        throws Exception
    {
        super(name, args, false);

        _engine = engine;
        _nucleus = getNucleus();
        setCommandExceptionEnabled(true);
        try {
            if (args.argc() < 1) {
                throw new
                        IllegalArgumentException("Usage : ... <commandClassName>");
            }

            tryToSetHistoryFile( args.getOpt("history"));

            _useColors =
                Boolean.valueOf(args.getOpt("useColors")) &&
                _engine.getTerminalType() != null;

            _log.info("StreamObjectCell " + getCellName() + "; arg0=" + args.argv(0));

            _subject = engine.getSubject();

            prepareClass(args.argv(0));
        } catch (Exception e) {
            start();
            kill();
            throw e;
        }
        useInterpreter(false);
        start();
        _workerThread = _nucleus.newThread(this, "Worker");
        _workerThread.start();
    }

    private void tryToSetHistoryFile( String filename) {
        if( filename == null || filename.isEmpty()) {
            return;
        }

        try {
            setHistoryFile(filename);
        } catch( IllegalArgumentException e) {
            _log.error( e.getMessage());
        }
    }

    private void setHistoryFile( String filename) {
        File file = new File(filename);
        try {
            if( file.createNewFile()) {
                _log.info( "History file " + file + " has been created.");
            } else {
                guardFileIsFile(file);
                guardFileIsWriteable(file);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("History file " + file + " does not exist and cannot be created.");
        }
        _historyFile = file;
    }

    private void guardFileIsFile(File file) {
        if( !file.isFile()) {
            throw new IllegalArgumentException( "History file " + file + " is not a simple file.");
        }
    }
    private void guardFileIsWriteable(File file) {
        if( !file.canWrite()) {
            throw new IllegalArgumentException( "History file " + file + " is not writeable.");
        }
    }

    private void prepareClass(String className)
        throws ClassNotFoundException,
               NoSuchMethodException,
               InstantiationException,
               IllegalAccessException,
               InvocationTargetException
    {
        int commandConstMode = -1;
        Constructor<?> commandConst = null;
        Class<?> commandClass = Class.forName(className);
        NoSuchMethodException nsme = null;

        _log.info("Using class : " + commandClass);
        for (int i = 0; i < CONST_SIGNATURE.length; i++) {
            nsme = null;
            Class<?>[] x = CONST_SIGNATURE[i];
            _log.info("Looking for constructor : " + i);
            for (int ix = 0; ix < x.length; ix++) {
                _log.info("   arg["+ix+"] "+x[ix]);
            }
            try {
                commandConst = commandClass.getConstructor(CONST_SIGNATURE[i]);
            } catch (NoSuchMethodException e) {
                _log.info("Constructor not found : " + CONST_SIGNATURE[i]);
                nsme = e;
                continue;
            }
            commandConstMode = i;
            break;
        }
        if (nsme != null) {
            throw nsme;
        }
        _log.info("Using constructor : " + commandConst);

        int validMethods = 0;
        for (int i= 0; i < COM_SIGNATURE.length; i++) {
            try {
                _commandMethod[i] = commandClass.getMethod("executeCommand",
                                                           COM_SIGNATURE[i]);
                validMethods ++;
            } catch (Exception e) {
                _commandMethod[i]= null;
                continue;
            }
            _log.info("Using method [" + i + "] " + _commandMethod[i]);
        }
        if (validMethods == 0) {
            throw new
                    IllegalArgumentException("no valid executeCommand found");
        }

        try {
            _promptMethod = commandClass.getMethod("getPrompt", new Class[0]);
        } catch (Exception e) {
            _promptMethod = null;
        }
        if (_promptMethod != null) {
            _log.info("Using promptMethod : " + _promptMethod);
        }
        try {
            _helloMethod = commandClass.getMethod("getHello", new Class[0]);
        }catch(Exception ee){
            _helloMethod = null;
        }
        if (_helloMethod != null) {
            _log.info( "Using helloMethod : " + _helloMethod);
        }

        Args extArgs = new Args(getArgs());
        Object [] args = null;
        extArgs.shift();
        switch (commandConstMode) {
        case 0:
            args = new Object[3];
            args[0] = firstNonNull(Subjects.getUserName(_subject), Subjects.UNKNOWN);
            args[1] = this;
            args[2] = extArgs;
            break;
        case 1:
            args = new Object[3];
            args[0] = firstNonNull(Subjects.getUserName(_subject), Subjects.UNKNOWN);
            args[1] = getNucleus();
            args[2] = extArgs;
            break;
        case 2:
            args = new Object[2];
            args[0] = getNucleus();
            args[1] = extArgs;
            break;
        case 3:
            args = new Object[1];
            args[0] = getNucleus();
            break;
        case 4:
            args = new Object[1];
            args[0] = extArgs;
            break;
        case 5:
            args = new Object[0];
            break;

        }
        _commandObject = commandConst.newInstance(args);
    }

    private String getPrompt()
    {
        if (_promptMethod == null) {
            return "";
        }
        try {
            String s =
                (String) _promptMethod.invoke(_commandObject);
            return (s == null) ? "" : s;
        } catch (Exception e) {
            return "";
        }
    }

    private String getHello()
    {
        if (_helloMethod == null) {
            return null;
        }
        try {
            String s =
                (String) _helloMethod.invoke(_commandObject);
            return (s == null) ? "" : s;
        } catch (Exception e) {
            return "";
        }
    }

    @Override
    public void run()
    {
        try {
            History history;
            if (_historyFile != null) {
                history  = new History(_historyFile);
            } else {
                history = new History();
            }
            try {
                final ConsoleReader console =
                    new ConsoleReader(_engine.getInputStream(),
                                      _engine.getWriter(),
                                      null,
                                      new StreamObjectCellTerminal());
                history.setMaxSize(HISTORY_SIZE);
                console.setHistory(history);
                console.setUseHistory(true);
                if (_commandObject instanceof Completor) {
                    console.addCompletor((Completor) _commandObject);
                }
                console.addTriggeredAction(ConsoleReader.CTRL_C, new ActionListener()
                    {
                        @Override
                        public void actionPerformed(ActionEvent event)
                        {
                            try {
                                console.printString(CONTROL_C_ANSWER);
                                console.flushConsole();
                            } catch (IOException e) {
                                _log.warn("I/O failure: " + e);
                            }
                        }
                    });


                String hello = getHello();
                if (hello != null) {
                    console.printString(hello);
                    console.flushConsole();
                }

                runAsciiMode(console);

                console.flushConsole();
            } finally {
                /* ConsoleReader doesn't close the history file itself.
                 */
                PrintWriter out = history.getOutput();
                if (out != null) {
                    out.close();
                }
            }
        } catch (IllegalAccessException e) {
            _log.error("Failed to execute command: " + e);
        } catch (ClassNotFoundException e) {
            _log.warn("Binary mode failure: " + e);
        } catch (IOException e) {
            _log.warn("I/O Failure: " + e);
        } finally {
            _log.debug("worker done, killing off cell");
            kill();
        }
    }

    private class BinaryExec implements Runnable
    {
        private final ObjectOutputStream _out;
        private final DomainObjectFrame _frame;
        private final Thread _parent;

        BinaryExec(ObjectOutputStream out,
                   DomainObjectFrame frame, Thread parent)
        {
            _out = out;
            _frame  = frame;
            _parent = parent;
            _nucleus.newThread(this).start();
        }

        @Override
        public void run()
        {
            Object result;
            boolean done = false;
            _log.info("Frame id "+_frame.getId()+" arrived");
            try {
                if (_frame.getDestination() == null) {
                    Object [] array  = new Object[1];
                    array[0] = _frame.getPayload();
                    if (_commandMethod[0] != null) {
                        _log.info("Choosing executeCommand(Object)");
                        result = _commandMethod[0].invoke(_commandObject, array);
                    } else if(_commandMethod[1] != null) {
                        _log.info("Choosing executeCommand(String)");
                        array[0] = array[0].toString();
                        result = _commandMethod[1].invoke(_commandObject, array);

                    } else {
                        throw new
                            Exception("PANIC : not found : executeCommand(String or Object)");
                    }
                } else {
                    Object [] array  = new Object[2];
                    array[0] = _frame.getDestination();
                    array[1] = _frame.getPayload();
                    if (_commandMethod[2] != null) {
                        _log.info("Choosing executeCommand(String destination, Object)");
                        result = _commandMethod[2].invoke(_commandObject, array);

                    } else if (_commandMethod[3] != null) {
                        _log.info("Choosing executeCommand(String destination, String)");
                        array[1] = array[1].toString();
                        result = _commandMethod[3].invoke(_commandObject, array);
                    } else {
                        throw new
                            Exception("PANIC : not found : "+
                                       "executeCommand(String/String or Object/String)");
                    }
                }
            } catch (InvocationTargetException ite) {
                result = ite.getTargetException();
                done = result instanceof CommandExitException;
            } catch (Exception ae) {
                result = ae;
            }
            _frame.setPayload(result);
            try {
                synchronized(_out){
                    _out.writeObject(_frame);
                    _out.flush();
                    _out.reset();  // prevents memory leaks...
                }
            } catch (IOException e) {
                _log.error("Problem sending result : " + e);
            }
            if (done) {
                _parent.interrupt();
            }
        }
    }

    private void runBinaryMode()
        throws IOException, ClassNotFoundException
    {
        ObjectOutputStream out =
            new ObjectOutputStream(_engine.getOutputStream());
        ObjectInputStream in =
            new ObjectInputStream(_engine.getInputStream());
        Object obj;
        while ((obj = in.readObject()) != null) {
            if (obj instanceof DomainObjectFrame) {
                new BinaryExec(out, (DomainObjectFrame)obj, Thread.currentThread());
            } else {
                _log.error("Won't accept non DomainObjectFrame : " + obj.getClass());
            }
        }
    }

    private void runAsciiMode(ConsoleReader console)
        throws IOException, ClassNotFoundException, IllegalAccessException
    {
        Method com =
            (_commandMethod[1] != null) ? _commandMethod[1] : _commandMethod[0];

        boolean done = false;
        while (!done) {
            String prompt =
                new ANSIBuffer().green(getPrompt()).toString(_useColors);
            String str = console.readLine(prompt);
            if (str == null) {
                _log.debug( "\"null\" input (e.g., Ctrl-D) received.");
                break;
            }

            _log.debug( "received line: {}", str);

            if (str.equals("$BINARY$")) {
                _log.info("Opening Object Streams");
                console.printString(str);
                console.printNewline();
                console.flushConsole();
                runBinaryMode();
                break;
            }

            Object result;
            try {
                result = com.invoke(_commandObject, str);
            } catch (InvocationTargetException ite) {
                result = ite.getTargetException();
                if(result instanceof CommandExitException) {
                    _log.debug( "User requested to logout.");
                    done = true;
                }
            }
            if (result != null) {
                String s;
                if (result instanceof CommandSyntaxException) {
                    CommandSyntaxException e = (CommandSyntaxException) result;
                    ANSIBuffer sb = new ANSIBuffer();
                    sb.red("Syntax error: " + e.getMessage() + "\n");
                    String help  = e.getHelpText();
                    if (help != null) {
                        sb.cyan("Help : \n");
                        sb.cyan(help);
                    }
                    s = sb.toString(_useColors);
                } else {
                    s = result.toString();
                }
                if (!s.isEmpty()){
                    console.printString(s);
                    if (s.charAt(s.length() - 1) != '\n') {
                        console.printNewline();
                    }
                    console.flushConsole();
                }
            }
        }
    }

    @Override
    public void cleanUp()
    {
        try {
            _log.debug("Shutting down the SSH connection");
            _engine.getInputStream().close();
        } catch (IOException e) {
            _log.error("Failed to close socket: " + e);
        }
        if (_workerThread != null) {
            _workerThread.interrupt();
        }
    }

    private class StreamObjectCellTerminal extends UnixTerminal
    {
        private final static int DEFAULT_WIDTH = 80;
        private final static int DEFAULT_HEIGHT = 24;

        private boolean _swapNext;

        @Override
        public void initializeTerminal()
            throws IOException, InterruptedException
        {
            /* UnixTerminal expects a tty to have been allocated. That
             * is not the case for StreamObjectCell and hence we skip
             * the usual initialization.
             */
        }

        @Override
        public int readCharacter(InputStream in) throws IOException
        {
            int c = super.readCharacter(in);
            if (_swapNext) {
                /* UnixTerminal has built in support for reversing
                 * backspace and delete. The field that controls the
                 * behaviour is however private and hence we have to
                 * make this hack to translate DEL to BS.
                 *
                 * The background for the reversal is that at least on
                 * Linux backspace sends a DEL character.
                 */
                if (c == DELETE) {
                    c = BACKSPACE;
                }
                _swapNext = false;
            }
            return c;
        }

        @Override
        public int readVirtualKey(InputStream in) throws IOException
        {
            _swapNext = true;
            return super.readVirtualKey(in);
        }

        @Override
        public int getTerminalWidth()
        {
            int width = _engine.getTerminalWidth();
            return (width == 0) ? DEFAULT_WIDTH : width;
        }

        @Override
        public int getTerminalHeight()
        {
            int height = _engine.getTerminalHeight();
            return (height == 0) ? DEFAULT_HEIGHT : height;
        }
    }
}

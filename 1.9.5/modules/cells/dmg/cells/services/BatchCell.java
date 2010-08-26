package dmg.cells.services;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellShell;
import dmg.util.Args;
import dmg.util.Log4jWriter;
import dmg.util.CommandExitException;
import dmg.util.Exceptions;

import java.io.Reader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class BatchCell extends CellAdapter implements Runnable
{
    private final static Logger _log = Logger.getLogger(BatchCell.class);

    private final Reader _in;
    private final String _source;
    private CellShell _shell;

    public BatchCell(String name, String [] argStrings)
        throws Exception
    {
        super(name, "", true);

        try {
            useInterpreter(false);
            StringBuilder input = new StringBuilder();
            for (String s: argStrings) {
                input.append(s).append('\n');
            }

            _in = new StringReader(input.toString());
            _source = name;
            Thread worker = new Thread(this);
            worker.start();
        } catch (Exception e) {
            kill();
            throw e;
        }
    }

    public BatchCell(String name, String argString)
        throws Exception
    {
        super(name, argString, true);

        try {
            Args args = getArgs();
            if (args.argc() < 1)
                throw new IllegalArgumentException("Usage : ... <batchFilename>");
            _source = args.argv(0);
            if (args.getOpt("jar") != null) {
                InputStream input =
                    ClassLoader.getSystemResourceAsStream(_source);
                if (input == null)
                    throw new IllegalArgumentException("Resource not found : " + _source);
                _in = new InputStreamReader(input);
            } else {
                _in = new FileReader(_source);
            }
            Thread worker = new Thread(this);
            worker.start();
        } catch (Exception e) {
            kill();
            throw e;
        }
    }

    public void run()
    {
        try {
            initLoggingContext();
            _shell = new CellShell(getNucleus());
            _shell.execute(_source,
                           _in,
                           new Log4jWriter(_log, Level.INFO),
                           new Log4jWriter(_log, Level.ERROR),
                           new Args(""));
        } catch (CommandExitException e) {
            int rc = e.getErrorCode();
            if (rc == 666) {
                _log.fatal(Exceptions.getMessageWithCauses(e));
                System.exit(6);
            } else {
                _log.error(Exceptions.getMessageWithCauses(e));
            }
        } catch (IOException e) {
            _log.error("I/O error: " + e.getMessage());
        } finally {
            try {
                _in.close();
            } catch (IOException e) {
            }
            kill();
        }
    }
}

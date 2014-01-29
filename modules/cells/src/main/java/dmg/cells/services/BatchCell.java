package dmg.cells.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellShell;
import dmg.util.CommandExitException;
import dmg.util.Exceptions;

import org.dcache.util.Args;

public class BatchCell extends CellAdapter implements Runnable
{
    private final static Logger _log = LoggerFactory.getLogger(BatchCell.class);

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
            if (args.argc() < 1) {
                throw new IllegalArgumentException("Usage : ... <batchFilename>");
            }
            _source = args.argv(0);
            if (args.hasOption("jar")) {
                InputStream input =
                    ClassLoader.getSystemResourceAsStream(_source);
                if (input == null) {
                    throw new IllegalArgumentException("Resource not found : " + _source);
                }
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

    @Override
    public void run()
    {
        try {
            initLoggingContext();
            CellNucleus nucleus = getNucleus();
            _shell = new CellShell(nucleus);
            _shell.execute(_source,
                           _in,
                           new Args(""));
        } catch (CommandExitException e) {
            _log.error(Exceptions.getMessageWithCauses(e));
            if (e.getErrorCode() == 666) {
                System.exit(6);
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

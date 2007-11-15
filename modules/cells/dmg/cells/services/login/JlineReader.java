package dmg.cells.services.login;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;

import jline.ConsoleReader;
import jline.History;

public class JlineReader implements InputHandler {


    private ConsoleReader _reader;
    private String _historyFileName = "/var/log/.adminshell_history";
    private boolean _append;

    public JlineReader(ConsoleReader reader, boolean append) {
        _reader = reader;
        _append = append;
    }

    public void close() throws IOException {


        /*
         * use history file if it exist and we can write into it
         *  or
         * do not exist, but we are allowed to create it
         */
        File historyFile = new File(_historyFileName);
        if( (historyFile.exists() && historyFile.canWrite()) ||
                ( !historyFile.exists() && historyFile.getParentFile().canWrite() )) {

            PrintWriter updatedHistoryFile = null;
            LineNumberReader outdatedHistoryFile = null;
            try {

                if (_append) {

                    //
                    // append session history to stored history file
                    //

                    // initialise a temp history for merging purposes
                    History merged = new History();
                    // same size as the history of this ssh-session
                    merged.setMaxSize(_reader.getHistory().getMaxSize());

                    // try to load an existing history from a file
                    try {
                        outdatedHistoryFile = new LineNumberReader(new FileReader(
                                _historyFileName));
                        String entry;
                        while ((entry = outdatedHistoryFile.readLine()) != null) {
                            merged.addToHistory(entry);
                        }
                        outdatedHistoryFile.close();
                    } catch (IOException e) {
                        // ok, no pre-existing history
                    }

                    // the merging: append the content of our ssh-session
                    for (Object entry : _reader.getHistory().getHistoryList()) {
                        merged.addToHistory((String) entry);
                    }

                    // save merged history
                    updatedHistoryFile = new PrintWriter(_historyFileName);
                    for (Object entry : merged.getHistoryList()) {
                        updatedHistoryFile.print((String) entry);
                        updatedHistoryFile.println();
                    }

                } else {

                    //
                    // overwrite history file with current history
                    //

                    updatedHistoryFile = new PrintWriter(_historyFileName);
                    for (Object entry : _reader.getHistory().getHistoryList()) {
                        updatedHistoryFile.print((String) entry);
                        updatedHistoryFile.println();
                    }

                }

            } finally {
                if (outdatedHistoryFile != null) {
                    outdatedHistoryFile.close();
                }
                if (updatedHistoryFile != null) {
                    updatedHistoryFile.close();
                }
            }

        }
    }

    public String readLine() throws IOException {
        return _reader.readLine();
    }

    public void setHistoryFile(String filename) {
        _historyFileName = filename;
    }

}

package org.dcache.acl.util.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class FileTools {
    private static final Logger logger = Logger.getLogger("logger.org.dcache.authorization." + FileTools.class.getName());

    public static int FILTER_FILES = 1;

    public static int FILTER_FOLDERS = 2;

    public static int FILTER_ALL = 3;

    private static FileTools _SINGLETON;
    static {
        _SINGLETON = new FileTools();
    }

    private FileTools() {
    }

    public static FileTools getInstance() {
        return _SINGLETON;
    }

    // Arguments
    // f: root folder location
    // nMode: 0, 1, 2, 3 (0 - nothing, 1 - only files, 2 - only folders, 3 -
    // both files and folders)
    // bSubFolders: 0, 1 (0/1 - with/without subfolders)
    // bFirst: 0, 1 (0/1 - inludes root folder in list)
    // filter: filtering files/folders
    public static void getFileSubList(File f, List<String> list, int nMode, boolean bSubFolders, boolean bFirst, FilenameFilter filter) {
        try {
            boolean bFiles = ((nMode % 2) == 1);
            boolean bFolders = ((nMode / 2) == 1);

            if ( f.isDirectory() ) {
                if ( !bFirst && bFolders )
                    list.add(f.getPath());

                if ( bFirst || bSubFolders ) {
                    String entries[];
                    if ( filter == null || bFiles )
                        entries = f.list();
                    else
                        entries = f.list(filter);

                    int maxlen = (entries == null ? 0 : entries.length);
                    for (int i = 0; i < maxlen; i++)
                        getFileSubList(new File(f, entries[i]), list, nMode, bSubFolders, false, filter);
                }

            } else if ( f.isFile() && bFiles )
                list.add(f.getPath());

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * @param aFile
     * @param dir
     *            True if directory
     * @param exist
     *            True if exist
     * @param write
     *            True if write
     * @return boolean
     * @throws NullPointerException
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static boolean checkFile(File aFile, boolean dir, boolean exist, boolean write) throws NullPointerException, FileNotFoundException, IOException {
        final String name = (dir ? "Directory" : "File");

        if ( aFile == null )
            throw new NullPointerException(name + " should not be null.");

        boolean fileExists = aFile.exists();
        if ( exist && fileExists == false )
            throw new FileNotFoundException(name + " does not exist: " + aFile);

        if ( fileExists ) {
            boolean fileIsDirectory = aFile.isDirectory();
            if ( dir == false && fileIsDirectory )
                throw new IOException("Should not be a directory: " + aFile);

            if ( dir && fileIsDirectory == false )
                throw new IOException("Should not be a file: " + aFile);

            if ( aFile.canRead() == false )
                throw new IOException(name + " cannot be read: " + aFile);

            if ( write && aFile.canWrite() == false )
                throw new IOException(name + " cannot be written: " + aFile);
        }
        return true;
    }

    /**
     * Fetch the entire contents of a text file, and return it in a String. This
     * style of implementation does not throw Exceptions to the caller.
     *
     * @param aFile
     *            is a file which already exists and can be read.
     * @throws IOException
     *             if problem encountered during read.
     */
    public static String getContents(File aFile) throws IOException {
        StringBuilder contents = new StringBuilder();

        // declared here only to make visible to finally clause
        BufferedReader input = null;
        try {
            // use buffering, this implementation reads one line at a time
            input = new BufferedReader(new FileReader(aFile));
            String line; // not declared within while loop
            while ((line = input.readLine()) != null)
                contents.append(line).append("\n");
            return contents.toString();

        } finally {
            // flush and close both "input" and its underlying FileReader
            if ( input != null )
                input.close();
        }
    }

    /**
     * Change the contents of text file in its entirety, appending any existing
     * text. This style of implementation throws all exceptions to the caller.
     *
     * @param aFile
     *            is an existing file which can be written to.
     * @throws IOException
     *             if problem encountered during write.
     */
    public static void setContents(File aFile, String aContents) throws IOException {
        logger.debug("Store contents to file " + aFile);

        // declared here only to make visible to finally clause; generic
        // reference
        BufferedWriter output = null;
        try {
            // use buffering
            output = new BufferedWriter(new FileWriter(aFile));
            output.write(aContents);

        } finally {
            // flush and close both "output" and its underlying FileWriter
            if ( output != null )
                output.close();
        }
    }

    /**
     * @param aFile
     * @throws NullPointerException
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void checkReadFile(final File aFile) throws NullPointerException, FileNotFoundException, IOException {
        if ( aFile == null )
            throw new NullPointerException("File should not be NULL.");

        if ( aFile.exists() == false )
            throw new FileNotFoundException("File does not exist: " + aFile);

        if ( aFile.isDirectory() )
            throw new IOException("Should not be a directory: " + aFile);

        if ( aFile.canRead() == false )
            throw new IOException("File cannot be read: " + aFile);
    }

}
package org.dcache.util;

import java.io.File;
import java.io.IOException;

/**
 * A collection of utility methods for handling files and directories in
 * the host computer's filesystem.
 */
public class Files
{
    /**
     * Check that the supplied path exists on the local filesystem, that it
     * is a file and that the JVM process has permission to read the file.
     * @param path the file to check
     * @throws IOException if there is a problem with the file
     */
    public static void checkFile(String path) throws IOException
    {
        checkFile(new File(path));
    }


    /**
     * Check that the supplied path exists on the local filesystem, that it
     * is a file and that the JVM process has permission to read the file.
     * @param file the file to check
     * @throws IOException if there is a problem with the file
     */
    public static void checkFile(File file) throws IOException
    {
        if(!file.exists()) {
            throw new IOException("file not found: " + file);
        }

        if(!file.isFile()) {
            throw new IOException("not a file: " + file);
        }

        if(!file.canRead()) {
            throw new IOException("permission denied: " + file);
        }
    }


    /**
     * Check that the supplied path exists on the local filesystem, that it
     * is a directory and that the JVM process has permission to read
     * the contents of this directory.
     * @param path the directory to check
     * @throws IOException if there is a problem with the file
     */
    public static void checkDirectory(String path) throws IOException
    {
        checkDirectory(new File(path));
    }


    /**
     * Check that the supplied path exists on the local filesystem, that it
     * is a directory and that the JVM process has permission to read
     * the contents of this directory.
     * @param file the directory to check
     * @throws IOException if there is a problem with the file
     */
    public static void checkDirectory(File directory) throws IOException
    {
        if(!directory.exists()) {
            throw new IOException("directory not found: " + directory);
        }

        if(!directory.isDirectory()) {
            throw new IOException("not a directory: " + directory);
        }

        if(directory.list() == null) {
            throw new IOException("permission denied: " + directory);
        }
    }
}

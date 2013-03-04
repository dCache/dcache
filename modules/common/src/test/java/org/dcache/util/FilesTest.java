package org.dcache.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.fail;


/**
 *  Verify correct behaviour of files class.
 */
public class FilesTest
{
    File _directory;
    File _test;

    @Before
    public void setup() throws IOException
    {
        _directory = File.createTempFile("test-FilesTest-", "-dir");
        _directory.delete();
        _directory.mkdir();
    }

    @After
    public void teardown()
    {
        deleteEverythingIn(_directory);
        _directory.delete();
    }

    private void deleteEverythingIn(File dir)
    {
        for(File file : dir.listFiles()) {
            if(file.isDirectory()) {
                deleteEverythingIn(file);
            }

            file.delete();
        }
    }

    @Test
    public void checkFileShouldNotThrowExceptionForFile() throws IOException
    {
        givenANormalFile();

        checkFile();
    }

    @Test(expected=IOException.class)
    public void checkFileShouldThrowExceptionForMissingFile()
            throws IOException
    {
        givenAnAbsentFile();

        checkFile();
    }

    @Test(expected=IOException.class)
    public void checkFileShouldThrowExceptionForDirectory() throws IOException
    {
        givenANormalDirectory();

        checkFile();
    }

    @Test
    public void checkDirectoryShouldNotThrowExceptionForDirectory()
            throws IOException
    {
        givenANormalDirectory();

        checkDirectory();
    }

    @Test(expected=IOException.class)
    public void checkDirectoryShouldThrowExceptionForMissingDirectory()
            throws IOException
    {
        givenAnAbsentFile();

        checkDirectory();
    }

    @Test(expected=IOException.class)
    public void checkDirectoryShouldThrowExceptionForFile() throws IOException
    {
        givenANormalFile();

        checkDirectory();
    }

    public void checkFile() throws IOException
    {
        Files.checkFile(_test);
    }

    public void checkDirectory() throws IOException
    {
        Files.checkDirectory(_test);
    }

    public void givenAnAbsentFile()
    {
        _test = new File(_directory, "missing");
    }

    public void givenANormalFile() throws IOException
    {
        _test = new File(_directory, "present");
        if(!_test.createNewFile()) {
            fail("Unable to create file " + _test);
        }
    }

    public void givenANormalDirectory()
    {
        _test = new File(_directory, "present");
        _test.mkdir();
    }

}

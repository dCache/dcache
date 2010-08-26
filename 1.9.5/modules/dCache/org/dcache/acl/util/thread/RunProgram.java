package org.dcache.acl.util.thread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;

import org.apache.log4j.Logger;

/**
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class RunProgram {
    static Logger logger = Logger.getLogger("logger.org.dcache.authorization." + RunProgram.class.getName());

    public static String LINE_SEPARATOR = System.getProperty("line.separator");

    private static RunProgram _SINGLETON;
    static {
        _SINGLETON = new RunProgram();
    }

    private RunProgram() {
    }

    public static RunProgram getInstance() {
        return _SINGLETON;
    }

    /**
     * Wrapper for Runtime.exec. Optionally wait for child to finish.
     *
     * @param command
     *            fully qualified *.exe or *.com command
     * @param wait
     *            true if you want to wait for the child to finish.
     * @return output string
     */
    public static String exec(String command, boolean wait) throws RuntimeException, java.io.IOException, InterruptedException {
        String response;
        Process process = null;
        try {
            logger.debug("Trying to execute command line: " + command);

            Runtime runtime = Runtime.getRuntime();
            process = runtime.exec(command);
            if ( wait )
                process.waitFor();

            int ret = process.exitValue();
            if ( ret != 0 )
                throw new RuntimeException("Not a normal termination. Exit value is: " + ret);

            InputStreamReader isr = new InputStreamReader(process.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            StringWriter sw = new StringWriter();
            String line;
            while ((line = br.readLine()) != null)
                sw.write(line + LINE_SEPARATOR);

            response = sw.getBuffer().toString();
            sw.close();

        } catch (RuntimeException e) {
            logger.error(e.getMessage());
            throw e;

        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage());
            throw e;

        } finally {
            if ( process != null ) {
                BufferedReader er = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                StringWriter swe = new StringWriter();
                String line;
                while ((line = er.readLine()) != null)
                    swe.write(line + LINE_SEPARATOR);

                logger.debug("Reading standard output ...");
                logger.error(swe.getBuffer().toString());

                process.getInputStream().close();
                process.getOutputStream().close();
                process.getErrorStream().close();
            }
        }
        return response;
    }

    /**
     * Wrapper for Runtime.exec. Wait for child to finish.
     *
     * @param command
     *            fully qualified *.exe/com command
     * @return Status code
     */
    public static int exec(String command) throws java.io.IOException, InterruptedException {
        int ret = -1;
        Process process = null;
        try {
            logger.debug("Trying to execute command line: " + command);

            Runtime runtime = Runtime.getRuntime();
            process = runtime.exec(command);
            process.waitFor();
            ret = process.exitValue();

        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage());
            throw e;

        } finally {
            if ( process != null ) {
                process.getInputStream().close();
                process.getOutputStream().close();
                process.getErrorStream().close();
            }
        }
        return ret;
    }

    public static int execute(String command, StringBuilder out, StringBuilder err) throws Exception {
        Process process = null;
        int result = -1;
        try {
            logger.debug("Executing command: " + command);

            Runtime runtime = Runtime.getRuntime();
            process = runtime.exec(command);

            // create thread for reading inputStream (process' stdout)
            StreamReaderThread outThread = new StreamReaderThread(process.getInputStream(), out);

            // create thread for reading errorStream (process' stderr)
            StreamReaderThread errThread = new StreamReaderThread(process.getErrorStream(), err);

            // start both threads
            outThread.start();
            errThread.start();

            // wait for process to end
            result = process.waitFor();

            // finish reading whatever's left in the buffers
            outThread.join();
            errThread.join();

            if ( logger.isDebugEnabled() ) {
                String outStr = out.toString();
                String errStr = err.toString();
                if ( outStr != null && outStr.length() != 0 )
                    logger.debug("Process output:\n" + outStr);
                if ( errStr != null && errStr.length() != 0 )
                    logger.debug("Process error:\n" + errStr);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error(e.getMessage());
            throw new Exception(e);

        } catch (Exception e) {
            logger.error("Error executing command: " + e.getMessage());
            throw new Exception(e);

        } finally {
            if ( process != null ) {
                process.getInputStream().close();
                process.getOutputStream().close();
                process.getErrorStream().close();
            }
        }
        return result;
    }
}

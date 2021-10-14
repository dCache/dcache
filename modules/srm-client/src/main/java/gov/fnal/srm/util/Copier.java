// $Id$

/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */


/*
 * Copier.java
 *
 * Created on January 28, 2003, 1:34 PM
 */

package gov.fnal.srm.util;

import static org.dcache.util.ByteUnit.KiB;
import static org.dcache.util.URIs.portWithDefault;

import eu.emi.security.authn.x509.CrlCheckingMode;
import eu.emi.security.authn.x509.NamespaceCheckingMode;
import eu.emi.security.authn.x509.OCSPCheckingMode;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.PEMCredential;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.dcache.ftp.client.exception.ServerException;
import org.dcache.ftp.client.exception.UnexpectedReplyCodeException;
import org.dcache.ftp.client.vanilla.Reply;
import org.dcache.srm.Logger;
import org.dcache.srm.util.GridftpClient;
import org.dcache.util.Exceptions;
import org.dcache.util.PortRange;


/**
 * @author timur
 */

public class Copier implements Runnable {

    private final Set<CopyJob> copy_jobs = new HashSet<>();
    private final String urlcopy;
    private final boolean debug;

    private boolean doneAddingJobs;
    private boolean stop;
    private Thread hook;
    private Configuration configuration;
    private boolean completed;
    private boolean completed_successfully = true;
    private Exception error;
    private Logger logger;
    private long retry_timeout;
    private int retry_num;
    private int num_jobs;
    private int num_completed_successfully;
    private boolean dryRun;

    public final void say(String msg) {
        if (logger != null) {
            logger.log(msg);
        }
    }

    public final void dsay(String msg) {
        if (logger != null) {
            logger.log(msg);
        }
    }

    //error say
    public final void esay(String err) {
        if (logger != null) {
            logger.elog(err);
        }
    }

    public final void esay(Throwable t) {
        if (logger != null) {
            logger.elog(t.toString());
        }
    }

    public Copier(Configuration configuration) {
        this.urlcopy = configuration.getUrlcopy();
        this.debug = configuration.isDebug();

        this.configuration = configuration;
        this.retry_num = configuration.getRetry_num();
        this.retry_timeout = configuration.getRetry_timeout();
        this.dryRun = configuration.isDryRun();

        logger = configuration.getLogger();
    }

    public void addCopyJob(CopyJob job) {

        synchronized (copy_jobs) {
            copy_jobs.add(job);
            num_jobs++;
        }
        synchronized (this) {
            notify();
        }
    }

    public void doneAddingJobs() {
        synchronized (copy_jobs) {
            doneAddingJobs = true;
        }
        synchronized (this) {
            notify();
        }
    }

    //this method will wait for all the individual file transfers to complete
    //when all transfers are complete, then it will notify all threads.

    public synchronized void waitCompletion() throws Exception {
        if (completed) {
            if (!completed_successfully) {
                throw error;
            }
            if (num_completed_successfully != num_jobs) {
                throw new Exception("number of jobs = " + num_jobs +
                      " successfully completed=" + num_completed_successfully);
            }
            return;
        }
        while (true) {
            try {
                wait();
            } catch (InterruptedException ie) {
                esay("waitCompletion is interrupted");
                notifyAll();
                throw ie;
            }
            if (completed) {
                if (!completed_successfully) {
                    throw error;
                }
                if (num_completed_successfully != num_jobs) {
                    throw new Exception("number of jobs = " + num_jobs +
                          " successfully completed=" + num_completed_successfully);
                }
                return;
            } else {
                notify();
            }
        }
    }

    public void stop() {
        synchronized (this) {
            stop = true;
            this.notifyAll();
        }

        while (true) {
            if (copy_jobs.isEmpty()) {
                return;
            }

            CopyJob nextJob = copy_jobs.iterator().next();

            copy_jobs.remove(nextJob);
            nextJob.done(false, "stopped");
        }
    }

    @Override
    public void run() {
        if (Thread.currentThread() == hook) {
            cleanup();
            return;
        }

        hook = new Thread(this);
        Runtime.getRuntime().addShutdownHook(hook);

        while (true) {
            synchronized (this) {
                if (stop) {
                    say("going to stop....");
                    completed = true;
                    completed_successfully = false;
                    error = new Exception(" stopped ");
                    notifyAll();
                    return;
                }
            }
            CopyJob nextJob = null;
            synchronized (copy_jobs) {
                if (copy_jobs.isEmpty()) {

                    say("copy_jobs is empty");
                } else {
                    say("copy_jobs is not empty");
                }

                if (doneAddingJobs && copy_jobs.isEmpty()) {
                    say("stopping copier");
                    Runtime.getRuntime().removeShutdownHook(hook);
                    break;
                }

                if (!copy_jobs.isEmpty()) {
                    nextJob = copy_jobs.iterator().next();
                }
            }

            if (nextJob != null) {
                boolean job_success = false;
                Exception job_error = null;
                try {
                    int i = 0;
                    while (true) {

                        try {
                            copy(nextJob);
                            job_success = true;
                            say("execution of " + nextJob + " completed");
                            break;
                        } catch (Exception e) {
                            esay("copy failed: " + Exceptions.messageOrClassName(e));
                            throwIfPermanent(e);
                            if (i < retry_num) {
                                i++;
                                esay(" try again");
                            } else {
                                throw e;
                            }
                        }
                        try {
                            esay("sleeping for " + (retry_timeout * i) + " before retrying");
                            Thread.sleep(retry_timeout * i);
                        } catch (InterruptedException ie) {
                        }

                    }
                } catch (Exception e) {
                    synchronized (this) {
                        job_error = e;
                        completed = true;
                        completed_successfully = false;
                        error = e;
                        notifyAll();
                        return;

                    }
                } finally {
                    try {
                        nextJob.done(job_success,
                              job_error == null ? null : job_error.getMessage());
                    } catch (Exception e) {
                        esay("setting File Request to \"Done\" failed");
                        esay(e);
                        if (!doneAddingJobs || !copy_jobs.isEmpty()) {
                            synchronized (this) {
                                completed = true;
                                completed_successfully = false;
                                error = e;
                                notifyAll();
                            }
                            return;
                        }
                        break;
                    }
                }

                synchronized (copy_jobs) {
                    copy_jobs.remove(nextJob);
                }

                continue;
            }

            synchronized (this) {
                try {
                    this.wait();
                } catch (InterruptedException ie) {
                    completed = true;
                    completed_successfully = false;
                    error = new Exception(" copier was interrupted ");
                    notifyAll();
                    esay(" copier was interrupted");
                    this.notify();
                }
            }
        }

        synchronized (this) {
            completed = true;
            notifyAll();
        }

    }

    private void throwIfPermanent(Exception e) throws Exception {
        if (e instanceof ServerException) {
            ServerException serverException = (ServerException) e;
            if (serverException.getRootCause() instanceof UnexpectedReplyCodeException) {
                UnexpectedReplyCodeException rootCause = (UnexpectedReplyCodeException) serverException.getRootCause();
                if (Reply.isPermanentNegativeCompletion(rootCause.getReply())) {
                    throw e;
                }
            }
        }
    }

    public void copy(CopyJob job) throws Exception {
        java.net.URI from = job.getSource();
        java.net.URI to = job.getDestination();
        int totype = SRMDispatcher.getUrlType(to);

        // handle directory
        if ((totype & SRMDispatcher.DIRECTORY_URL) == SRMDispatcher.DIRECTORY_URL) {
            String filename = from.getPath();
            int lastSlash = filename.lastIndexOf('/');
            if (lastSlash != -1) {
                filename = filename.substring(lastSlash);
            }
            to = to.resolve(filename);
        }

        dsay("copying " + job);
        if (from.getScheme().equals("dcap") ||
              to.getScheme().equals("dcap") ||
              configuration.isUse_urlcopy_script()) {
            try {
                say("trying script copy");
                String[] script_protocols;
                try {
                    script_protocols = scriptCopyGetSupportedProtocols();
                    for (String script_protocol : script_protocols) {
                        dsay(urlcopy + " supports " + script_protocol);
                    }
                } catch (Exception e) {
                    esay("could not get supported protocols ");
                    script_protocols = null;
                }
                boolean from_protocol_supported = false;
                boolean to_protocol_supported = false;
                if (script_protocols != null) {
                    for (String script_protocol : script_protocols) {
                        if (script_protocol.equals(from.getScheme())) {
                            from_protocol_supported = true;
                        }

                        if (script_protocol.equals(to.getScheme())) {
                            to_protocol_supported = true;
                        }

                        if (from_protocol_supported && to_protocol_supported) {
                            break;
                        }
                    }
                }

                if (from_protocol_supported && to_protocol_supported) {
                    scriptCopy(from, to);
                    return;
                }
            } catch (Exception e) {
                esay("script copy failed with " + e);
                esay("trying native java copy");
            }
        }

        if (((from.getScheme().equals("gsiftp") ||
              from.getScheme().equals("gridftp")) &&
              to.getScheme().equals("file")) ||
              (from.getScheme().equals("file") &&
                    (to.getScheme().equals("gsiftp") ||
                          to.getScheme().equals("gridftp")))
        ) {
            X509Credential credential;
            if (configuration.isUseproxy()) {
                credential = new PEMCredential(configuration.getX509_user_proxy(), (char[]) null);
            } else {
                credential = new PEMCredential(configuration.getX509_user_key(),
                      configuration.getX509_user_cert(), null);
            }
            javaGridFtpCopy(from, to, credential, logger);
        } else {
            javaUrlCopy(from.toURL(), to.toURL());
        }
    }

    public String[] scriptCopyGetSupportedProtocols() {
        String command = urlcopy;
        command = command + " -get-protocols";
        return ShellCommandExecuter.executeAndReturnOutput(command, logger);
    }

    public void scriptCopy(java.net.URI from, java.net.URI to) throws Exception {
        String command = urlcopy;
        if (debug) {
            command = command + " -debug true";
        }

        String x509_proxy = configuration.getX509_user_proxy();
        if (x509_proxy != null) {
            command = command + " -x509_user_proxy " + x509_proxy;
        }

        String x509_key = configuration.getX509_user_key();
        if (x509_key != null) {
            command = command + " -x509_user_key " + x509_key;
        }

        String x509_cert = configuration.getX509_user_cert();
        if (x509_cert != null) {
            command = command + " -x509_user_cert " + x509_cert;
        }

        String x509_cert_dir =
              configuration.getX509_user_trusted_certificates();
        if (x509_cert_dir != null) {
            command = command + " -x509_user_certs_dir " + x509_cert_dir;
        }

        int tcp_buffer_size = configuration.getTcp_buffer_size();
        if (tcp_buffer_size > 0) {
            command = command + " -tcp_buffer_size " + tcp_buffer_size;
        }

        int buffer_size = configuration.getBuffer_size();
        if (buffer_size > 0) {
            command = command + " -buffer_size " + buffer_size;
        }

        command = command +
              " -src-protocol " + from.getScheme();
        if (from.getScheme().equals("file")) {
            command = command + " -src-host-port localhost";
        } else {
            command = command +
                  " -src-host-port " + from.getHost() + ":" + portWithDefault(from);
        }
        command = command +
              " -src-path " + from.getPath() +
              " -dst-protocol " + to.getScheme();
        if (to.getScheme().equals("file")) {
            command = command + " -dst-host-port localhost";
        } else {
            command = command +
                  " -dst-host-port " + to.getHost() + ":" + portWithDefault(to);
        }
        command = command +
              " -dst-path " + to.getPath();
        int rc = 0;
        if (!dryRun) {
            rc = ShellCommandExecuter.execute(command, logger);
        }
        if (rc == 0) {
            say(" successfuly copied " + from + " to " + to);
            num_completed_successfully++;
        } else {
            esay(" failed to copy " + from + " to " + to);
            esay(urlcopy + " return code = " + rc);
            throw new Exception(urlcopy + " return code = " + rc);
        }
    }

    public void javaGridFtpCopy(java.net.URI src_url,
          java.net.URI dst_url,
          X509Credential credential,
          Logger logger) throws Exception {
        String serverMode = configuration.getServerMode();
        int numberOfStreams = configuration.getStreams_num();
        if ((src_url.getScheme().equals("gsiftp") || src_url.getScheme().equals("gridftp")) &&
              dst_url.getScheme().equals("file")) {
            //
            // case of read
            //
            boolean passive_server_mode;
            if (serverMode == null) {
                // this means server_mode option was not specified at all,
                // we preserve default behavior (passive, any number of streams)
                passive_server_mode = true;
            } else if (serverMode.equalsIgnoreCase("passive")) {
                // server_mode specified to passive, make sure number of streams is 1
                passive_server_mode = true;
                if (numberOfStreams != 1) {
                    logger.elog(
                          "server_mode is specified as passive, setting number of streams to 1");
                    numberOfStreams = 1;
                }
            } else if (serverMode.equalsIgnoreCase("active")) {
                passive_server_mode = false;
            } else {
                throw new IllegalArgumentException(
                      "Unknown server_mode option specified \"" + serverMode +
                            "\". Allowed options \"passive\" or \"active\"");
            }
            boolean emode = (numberOfStreams != 1);
            if (!dryRun) {
                GridftpClient client = new GridftpClient(src_url.getHost(),
                      portWithDefault(src_url),
                      configuration.getBuffer_size(),
                      configuration.getGlobus_tcp_port_range() != null
                            ? PortRange.valueOf(configuration.getGlobus_tcp_port_range())
                            : new PortRange(0),
                      credential,
                      new String[0],
                      configuration.getX509_user_trusted_certificates(),
                      CrlCheckingMode.IF_VALID, NamespaceCheckingMode.EUGRIDPMA_GLOBUS,
                      OCSPCheckingMode.IF_AVAILABLE);
                client.setStreamsNum(numberOfStreams);
                client.setChecksum(configuration.getCksmType(),
                      configuration.getCksmValue());
                client.setNextByteTimeout(
                      TimeUnit.SECONDS.toMillis(configuration.getNextByteTimeout()));
                client.setFirstByteTimeout(
                      TimeUnit.SECONDS.toMillis(configuration.getFirstByteTimeout()));
                try {
                    client.gridFTPRead(src_url.getPath(),
                          dst_url.getPath(),
                          emode,
                          passive_server_mode);
                    num_completed_successfully++;
                } finally {
                    client.close();
                }
            } else {
                num_completed_successfully++;
            }
            return;
        }

        if (src_url.getScheme().equals("file") &&
              (dst_url.getScheme().equals("gsiftp") || dst_url.getScheme().equals("gridftp"))) {
            //
            // case of write
            //
            boolean passive_server_mode;
            if (serverMode == null) {
                // this means server_mode option was not specified at all,
                // we preserve default behavior (passive, any number of streams)
                passive_server_mode = true;
            } else if (serverMode.equalsIgnoreCase("passive")) {
                passive_server_mode = true;
            } else if (serverMode.equalsIgnoreCase("active")) {
                passive_server_mode = false;
                if (numberOfStreams != 1) {
                    logger.elog(
                          "server_mode is specified as active, setting number of streams to 1");
                    numberOfStreams = 1;
                }
            } else {
                throw new IllegalArgumentException(
                      "Unknown server_mode option specified \"" + serverMode +
                            "\". Allowed options \"passive\" or \"active\"");
            }
            boolean emode = (numberOfStreams != 1);
            if (!dryRun) {
                GridftpClient client = new GridftpClient(dst_url.getHost(),
                      portWithDefault(dst_url),
                      configuration.getBuffer_size(),
                      configuration.getGlobus_tcp_port_range() != null
                            ? PortRange.valueOf(configuration.getGlobus_tcp_port_range())
                            : new PortRange(0),
                      credential, new String[0],
                      configuration.getX509_user_trusted_certificates(),
                      CrlCheckingMode.IF_VALID, NamespaceCheckingMode.EUGRIDPMA_GLOBUS,
                      OCSPCheckingMode.IF_AVAILABLE);
                client.setStreamsNum(numberOfStreams);
                client.setChecksum(configuration.getCksmType(),
                      configuration.getCksmValue());
                client.setNextByteTimeout(
                      TimeUnit.SECONDS.toMillis(configuration.getNextByteTimeout()));
                client.setFirstByteTimeout(
                      TimeUnit.SECONDS.toMillis(configuration.getFirstByteTimeout()));
                try {
                    client.gridFTPWrite(src_url.getPath(),
                          dst_url.getPath(),
                          emode,
                          configuration.getDoSendCheckSum(),
                          passive_server_mode);
                    num_completed_successfully++;
                } finally {
                    client.close();
                }
            } else {
                num_completed_successfully++;
            }
            return;
        }
        throw new IllegalArgumentException("need file-gridftp or gridftp-file combo");
    }

    public void javaUrlCopy(URL from, URL to) throws Exception {
        InputStream in;
        if (from.getProtocol().equals("file")) {
            in = new FileInputStream(from.getPath());
        } else {
            in = from.openConnection().getInputStream();
        }
        OutputStream out;

        if (to.getProtocol().equals("file")) {
            out = new FileOutputStream(to.getPath());
        } else {
            URLConnection to_connect = to.openConnection();
            to_connect.setDoInput(false);
            to_connect.setDoOutput(true);
            out = to_connect.getOutputStream();
        }
        int buffer_size = configuration.getBuffer_size();
        if (buffer_size <= 0) {
            buffer_size = KiB.toBytes(4);
        }
        byte[] bytes = new byte[buffer_size];
        long total = 0;
        int l;
        while ((l = in.read(bytes)) != -1) {
            total += l;
            out.write(bytes, 0, l);
        }
        say("successfuly copied " + total + " bytes from " + from + " to " + to);
        num_completed_successfully++;
    }

    private void cleanup() {
        CopyJob jobs[] = new CopyJob[0];
        jobs = copy_jobs.toArray(jobs);

        if (jobs == null) {
            return;
        }

        for (CopyJob job : jobs) {
            job.done(false, "stopped by cleanup");
        }
    }

}

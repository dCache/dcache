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
package org.dcache.alarms.server;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.net.SimpleSocketServer;
import ch.qos.logback.core.joran.spi.JoranException;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import org.dcache.cells.UniversalSpringCell;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple POJO wrapper around {@link SimpleSocketServer} to be run inside a
 * {@link UniversalSpringCell}.
 *
 * @author arossi
 */
public class LogEntryServerWrapper {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(LogEntryServerWrapper.class);

    private String baseDir;
    private String configFile;
    private String definitionsPath;
    private String path;
    private String properties;
    private String url;
    private String user;
    private String pass;
    private String level;
    private String smtpHost;
    private String smtpPort;
    private String startTls;
    private String ssl;
    private String emailUser;
    private String emailPassword;
    private String emailRecipients;
    private String emailSender;
    private String emailSubject;
    private String emailBufferSize;
    private String emailEnabled;
    private String historyEnabled;
    private Integer port;

    private SimpleSocketServer server;

    public void setBaseDir(String baseDir) {
        this.baseDir = Strings.emptyToNull(baseDir);
    }

    public void setConfigFile(String configFile) {
        this.configFile = Strings.emptyToNull(configFile);
    }

    public void setDefinitions(String definitionsPath) {
        this.definitionsPath = Strings.emptyToNull(definitionsPath);
    }

    public void setEmailBufferSize(Integer emailBufferSize) {
        this.emailBufferSize = emailBufferSize == null ? null : emailBufferSize.toString();
    }

    public void setEmailEnabled(Boolean emailEnabled) {
        this.emailEnabled = emailEnabled == null ? null : emailEnabled.toString();
    }

    public void setEmailPassword(String emailPassword) {
        this.emailPassword = Strings.emptyToNull(emailPassword);
    }

    public void setEmailRecipients(String emailRecipients) {
        this.emailRecipients = Strings.emptyToNull(emailRecipients);
    }

    public void setEmailSender(String emailSender) {
        this.emailSender = Strings.emptyToNull(emailSender);
    }

    public void setEmailSubject(String emailSubject) {
        this.emailSubject = emailSubject;
    }

    public void setEmailUser(String emailUser) {
        this.emailUser = Strings.emptyToNull(emailUser);
    }

    public void setHistoryEnabled(Boolean historyEnabled) {
        this.historyEnabled = historyEnabled == null ? null : historyEnabled.toString();
    }

    public void setLevel(String level) {
        this.level = Strings.emptyToNull(level);
    }

    public void setPass(String pass) {
        this.pass = Strings.emptyToNull(pass);
    }

    public void setPath(String path) {
        this.path = Strings.emptyToNull(path);
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void setProperties(String properties) {
        this.properties = Strings.emptyToNull(properties);
    }

    public void setServer(SimpleSocketServer server) {
        this.server = server;
    }

    public void setSmtpHost(String smtpHost) {
        this.smtpHost = Strings.emptyToNull(smtpHost);
    }

    public void setSmtpPort(Integer smtpPort) {
        this.smtpPort = smtpPort == null ? null : smtpPort.toString();
    }

    public void setSsl(Boolean ssl) {
        this.ssl = ssl == null ? null : ssl.toString();
    }

    public void setStartTls(Boolean startTls) {
        this.startTls = startTls == null ? null : startTls.toString();
    }

    public void setUrl(String url) {
        this.url = Strings.emptyToNull(url);
    }

    public void setUser(String user) {
        this.user = Strings.emptyToNull(user);
    }

    public void shutDown() {
        if (server != null) {
            server.close();
        }
    }

    public void startUp() {
        if (Strings.isNullOrEmpty(url)) {
            LOGGER.warn("Alarms database type is OFF; server will not be started.");
            System.exit(0);
        }

        File alarmsDirectory;

        try {
            checkNotNull(configFile);
            checkNotNull(baseDir);
            alarmsDirectory = new File(baseDir);
            checkArgument(alarmsDirectory.isDirectory());
            checkNotNull(port);
            checkArgument(port > 0);
        } catch (IllegalArgumentException ie) {
            LOGGER.error("Configuration precondition failure: {}; "
                            + "server will not be started.", ie.getMessage());
            System.exit(-1);
            /*
             * This is really stupid, but Eclipse doesn't
             * understand that System.exit() is equivalent to
             * return insofar as preventing the alarmsDirectory
             * reference below from being uninitialized.
             */
            return;
        }

        LoggerContext loggerContext
            = (LoggerContext) LoggerFactory.getILoggerFactory();

        try {
            loggerContext.reset();

            loggerContext.putProperty("alarms.dir", alarmsDirectory.getAbsolutePath());
            loggerContext.putProperty("alarms.db.xml.path", path);
            loggerContext.putProperty("alarms.db.url", url);
            loggerContext.putProperty("alarms.db.user", user);
            loggerContext.putProperty("alarms.db.password", pass);
            loggerContext.putProperty("alarms.db.config.path", properties);
            loggerContext.putProperty("alarms.definitions.path", definitionsPath);
            loggerContext.putProperty("alarms.log.root-level", level);
            loggerContext.putProperty("alarms.email.smtp-host", smtpHost);
            loggerContext.putProperty("alarms.email.smtp-port", smtpPort);
            loggerContext.putProperty("alarms.email.start-tls", startTls);
            loggerContext.putProperty("alarms.email.ssl", ssl);
            loggerContext.putProperty("alarms.email.user", emailUser);
            loggerContext.putProperty("alarms.email.password", emailPassword);
            loggerContext.putProperty("alarms.email.to", emailRecipients);
            loggerContext.putProperty("alarms.email.from", emailSender);
            loggerContext.putProperty("alarms.email.subject", emailSubject);
            loggerContext.putProperty("alarms.email.buffer-size", emailBufferSize);
            loggerContext.putProperty("alarms.enable.email", emailEnabled);
            loggerContext.putProperty("alarms.enable.history", historyEnabled);

            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(loggerContext);

            configurator.doConfigure(configFile);
        } catch (JoranException je) {
            /*
             * Using the logger here is problematic because the exception
             * may actually have affected the logging system.
             */
            System.err.println("Configuration error: server will not be started. "
                            + je.getMessage());

            System.exit(-2);
        } catch (RuntimeException e) {
            /*
             * Using the logger here is problematic because the exception
             * may actually have affected the logging system.
             */
            System.err.println("Alarm server failed to start, unexpected error; "
                            + "this is probably a bug.");
            e.printStackTrace();

            System.exit(-3);
        }

        server = new SimpleSocketServer(loggerContext, port);
        server.start();
    }
}

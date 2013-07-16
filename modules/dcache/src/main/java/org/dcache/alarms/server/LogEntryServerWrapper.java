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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Strings;

import java.io.File;

import org.dcache.cells.UniversalSpringCell;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.net.SimpleSocketServer;
import ch.qos.logback.core.joran.spi.JoranException;

/**
 * Simple POJO wrapper around {@link SimpleSocketServer} to be run inside a
 * {@link UniversalSpringCell}.
 *
 * @author arossi
 */
public class LogEntryServerWrapper {

    private String baseDir;
    private String configFile;
    private String definitionsPath;
    private String path;
    private String properties;
    private String driver;
    private String url;
    private String user;
    private String pass;
    private String level;
    private Integer port;

    private SimpleSocketServer server;

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public void setDefinitions(String definitionsPath) {
        this.definitionsPath = definitionsPath;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public void setServer(SimpleSocketServer server) {
        this.server = server;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void shutDown() {
        if (server != null) {
            server.close();
        }
    }

    public void startUp() throws JoranException {
        if (Strings.isNullOrEmpty(url)) {
            LoggerFactory.getLogger("root")
                .warn("alarms database type is OFF; server will not be started");
            return;
        }

        checkNotNull(configFile);
        checkNotNull(baseDir);
        File f = new File(baseDir);
        checkArgument(f.isDirectory());
        checkNotNull(port);
        checkArgument(port > 0);
        LoggerContext loggerContext
            = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.reset();

        loggerContext.putProperty("alarms.dir", f.getAbsolutePath());
        loggerContext.putProperty("alarms.db.xml.path", path);
        loggerContext.putProperty("alarms.db.driver", driver);
        loggerContext.putProperty("alarms.db.url", url);
        loggerContext.putProperty("alarms.db.user", user);
        loggerContext.putProperty("alarms.db.password", pass);
        loggerContext.putProperty("alarms.db.config.path", properties);
        loggerContext.putProperty("alarms.definitions.path", definitionsPath);
        loggerContext.putProperty("alarms.log.root-level", level);

        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(loggerContext);
        configurator.doConfigure(configFile);

        server = new SimpleSocketServer(loggerContext, port);
        server.start();
    }
}

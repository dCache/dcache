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
package org.dcache.alarms.shell;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.URL;

import org.dcache.alarms.Alarm;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.Args;

/**
 * Allows the user to send an alarm event directly to the alarm server.
 * <br>
 * <br>
 * Arguments take the form of [options] message. <br><br>
 *
 * Options:
 * <p>
 * <table>
 *     <tr>
 *         <th>OPTION</th>
 *         <th>REQUIRED</th>
 *         <th>DESCRIPTION</th>
 *     </tr>
 *     <tr>
 *         <td>r [remote host]</td>
 *         <td>YES</td>
 *         <td>logging server name (not exposed to user as option; should be set
 *             via the dcache configuration or layout).</td>
 *     </tr>
 *     <tr>
 *         <td>p [remote port]</td>
 *         <td>NO</td>
 *         <td>logging server port; defaults to 9867
 *             (not exposed to user as option; should be set
 *             via the dcache configuration or layout).</td>
 *     </tr>
 *     <tr>
 *         <td>t [type]</td>
 *         <td>NO</td>
 *         <td>alarm subtype tag; if used, this must be a {@link PredefinedAlarm}.
 *             <br>If no type is specified, an attempt to infer the type
 *             from definitions will be made by the server; failing that,
 *             the type will be 'GENERIC'.</td>
 *     </tr>
 *     <tr>
 *         <td>h [source host]</td>
 *         <td>NO</td>
 *         <td>host where alarm originates; defaults to canonical
 *             name of local host.</td>
 *     </tr>
 *     <tr>
 *         <td>d [source domain]</td>
 *         <td>NO</td>
 *         <td>domain where alarm originates; defaults to &lt;na&gt;.</td>
 *     </tr>
 *     <tr>
 *         <td>s [source servce]</td>
 *         <td>NO</td>
 *         <td>service or cell where alarm originates; defaults
 *             to 'user-command'.</td>
 *     </tr>
 * </table>
 *
 * @author arossi
 */
public class SendAlarm {
    private static final String CONFIG
        = "org/dcache/alarms/commandline/logback.xml";
    static final String INDENT = "   ";

    public static void main(String[] args) {
        try {
            System.out.println(processRequest(args, null));
        } catch (Throwable t) {
            AlarmDefinitionManager.printError(t);
        }
    }

    public static String processRequest(String[] args, org.slf4j.Logger logger)
                    throws Exception {
        return sendAlarm(new AlarmArguments(new Args(args)), logger);
    }

    private static Logger configureLogger(String host, String port)
                    throws JoranException {
        Logger logger = (Logger) LoggerFactory.getLogger("Commandline");
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        lc.reset();
        lc.putProperty("remote.server.host", host);
        lc.putProperty("remote.server.port", port);
        lc.putProperty("remote.log.level", Level.ERROR.toString());

        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);

        URL logbackConfig
            = Thread.currentThread().getContextClassLoader().getResource(CONFIG);

        configurator.doConfigure(logbackConfig);
        return logger;
    }

    private static String sendAlarm(AlarmArguments alarmArgs,
                                    org.slf4j.Logger logger)
                   throws JoranException {
        MDC.put(Alarm.HOST_TAG, alarmArgs.sourceHost);
        MDC.put(Alarm.DOMAIN_TAG, alarmArgs.sourceDomain);
        MDC.put(Alarm.SERVICE_TAG, alarmArgs.sourceService);
        if (logger == null) {
            logger = configureLogger(alarmArgs.destinationHost,
                                     alarmArgs.destinationPort);
        }
        logger.error(alarmArgs.marker, alarmArgs.message);
        return "sending alarm to "
            + alarmArgs.destinationHost + ":" + alarmArgs.destinationPort;
    }
}

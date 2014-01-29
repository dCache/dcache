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
package org.dcache.alarms.commandline;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.google.common.collect.ImmutableMap;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;

import java.net.InetAddress;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dcache.alarms.IAlarms;
import org.dcache.alarms.logback.AlarmDefinition;
import org.dcache.util.Args;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Allows the user to send an ad hoc alarm event directly to the alarm server.
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
 *         <td>-d=[destination]</td>
 *         <td>YES</td>
 *         <td>logging server uri (i.e., "dst://[host]:[port]"; port may be blank)</td>
 *     </tr>
 *     <tr>
 *         <td>-l=[level]</td>
 *         <td>NO</td>
 *         <td>logging level</td>
 *     </tr>
 *     <tr>
 *         <td>-t=[type]</td>
 *         <td>NO</td>
 *         <td>alarm subtype tag</td>
 *     </tr>
 *     <tr>
 *         <td>-s=[source]</td>
 *         <td>NO</td>
 *         <td>source info uri (i.e., "src://[host]/[domain]/[service]")</td>
 *     </tr>
 * </table>
 *
 * @author arossi
 */
public class SendAlarm {

    /**
     * Wraps the parsing and management of commanline arguments for sending
     * an alarm.
     */
    private static class AlarmArguments {
        private static final String DEFAULT_SOURCE_PATH = "/NA/command-line";
        private static final String DEFAULT_SOURCE = "src://" + LOCAL_HOST
                        + DEFAULT_SOURCE_PATH;
        private static final String DEFAULT_PORT = "60001";

        private static final String LEVEL = "l";
        private static final String TYPE = "t";
        private static final String SOURCE = "s";
        private static final String DESTINATION = "d";

        private static final Map<String, String> HELP_MESSAGES
            = ImmutableMap.of
                (LEVEL,       "-l=<level>       (optional): logging level [WARN, ERROR (default)]",
                 TYPE,        "-t=<type>        (optional): alarm subtype tag",
                 SOURCE,      "-s=<source>      (optional): source info uri"
                              + " (i.e., \"src://[host]/[domain]/[service]\")",
                 DESTINATION, "-d=<destination> (required): logging server uri"
                              + " (i.e., \"dst://[host]:[port]\"; port may be blank)");

        private final Level level;
        private final Marker marker;
        private final String sourceHost;
        private final String sourceService;
        private final String sourceDomain;
        private final String destinationHost;
        private final String destinationPort;
        private final String message;

        private AlarmArguments(Args parsed) throws Exception {
            List<String> arguments = parsed.getArguments();
            checkArgument(arguments.size() > 0,
                            "please provide a non-zero-length alarm message"
                            + "; -h[elp] for options");
            Iterator<String> it = arguments.iterator();
            StringBuilder msg = new StringBuilder(it.next());
            while (it.hasNext()) {
               msg.append(" ").append(it.next());
            }
            message = msg.toString();

            String arg = parsed.getOption(DESTINATION);
            checkNotNull(arg, "please provide a uri:"
                            + LBRK + INDENT + HELP_MESSAGES.get(DESTINATION)
                            + "; -h[elp] for options");

            URI uri = new URI(arg);
            destinationHost = uri.getHost();
            checkNotNull(destinationHost,
                            "please provide a host in the uri:"
                            + LBRK + INDENT + HELP_MESSAGES.get(DESTINATION)
                            + "; -h[elp] for other options");

            arg = String.valueOf(uri.getPort());
            if ("-1".equals(arg)) {
                destinationPort = DEFAULT_PORT;
            } else {
                destinationPort = arg;
            }

            arg = parsed.getOption(SOURCE);
            if (arg != null) {
                uri = new URI(arg);
            } else {
                uri = new URI(DEFAULT_SOURCE);
            }

            arg = uri.getHost();
            if (arg == null) {
                arg = LOCAL_HOST;
            }

            sourceHost = arg;

            arg = uri.getPath();
            if (arg == null || arg.isEmpty()) {
                arg = DEFAULT_SOURCE_PATH;
            }

            String[] parts = arg.substring(1).split("[/]");
            sourceDomain = parts[0];
            if (parts.length > 1) {
                sourceService = parts[1];
            } else {
                sourceService = sourceDomain;
            }

            marker = AlarmDefinition.getMarker(parsed.getOption(TYPE));

            arg = parsed.getOption(LEVEL);
            if (arg == null) {
                level = Level.ERROR;
            } else {
                level = Level.valueOf(arg);
            }
        }
    }

    private static final String CONFIG = "org/dcache/alarms/commandline/logback.xml";
    private static final String HELP = "help";
    private static final String HELP_ABBR = "h";
    private static final String LOCAL_HOST;
    private static final String LBRK = System.getProperty("line.separator");
    private static final String INDENT = "   ";

    static {
        String host;
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            host = IAlarms.UNKNOWN_HOST;
        }
        LOCAL_HOST = host;
    }

    public static void main(String[] args) {
        try {
            Args options = new Args(args);
            if (options.hasOption(HELP_ABBR) || options.hasOption(HELP)) {
                printHelp();
            } else {
                AlarmArguments alarmArgs = new AlarmArguments(options);
                System.out.println("sending alarm to "
                                + alarmArgs.destinationHost
                                + ":" + alarmArgs.destinationPort );
                sendAlarm(alarmArgs);
            }
        } catch (Throwable t) {
            AlarmDefinitionManager.printError(t);
        }
    }

    private static Logger configureLogger(String host, String port) throws JoranException {
        Logger logger = (Logger) LoggerFactory.getLogger("Commandline");
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        lc.reset();
        lc.putProperty("remote.server.host", host);
        lc.putProperty("remote.server.port", port);
        /*
         * Allow all levels to be sent.  The remote server will be set
         * to intercept only messages sent at the defined dcache.log.remote.level.
         */
        lc.putProperty("remote.log.level", Level.DEBUG.toString());

        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);

        URL logbackConfig
            = Thread.currentThread().getContextClassLoader().getResource(CONFIG);

        configurator.doConfigure(logbackConfig);
        return logger;
    }

    private static void printHelp() {
        StringBuilder message = new StringBuilder();
        message.append("COMMAND LINE: dcache alarm ")
               .append("[<options>] message:")
               .append(LBRK)
               .append(INDENT)
               .append("OPTIONS:")
               .append(LBRK);
        for (String help : AlarmArguments.HELP_MESSAGES.values()) {
            message.append(INDENT).append(help).append(LBRK);
        }
        message.append(INDENT)
               .append("('dcache send' automatically provides destination uri")
               .append(" based on dcache.log.server.host and dcache.log.server.port)")
               .append(LBRK);
        System.out.println(message);
    }

    private static void sendAlarm(AlarmArguments alarmArgs) throws JoranException {
        MDC.put(IAlarms.HOST_TAG, alarmArgs.sourceHost);
        MDC.put(IAlarms.DOMAIN_TAG, alarmArgs.sourceDomain);
        MDC.put(IAlarms.SERVICE_TAG, alarmArgs.sourceService);
        Logger logger = configureLogger(alarmArgs.destinationHost,
                        alarmArgs.destinationPort);
        if (Level.ERROR.equals(alarmArgs.level)) {
            logger.error(alarmArgs.marker, alarmArgs.message);
        } else if (Level.WARN.equals(alarmArgs.level)) {
            logger.warn(alarmArgs.marker, alarmArgs.message);
        } else if (Level.INFO.equals(alarmArgs.level)) {
            logger.info(alarmArgs.marker, alarmArgs.message);
        } else if (Level.DEBUG.equals(alarmArgs.level)) {
            logger.debug(alarmArgs.marker, alarmArgs.message);
        }
    }
}

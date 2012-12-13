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
package org.dcache.alarms.logback;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.regex.Pattern;

import org.dcache.alarms.IAlarms;
import org.dcache.alarms.Severity;
import org.dcache.util.RegexUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.filter.Filter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Provides the definition of an alarm to be used by the
 * {@link AlarmDefinitionFilter}.
 *
 * The filter must be expressed as a JSON string (but without beginning or
 * ending braces), and should define the following properties:<br>
 * <br>
 *
 * <table>
 * <tr>
 * <td>PROPERTY</td>
 * <td>REQUIRED</td>
 * <td>POSSIBLE VALUES</td>
 * </tr>
 * <tr>
 * <td>level</td>
 * <td>YES</td>
 * <td>see {@link Level}</td>
 * </tr>
 * <tr>
 * <td>logger</td>
 * <td>(at least one of logger, regex)</td>
 * <td>FQN of logger class</td>
 * </tr>
 * <tr>
 * <td>regex</td>
 * <td>(at least one of logger, regex)</td>
 * <td>a pattern to match the message with; note: it is advisable to place this
 * pattern in double quotes, so that JSON will accept special characters: e.g.,
 * "[=].+[\w]*"</td>
 * </tr>
 * <tr>
 * <td>regex-flags</td>
 * <td>NO</td>
 * <td>options for regex (these are string representations of the
 * {@link Pattern} options, joined by the 'or' pipe symbol: e.g.,
 * "CASE_INSENSITIVE | DOTALL")</td>
 * </tr>
 * <tr>
 * <td>type</td>
 * <td>YES</td>
 * <td>name serves as sub-marker</td>
 * </tr>
 * <tr>
 * <td>severity</td>
 * <td>NO</td>
 * <td>see {@link Severity}</td>
 * </tr>
 * <tr>
 * <td>thread</td>
 * <td>NO</td>
 * <td>thread name limits alarm only to this thread</td>
 * </tr>
 * <tr>
 * <td>include-in-key</td>
 * <td>YES</td>
 * <td>whitespace-delimited concatenation of key field names (see below)</td>
 * </tr>
 * </table>
 *
 * <p>
 * An example:
 * </p>
 *
 * <pre>
 *    &lt;alarmType&gt;
 *               level:ERROR,
 *               logger:AdHocAlarmTest,
 *               type:TEST,
 *               include-in-key:timestamp.message.type
 *    &lt;/alarmType&gt;
 * </pre>
 *
 * <p>
 * The field names which can be used to constitute the unique key of the alarm
 * include the properties defined for all alarms (see {@link IAlarms}):
 * </p>
 *
 * <ol>
 * <li>timestamp</li>
 * <li>message</li>
 * <li>logger</li>
 * <li>type</li>
 * <li>domain</li>
 * <li>service</li>
 * <li>host</li>
 * <li>thread</li>
 * </ol>
 *
 * <p>
 * For instance, the checksum alarm for files would include type, message, host,
 * domain and service, but not timestamp (as all reports for that physical file
 * would be treated as duplicates after the first). These tags are to be separated
 * by (an arbitrary number of ) whitespace characters. Note that logger,
 * timestamp and message come from the logging event, host is determined by a
 * static lookup, and domain and service correspond to the <code>cells.domain</code>
 * and <code>cells.cell</code> properties in the event's MDC map.
 * </p>
 *
 * <p>
 * {@link #matches(ILoggingEvent)} relies on an implicit function
 * (logger,level,regex,thread)->definition; hence a given alarm type can be
 * generated by more than one logger; a logger in turn can send multiple types
 * of alarms if these are mapped to different logging levels (e.g., fatal,
 * error, warn), thread names and/or regex matches on the message string.
 * </p>
 *
 * <p>
 * {@link #embedAlarm(ILoggingEvent)} embeds the alarm type and JSON
 * serialization into the current event's MDC property map for immediate
 * downstream use by an {@link AlarmDefinitionAppender} (this assumes the same
 * thread context is maintained on this logging object from
 * {@link Filter#decide(Object)} to {@link Appender} protected append method).
 * </p>
 */
public class AlarmDefinition {

    public static final String LOGGER_TAG = "logger";
    public static final String LEVEL_TAG = "level";
    public static final String THREAD_TAG = "thread";
    public static final String REGEX_TAG = "regex";
    public static final String REGEX_FLAGS_TAG = "regex-flags";
    public static final String INCLUDE_IN_KEY_TAG = "include-in-key";
    public static final String EMBEDDED_ALARM_INFO_TAG = "embedded.alarm.info";
    public static final String EMBEDDED_TYPE_TAG = "embedded.type";
    public static final String INCLUDE_IN_KEY_DELIMITER = "[\\s]";

    private static String host;

    static {
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            host = IAlarms.UNKNOWN_HOST;
        }
    }

    /*
     * required
     */
    private final Level level;
    private final String type;
    private final Set<String> includeInKey = Sets.newHashSet();

    /*
     * optional
     */
    private Severity severity = Severity.INDETERMINATE;
    private String logger;
    private String thread;
    private Pattern regex;

    public static Marker getMarker(String subMarker) {
        Marker alarmMarker = MarkerFactory.getIMarkerFactory().getMarker(
                        IAlarms.ALARM_MARKER);

        if (subMarker != null) {
            alarmMarker.add(MarkerFactory.getIMarkerFactory().getDetachedMarker(
                            subMarker));
        }

        return alarmMarker;
    }

    /**
     * @param formattedJSONString
     *            Note that the definition expects a full, correctly formed JSON
     *            string (with beginning and end braces included).
     */
    public AlarmDefinition(String formattedJSONString) throws JSONException {
        Preconditions.checkNotNull(formattedJSONString);

        JSONObject jsonObject = new JSONObject(formattedJSONString);

        /*
         * these fields are required and will throw a JSONException if not
         * defined
         */
        type = jsonObject.getString(IAlarms.TYPE_TAG);
        level = Level.valueOf(jsonObject.getString(LEVEL_TAG));

        String includeString = jsonObject.getString(INCLUDE_IN_KEY_TAG);
        String[] keyNames = includeString.split(INCLUDE_IN_KEY_DELIMITER);
        for (String name : keyNames) {
            includeInKey.add(name);
        }

        try {
            logger = jsonObject.getString(LOGGER_TAG);
        } catch (JSONException notFound) {
            // ignore, as field is optional
        }

        try {
            thread = jsonObject.getString(THREAD_TAG);
        } catch (JSONException notFound) {
            // ignore, as field is optional
        }

        try {
            severity = Severity.valueOf(jsonObject.getString(IAlarms.SEVERITY_TAG));
        } catch (JSONException notFound) {
            // ignore, as field is optional
        }

        String regex = null;
        String flags = null;
        try {
            regex = jsonObject.getString(REGEX_TAG);
            flags = jsonObject.getString(REGEX_FLAGS_TAG);
        } catch (JSONException notFound) {
            // ignore, as fields are optional
        }

        if (regex != null) {
            this.regex = Pattern.compile(regex, RegexUtils.parseFlags(flags));
        }
    }

    public void embedAlarm(ILoggingEvent event) throws JSONException {
        if (!(event instanceof LoggingEvent)) {
            return;
        }

        LoggingEvent alarm = (LoggingEvent) event;
        alarm.getMDCPropertyMap().put(EMBEDDED_TYPE_TAG, type);
        alarm.getMDCPropertyMap().put(EMBEDDED_ALARM_INFO_TAG, getInfo(event));
    }

    public boolean matches(ILoggingEvent event) {
        if (!(event instanceof LoggingEvent)) {
            return false;
        }

        if (!event.getLevel().isGreaterOrEqual(level)) {
            return false;
        }

        if (logger != null && !event.getLoggerName().equals(logger)) {
            return false;
        }

        if (thread != null && !event.getThreadName().equals(thread)) {
            return false;
        }

        if (regex != null && !regex.matcher(event.getMessage()).find()) {
            return false;
        }

        return true;
    }

    private String getKey(ILoggingEvent event, String domain, String service) {
        StringBuilder key = new StringBuilder();

        for (String s : includeInKey) {
            switch(s) {
              case IAlarms.TIMESTAMP_TAG:
                  key.append(event.getTimeStamp());
                  break;
              case IAlarms.MESSAGE_TAG:
                  key.append(event.getFormattedMessage());
                  break;
              case IAlarms.TYPE_TAG:
                  key.append(type);
                  break;
              case IAlarms.HOST_TAG:
                  key.append(host);
                  break;
              case IAlarms.DOMAIN_TAG:
                  key.append(domain);
                  break;
              case IAlarms.SERVICE_TAG:
                  key.append(service);
                  break;
              case LOGGER_TAG:
                  key.append(event.getLoggerName());
                  break;
              case THREAD_TAG:
                  key.append(event.getThreadName());
                  break;
            }
        }

        return key.toString();
    }

    private String getInfo(ILoggingEvent event) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(IAlarms.TIMESTAMP_TAG, event.getTimeStamp());

        String domain = event.getMDCPropertyMap().get(IAlarms.DOMAIN);
        if (domain == null) {
            domain = IAlarms.UNKNOWN_DOMAIN;
        }

        String service = event.getMDCPropertyMap().get(IAlarms.CELL);
        if (service == null) {
            service = IAlarms.UNKNOWN_SERVICE;
        }

        jsonObject.put(IAlarms.DOMAIN_TAG, domain);
        jsonObject.put(IAlarms.SERVICE_TAG, service);
        jsonObject.put(IAlarms.HOST_TAG, host);
        jsonObject.put(IAlarms.TYPE_TAG, type);
        jsonObject.put(IAlarms.SEVERITY_TAG, severity);
        jsonObject.put(IAlarms.KEY_TAG, getKey(event, domain, service));
        jsonObject.put(IAlarms.MESSAGE_TAG, event.getFormattedMessage());

        return jsonObject.toString();
    }
}

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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.jdom.Element;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.dcache.alarms.AlarmDefinitionValidationException;
import org.dcache.alarms.IAlarms;
import org.dcache.alarms.Severity;
import org.dcache.util.RegexUtils;

/**
 * Provides the definition of an alarm to be used by the
 * {@link LogEntryAppender}.
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
 * <td>regexFlags</td>
 * <td>NO</td>
 * <td>options for regex (these are string representations of the
 * {@link Pattern} options, joined by the 'or' pipe symbol: e.g.,
 * "CASE_INSENSITIVE | DOTALL")</td>
 * </tr>
 * <tr>
 * <td>matchException</td>
 * <td>NO</td>
 * <td>true = recur over embedded exception messages when applying regex match
 * (default is false)</td>
 * </tr>
 * <tr>
 * <td>depth</td>
 * <td>NO</td>
 * <td>depth of exception trace to examine when applying match-exception;
 * undefined means unbounded (default)</td>
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
 * <td>includeInKey</td>
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
 *       &lt;alarmType&gt;
 *          &lt;type&gt;SERVICE_CREATION_FAILURE&lt;/type&gt;
 *          &lt;regex&gt;(.+) from ac_create&lt;/regex&gt;
 *          &lt;level&gt;ERROR&lt;/level&gt;
 *          &lt;severity&gt;CRITICAL&lt;/severity&gt;
 *          &lt;includeInKey&gt;group1 type host domain service&lt;/includeInKey&gt;
 *       &lt;/alarmType&gt;
 * </pre>
 *
 * <p>
 * The field names which can be used to constitute the unique key of the alarm
 * include the properties defined for all alarms (see {@link IAlarms}), plus the
 * parsing of the message field into regex groups:
 * </p>
 *
 * <ol>
 * <li>timestamp</li>
 * <li>message</li>
 * <li>group + number</li>
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
 * would be treated as duplicates after the first). These tags are to be
 * separated by (an arbitrary number of ) whitespace characters. Note that
 * logger, timestamp and message come from the logging event, host is determined
 * by a static lookup, and domain and service correspond to the
 * <code>cells.domain</code> and <code>cells.cell</code> properties in the
 * event's MDC map.
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
 * <p>Also implements JDom marshaling and unmarshaling methods.</p>
 */
public class AlarmDefinition {
    public static final String ALARM_TYPE_TAG = "alarmType";
    public static final String DEPTH_TAG = "depth";
    public static final String INCLUDE_IN_KEY_TAG = "includeInKey";
    public static final String LEVEL_TAG = "level";
    public static final String LOGGER_TAG = "logger";
    public static final String MATCH_EXCEPTION_TAG = "matchException";
    public static final String REGEX_TAG = "regex";
    public static final String REGEX_FLAGS_TAG = "regexFlags";
    public static final String THREAD_TAG = "thread";
    public static final String INCLUDE_IN_KEY_DELIMITER = "[\\s]";
    public static final String RM = "-";

    public static final ImmutableList<String> ATTRIBUTES
        = new ImmutableList.Builder<String>()
            .add(DEPTH_TAG)
            .add(INCLUDE_IN_KEY_TAG)
            .add(LEVEL_TAG)
            .add(LOGGER_TAG)
            .add(MATCH_EXCEPTION_TAG)
            .add(REGEX_TAG)
            .add(REGEX_FLAGS_TAG)
            .add(IAlarms.SEVERITY_TAG)
            .add(THREAD_TAG)
            .add(IAlarms.TYPE_TAG)
            .build();

    private static final String REQUIRED = " is a required attribute";

    private static final ImmutableSet<String> KEY_VALUES
        = new ImmutableSet.Builder<String>()
            .add(IAlarms.TIMESTAMP_TAG)
            .add(IAlarms.MESSAGE_TAG)
            .add(IAlarms.GROUP_TAG + "N")
            .add(LOGGER_TAG)
            .add(IAlarms.TYPE_TAG)
            .add(IAlarms.DOMAIN_TAG)
            .add(IAlarms.SERVICE_TAG)
            .add(IAlarms.HOST_TAG)
            .add(THREAD_TAG)
            .build();

    private static final ImmutableSet<String> LEVEL_VALUES
        = new ImmutableSet.Builder<String>()
            .add(Level.ERROR.toString())
            .add(Level.WARN.toString())
            .add(Level.INFO.toString())
            .add(Level.DEBUG.toString())
            .add(Level.TRACE.toString())
            .build();

    private static final ImmutableList<String> DEFINITIONS
        = new ImmutableList.Builder<String>()
            .add("match nested exception messages using regex only to this level"
                        + " (integer, optional; default: undefined)")
            .add("create the unique identifier for this alarm event based on"
                        + " the selected fields (whitespace delimited) "
                        + KEY_VALUES
                        + " (required)")
            .add("match events at this logging level or greater "
                        + LEVEL_VALUES
                        + " (required)")
            .add("match events only generated by the named logger "
                        + "[usually a fully qualified Java class name]"
                        + " (required if regex not specified)")
            .add("apply the regex to nested exception messages "
                        + " (boolean, optional; default: false)")
            .add("Java-style regular expression used to match messages "
                        + " (required if logger is not specified)")
            .add("Java-style flag options for regex; join using '|' (or) "
                        + RegexUtils.FLAG_VALUES
                        + " (optional; default: none)")
            .add("alarm-specific level "
                        + Severity.asList()
                        + " (optional; default: MODERATE)]")
            .add("match events emanating only from this JVM thread"
                        + " [JVM thread name] (optional; default: none)")
            .add("choose a name to call this type of alarm (required)")
            .build();

    public static String getAttributeDescription(String attribute)
                    throws AlarmDefinitionValidationException {
        if (!ATTRIBUTES.contains(attribute)) {
            throw new AlarmDefinitionValidationException
            ("unrecognized attribute: " + attribute);
        }
        return DEFINITIONS.get(ATTRIBUTES.indexOf(attribute));
    }

    public static String getAttributesDescription() {
        StringBuilder builder = new StringBuilder();
        String newLine = System.getProperty("line.separator");
        builder.append("ALARM DEFINITION ATTRIBUTES").append(newLine);
        for (int i = 0; i < ATTRIBUTES.size(); i++) {
            builder .append("   (")
                    .append(i)
                    .append(") ")
                    .append(ATTRIBUTES.get(i))
                    .append(":")
                    .append(newLine)
                    .append("       -- ")
                    .append((DEFINITIONS.get(i)))
                    .append(newLine);
        }
        return builder.toString();
    }

    private final Set<String> hashedKeyElements = Sets.newHashSet();

    private Level level = Level.WARN;
    private Severity severity = Severity.MODERATE;
    private Boolean matchException = false;
    private String type;
    private String logger;
    private String thread;
    private String regexStr;
    private String regexFlags;
    private Integer depth;
    private Pattern regex;

    public AlarmDefinition() {
    }

    public AlarmDefinition(Element alarmType) {
        Element child = alarmType.getChild(DEPTH_TAG);
        if (child != null) {
            setDepth(Integer.parseInt(child.getTextTrim()));
        }
        child = alarmType.getChild(INCLUDE_IN_KEY_TAG);
        if (child != null) {
            setIncludeInKey(child.getTextTrim());
        }
        child = alarmType.getChild(LEVEL_TAG);
        if (child != null) {
            setLevel(child.getTextTrim());
        }
        child = alarmType.getChild(LOGGER_TAG);
        if (child != null) {
            setLevel(child.getTextTrim());
        }
        child = alarmType.getChild(MATCH_EXCEPTION_TAG);
        if (child != null) {
            setMatchException(Boolean.valueOf(child.getTextTrim()));
        }
        child = alarmType.getChild(REGEX_TAG);
        if (child != null) {
            setRegex(child.getTextTrim());
        }
        child = alarmType.getChild(REGEX_FLAGS_TAG);
        if (child != null) {
            setRegexFlags(child.getTextTrim());
        }
        child = alarmType.getChild(IAlarms.SEVERITY_TAG);
        if (child != null) {
            setSeverity(child.getTextTrim());
        }
        child = alarmType.getChild(THREAD_TAG);
        if (child != null) {
            setThread(child.getTextTrim());
        }
        child = alarmType.getChild(IAlarms.TYPE_TAG);
        if (child != null) {
            setType(child.getTextTrim());
        }
    }

    public Integer getDepth() {
        return depth;
    }

    public String getIncludeInKey() {
        StringBuilder result = new StringBuilder();
        Joiner.on(" ").appendTo(result, hashedKeyElements.iterator());
        if (result.length() == 0) {
            return null;
        }
        return result.toString();
    }

    public String getKey(ILoggingEvent event, String host, String domain,
                    String service) {
        StringBuilder key = new StringBuilder();

        for (String s : hashedKeyElements) {
            if (s.startsWith(IAlarms.GROUP_TAG)) {
                if (regex != null) {
                    Matcher m = regex.matcher(event.getFormattedMessage());
                    if (m.find()) {
                        int group
                            = Integer.parseInt(s.substring(IAlarms.GROUP_TAG.length()));
                        key.append(m.group(group));
                        continue;
                    }
                }
            }

            switch (s) {
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

    public Level getLevel() {
        return level;
    }

    public String getLogger() {
        return logger;
    }

    public Boolean getMatchException() {
        return matchException;
    }

    public Pattern getRegex() {
        return regex;
    }

    public String getRegexFlags() {
        return regexFlags;
    }

    public String getRegexStr() {
        return regexStr;
    }

    public Severity getSeverity() {
        return severity;
    }

    public Severity getSeverityEnum() {
        return severity;
    }

    public String getThread() {
        return thread;
    }

    public String getType() {
        return type;
    }

    public boolean matches(ILoggingEvent event) {
        if (!event.getLevel().isGreaterOrEqual(level)) {
            return false;
        }

        if (logger != null && !event.getLoggerName().equals(logger)) {
            return false;
        }

        if (thread != null && !event.getThreadName().equals(thread)) {
            return false;
        }

        if (regex == null && regexStr != null) {
            regex = Pattern.compile(regexStr, RegexUtils.parseFlags(regexFlags));
        }

        if (regex != null && !doMatch(event)) {
            return false;
        }

        return true;
    }

    public void setDepth(Integer depth) {
        this.depth = depth;
    }

    public void setIncludeInKey(String includeInKey) {
        Preconditions.checkNotNull(includeInKey);
        String[] keyNames = includeInKey.split(INCLUDE_IN_KEY_DELIMITER);
        hashedKeyElements.clear();
        Collections.addAll(hashedKeyElements, keyNames);
    }

    public void setLevel(String level) {
        Preconditions.checkNotNull(level);
        this.level = Level.valueOf(level);
    }

    public void setLogger(String logger) {
        this.logger = logger;
    }

    public void setMatchException(Boolean matchException) {
        this.matchException = matchException;
    }

    public void setRegex(String regexStr) {
        this.regexStr = regexStr;
    }

    public void setRegexFlags(String regexFlags) {
        this.regexFlags = regexFlags;
    }

    public void setSeverity(String severity) {
        Preconditions.checkNotNull(severity);
        this.severity = Severity.valueOf(severity);
    }

    public void setThread(String thread) {
        this.thread = thread;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Element toElement() {
        Element alarmType = new Element(ALARM_TYPE_TAG);
        if (depth != null) {
            alarmType.addContent(new Element(DEPTH_TAG)
            .setText(String.valueOf(depth)));
        }
        String key = getIncludeInKey();
        if (key != null && !key.isEmpty()) {
            alarmType.addContent(new Element(INCLUDE_IN_KEY_TAG)
            .setText(key));
        }
        if (level != null) {
            alarmType.addContent(new Element(LEVEL_TAG)
            .setText(level.toString()));
        }
        if (logger != null && !logger.isEmpty()) {
            alarmType.addContent(new Element(LOGGER_TAG)
            .setText(logger));
        }
        if (matchException != null && matchException) {
            alarmType.addContent(new Element(MATCH_EXCEPTION_TAG)
            .setText(matchException.toString()));
        }
        if (regexStr != null && !regexStr.isEmpty()) {
            alarmType.addContent(new Element(REGEX_TAG)
            .setText(regexStr));
        }
        if (regexFlags != null && !regexFlags.isEmpty()) {
            alarmType.addContent(new Element(REGEX_FLAGS_TAG)
            .setText(regexFlags));
        }
        if (severity != null) {
            alarmType.addContent(new Element(IAlarms.SEVERITY_TAG)
            .setText(severity.toString()));
        }
        if (thread != null && !thread.isEmpty()) {
            alarmType.addContent(new Element(THREAD_TAG)
            .setText(thread));
        }
        if (type != null && !type.isEmpty()) {
            alarmType.addContent(new Element(IAlarms.TYPE_TAG)
            .setText(type));
        }
        return alarmType;
    }

    public void validate() throws AlarmDefinitionValidationException {
        if (type == null) {
            throw new AlarmDefinitionValidationException(IAlarms.TYPE_TAG + REQUIRED);
        }
        if (level == null) {
            throw new AlarmDefinitionValidationException(LEVEL_TAG + REQUIRED);
        }
        if (hashedKeyElements.isEmpty()) {
            throw new AlarmDefinitionValidationException
                (INCLUDE_IN_KEY_TAG + REQUIRED);
        }
        if (logger == null && regexStr == null) {
            throw new AlarmDefinitionValidationException
                ("either " + LOGGER_TAG + " or " + REGEX_TAG + REQUIRED);
        }
    }

    public void validateAndSet(String name, String value)
                    throws AlarmDefinitionValidationException {
        value = value.trim();

        if (value.length() == 0 || RM.equals(value)) {
            value = null;
        }

        switch(name) {
            case DEPTH_TAG:
                try {
                    if (value == null) {
                        depth = null;
                    } else {
                        depth = Integer.parseInt(value);
                    }
                } catch (NumberFormatException e) {
                    throw new AlarmDefinitionValidationException(value
                                    + " is not an integer");
                }
                break;
            case INCLUDE_IN_KEY_TAG:
                if (value == null) {
                    hashedKeyElements.clear();
                    return;
                }

                String[] parts = value.split("[\\s]");
                for (String part : parts) {
                    if (part.startsWith(IAlarms.GROUP_TAG)) {
                        try {
                             Integer.parseInt(part.substring(5));
                        } catch (NumberFormatException e) {
                            throw new AlarmDefinitionValidationException
                                (IAlarms.GROUP_TAG + " must end in an integer");
                        }
                    } else if (!KEY_VALUES.contains(part.trim())) {
                        throw new AlarmDefinitionValidationException
                            (part + " is not a valid key field");
                    }
                }
                setIncludeInKey(value);
                break;
            case LEVEL_TAG:
                if (value == null) {
                    level = null;
                    return;
                }

                if (!LEVEL_VALUES.contains(value)) {
                    throw new AlarmDefinitionValidationException(value
                                    + " is not a valid level");
                }
                setLevel(value);
                break;
            case MATCH_EXCEPTION_TAG:
                if (value == null) {
                    matchException = null;
                    return;
                }
                try {
                    matchException = Boolean.parseBoolean(value);
                } catch (Exception e) {
                    throw new AlarmDefinitionValidationException(value
                                    + " is not a boolean");
                }
                break;
            case REGEX_TAG:
                if (value == null) {
                    regexStr = null;
                    return;
                }

                try {
                    Pattern.compile(value);
                } catch (PatternSyntaxException e) {
                    throw new AlarmDefinitionValidationException(value
                                    + " is not a valid regular expression");
                }
                regexStr = value;
                break;
            case REGEX_FLAGS_TAG:
                if (value == null) {
                    regexFlags = null;
                    return;
                }

                parts = value.split("[|]");
                for (String part : parts) {
                    if (!RegexUtils.FLAG_VALUES.contains(part.trim())) {
                        throw new AlarmDefinitionValidationException(part
                                        + " is not a valid flag");
                    }
                }

                regexFlags = value;
                break;
            case IAlarms.SEVERITY_TAG:
                if (value == null) {
                    severity = null;
                    return;
                }

                if (!Severity.asList().contains(value)) {
                    throw new AlarmDefinitionValidationException(value
                                    + " is not a valid severity value");
                }
                setSeverity(value);
                break;
            case LOGGER_TAG:
                logger = value;
                break;
            case THREAD_TAG:
                thread = value;
                break;
            case IAlarms.TYPE_TAG:
                type = value;
                break;
            default:
                throw new AlarmDefinitionValidationException
                    ("unrecognized attribute: " + name);
        }
    }

    private boolean doMatch(ILoggingEvent event) {
        if (regex.matcher(event.getFormattedMessage()).find()) {
            return true;
        }

        int d = depth == null ? Integer.MAX_VALUE : depth - 1;

        if (matchException) {
            IThrowableProxy p = event.getThrowableProxy();
            while (p != null && d >= 0) {
                if (regex.matcher(p.getMessage()).find()) {
                    return true;
                }
                p = p.getCause();
                d--;
            }
        }

        return false;
    }
}

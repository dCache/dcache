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
package org.dcache.alarms.jdom;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.dcache.alarms.Alarm;
import org.dcache.alarms.AlarmDefinition;
import org.dcache.alarms.AlarmDefinitionValidationException;
import org.dcache.util.RegexUtils;

/**
 * Implements JDOM marshaling and unmarshaling methods.  Definition
 * can be stored to XML file or database.
 *
 * @author arossi
 */
public final class JDomAlarmDefinition implements AlarmDefinition {
    public static final String ROOT_NAME = "definitions";

    private static final String UNINTIALIZED = "INVALID";

    private final Set<String> hashedKeyElements = Sets.newHashSet();

    private Boolean matchException = false;
    private String type = UNINTIALIZED;
    private String regexStr = UNINTIALIZED;
    private String regexFlags;
    private Integer depth;
    private Pattern regex;

    public JDomAlarmDefinition() {
    }

    public JDomAlarmDefinition(Element alarmType) {
        Element child = alarmType.getChild(TYPE_TAG);
        if (child != null) {
            setType(child.getTextTrim());
        }
        child = alarmType.getChild(KEY_WORDS_TAG);
        if (child != null) {
            setKeyWords(child.getTextTrim());
        }
        child = alarmType.getChild(REGEX_TAG);
        if (child != null) {
            setRegexString(child.getTextTrim());
        }
        child = alarmType.getChild(REGEX_FLAGS_TAG);
        if (child != null) {
            setRegexFlags(child.getTextTrim());
        }
        child = alarmType.getChild(MATCH_EXCEPTION_TAG);
        if (child != null) {
            setMatchException(Boolean.valueOf(child.getTextTrim()));
        }
        child = alarmType.getChild(DEPTH_TAG);
        if (child != null) {
            setDepth(Integer.parseInt(child.getTextTrim()));
        }
    }

    public String createKey(String formattedMessage,
                            long timestamp,
                            String host,
                            String domain,
                            String service) {
        StringBuilder key = new StringBuilder();

        for (String s : hashedKeyElements) {
            if (s.startsWith(GROUP_TAG)) {
                Matcher m = getRegexPattern().matcher(formattedMessage);
                if (m.find()) {
                    int group = Integer.parseInt(s.substring(GROUP_TAG_LENGTH));
                    key.append(m.group(group));
                    continue;
                }
            }

            switch (s) {
                case TIMESTAMP_TAG:
                    key.append(timestamp);
                    break;
                case MESSAGE_TAG:
                    key.append(formattedMessage);
                    break;
                case TYPE_TAG:
                    key.append(type);
                    break;
                case Alarm.HOST_TAG:
                    key.append(host);
                    break;
                case Alarm.DOMAIN_TAG:
                    key.append(domain);
                    break;
                case Alarm.SERVICE_TAG:
                    key.append(service);
                    break;
            }
        }
        return key.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof JDomAlarmDefinition)) {
            return false;
        }

        return type.equals(((JDomAlarmDefinition)obj).getType());
    }

    public Integer getDepth() {
        return depth;
    }

    public String getKeyWords() {
        StringBuilder result = new StringBuilder();
        Joiner.on(" ").appendTo(result, hashedKeyElements.iterator());
        if (result.length() == 0) {
            return null;
        }
        return result.toString();
    }

    @Override
    public String getRegexFlags() {
        return regexFlags;
    }

    @Override
    public Pattern getRegexPattern() {
        if (regex == null) {
            if (regexFlags == null) {
                regex = Pattern.compile(regexStr);
            } else {
                regex = Pattern.compile(regexStr,
                                        RegexUtils.parseFlags(regexFlags));
            }
        }
        return regex;
    }

    @Override
    public String getRegexString() {
        return regexStr;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public Boolean isMatchException() {
        return matchException;
    }

    @Override
    public void setDepth(Integer depth) {
        this.depth = depth;
    }

    @Override
    public void setKeyWords(String keyWords) {
        Preconditions.checkNotNull(keyWords);
        String[] keyNames = keyWords.split(KEY_WORD_DELIMITER);
        hashedKeyElements.clear();
        Collections.addAll(hashedKeyElements, keyNames);
    }

    @Override
    public void setMatchException(Boolean matchException) {
        this.matchException = matchException;
    }

    @Override
    public void setRegexFlags(String regexFlags) {
        this.regexFlags = regexFlags;
    }

    @Override
    public void setRegexString(String regexString) {
        this.regexStr = Preconditions.checkNotNull(regexString);
    }

    @Override
    public void setType(String type) {
        this.type = Preconditions.checkNotNull(type);
    }

    public Element toElement() throws JDOMException {
        try {
            validate();
        } catch (AlarmDefinitionValidationException e) {
            throw new JDOMException("toElement() failed.", e);
        }

        Element alarmType = new Element(ALARM_TAG);

        alarmType.addContent(new Element(TYPE_TAG).setText(type));

        String key = getKeyWords();
        alarmType.addContent(new Element(KEY_WORDS_TAG).setText(key));

        alarmType.addContent(new Element(REGEX_TAG).setText(regexStr));

        if (regexFlags != null && !regexFlags.isEmpty()) {
            alarmType.addContent(new Element(REGEX_FLAGS_TAG).setText(regexFlags));
        }

        if (matchException != null && matchException) {
            alarmType.addContent(new Element(MATCH_EXCEPTION_TAG)
                                     .setText(matchException.toString()));
        }

        if (depth != null) {
            alarmType.addContent(new Element(DEPTH_TAG)
                                     .setText(String.valueOf(depth)));
        }

        return alarmType;
    }

    public String toString() {
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        StringWriter writer = new StringWriter();
        try {
            toXML(outputter, writer);
        } catch (Exception t) {
            throw new RuntimeException(t);
        }
        return writer.toString();
    }

    public void toXML(XMLOutputter outputter, Writer writer) throws IOException,
         JDOMException {
        writer.append("Alarm Definition:")
              .append("\n----------------------------------\n");
        outputter.output(toElement(), writer);
        writer.append("\n----------------------------------");
    }

    @Override
    public void validate() throws AlarmDefinitionValidationException {
        if (UNINTIALIZED.equals(type)) {
            throw new AlarmDefinitionValidationException
                (TYPE_TAG + REQUIRED);
        }
        if (hashedKeyElements.isEmpty()) {
            throw new AlarmDefinitionValidationException
                (KEY_WORDS_TAG + REQUIRED);
        }
        if (UNINTIALIZED.equals(regexStr)) {
            throw new AlarmDefinitionValidationException
                (REGEX_TAG + REQUIRED);
        }
    }

    @Override
    public void validateAndSet(String name, String value)
                    throws AlarmDefinitionValidationException {
        value = Strings.nullToEmpty(value).trim();

        if (value.equals(UNINTIALIZED)) {
            throw new AlarmDefinitionValidationException("Attributes cannot be "
                            + "set to the value " + UNINTIALIZED +".");
        }

        if (value.isEmpty() || RM.equals(value)) {
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
            case KEY_WORDS_TAG:
                if (value == null) {
                    throw new AlarmDefinitionValidationException(KEY_WORDS_TAG
                                    + REQUIRED);
                }

                String[] parts = value.split("[\\s]");
                for (String part : parts) {
                    if (part.startsWith(GROUP_TAG)) {
                        try {
                             Integer.parseInt(part.substring(GROUP_TAG.length()));
                        } catch (NumberFormatException e) {
                            throw new AlarmDefinitionValidationException
                                (GROUP_TAG + " must end in an integer");
                        }
                    } else if (!KEY_VALUES.contains(part.trim())) {
                        throw new AlarmDefinitionValidationException
                            (part + " is not a valid key field");
                    }
                }

                setKeyWords(value);
                break;
            case MATCH_EXCEPTION_TAG:
                if (value == null) {
                    matchException = null;
                    return;
                }

                if (!value.equalsIgnoreCase("true")
                                && !value.equalsIgnoreCase("false")) {
                    throw new AlarmDefinitionValidationException(value
                                    + " is not a boolean (true|false)");
                }

                matchException = Boolean.valueOf(value);
                break;
            case REGEX_TAG:
                if (value == null) {
                    throw new AlarmDefinitionValidationException(REGEX_TAG
                                    + REQUIRED);
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

                value = value.toUpperCase();
                parts = value.split("[|]");
                for (String part : parts) {
                    if (!RegexUtils.FLAG_VALUES.contains(part.trim())) {
                        throw new AlarmDefinitionValidationException(part
                                        + " is not a valid flag");
                    }
                }

                regexFlags = value;
                break;
            case TYPE_TAG:
                if (value == null) {
                    throw new AlarmDefinitionValidationException(TYPE_TAG
                                    + REQUIRED);
                }

                if (!UNINTIALIZED.equals(type)) {
                    throw new AlarmDefinitionValidationException(TYPE_TAG
                                    + ", once set, cannot be modified.");
                }

                type = value;
                break;
            default:
                throw new AlarmDefinitionValidationException
                    ("unrecognized attribute: " + name);
        }
    }
}

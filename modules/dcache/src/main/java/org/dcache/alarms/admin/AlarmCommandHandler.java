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
package org.dcache.alarms.admin;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.StringWriter;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.Callable;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.alarms.AlarmDefinition;
import org.dcache.alarms.AlarmDefinitionValidationException;
import org.dcache.alarms.AlarmDefinitionsMap;
import org.dcache.alarms.AlarmPriority;
import org.dcache.alarms.AlarmPriorityMap;
import org.dcache.alarms.jdom.JDomAlarmDefinition;
import org.dcache.alarms.shell.ListPredefinedTypes;
import org.dcache.alarms.shell.SendAlarmCLI;

/**
 * Provides commands for adding, setting, removing, listing, loading and saving
 * external (custom) alarm definitions; for setting, listing, loading and saving
 * priority mappings, for setting the priority default or for restoring the
 * default value to all alarm types, and for pausing and restarting the server.
 *
 * @author arossi
 */
public final class AlarmCommandHandler implements CellCommandListener {
    @Command(name = "definition add",
                    hint = "Add a new custom definition; if a definition of "
                                    + "this type already exists, the new "
                                    + "definition will overwrite it.",
                    description = "To save this "
                                    + "definition for future reloading, "
                                    + "use the 'definition save' command.")
    class DefinitionAddCommand implements Callable<String> {
        @Option(name = "type",
                usage = "Name of alarm type.",
                required=true)
        String type;

        @Option(name = "keyWords",
                usage = "Whitespace delimited set of attribute names whose "
                                + "values constitute a unique identifier for "
                                + "this alarm.  For possible attribute names,"
                                + "use the command 'definition keywords'.",
                required=true)
        String keyWords;

        @Option(name = "regex",
                usage = "Java-style regular expression used to match messages.",
                required=true)
        String regex;

        @Option(name = "regexFlags",
                usage = "Java-style flag options for regex; join using '|' (or).",
                valueSpec="CASE_INSENSITIVE|MULTILINE|DOTALL|UNICODE_CASE|"
                                + "CANON_EQ|LITERAL|COMMENTS|UNIX_LINES ")
        String regexFlags;

        @Option(name = "matchException",
                usage = "Apply the regex to nested exception messages "
                                + "(default is false).",
                valueSpec="true|false ")
        String matchException;

        @Option(name = "depth",
                usage = "Match nested exception messages only down to this level"
                    + " (integer; default is undefined).")
        String depth;

        public String call() throws Exception {
            AlarmDefinition definition = new JDomAlarmDefinition();
            try {
                definition.validateAndSet(AlarmDefinition.TYPE_TAG,
                                          type);
                definition.validateAndSet(AlarmDefinition.KEY_WORDS_TAG,
                                          keyWords);
                definition.validateAndSet(AlarmDefinition.REGEX_TAG,
                                          regex);
                definition.validateAndSet(AlarmDefinition.REGEX_FLAGS_TAG,
                                          regexFlags);
                definition.validateAndSet(AlarmDefinition.MATCH_EXCEPTION_TAG,
                                          matchException);
                definition.validateAndSet(AlarmDefinition.DEPTH_TAG, depth);
            } catch (AlarmDefinitionValidationException e) {
                return "Invalid definition: " + e.getMessage();
            } catch (Exception e) {
                return "Failed to create defintion: " + e.getMessage();
            }

            alarmDefinitionsMap.add(definition);

            return "Added:\n\n" + definition.toString();
        }
    }

    @Command(name = "definition keywords",
             hint = "Print the list of attribute names whose values can be "
                     + "used as keyword identifiers for the alarm.",
             description = "These are attributes which belong to all "
                             + "alarm definitions; 'N' in 'groupN' refers to "
                             + "the regular expression group index, with "
                             + "group0 meaning the entire message.")
    class DefinitionKeywordsCommand implements Callable<String> {
        public String call() throws Exception {
            StringBuilder builder = new StringBuilder();
            builder.append("POSSIBLE KEY WORD NAMES:\n\n");
            for (String key: AlarmDefinition.KEY_VALUES) {
                builder.append(key).append("\n");
            }
            builder.append("---------------------------\n");
            return builder.toString();
        }
    }

    @Command(name = "definition ls",
                    hint = "Print a single definition or sorted list of definitions.",
                    description = "External/custom definitions are those provided by users; "
                                   + "they match against logging message content "
                                   + "when the event arrives at the remote server "
                                   + "and mark them matched events as alarms/alerts.")
    class DefinitionListCommand implements Callable<String> {
        @Argument(required = false,
                  usage="Name of alarm type; if not specified, all are listed.")
        String type;

        public String call() throws Exception {
            if (Strings.emptyToNull(type) == null) {
                return listCustomDefinitions();
            }
            try {
                AlarmDefinition definition = alarmDefinitionsMap.getDefinition(type);
                return definition.toString();
            } catch (NoSuchElementException noSuchDef) {
                return noSuchDef.getMessage();
            }
        }
    }

    @Command(name = "definition reload",
                    hint = "Reinitialize the definitions from the saved changes.",
                    description = "The default implementation of the backup store"
                                        + " is an XML file.")
    class DefinitionReloadCommand implements Callable<String> {
        @Argument(required = false,
                  usage="Optional path of an alternative XML file to use. NOTE: "
                                  + "the path defined in the local dcache.conf "
                                  + "or layout file will remain as the working "
                                  + "store after this command completes.")
        String path;

        public String call() throws Exception {
            Properties env = new Properties();
            if (Strings.emptyToNull(path) != null) {
                env.setProperty(AlarmPriorityMap.PATH, path);
            }
            alarmDefinitionsMap.load(env);
            return listCustomDefinitions();
        }
    }

    @Command(name = "definition rm",
             hint = "Remove the existing alarm definition.",
             description = "To remove this definition from future reloading, "
                                   + "use the 'definitions save' command.")
    class DefinitionRemoveCommand implements Callable<String> {
        @Argument(required = true,
                  usage="Name of alarm type.")
        String type;

        public String call() throws Exception {
            if (alarmDefinitionsMap.removeDefinition(type) == null) {
                return type + " undefined; nothing removed.";
            }
            return listCustomDefinitions();
        }
    }

    @Command(name = "definition save",
                    hint = "Save the current definitions to persistent backup.",
                    description = "The default implementation of the backup store"
                                        + " is an XML file).")
    class DefinitionSaveCommand implements Callable<String> {
        @Argument(required = false,
                  usage="Optional path of an alternative XML file to use. NOTE: "
                                      + "the path defined in the local dcache.conf "
                                      + "or layout file will remain as the working "
                                      + "store after this command completes.")
        String path;

        public String call() throws Exception {
            Properties env = new Properties();
            if (Strings.emptyToNull(path) != null) {
                env.setProperty(AlarmPriorityMap.PATH, path);
            }
            alarmDefinitionsMap.save(env);
            return listCustomDefinitions();
        }
    }

    @Command(name = "definition set",
             hint = "Set the attribute of an existing alarm definition.",
             description = "To save this definition for future reloading, "
                                   + "use the 'definitions save' command.")
    class DefinitionSetCommand implements Callable<String> {
        @Argument(index=0,
                  required = true,
                  usage="Name of alarm type.")
        String type;

        @Argument(index=1,
                  required = true,
                  usage="Name of alarm attribute.")
        String name;

        @Argument(index=2,
                  required = true,
                  usage="Value of alarm attribute.")
        String value;

        public String call() throws Exception {
            AlarmDefinition definition;

            try {
                definition = alarmDefinitionsMap.getDefinition(type);
            } catch (NoSuchElementException e) {
                return e.getMessage();
            }

            if (value.trim().equals(AlarmDefinition.RM)) {
                return "To remove an attribute definition, "
                                + "use the 'definition unset' command.";
            }

            try {
                definition.validateAndSet(name, value);
            } catch (AlarmDefinitionValidationException e) {
                return "Invalid definition: " + e.getMessage();
            }

            return "Modified:\n\n" + definition.toString();
        }
    }

    @Command(name = "definition unset",
             hint = "Unset (remove) the attribute of an existing alarm definition.",
             description = "To save this definition for future reloading, "
                                   + "use the 'definitions save' command.")
    class DefinitionUnsetCommand implements Callable<String> {
        @Argument(index=0,
                  required = true,
                  usage="Name of alarm type.")
        String type;

        @Argument(index=1,
                  required = true,
                  usage="Name of alarm attribute.")
        String name;

        public String call() throws Exception {
            AlarmDefinition definition;

            try {
                definition = alarmDefinitionsMap.getDefinition(type);
            } catch (NoSuchElementException e) {
                return e.getMessage();
            }

            try {
                definition.validateAndSet(name, AlarmDefinition.RM);
            } catch (AlarmDefinitionValidationException e) {
                return "Invalid definition: " + e.getMessage();
            }

            return "Modified:\n\n" + definition.toString();
        }
    }

    @Command(name = "predefined ls",
                    hint = "Print a list of all internally defined alarms.")
    class PredifinedListCommand implements Callable<String> {
        public String call() throws Exception {
            return "PREDEFINED DCACHE ALARM TYPES:\n\n"
                            + ListPredefinedTypes.getSortedList();
        }
    }

    @Command(name = "priority get default",
             hint = "Get the current default alarm priority value.")
    class PriorityGetDefaultCommand implements Callable<String> {
        public String call() throws Exception {
            return "Current default priority value is "
                            + alarmPriorityMap.getDefaultPriority() + ".";
        }
    }

    @Command(name = "priority ls",
                    hint = "Print a single priority level or sorted list of "
                                    + "priority levels for all known alarms.",
                    description = "There is only one set of such mappings used by any "
                                    + "given instance of the alarm service.")
    class PriorityListCommand implements Callable<String> {
        @Argument(required = false,
                  usage="Name of alarm type; if not specified, all are listed.")
        String type;

        public String call() throws Exception {
            if (Strings.emptyToNull(type) == null) {
                return listPriorityMappings();
            }

            try {
                return "Alarm type " + type + " currently set to "
                                     + alarmPriorityMap.getPriority(type) + ".";
            } catch (NoSuchElementException noSuchDef) {
                return noSuchDef.getMessage();
            }
        }
    }

    @Command(name = "priority reload",
                    hint = "Reinitialize priority mappings from saved changes.",
                    description = "Searches for internal and external alarm "
                                    + "types, initializing them all to the default "
                                    + "value, and then overriding these with the "
                                    + "values that have been saved to the "
                                    + "backup storage (the default implementation "
                                    + "of which is a properties file).")
    class PriorityReloadCommand implements Callable<String> {
        @Argument(required = false,
                  usage="Optional path of an alternative properties file to use. "
                                  + "NOTE: the path defined in the local dcache.conf "
                                  + "or layout file will remain as the working "
                                  + "store after this command completes.")
        String path;

        public String call() throws Exception {
            Properties env = new Properties();
            if (Strings.emptyToNull(path) != null) {
                env.setProperty(AlarmPriorityMap.PATH, path);
            }
            alarmPriorityMap.load(env);
            return listPriorityMappings();
        }
    }

    @Command(name = "priority restore all",
             hint = "Set all defined alarms to the current default priority value.",
             description = "Modifies the internal mapping; to save values for future "
                             + "reloading, use the 'priority save' command.")
    class PriorityRestoreAllCommand implements Callable<String> {
        public String call() throws Exception {
            alarmPriorityMap.restoreAllToDefaultPriority();
            return listPriorityMappings();
        }
    }

    @Command(name = "priority save",
                    hint = "Save the current priority mappings to persistent backup.",
                    description = "The default implementation of the backup store"
                                    + " is a properties file.")
    class PrioritySaveCommand implements Callable<String> {
        @Argument(required = false,
                  usage="Optional path of an alternative properties file to use. "
                                    + "NOTE: the path defined in the local dcache.conf "
                                    + "or layout file will remain as the working "
                                    + "store after this command completes.")
        String path;

        public String call() throws Exception {
            Properties env = new Properties();
            if (Strings.emptyToNull(path) != null) {
                env.setProperty(AlarmPriorityMap.PATH, path);
            }
            alarmPriorityMap.save(env);
            return listPriorityMappings();
        }
    }

    @Command(name = "priority set",
                    hint = "Set the priority of the alarm type.",
                    description = "The alarm must be either internal (predefined) or "
                                    + "custom (external); to see all current alarms, "
                                    + "use the 'priority list' command. To save "
                                    + "this mapping for future reloading, use the "
                                    + "'priority save' command.")
    class PrioritySetCommand implements Callable<String> {
        @Argument(index=0,
                  required = true,
                  usage="Name of alarm type (case sensitive); by convention, "
                                  + "internal alarm types are in upper case.")
        String type;

        @Argument(index=1,
                  required = true,
                  usage="New priority level to which to set this alarm.",
                  valueSpec="LOW|MODERATE|HIGH|CRITICAL ")
        String priority;

        public String call() throws Exception {
            try {
                alarmPriorityMap.setPriority(type,
                                AlarmPriority.valueOf(priority.toUpperCase()));
            } catch (NoSuchElementException noSuchDef) {
                return noSuchDef.getMessage();
            }
            return "Alarm type " + type + " has now been set to priority "
                + priority + "; to save this mapping for future reloading, "
                                + "use the 'priority save' command.";
        }
    }

    @Command(name = "priority set default",
             hint = "Set the default alarm priority value.")
    class PrioritySetDefaultCommand implements Callable<String> {
        @Argument(required=true,
                  valueSpec="LOW|MODERATE|HIGH|CRITICAL ")
        String priority;

        public String call() throws Exception {
            alarmPriorityMap.setDefaultPriority(priority.toUpperCase());
            return "Default priority value is now set to " + priority + ".";
        }
    }

    @Command(name = "send",
             hint = "Send an alarm to the alarm service.",
             description = "The alarm service host and port are those "
                           + "currently defined by the properties "
                           + "dcache.log.server.host and "
                           + "alarms.net.port.")
    class SendCommand implements Callable<String> {
        @Option(name = "t",
                usage = "Send an alarm of this predefined type; if"
                                + " undefined, an attempt will be made by the "
                                + " server to infer the type by matching against "
                                + " any custom definitions provided; failing "
                                + " that, the alarm will be marked 'GENERIC'.")
        String type;

        @Option(name = "d",
                usage = "Optional name of domain of origin of the alarm "
                                + "(defaults to '<na>').")
        String domain = MDC.get(CDC.MDC_DOMAIN);

        @Option(name = "s",
                usage = "Optional name of service of origin of the alarm "
                                + "(defaults to 'user-command').")
        String service = MDC.get(CDC.MDC_CELL);

        @Argument(required = true,
                  usage = "The actual alarm message (in single or double quotes).")
        String message;

        @Override
        public String call() throws Exception {
            SendAlarmCLI.sendEvent(serverHost, serverPort, SendAlarmCLI.createEvent(domain, service, type, message));
            return "sending alarm to " + serverHost + ":" + serverPort;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmCommandHandler.class);

    private AlarmPriorityMap alarmPriorityMap;
    private AlarmDefinitionsMap alarmDefinitionsMap;
    private String serverHost;
    private Integer serverPort;

    public void setAlarmDefinitionsMap(AlarmDefinitionsMap alarmDefinitionsMap) {
        this.alarmDefinitionsMap = alarmDefinitionsMap;
    }

    public void setAlarmPriorityMap(AlarmPriorityMap alarmPriorityMap) {
        this.alarmPriorityMap = alarmPriorityMap;
    }

    public void setServerHost(String serverHost) {
        this.serverHost = serverHost;
    }

    public void setServerPort(Integer serverPort) {
        this.serverPort = serverPort;
    }

    private String listCustomDefinitions() {
        StringWriter writer = new StringWriter();
        try {
            alarmDefinitionsMap.getSortedList(writer);
            return "Current (external) definitions:\n\n " + writer.toString();
        } catch (Exception e) {
            Throwable cause = e.getCause();
            String message = cause == null ? e.getMessage() : cause.getMessage();
            return "There is a problem with one or more definitions: "
                            + message;
        }
    }

    private String listPriorityMappings() {
        return "Current priority mappings:\n\n" + alarmPriorityMap.getSortedList();
    }
}

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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Marker;

import java.util.List;
import java.util.Map;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.Args;
import org.dcache.util.NetworkUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps the parsing and management of shell command-line arguments for sending
 * an alarm.
 *
 * @author arossi
 */
public final class AlarmArguments {
    /*
     * Command-line options for properties when sending an alarm.
     */
    public static final String TYPE_OPT = "t";
    public static final String SRC_HOST_OPT = "h";
    public static final String SRC_DOMAIN_OPT = "d";
    public static final String SRC_SERVICE_OPT = "s";

    /*
     * Not exposed to user, but used by internal command classes.
     */
    public static final String DST_HOST_OPT = "r";
    public static final String DST_PORT_OPT = "p";

    private static final String DEFAULT_PORT = "9867";

    static final Map<String, String> HELP_MESSAGES
        = ImmutableMap.of
            (TYPE_OPT,        "-t=<type>        (optional): predefined alarm subtype tag",
             SRC_HOST_OPT,    "-h=<host>        (optional): source host name",
             SRC_DOMAIN_OPT,  "-d=<domain>      (optional): source domain name",
             SRC_SERVICE_OPT, "-s=<service>     (optional): source service/cell name");

    final Marker marker;
    final String sourceHost;
    final String sourceService;
    final String sourceDomain;
    final String destinationHost;
    final String destinationPort;
    final String message;

    /**
     * Actual object is for package use only.
     */
    AlarmArguments(Args parsed) throws Exception {
        List<String> arguments = parsed.getArguments();
        checkArgument(arguments.size() > 0,
                        "Please provide a non-zero-length alarm message.");
        message = Joiner.on(" ").join(arguments);

        destinationHost = checkNotNull(parsed.getOption(DST_HOST_OPT));

        String arg = parsed.getOption(DST_PORT_OPT);
        if (arg != null) {
            destinationPort = arg;
        } else {
            destinationPort = DEFAULT_PORT;
        }

        arg = parsed.getOption(SRC_HOST_OPT);
        if (arg != null) {
            sourceHost = arg;
        } else {
            sourceHost = NetworkUtils.getCanonicalHostName();
        }

        arg = parsed.getOption(SRC_DOMAIN_OPT);
        if ( arg != null) {
            sourceDomain = arg;
        } else {
            sourceDomain = "<na>";
        }

        arg = parsed.getOption(SRC_SERVICE_OPT);
        if ( arg != null) {
            sourceService = arg;
        } else {
            sourceService = "user-command";
        }

        PredefinedAlarm type = null;
        arg = Strings.emptyToNull(parsed.getOption(TYPE_OPT));
        if (arg != null) {
            try {
                type = PredefinedAlarm.valueOf(arg.toUpperCase());
            } catch (IllegalArgumentException noSuchType) {
                // just allow the null type to stand
            }
        }

        marker = AlarmMarkerFactory.getMarker(type);
    }
}
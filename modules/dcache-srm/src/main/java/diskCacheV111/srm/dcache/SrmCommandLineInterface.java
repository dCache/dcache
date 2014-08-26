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
package diskCacheV111.srm.dcache;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellInfoProvider;

import org.dcache.commons.util.Strings;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.request.Job;
import org.dcache.srm.util.Configuration;
import org.dcache.util.Args;

public class SrmCommandLineInterface
        implements CellCommandListener, CellInfoProvider
{
    private static final Logger logger = LoggerFactory.getLogger(SrmCommandLineInterface.class);

    private static final ImmutableMap<String, String> OPTION_TO_PARAMETER_SET =
            new ImmutableMap.Builder<String, String>()
                    .put("get", Configuration.GET_PARAMETERS)
                    .put("put", Configuration.PUT_PARAMETERS)
                    .put("ls", Configuration.LS_PARAMETERS)
                    .put("bringonline", Configuration.BRINGONLINE_PARAMETERS)
                    .put("reserve", Configuration.RESERVE_PARAMETERS)
                    .build();

    private SRM srm;
    private Configuration config;

    public void setSrm(SRM srm)
    {
        this.srm = srm;
    }

    public void setConfiguration(Configuration configuration)
    {
        this.config = configuration;
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.println(config);
        pw.println(srm.getSchedulerInfo());
    }

    @Override
    public CellInfo getCellInfo(CellInfo info)
    {
        return info;
    }

    public static final String fh_cancel = " Syntax: cancel <id> ";
    public static final String hh_cancel = " <id> ";

    public String ac_cancel_$_1(Args args)
    {
        try {
            Long id = Long.valueOf(args.argv(0));
            StringBuilder sb = new StringBuilder();
            srm.cancelRequest(sb, id);
            return sb.toString();
        } catch (SRMInvalidRequestException ire) {
            return "Invalid request: " + ire.getMessage();
        } catch (NumberFormatException e) {
            return e.toString();
        }
    }

    public static final String fh_cancelall = " Syntax: cancel [-get] [-put] [-copy] [-bring] [-reserve] <pattern> ";
    public static final String hh_cancelall = " [-get] [-put] [-copy] [-bring] [-reserve] <pattern> ";

    public String ac_cancelall_$_1(Args args)
    {
        try {
            boolean get = args.hasOption("get");
            boolean put = args.hasOption("put");
            boolean copy = args.hasOption("copy");
            boolean bring = args.hasOption("bring");
            boolean reserve = args.hasOption("reserve");
            boolean ls = args.hasOption("ls");
            if (!get && !put && !copy && !bring && !reserve && !ls) {
                get = true;
                put = true;
                copy = true;
                bring = true;
                reserve = true;
                ls = true;
            }
            String pattern = args.argv(0);
            StringBuilder sb = new StringBuilder();
            if (get) {
                logger.debug("calling srm.cancelAllGetRequest(\"" + pattern + "\")");
                srm.cancelAllGetRequest(sb, pattern);
            }
            if (bring) {
                logger.debug("calling srm.cancelAllBringOnlineRequest(\"" + pattern + "\")");
                srm.cancelAllBringOnlineRequest(sb, pattern);
            }
            if (put) {
                logger.debug("calling srm.cancelAllPutRequest(\"" + pattern + "\")");
                srm.cancelAllPutRequest(sb, pattern);
            }
            if (copy) {
                logger.debug("calling srm.cancelAllCopyRequest(\"" + pattern + "\")");
                srm.cancelAllCopyRequest(sb, pattern);
            }
            if (reserve) {
                logger.debug("calling srm.cancelAllReserveSpaceRequest(\"" + pattern + "\")");
                srm.cancelAllReserveSpaceRequest(sb, pattern);
            }
            if (ls) {
                logger.debug("calling srm.cancelAllLsRequests(\"" + pattern + "\")");
                srm.cancelAllLsRequests(sb, pattern);
            }
            return sb.toString();
        } catch (DataAccessException | SRMException e) {
            logger.warn("Failure in cancelall: " + e.getMessage());
            return e.toString();
        }
    }

    public static final String fh_ls = " Syntax: ls [-get] [-put] [-copy] [-bring] [-reserve] [-ls] [-l] [<id>] " +
            "#will list all requests";
    public static final String hh_ls = " [-get] [-put] [-copy] [-bring] [-reserve] [-ls] [-l] [<id>]";

    public String ac_ls_$_0_1(Args args) throws SRMInvalidRequestException, DataAccessException
    {
        boolean get = args.hasOption("get");
        boolean put = args.hasOption("put");
        boolean copy = args.hasOption("copy");
        boolean bring = args.hasOption("bring");
        boolean reserve = args.hasOption("reserve");
        boolean ls = args.hasOption("ls");
        boolean longformat = args.hasOption("l");
        StringBuilder sb = new StringBuilder();
        if (args.argc() == 1) {
            try {
                Long reqId = Long.valueOf(args.argv(0));
                srm.listRequest(sb, reqId, longformat);
            } catch (NumberFormatException nfe) {
                return "id must be an integer, you gave id=" + args.argv(0);
            }
        } else {
            if (!get && !put && !copy && !bring && !reserve && !ls) {
                get = true;
                put = true;
                copy = true;
                bring = true;
                reserve = true;
                ls = true;
            }
            if (get) {
                sb.append("Get Requests:\n");
                srm.listGetRequests(sb);
            }
            if (put) {
                sb.append("Put Requests:\n");
                srm.listPutRequests(sb);
            }
            if (copy) {
                sb.append("Copy Requests:\n");
                srm.listCopyRequests(sb);
            }
            if (bring) {
                sb.append("Bring Online Requests:\n");
                srm.listBringOnlineRequests(sb);
            }
            if (reserve) {
                sb.append("Reserve Space Requests:\n");
                srm.listReserveSpaceRequests(sb);
            }
            if (ls) {
                sb.append("Ls Requests:\n");
                srm.listLsRequests(sb);
            }
        }
        return sb.toString();
    }

    public static final String fh_ls_queues = " Syntax: ls queues " +
            "[-get] [-put] [-copy] [-bring] [-ls] [-l]  " +
            "#will list schedule queues";
    public static final String hh_ls_queues = " [-get] [-put] [-copy] [-bring] [-ls] [-l] ";

    public String ac_ls_queues_$_0(Args args)
    {
        boolean get = args.hasOption("get");
        boolean put = args.hasOption("put");
        boolean ls = args.hasOption("ls");
        boolean copy = args.hasOption("copy");
        boolean bring = args.hasOption("bring");
        StringBuilder sb = new StringBuilder();

        if (!get && !put && !copy && !bring && !ls) {
            get = true;
            put = true;
            copy = true;
            bring = true;
            ls = true;
        }
        if (get) {
            sb.append("Get Request Scheduler:\n");
            sb.append(srm.getGetSchedulerInfo());
            sb.append('\n');
        }
        if (put) {
            sb.append("Put Request Scheduler:\n");
            sb.append(srm.getPutSchedulerInfo());
            sb.append('\n');
        }
        if (copy) {
            sb.append("Copy Request Scheduler:\n");
            sb.append(srm.getCopySchedulerInfo());
            sb.append('\n');
        }
        if (bring) {
            sb.append("Bring Online Request Scheduler:\n");
            sb.append(srm.getBringOnlineSchedulerInfo());
            sb.append('\n');
        }
        if (ls) {
            sb.append("Ls Request Scheduler:\n");
            sb.append(srm.getLsSchedulerInfo());
            sb.append('\n');
        }
        return sb.toString();
    }

    public static final String fh_ls_completed = " Syntax: ls completed [-get] [-put]" +
            " [-copy] [max_count]" +
            " #will list completed (done, failed or canceled) requests, " +
            "if max_count is not specified, it is set to 50";
    public static final String hh_ls_completed = " [-get] [-put] [-copy] [max_count]";

    public String ac_ls_completed_$_0_1(Args args) throws DataAccessException
    {
        boolean get = args.hasOption("get");
        boolean put = args.hasOption("put");
        boolean copy = args.hasOption("copy");
        int max_count = 50;
        if (args.argc() == 1) {
            max_count = Integer.parseInt(args.argv(0));
        }

        if (!get && !put && !copy) {
            get = true;
            put = true;
            copy = true;

        }
        StringBuilder sb = new StringBuilder();
        if (get) {
            sb.append("Get Requests:\n");
            srm.listLatestCompletedGetRequests(sb, max_count);
            sb.append('\n');
        }
        if (put) {
            sb.append("Put Requests:\n");
            srm.listLatestCompletedPutRequests(sb, max_count);
            sb.append('\n');
        }
        if (copy) {
            sb.append("Copy Requests:\n");
            srm.listLatestCompletedCopyRequests(sb, max_count);
            sb.append('\n');
        }
        return sb.toString();
    }

    public static final String fh_set_job_priority = " Syntax: set priority <requestId> <priority>" +
            "will set priority for the requestid";
    public static final String hh_set_job_priority = " <requestId> <priority>";

    public String ac_set_job_priority_$_2(Args args)
    {
        String s1 = args.argv(0);
        String s2 = args.argv(1);
        long requestId;
        int priority;
        try {
            requestId = Integer.parseInt(s1);
        } catch (NumberFormatException e) {
            return "Failed to parse request id: " + s1;
        }
        try {
            priority = Integer.parseInt(s2);
        } catch (Exception e) {
            return "Failed to parse priority: " + s2;
        }
        try {
            Job job = Job.getJob(requestId, Job.class);
            job.setPriority(priority);
            job.setPriority(priority);
            StringBuilder sb = new StringBuilder();
            srm.listRequest(sb, requestId, true);
            return sb.toString();
        } catch (SRMInvalidRequestException e) {
            return e.getMessage() + "\n";
        } catch (DataAccessException e) {
            logger.warn("Failure in set job priority: " + e.getMessage());
            return e.toString();
        }
    }


    public static final String fh_set_max_ready_put = " Syntax: set max ready put <count>" +
            " #will set a maximum number of put requests in the ready state";
    public static final String hh_set_max_ready_put = " <count>";

    public String ac_set_max_ready_put_$_1(Args args) throws Exception
    {
        if (args.argc() != 1) {
            throw new IllegalArgumentException("count is not specified");
        }
        int value = Integer.parseInt(args.argv(0));
        srm.setPutMaxReadyJobs(value);
        logger.info("put-req-max-ready-requests=" + value);
        return "put-req-max-ready-requests=" + value;
    }

    public static final String fh_set_max_ready_get = " Syntax: set max ready get <count>" +
            " #will set a maximum number of get requests in the ready state";
    public static final String hh_set_max_ready_get = " <count>";

    public String ac_set_max_ready_get_$_1(Args args) throws Exception
    {
        if (args.argc() != 1) {
            throw new IllegalArgumentException("count is not specified");
        }
        int value = Integer.parseInt(args.argv(0));
        srm.setGetMaxReadyJobs(value);
        logger.info("get-req-max-ready-requests=" + value);
        return "get-req-max-ready-requests=" + value;
    }

    public static final String fh_set_max_ready_bring_online = " Syntax: set max ready bring online <count>" +
            " #will set a maximum number of bring online requests in the ready state";
    public static final String hh_set_max_ready_bring_online = " <count>";

    public String ac_set_max_ready_bring_online_$_1(Args args) throws Exception
    {
        if (args.argc() != 1) {
            throw new IllegalArgumentException("count is not specified");
        }
        int value = Integer.parseInt(args.argv(0));
        srm.setBringOnlineMaxReadyJobs(value);
        logger.info("bring-online-req-max-ready-requests=" + value);
        return "bring-online-req-max-ready-requests=" + value;
    }

    public static final String fh_set_max_read_ls_ = " Syntax: set max read ls <count>\n" +
            " #will set a maximum number of ls requests in the ready state\n" +
            " #\"set max read ls\" is an alias for \"set max ready ls\" preserved for compatibility ";
    public static final String hh_set_max_read_ls = " <count>";

    public String ac_set_read_ls_$_1(Args args) throws Exception
    {
        return ac_set_max_ready_ls_$_1(args);
    }

    public static final String fh_set_max_ready_ls = " Syntax: set max ready ls <count>\n" +
            " #will set a maximum number of ls requests in the ready state";
    public static final String hh_set_max_ready_ls = " <count>";

    public String ac_set_max_ready_ls_$_1(Args args) throws Exception
    {
        if (args.argc() != 1) {
            throw new IllegalArgumentException("count is not specified");
        }
        int value = Integer.parseInt(args.argv(0));
        srm.setLsMaxReadyJobs(value);
        logger.info("ls-request-max-ready-requests=" + value);
        return "ls-request-max-ready-requests=" + value;
    }

    public static final String hh_print_srm_counters = "# prints the counters for all srm operations";

    public String ac_print_srm_counters_$_0(Args args)
    {
        return srm.getSrmServerV1Counters().toString() +
                '\n' +
                srm.getSrmServerV2Counters().toString() +
                '\n' +
                srm.getAbstractStorageElementCounters().toString() +
                '\n' +
                srm.getSrmServerV1Gauges().toString() +
                '\n' +
                srm.getSrmServerV2Gauges().toString() +
                '\n' +
                srm.getAbstractStorageElementGauges().toString();
    }

    public static final String fh_db_history_log = " Syntax: db history log [on|off] " +
            "# show status or enable db history log ";
    public static final String hh_db_history_log = "[-get] [-put] [-bringonline] [-ls] [-copy] [-reserve] [on|off] " +
            "# show status or enable db history log ";

    public String ac_db_history_log_$_0_1(Args args)
    {
        Collection<String> sets = new ArrayList<>();
        for (Map.Entry<String, String> e : OPTION_TO_PARAMETER_SET.entrySet()) {
            if (args.hasOption(e.getKey())) {
                sets.add(e.getValue());
            }
        }

        if (sets.isEmpty()) {
            sets = OPTION_TO_PARAMETER_SET.values();
        }

        if (args.argc() > 0) {
            String arg = args.argv(0);
            if (!arg.equals("on") && !arg.equals("off")) {
                return "syntax error";
            }
            for (String set : sets) {
                config.getDatabaseParameters(set).setRequestHistoryDatabaseEnabled(arg.equals("on"));
            }
        }

        StringBuilder s = new StringBuilder();
        for (String set : sets) {
            Configuration.DatabaseParameters parameters = config.getDatabaseParameters(set);
            s.append("db history logging for ").append(set).append(" is ")
                    .append((parameters.isRequestHistoryDatabaseEnabled()
                            ? "enabled"
                            : "disabled")).append("\n");
        }
        return s.toString();
    }

    public static final String hh_set_switch_to_async_mode_delay_get =
            "<milliseconds>";
    public static final String fh_set_switch_to_async_mode_delay_get =
            "Sets the time after which get requests are processed asynchronously.\n" +
                    "Use 'infinity' to always use synchronous replies and use 0 to\n" +
                    "always use asynchronous replies.";

    public String ac_set_switch_to_async_mode_delay_get_$_1(Args args)
    {
        config.setGetSwitchToAsynchronousModeDelay(Strings.parseTime(args.argv(0), TimeUnit.MILLISECONDS));
        return "";
    }

    public static final String hh_set_switch_to_async_mode_delay_put =
            "<milliseconds>";
    public static final String fh_set_switch_to_async_mode_delay_put =
            "Sets the time after which put requests are processed asynchronously.\n" +
                    "Use 'infinity' to always use synchronous replies and use 0 to\n" +
                    "always use asynchronous replies.";

    public String ac_set_switch_to_async_mode_delay_put_$_1(Args args)
    {
        config.setPutSwitchToAsynchronousModeDelay(Strings.parseTime(args.argv(0), TimeUnit.MILLISECONDS));
        return "";
    }

    public static final String hh_set_switch_to_async_mode_delay_ls =
            "<milliseconds>";
    public static final String fh_set_switch_to_async_mode_delay_ls =
            "Sets the time after which ls requests are processed asynchronously.\n" +
                    "Use 'infinity' to always use synchronous replies and use 0 to\n" +
                    "always use asynchronous replies.";

    public String ac_set_switch_to_async_mode_delay_ls_$_1(Args args)
    {
        config.setLsSwitchToAsynchronousModeDelay(Strings.parseTime(args.argv(0), TimeUnit.MILLISECONDS));
        return "";
    }

    public static final String hh_set_switch_to_async_mode_delay_bring_online =
            "<milliseconds>";
    public static final String fh_set_switch_to_async_mode_delay_bring_online =
            "Sets the time after which bring online requests are processed\n" +
                    "asynchronously. Use 'infinity' to always use synchronous replies\n" +
                    "and use 0 to always use asynchronous replies.";

    public String ac_set_switch_to_async_mode_delay_bring_online_$_1(Args args)
    {
        config.setBringOnlineSwitchToAsynchronousModeDelay(Strings.parseTime(args.argv(0), TimeUnit.MILLISECONDS));
        return "";
    }
}

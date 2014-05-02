/*
 * Main.java
 *
 * Created on July 19, 2004, 4:45 PM
 */

package org.dcache.srm.unixfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import dmg.util.CommandInterpreter;

import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.SchedulerContainer;
import org.dcache.util.Args;

/**
 *
 * @author  timur
 */
public class Main extends CommandInterpreter implements  Runnable {
    private Configuration config;
    private SRMAuthorization authorization;
    private SRM srm;
    private String name;
    /** Creates a new instance of Main */
    public Main(String[] args) throws Exception {
        String config_file = args[0];
        name = args[1];
        File f = new File(config_file);
        if(!f.exists())
        {
            Configuration configuration = new Configuration();
            configuration.write(config_file);
            System.out.println("configuration written to a file: "+config_file);
            return;
        }
        String gridftphost = args[2];
        int gridftpport = Integer.parseInt(args[3]);
	String stat=args[4];
	String chown=args[5];
        System.out.println("reading configuration from "+config_file);
        config = new Configuration(config_file);
        PrintStream out,err;

        if ( args.length >6) {
            String logfile = args[6];
            System.out.println("Logging to "+logfile);
            out = new PrintStream(new FileOutputStream(logfile));
            err = out;
        }
        else
        {
            System.out.println("Logging to stdout and stderr");
            out = System.out;
            err = System.err;
        }
        authorization = UnixfsAuthorization.getAuthorization(config.getKpwdfile());
        config.setAuthorization(authorization);
        Storage storage =
            new Storage(gridftphost,gridftpport,config,stat,chown,out,err);

        srm = new SRM(config, storage);
        srm.setSchedulers(new SchedulerContainer(
                buildRunningScheduler("get_" + name, GetFileRequest.class, "Get"),
                buildRunningScheduler("ls_" + name, GetFileRequest.class, "Ls"),
                buildRunningScheduler("bring_online_" + name,
                        BringOnlineFileRequest.class, "BringOnline"),
                buildRunningScheduler("put_" + name, PutFileRequest.class, "Put"),
                buildRunningScheduler("reserve_space_" + name,
                        ReserveSpaceRequest.class, "ReserveSpace"),
                // COPY must be the last in the list
                buildRunningScheduler("copy_" + name, Job.class, "Copy")));
        srm.start();

        new Thread(this).start();
    }

    private Scheduler buildRunningScheduler(String name, Class<? extends Job> type,
            String configPrefix)
    {
        Scheduler scheduler = new Scheduler(name, type);

        scheduler.setMaxRequests(getIntConfigValue(configPrefix, "ReqTQueueSize"));
        scheduler.setThreadPoolSize(getIntConfigValue(configPrefix, "ThreadPoolSize"));
        scheduler.setMaxInprogress(getIntConfigValue(configPrefix, "MaxWaitingRequests"));
        scheduler.setMaxNumberOfRetries(getIntConfigValue(configPrefix, "MaxNumOfRetries"));
        scheduler.setRetryTimeout(getLongConfigValue(configPrefix, "RetryTimeout"));
        scheduler.setMaxRunningByOwner(getIntConfigValue(configPrefix, "MaxRunningBySameOwner"));
        scheduler.setPriorityPolicyPlugin("DefaultJobAppraiser");

        if (!configPrefix.equals("Copy")) {
            scheduler.setMaxReadyJobs(getIntConfigValue(configPrefix, "MaxReadyJobs"));
        }

        scheduler.start();

        return scheduler;
    }

    private int getIntConfigValue(String prefix, String suffix)
    {
        return (Integer) getConfigValue(prefix, suffix);
    }

    private long getLongConfigValue(String prefix, String suffix)
    {
        return (Long) getConfigValue(prefix, suffix);
    }

    private Object getConfigValue(String prefix, String suffix)
    {
        String name = "get" + prefix + suffix;
        try {
            Method getter = config.getClass().getMethod(name);
            return getter.invoke(config);
        } catch (NoSuchMethodException | IllegalAccessException |
                IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run()
    { int failures = 0;
        while(failures < 100)
        {


        try
        {

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String nextCommand;
        System.out.println("<<<<Welcome to srm server admin shell>>>");
        System.out.println("type help to begin");
        System.out.print("[srm server "+name+ " ]# ");
        while ( (nextCommand = br.readLine()) != null) {
            System.out.println("Interpeting command : "+nextCommand);
            try
            {
                System.out.println(command(nextCommand));
            }catch(Throwable t)
            {
                t.printStackTrace();
            }
            System.out.print("[srm server "+name+ " ]# ");
        }
        return;
        }
        catch(Exception e)
        {
            e.printStackTrace();
            failures++;
        }

        }
        System.err.println("too many falures, exiting command interpreter loop");
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        if(args == null || args.length <6 ||
        args[0].equalsIgnoreCase("-h")  ||
        args[0].equalsIgnoreCase("-help")  ||
        args[0].equalsIgnoreCase("--h")  ||
        args[0].equalsIgnoreCase("--help")
        )
        {
            System.err.println("Usage: java [-classpath <CLASSPATH to all srm jars>] org.dcache.srm.unixfs  <configuration file> <instance_name> <gridftp server host> <gridftp server port> <stat command path> <chown path> [logfile] \n" +
                                "      if configuration file does not exist it will be created and program will exit \n" +
                                "       you can then review the configuration file and restart the program ");
            return;
        }

        new Main(args);
    }


        public void getInfo( PrintWriter printWriter ) {
            StringBuilder sb = new StringBuilder();
            sb.append("SRM Cell");
            sb.append(" storage info ");
            sb.append('\n');
            sb.append(config.toString()).append('\n');
            sb.append(srm.getSchedulerInfo());
            printWriter.println( sb.toString()) ;
        }

       public String getInfo(){
         StringWriter stringWriter = new StringWriter() ;
         PrintWriter   printWriter = new PrintWriter( stringWriter ) ;

         getInfo( printWriter ) ;
         printWriter.flush() ;
         return stringWriter.getBuffer().toString()  ;
       }

        public static final String fh_cancel= " Syntax: cancel <id> ";
        public static final String hh_cancel= " <id> ";
        public String ac_cancel_$_1(Args args) {
            try {
                long id = Long.parseLong(args.argv(0));
                StringBuilder sb = new StringBuilder();
                srm.cancelRequest(sb, id);
                return sb.toString();
            }catch (Exception e) {
                //esay(e);
                return e.toString();
            }
        }

        public static final String fh_ls= " Syntax: ls [-get] [-put] [-copy] [-l] [<id>] "+
        "#will list all requests";
        public static final String hh_ls= " [-get] [-put] [-copy] [-l] [<id>]";
        public String ac_ls_$_0_1(Args args) {
            try {
                boolean get=args.hasOption("get");
                boolean put=args.hasOption("put");
                boolean copy=args.hasOption("copy");
                boolean longformat = args.hasOption("l");
                StringBuilder sb = new StringBuilder();
                if(args.argc() == 1) {
                    try {
                        long reqId = Long.parseLong(args.argv(0));
                        srm.listRequest(sb, reqId, longformat);
                    }
                    catch( NumberFormatException nfe) {
                        return "id must be a nonnegative integer, you gave id="+args.argv(0);
                    }
                }
                else {
                    if( !get && !put && !copy ) {
                        get=true;
                        put=true;
                        copy=true;

                    }
                    if(get) {
                        sb.append("Get Requests:\n");
                        srm.listGetRequests(sb);
                        sb.append('\n');
                    }
                    if(put) {
                        sb.append("Put Requests:\n");
                        srm.listPutRequests(sb);
                        sb.append('\n');
                    }
                    if(copy) {
                        sb.append("Copy Requests:\n");
                        srm.listCopyRequests(sb);
                        sb.append('\n');
                    }
                }
                return sb.toString();
            }
            catch(Throwable t) {
                t.printStackTrace();
                return t.toString();
            }
        }
        public static final String fh_ls_queues= " Syntax: ls queues [-get] [-put] [-copy] [-l]  "+
        "#will list schedule queues";
        public static final String hh_ls_queues= " [-get] [-put] [-copy] [-l] ";
        public String ac_ls_queues_$_0(Args args) {
            try {
                boolean get=args.hasOption("get");
                boolean put=args.hasOption("put");
                boolean copy=args.hasOption("copy");
                boolean longformat = args.hasOption("l");
                StringBuilder sb = new StringBuilder();

                if( !get && !put && !copy ) {
                    get=true;
                    put=true;
                    copy=true;

                }
                if(get) {
                    sb.append("Get Request Scheduler:\n");
                    sb.append(srm.getGetSchedulerInfo());
                    sb.append('\n');
                }
                if(put) {
                    sb.append("Put Request Scheduler:\n");
                    sb.append(srm.getPutSchedulerInfo());
                    sb.append('\n');
                }
                if(copy) {
                    sb.append("Copy Request Scheduler:\n");
                    sb.append(srm.getCopySchedulerInfo());
                    sb.append('\n');
                }
                return sb.toString();
            }
            catch(Throwable t) {
                t.printStackTrace();
                return t.toString();
            }
        }

        public static final String fh_ls_completed= " Syntax: ls completed [-get] [-put] [-copy] [max_count]"+
        " #will list completed (done, failed or canceled) requests, if max_count is not specified, it is set to 50";
        public static final String hh_ls_completed= " [-get] [-put] [-copy] [[max_count]";
        public String ac_ls_completed_$_0_1(Args args) throws Exception{
            boolean get=args.hasOption("get");
            boolean put=args.hasOption("put");
            boolean copy=args.hasOption("copy");
            int max_count=50;
            if(args.argc() == 1) {
                max_count = Integer.parseInt(args.argv(0));
            }

            if( !get && !put && !copy ) {
                get=true;
                put=true;
                copy=true;

            }
            StringBuilder sb = new StringBuilder();
            if(get) {
                sb.append("Get Requests:\n");
                srm.listLatestCompletedGetRequests(sb, max_count);
                sb.append('\n');
            }
            if(put) {
                sb.append("Put Requests:\n");
                srm.listLatestCompletedPutRequests(sb, max_count);
                sb.append('\n');
            }
            if(copy) {
                sb.append("Copy Requests:\n");
                srm.listLatestCompletedCopyRequests(sb, max_count);
                sb.append('\n');
            }
            return sb.toString();
        }
     public static final String hh_info = "[-l|-a]" ;
   public String ac_info( Args args ) throws Exception {
       return getInfo();
   }
        public static final String fh_exit= " Syntax: exit "+
        " #will stop the server and exit the shell";
        public static final String hh_exit= " ";
        public String ac_exit_$_0_1(Args args) throws Exception{
            System.exit(0);
            return "exiting";
        }

}

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
import org.dcache.srm.SrmCommandLineInterface;
import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.request.sql.DatabaseRequestCredentialStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.SchedulerContainer;
import org.dcache.srm.scheduler.strategy.FifoSchedulingStrategyProvider;
import org.dcache.srm.scheduler.strategy.FirstComeFirstServedTransferStrategyProvider;
import org.dcache.util.Args;

/**
 *
 * @author  timur
 */
public class Main extends CommandInterpreter implements  Runnable {
    private Configuration config;
    private SRMAuthorization authorization;
    private SRM srm;
    private final String name;
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
        srm.setRequestCredentialStorage(new DatabaseRequestCredentialStorage(config));
        srm.start();

        addCommandListener(new SrmCommandLineInterface(srm, config));

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
        scheduler.setSchedulingStrategyProvider(new FifoSchedulingStrategyProvider());
        scheduler.setTransferStrategyProvider(new FirstComeFirstServedTransferStrategyProvider());

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
            printWriter.println("SRM Cell storage info ");
            printWriter.println(config);
            printWriter.println(srm.getSchedulerInfo());
        }

       public String getInfo(){
         StringWriter stringWriter = new StringWriter() ;
         PrintWriter   printWriter = new PrintWriter( stringWriter ) ;

         getInfo( printWriter ) ;
         printWriter.flush() ;
         return stringWriter.getBuffer().toString()  ;
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

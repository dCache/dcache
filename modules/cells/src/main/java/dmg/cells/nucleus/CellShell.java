package dmg.cells.nucleus ;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import dmg.cells.network.PingMessage;
import dmg.util.BufferedLineWriter;
import dmg.util.CommandEvaluationException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import dmg.util.Exceptions;
import dmg.util.Formats;
import dmg.util.PropertiesBackedReplaceable;
import dmg.util.Replaceable;
import dmg.util.ReplaceableBackedProperties;
import dmg.util.Slf4jErrorWriter;
import dmg.util.Slf4jInfoWriter;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.util.Args;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellShell extends CommandInterpreter
       implements Replaceable
{
    private static final Logger _log =
        LoggerFactory.getLogger(CellShell.class);
    private static final Logger _logNucleus =
        LoggerFactory.getLogger(CellNucleus.class);

   private final CellNucleus  _nucleus ;
   private StringBuilder _contextString;
   private String       _contextName;
   private String       _contextDelimiter;
   private StringBuilder _envString;
   private String       _envName;
   private String       _envDelimiter;
   private int          _helpMode       = 1 ;
   private int          _errorCode;
   private String       _errorMsg;
   private String       _doOnExit;
   private final Map<String, Object> _environment =
           new ConcurrentHashMap<>();
   private CommandInterpreter _externalInterpreter;
   private List<String>       _argumentVector      = new Vector<>() ;

   public CellShell( CellNucleus nucleus ){
      _nucleus = nucleus ;
      try {
         objectCommand( "exec context shellProfile" ) ;
      } catch (CommandExitException e) {
      }
   }

    public CellShell(CellNucleus nucleus, CommandInterpreter interpreter)
    {
        this(nucleus);
        _externalInterpreter = interpreter;
    }

    /**
     * Returns the environment of the shell.
     *
     * The map is backed by the shell's environment. Any modification
     * to the map changes the shell environment and any change in the
     * shell environment is reflected in the map.
     */
    public Map<String,Object> environment()
    {
        return _environment;
    }

    @Override
    public String getReplacement(String name)
    {
        Object o = getDictionaryEntry(name);
        return (o == null) ? null : o.toString();
    }

   private static long __sequenceNumber = 1000000L ;
   private static synchronized long nextSequenceNumber(){
      return __sequenceNumber ++ ;
   }
   public Object getDictionaryEntry( String name ){
       switch (name) {
       case "rc":
           return "" + _errorCode;
       case "rmsg":
           return (_errorMsg == null ? "(0)" : _errorMsg);
       case "thisDomain":
           return _nucleus.getCellDomainName();
       case "thisCell":
           return _nucleus.getCellName();
       case "nextSequenceNumber":
           return "" + nextSequenceNumber();
       case "thisHostname":
           try {
               String xname = InetAddress.getLocalHost().getHostName();
               return new StringTokenizer(xname, ".").nextToken();
           } catch (UnknownHostException e) {
               return "UnknownHostname";
           }
       case "thisFqHostname":
           try {
               return InetAddress.getLocalHost().getCanonicalHostName();
           } catch (UnknownHostException e) {
               return "UnknownHostname";
           }
       default:
           try {
               int position = Integer.parseInt(name);
               if (position >= 0 && position < _argumentVector.size()) {
                   Object o = _argumentVector.get(position);
                   if (o == null) {
                       throw new IllegalArgumentException("");
                   }
                   return o;
               }
           } catch (NumberFormatException e) {
           }
           Object o = _environment.get(name);
           if (o == null) {
               o = _nucleus.getDomainContext().get(name);
           }
           return o;
       }
   }
   private String prepareCommand( String string ){
      //
      // replace the variables ${...}
      //
      String str = Formats.replaceKeywords( string , this ) ;

      if( _contextString != null  ){
         //
         // are we in the define context ...
         //
         if( ( str.length()   > 0          ) &&
             ( str.equals(_contextDelimiter)    ) ){

             _nucleus.getDomainContext().
                      put( _contextName , _contextString.toString() ) ;
             _contextString = null ;
             return null  ;

         }
         _contextString.append( str ).append("\n");
         return null ;
      }else if( _envString != null  ){
         //
         // are we in the define environment
         //
         if( ( str.length()    > 0         ) &&
             ( str.equals(_envDelimiter)   ) ){

             _environment.put( _envName , _envString.toString() ) ;
             _envString = null ;
             return null  ;

         }
         _envString.append( str ).append("\n");
         return null ;
      }
      return str ;
   }

   public Serializable objectCommand2(String strin) {
      String str;
      if( ( str = prepareCommand( strin ) ) == null ) {
          return "";
      }
      try{
          Args args = new Args(str);

          if (args.argc() == 0) {
              return "";
          }

         Serializable o;
         if( _externalInterpreter != null ){
            o =  _externalInterpreter.command(args);
         }else{
            o =  command(args);
         }
         _errorCode = 0 ;
         _errorMsg  = null ;
         if( o == null ) {
             return "";
         }
         return o ;
      }catch( CommandException ce ){
         _errorCode = ce.getErrorCode() ;
         _errorMsg  = ce.getErrorMessage() ;
         return ce ;
      }
   }
   public Object objectCommand( String strin ) throws CommandExitException {
      String str;
      if( ( str = prepareCommand( strin ) ) == null ) {
          return "";
      }
      try{
          Args args = new Args(strin);

          if (args.argc() == 0) {
              return "";
          }

         Object o;
         if( _externalInterpreter != null ){
            o =  _externalInterpreter.command(args) ;
         }else{
            o =  command(args) ;
         }
         _errorCode = 0 ;
         _errorMsg  = null ;
         if( o == null ) {
             return "";
         }
         return o ;
      }catch( CommandException ce ){
         _errorCode = ce.getErrorCode() ;
         _errorMsg  = ce.getErrorMessage() ;

         if( _doOnExit != null ){
            if( _doOnExit.equals( "shutdown" ) ) {
                throw new CommandExitException(ce.toString(), 666);
            } else {
                throw new CommandExitException(ce.getErrorMessage(),
                        ce.getErrorCode());
            }

         }
         if( ce instanceof CommandSyntaxException ){
            CommandSyntaxException cse = (CommandSyntaxException)ce ;

            StringBuilder sb = new StringBuilder() ;
            sb.append( "Syntax Error : " ).
               append( cse.getMessage() ) ;
            if( _helpMode == 1 ){
               sb.append( "\nUse 'help' for more information\n" ) ;
            }else if( _helpMode == 2 ){
               String help = cse.getHelpText() ;
               if( help != null ) {
                   sb.append("\n").append(help).append("\n");
               }
            }
            return sb.toString() ;
         }else if( ce instanceof CommandExitException ){
            if( _externalInterpreter != null ){
               _externalInterpreter = null ;
               return "external shell exited ... " ;
            }else{
               throw (CommandExitException) ce ;
            }
         }else if( ce instanceof CommandThrowableException ){
            CommandThrowableException cte = (CommandThrowableException)ce ;
            StringBuilder sb = new StringBuilder() ;
            sb.append( cte.getMessage()).append(" -> " ) ;
            Throwable t = cte.getTargetException() ;
            sb.append( t.getClass().getName()).append(" : ").append(t.getMessage()).append("\n" ) ;
            return sb.toString() ;
         }else if( ce instanceof CommandPanicException ){
            CommandPanicException cpe = (CommandPanicException)ce ;
            StringBuilder sb = new StringBuilder() ;
            sb.append( "Panic : ").append(cpe.getMessage()).append("\n" ) ;
            Throwable t = cpe.getTargetException() ;
            sb.append( t.getClass().getName()).append(" : ").append(t.getMessage()).append("\n" ) ;
            return sb.toString() ;
         }else{
            return "CommandException  :"+ce.getMessage() ;
         }
      }
   }

   @Override
   public String command( String c ) throws CommandExitException {
      StringTokenizer st = new StringTokenizer( c , "\n" ) ;
      StringBuilder    sb = new StringBuilder();
       while (st.hasMoreTokens()) {
           sb.append(commandLine(st.nextToken()));
       }
      return sb.toString() ;
   }
   private String commandLine( String c ) throws CommandExitException {
      if( _contextString != null ){
         _contextString.append( c ).append("\n");
         return "" ;
      }else {
          return super.command(c);
      }
   }

   ////////////////////////////////////////////////////////////
   //
   //  version
   //
   public static final String hh_version = "[<package>] ; package info of dmg/cells/nucleus" ;
   public Object ac_version_$_0_1( Args args ){
      Package p = Package.getPackage( args.argc() == 0 ? "dmg.cells.nucleus" : args.argv(0) );
      StringBuilder sb = new StringBuilder();
      if( p != null ){
          // FIXME: wrong description returned for backwards compatibility.
          String tmp = p.getImplementationTitle();
          sb.append("SpecificationTitle:   ").append(tmp==null?"(Unknown)":tmp).append("\n");
          tmp = p.getImplementationVendor();
          sb.append("SpecificationVendor:  ").append(tmp==null?"(Unknown)":tmp).append("\n");
          tmp = p.getImplementationVersion();
          sb.append("SpecificationVersion: ").append(tmp==null?"(Unknown)":tmp).append("\n");
      }else{
          sb.append("No version version found");
      }
      return sb.toString() ;

   }
   ////////////////////////////////////////////////////////////
   //
   //   getroutes, getcelltunnelinfos, getcellinfos
   //
    @Command(name = "getroutes", hint = "list all routes",
            description = "List all message routes available in " +
                    "this domain. The returned information " +
                    "comprises of the target cell name, target domain name, " +
                    "gateway for this route and the route type (such as " +
                    "default, alias, domain, topic, etc).")
    public class GetroutesCommand implements Callable<CellRoute[]>
    {
        @Override
        public CellRoute[] call() throws Exception
        {
            return _nucleus.getRoutingList() ;
        }
    }

   public CellTunnelInfo[] ac_getcelltunnelinfos( Args args ){
       List<CellTunnelInfo> cellTunnelInfos = _nucleus.getCellTunnelInfos();
       return cellTunnelInfos.toArray(new CellTunnelInfo[cellTunnelInfos.size()]);
   }

    @Command(name = "getcellinfo", hint = "display cell information",
            description = "Shows a brief information on a specified cell. " +
                    "This information consist of the cell name, " +
                    "the state of the cell " +
                    "(a cell can be in one of these following states: Initial, " +
                    "Active, Removing, Dead and Unknown state which are " +
                    "denoted by I, A, R, D and U respectively), " +
                    "the event queue size, thread count,  " +
                    "the class the cell belong to and, lastly, a short cell " +
                    "specific information.")
    public class GetcellinfoCommand implements Callable<CellInfo>
    {
        @Argument(usage = "The cell name")
        String cellName;

        @Override
        public CellInfo call() throws Exception
        {
            CellInfo info = _nucleus.getCellInfo(cellName);
            if(info == null ) {
                throw new CommandException(68, "not found : " + cellName);
            }

            return info;
        }
    }

    @Command(name = "getcellinfos", hint = "get information on all cells",
            description = "Display a summarised information of all cells " +
                    "in this domain. This " +
                    "information (starting from left to right) comprises of " +
                    "the cell name, cell state " +
                    "(a cell can be in one of these following states: Initial, " +
                    "Active, Removing, Dead and Unknown state which are " +
                    "denoted by I, A, R, D and U respectively), " +
                    "the event queue size, thread count, " +
                    "cell class and lastly a short cell specific information.")
    public class GetcellinfosCommand implements Callable<CellInfo[]>
    {
        @Override
        public CellInfo[] call() throws Exception
        {
            List<String> names = _nucleus.getCellNames();

            List<CellInfo> infoList = new ArrayList<>(names.size());

            for(String name : names) {
                CellInfo info = _nucleus.getCellInfo(name);
                if(info != null) {
                    infoList.add(info);
                }
            }

            return infoList.toArray(new CellInfo[infoList.size()]);
        }
    }

    @Command(name = "getcontext", hint = "list all contexts",
            description = "Returns a " +
                    "list of all the contexts in your current domain. " +
                    "When a context name (within the current domain) is specified, " +
                    "it will return the content of that context.")
    public class GetcontextCommand implements Callable<Serializable>
    {
        @Argument(required = false,
                usage = "The context name")
        String contextName;

        @Override
        public Serializable call() throws Exception
        {
            if (contextName == null) {
                return _nucleus.getDomainContext().keySet().toArray();
            }else{
                Object o = _nucleus.getDomainContext( contextName ) ;
                if( o == null ) {
                    throw new CommandException("Context not found : " + contextName);
                }
                return (Serializable) o;
            }
        }
    }
   ////////////////////////////////////////////////////////////
   //
   //   waitfor cell/domain/context
   //
   public static final String hh_waitfor=
       "context|cell|domain <objectName> [<domain>] [-i=<checkInterval>] [-wait=<maxTime>]" ;
   public static final String fh_waitfor =
       "waitfor [options]  context  <contextName> [<domainName]\n" +
       "waitfor [options]  cell     <cellPath>\n" +
       "waitfor [options]  domain   <domainName>\n"+
       "    Options : -i=<probeInterval   -wait=<maxWaitSeconds>\n" ;

   public String ac_waitfor_$_2_3( Args args ) throws CommandException{
      int waitTime = 0 ;
      int check    = 1 ;
      for( int i = 0 ; i < args.optc() ; i ++ ){
        if( args.optv(i).startsWith("-i=") ) {
            check = Integer.parseInt(args.optv(i).substring(3));
        } else if( args.optv(i).startsWith("-wait=") ) {
            waitTime = Integer.parseInt(args.optv(i).substring(6));
        }
      }
      if( waitTime < 0 ) {
          waitTime = 0;
      }
      String what = args.argv(0) ;
      String name = args.argv(1) ;

       switch (what) {
       case "cell":
           return _waitForCell(name, waitTime, check, null);
       case "domain":
           return _waitForCell("System@" + name, waitTime, check, null);
       case "context":
           if (args.argc() > 2) {
               return _waitForCell("System@" + args.argv(2),
                       waitTime, check, "test context " + name);
           } else {
               return _waitForContext(name, waitTime, check);
           }
       }

      throw new CommandException( "Unknown Observable : "+what ) ;
   }
   private String _waitForContext( String contextName , int waitTime , int check )
           throws CommandException {


      if( check <= 0 ) {
          check = 1;
      }
      long finish = System.currentTimeMillis() + ( waitTime * 1000 ) ;
      while( true ){
         Object o = _nucleus.getDomainContext( contextName ) ;
         if( o != null ) {
             break;
         }
         if( ( waitTime == 0 ) || ( finish > System.currentTimeMillis()  ) ){
            try{ Thread.sleep(((long)check)*1000) ; }
            catch( InterruptedException ie ){
               throw new
               CommandException( 2 , "Command Was interrupted" ) ;
            }
            continue ;
         }
         throw new
         CommandException( 1 , "Command Timed Out" ) ;
      }
      return "" ;
   }
   private String _waitForCell( String cellName ,
                               int waitTime , int check ,
                               String command  )
           throws CommandException {

      if( check <= 4 ) {
          check = 5;
      }
      CellPath destination = new CellPath( cellName ) ;
      long finish = System.currentTimeMillis() + ( waitTime * 1000 ) ;
      CellMessage answer = null ;
      //
      // creating the message now and send it forever does not
      // allow time messurements.
      //
      CellMessage request  =
          new CellMessage( destination ,
                           (command == null ?
                                   new PingMessage() : command) ) ;

      Object o;
      boolean noRoute;
      while( true ){
          noRoute = false ;
          answer = null ;
          try{
            _log.warn( "waitForCell : Sending request" ) ;
            answer = _nucleus.sendAndWait( request , ((long) check) * 1000);
            _log.warn( "waitForCell : got "+answer ) ;
         } catch (NoRouteToCellException e) {
            noRoute = true ;
         } catch (ExecutionException ignored) {
         } catch (InterruptedException e) {
            throw new CommandException(66, "sendAndWait problem : " + e.toString(), e);
         }
         if( ( answer != null ) &&
             ( ( o = answer.getMessageObject() ) != null ) &&
             ( ( o instanceof PingMessage ) || (o instanceof String) )
           ) {
             break;
         }

         if( ( waitTime == 0 ) ||
             ( finish > System.currentTimeMillis() )  ){

            //
            // not to waste cpu time, we should distinquish between
            // between timeout and NoRouteToCellException
            //
            if( ( ! noRoute ) && ( answer == null ) ) {
                continue;
            }
            //
            // this answer was to fast to try it again, so we wait
            //
            try{ Thread.sleep(((long)check)*1000) ; }
            catch( InterruptedException ie ){
               throw new
               CommandException( 2 , "Command Was interrupted" ) ;
            }
            continue ;
         }
         throw new
         CommandException( 1 , "Command Timed Out" ) ;
      }
      return "" ;
   }
   ////////////////////////////////////////////////////////////
   //
   //   route
   //
   public static final String fh_route =
          " Syntax : route      # show all routes\n"+
          "          route add|delete [options] <source> <destination>\n" ;

   public String ac_route_$_0( Args args ){
       return  _nucleus.getRoutingTable().toString() ;
   }
   public static final String hh_route_add = "-options <source> <destination>" ;
   public static final String fh_route_add = fh_route ;
   public String ac_route_add_$_1_2(Args args)
       throws IllegalArgumentException
   {
       _nucleus.routeAdd( new CellRoute( args ) );
       return "Done\n" ;
   }
   public static final String hh_route_delete = "-options <source> <destination>" ;
   public static final String fh_route_delete = fh_route ;
   public String ac_route_delete_$_1_2(Args args)
       throws IllegalArgumentException
   {
       _nucleus.routeDelete(new CellRoute(args));
       return "Done\n" ;
   }
   public static final String hh_route_find = "<address>" ;
   public String ac_route_find_$_1( Args args )
       throws IllegalArgumentException
   {
       CellAddressCore addr = new CellAddressCore( args.argv(0) ) ;
       CellRoute route = _nucleus.routeFind( addr );
       if( route != null ) {
           return route.toString() + "\n";
       } else {
           return "No Route To cell : " + addr.toString() + "\n";
       }
   }
   ////////////////////////////////////////////////////////////
   //
   //   ps -af <cellname>
   //
   @Command(name = "ps", hint = "list cells in the domain",
           description = "List all cells within the current domain. " +
                   "The option \'-f\' provides information about the " +
                   "cells in the domain. This information comprises " +
                   "of the cell name, the cell current state (a cell " +
                   "can be in one of these following states: Initial, " +
                   "Active, Removing, Dead and Unknown state which " +
                   "are denoted by I, A, R, D and U respectively), " +
                   "the number of message queues, the thread count, " +
                   "the class name of the cell and lastly, a short " +
                   "description of the cell itself." +
                   "\n\n" +
                   "When a particular cell is specify, a summarised " +
                   "information on the cell is returned. With the option " +
                   "'-f', all information about the cell will be return in a " +
                   "comprehensive and detailed manner.")
   public class PsCommand implements Callable<String>
   {
       @Argument(usage = "specify a cell or list of cell names", required = false)
       String[] cellName;

       @Option(name="f",
               usage = "display with the full attributes" )
       boolean full;

       @Override
       public String call()
       {
           StringBuilder sb = new StringBuilder() ;
           if( cellName == null ){
               List<String> list = _nucleus.getCellNames();
               if (full) {
                   for (String name: list) {
                       CellInfo info = _nucleus.getCellInfo(name);
                       if (info == null){
                           sb.append(name).append(" (defunc)\n" ) ;
                       } else {
                           sb.append(info).append( "\n" ) ;
                       }
                   }
               } else {
                   for (String name: list) {
                       sb.append(name).append("\n");
                   }
               }
           } else {
               for (String aCellName : cellName) {
                   CellInfo info = _nucleus.getCellInfo(aCellName);
                   if (info == null) {
                       sb.append(aCellName).append(" Not found\n");
                       continue;
                   }
                   if (full) {
                       sb.append("  -- Short Info about Cell ").append(aCellName)
                               .append(" --\n");
                       sb.append(info.toString()).append("\n");
                       CellVersion version = info.getCellVersion();
                       if (version != null) {
                           sb.append("  -- Version : ").append(version.toString())
                                   .append("\n");
                       }
                       sb.append("  -- Threads --\n");
                       Thread[] threads = _nucleus.getThreads(aCellName);
                       for (int j = 0;
                            (j < threads.length) && (threads[j] != null); j++) {
                           boolean isAlive = threads[j].isAlive();
                           sb.append(CellInfo.f(threads[j].getName(), 20))
                                   .append(CellInfo
                                           .f("" + threads[j].getPriority(), 2))
                                   .append(isAlive ? "  Alive" : "  Dead")
                                   .append("\n");
                       }
                       sb.append("  -- Private Infos --\n");
                   }
                   sb.append(info.getPrivatInfo()).append("\n");
               }
           }
           return sb.toString() ;
       }
   }

   ////////////////////////////////////////////////////////////
   //
   //   kill
   //
   @Command(name = "kill", hint = "kill a cell",
           description = "Kills the named CELL.  If CELL is 'System' then, " +
                   "in addition to killing the System cell, the domain " +
                   "shutdown sequence is triggered.  This will result " +
                   "in the JVM process ending.")
   public class KillCommand implements Callable<Reply>
   {
       @Argument(index = 0, usage = "specify the cell name")
       String cellName;

       @Override
       public Reply call() throws IllegalArgumentException, InterruptedException
       {
           // The killing of a cell requires deliver of a message to the targeted
           // cell.  If CellShell is running in the targeted shell then the call
           // to _nucleus.join will never return unless we free up the
           // message-processing thread by returning from this method.  We return a
           // delayed reply to achieve this while also not delivering the reply
           // until after the cell has terminated.
           final DelayedReply future = new DelayedReply();

           Thread thread = new Thread("kill "+cellName+" command") {
               @Override
               public void run()
               {
                   Serializable response = "";
                   try {
                       try {
                           _nucleus.kill(cellName);
                           _nucleus.join(cellName, 0);
                       } catch (IllegalArgumentException e) {
                           response = e;
                       }

                       future.reply(response);
                   } catch (InterruptedException e) {
                       // Do nothing, dCache is shutting down.
                   }
               }
           };

           thread.setDaemon(true);
           thread.start();

           return future;
       }
   }

   @Command(name = "send", hint = "send message to cell",
            description = "Sends MESSAGE to ADDRESS.")
   class SendCommand extends DelayedReply implements Callable<Serializable>, CellMessageAnswerable
   {
       @Option(name = "w", usage = "wait 10 seconds for answer to arrive")
       boolean wait;

       @Option(name = "nolocal", usage = "don't deliver locally")
       boolean nolocal;

       @Option(name = "noremote", usage = "don't deliver remotely")
       boolean noremote;

       @Argument(index = 0, metaVar = "address",
                 usage = "Colon separated path of cell addresses.")
       CellPath address;

       @Argument(index = 1, metaVar = "message")
       String message;

       @Override
       public Serializable call()
       {
           CellMessage msg = new CellMessage(address, message);
           if (wait) {
               _nucleus.sendMessage(msg, !nolocal, !noremote, this, MoreExecutors.directExecutor(), 10000);
               return this;
           } else {
               _nucleus.sendMessage(msg, !nolocal, !noremote);
               return "UOID = " + msg.getUOID();
           }
       }

       @Override
       public void answerArrived(CellMessage request, CellMessage answer)
       {
           Serializable obj = answer.getMessageObject();
           reply(obj == null ? answer : obj);
       }

       @Override
       public void exceptionArrived(CellMessage request, Exception exception)
       {
           reply(exception);
       }

       @Override
       public void answerTimedOut(CellMessage request)
       {
           reply("Timeout... ");
       }
   }


   ////////////////////////////////////////////////////////////
   //
   //   sleep
   //
   public static final String hh_sleep = "<secondsToSleep>" ;
   public String ac_sleep_$_1( Args args ) throws InterruptedException {
      int s = Integer.parseInt(args.argv(0));
      Thread.sleep( s*1000) ;
      return "Ready\n" ;

   }

    @Command(name = "ping")
    class Pingcommand extends DelayedReply implements Callable<Pingcommand>
    {
        @Argument(index = 0, metaVar = "destinationCell")
        CellPath destination;

        @Argument(index = 1, metaVar = "packetSize", required = false)
        int size;

        @Argument(index = 2, metaVar = "numOfPackets", required = false)
        int packets = 1;

        @Option(name = "timeout", metaVar = "millis")
        int timeout = 1000;

        private int count;

        private final Stopwatch sw = Stopwatch.createUnstarted();

        @Override
        public Pingcommand call() throws Exception
        {
            sw.start();
            ping();
            return this;
        }

        private void ping()
        {
            if (count < packets) {
                count++;
                _nucleus.sendMessage(new CellMessage(destination, new PingMessage(size)), true, true,
                                     new CellMessageAnswerable()
                                     {
                                         @Override
                                         public void answerArrived(CellMessage request, CellMessage answer)
                                         {
                                             ping();
                                         }

                                         @Override
                                         public void exceptionArrived(CellMessage request, Exception exception)
                                         {
                                             reply(exception);
                                         }

                                         @Override
                                         public void answerTimedOut(CellMessage request)
                                         {
                                             reply("Timeout");
                                         }
                                     }, MoreExecutors.directExecutor(), timeout);
            } else {
                reply(packets + " pings  in " + sw);
            }
        }
    }

   ////////////////////////////////////////////////////////////
   //
   //   create
   //
   @Command(name = "create", hint = "create a cell",
           description = "Create a cell within the current dCache domain. " +
                   "To create a cell requires, the cell name and the class " +
                   "it belongs. Depending on the class type of the cell, " +
                   "some necessary configuration might be required in other " +
                   "to instantiate the cell. This can be supply through the " +
                   "cellArg argument. If the configuration is more than one, " +
                   "since cellArg is a single argument, the configuration " +
                   "settings must be inside a quotation mark.")
   public class CreateCommand implements Callable<String>
   {
       @Argument(index = 0, usage = "Fully qualified name of the cell class. For example, " +
                                    "creating a topo cell requires specifying the following " +
                                    "class name: dmg.cells.network.TopoCell")
       String className;

       @Argument(index = 1, usage = "Name of the cell to be created.")
       String cellName;

       @Argument(index = 2, required = false, usage = "Arguments passed to the cell. The supported " +
                                                      "arguments are cell specific.")
       String cellArg = "";

       @Override
       public String call() throws ClassNotFoundException, NoSuchMethodException,
               InstantiationException, IllegalAccessException, InvocationTargetException,
               ClassCastException, CommandThrowableException, InterruptedException
       {
           Constructor<? extends CellAdapter> constructor =
                   Class.forName(className).asSubclass(CellAdapter.class).getConstructor(String.class, String.class);
           try {
               CellAdapter cell = constructor.newInstance(cellName, cellArg);
               if (cell instanceof EnvironmentAware) {
                   ((EnvironmentAware) cell).setEnvironment(Collections.unmodifiableMap(_environment));
               }
               cell.start();
               return "created : " + cell;
           } catch (InvocationTargetException e) {
               throw Throwables.propagate(e.getTargetException());
           } catch (ExecutionException e) {
               throw new CommandThrowableException(e.getCause().getMessage(), e.getCause());
           }
       }
   }

   ////////////////////////////////////////////////////////////
   //
   //   this and that
   //
   public static final String hh_onerror = "shutdown|exit|continue" ;
   public String ac_onerror_$_1( Args args ){
      if( args.argv(0).equals( "continue" ) ) {
          _doOnExit = null;
      } else {
          _doOnExit = args.argv(0);
      }
      return "" ;
   }
   public String ac_show_onexit( Args args ){
      return _doOnExit != null ? _doOnExit : "" ;
   }


    private static final int  PRINT_CELL          =    1;
    private static final int  PRINT_ERROR_CELL    =    2;
    private static final int  PRINT_NUCLEUS       =    4;
    private static final int  PRINT_ERROR_NUCLEUS =    8;
    private static final int  PRINT_FATAL         = 0x10;

   public static final String hh_say = "<things to echo ...> [-level=<level>]" ;
   public static final String fh_say =
                  "<things to echo ...> [-level=<level>]\n"+
                  " Levels :\n" +
                  "   say,esay,fsay\n"+
                  "   PRINT_CELL          =    1\n" +
                  "   PRINT_ERROR_CELL    =    2\n" +
                  "   PRINT_NUCLEUS       =    4\n" +
                  "   PRINT_ERROR_NUCLEUS =    8\n" +
                  "   PRINT_FATAL         = 0x10" ;

   public String ac_say_$_1_99( Args args )
   {
      StringBuilder sb = new StringBuilder() ;

      for( int i = 0 ; i < args.argc() ; i++ ) {
          sb.append(args.argv(i)).append(' ');
      }

      String msg = sb.toString() ;

      String levelString = args.getOpt("level") ;

      if( ( levelString != null ) && ( levelString.length() > 0 ) ){
          switch (levelString) {
          case "say":
              _log.info(msg);
              break;
          case "esay":
              _log.warn(msg);
              break;
          case "fsay":
              _log.error(msg);
              break;
          default:
              try {
                  int level = Integer.parseInt(levelString);
                  if ((level & PRINT_CELL) != 0) {
                      _log.info(msg);
                  }
                  if ((level & PRINT_ERROR_CELL) != 0) {
                      _log.warn(msg);
                  }
                  if ((level & PRINT_FATAL) != 0) {
                      _log.error(msg);
                  }
                  if ((level & PRINT_NUCLEUS) != 0) {
                      _logNucleus.info(msg);
                  }
                  if ((level & PRINT_ERROR_NUCLEUS) != 0) {
                      _logNucleus.warn(msg);
                  }
              } catch (NumberFormatException e) {
                  throw new IllegalArgumentException("Illegal Level string: " + levelString);
              }
              break;
          }
       }
      return msg ;
   }
   public static final String hh_echo = "<things to echo ...>" ;
   public String ac_echo_$_1_99( Args args ){
      StringBuilder sb = new StringBuilder() ;
      for( int i = 0 ; i < args.argc() ; i++ ) {
          sb.append(args.argv(i)).append(' ');
      }
      return sb.toString() ;
   }
   public static final String hh_show_error = "   # shows last errorCode and Message ";
   public String ac_show_error( Args args ){
     if( _errorCode == 0 ) {
         return "No Error found";
     }
     return "errorCode="+_errorCode+"; Msg = "+
           (_errorMsg==null?"None":_errorMsg) ;
   }
   public static final String hh_set_helpmode = "none|full" ;
   public String ac_set_helpmode_$_1( Args args ) throws CommandException {
      String mode = args.argv(0) ;
       switch (mode) {
       case "none":
           _helpMode = 0;
           break;
       case "full":
           _helpMode = 2;
           break;
       default:
           throw new CommandException(22, "Illegal Help Mode : " + mode);
       }
      return "" ;
   }
   public String ac_id( Args args ){
      return _nucleus.getCellDomainName()+"\n" ;
   }

   public static final String fh_check =
      " check [-strong] <var1> [<var2> [] ... ]\n"+
      "        checks if all of the specified variables are set.\n"+
      "        Returns an error it not.\n"+
      "        The -strong option requires that all variables must not be\n"+
      "        the zero string and must not only contain blanks\n" ;

   public static final String hh_check = "[-strong] <var1> [<var2> [] ... ]" ;
   public String ac_check_$_1_99( Args args )throws CommandException {

      boolean strong = args.hasOption("strong") ;

      String varName;
      Object value;
      for( int i= 0 ;i < args.argc() ; i++ ){
         varName = args.argv(i) ;
         if( ( value = _environment.get( varName ) ) == null ) {
             value = _nucleus.getDomainContext().get(varName);
         }
         if( value == null ) {
             throw new
                     CommandException(1, "variable is not defined : " + varName);
         }

         if( strong ){
             String strValue = value.toString() ;
             if( strValue.trim().equals("") ) {
                 throw new
                         CommandException(2, "variable is defined but empty : " + varName);
             }
         }
      }
      return "" ;

   }
   public static final String fh_import_context =
     "  import  context|env  [options] <variableName>\n" +
     "           options :\n"+
     "               -c                  : don't overwrite\n"+
     "               -source=env|context : only check the specifed\n"+
     "                                     source for the variableName\n"+
     "               -nr                 : don't run the variable resolver\n"+
     "\n"+
     "      The source is interpreted as a set of lines separated by\n"+
     "      newlines. Each line is assumed to contain a key value pair\n"+
     "      separated by the '=' sign.\n"+
     "      The context/environment variables are set according to\n"+
     "      the assignment.\n" ;
   public static final String fh_import_env = fh_import_context ;

   public static final String hh_import_context = "[-source=context|env] [-nr]"+
                                     "<contextVariableName>" ;
   public static final String hh_import_env     = "[-source=context|env] [-nr]"+
                                     "<environmentVariableName>" ;

   public String ac_import_context_$_1( Args args )throws CommandException {
       return imprt_dict( args , _nucleus.getDomainContext() ) ;
   }
   public String ac_import_env_$_1( Args args )throws CommandException {
       return imprt_dict( args , _environment ) ;
   }

    private String imprt_dict(Args args, Map<String,Object> dict)
        throws CommandException
    {
      String  varName = args.argv(0);
      boolean opt_overwrite = !args.hasOption("c");
      boolean resolve = !args.hasOption( "nr" );

      String src = args.getOpt("source");
      Object input;
      if (src == null) {
          input = _environment.get(varName);
          if (input == null) {
              input = _nucleus.getDomainContext().get(varName);
          }
      } else if (src.equals("env")) {
          input = _environment.get(varName);
      } else if (src.equals("context")) {
          input = _nucleus.getDomainContext().get(varName);
      } else {
          throw new CommandException("Invalid value for -source=" + src);
      }

      if (input == null) {
          throw new CommandException("Variable is not defined: " + varName);
      }

      try {
          Properties properties = new ReplaceableBackedProperties(this);
          properties.load(new StringReader(input.toString()));

          for (String key: properties.stringPropertyNames()) {
              if (opt_overwrite || (dict.get(key) == null)) {
                  String value = properties.getProperty(key);

                  int length = value.length();
                  if (length > 1 &&
                      value.charAt(0) == '"' &&
                      value.charAt(length - 1) == '"') {
                      value = value.substring(1, length - 1);
                  }

                  if (resolve) {
                      value = Formats.replaceKeywords(value, new PropertiesBackedReplaceable(properties));
                  }

                  dict.put(key, value);
              }
          }
      } catch (IllegalArgumentException | IOException e) {
          throw new CommandException(3, "Failed to read " + varName + ": " + e);
      }

        return "";
   }

   public static final String fh_set_context =
      "set context|env  [options]  <variableName>  <value>\n"+
      "        options :\n"+
      "          -c   :  do not overwrite the variable if it's already set\n"+
      "          -s   :  run the value through the interpreter and\n"+
      "                  convert  '\\n' to a real newline" ;
   public static final String fh_set_env = fh_set_context ;
   public static final String hh_set_context = "[-c][-s] <contextName> <value>" ;
   public static final String hh_set_env     = "[-c][-s] <environmentName> <value>" ;
   public String ac_set_context_$_2( Args args )throws CommandException{
      return set_dict( args , _nucleus.getDomainContext() ) ;
   }
   public String ac_set_env_$_2( Args args )throws CommandException{
      return set_dict( args , _environment ) ;
   }
    private String set_dict(Args args, Map<String,Object> dict)
        throws CommandException
    {
      String name  = args.argv(0) ;
      String value = args.argv(1) ;
      boolean opt_overwrite   = !args.hasOption("c") ;
      boolean opt_interpreter = args.hasOption("s") ;

      if( ( ! opt_overwrite ) && ( dict.get( name ) != null ) ) {
          throw new
                  CommandEvaluationException(1, "Variable " + name + " is already set and can't be overwritten due to '-c'");
      }


      if( opt_interpreter ){
         final int I_IDLE = 0 ;
         final int I_BS   = 1 ;
         int state = I_IDLE ;
         StringBuilder sb = new StringBuilder();
         for( int i = 0 ; i < value.length() ; i++ ){
            char c = value.charAt(i) ;
            switch( state ){
               case I_IDLE :
                  if( c == '\\' ){
                     state = I_BS ;
                  }else{
                     sb.append( c ) ;
                  }
               break ;
               case I_BS :
                  if( c == 'n' ){
                     state = I_IDLE ;
                     sb.append( '\n' ) ;
                  }else{
                     sb.append( '\\' ) ;
                     sb.append( c ) ;
                  }
               break ;

            }
         }
         value = sb.toString() ;

      }
      dict.put( name , value ) ;
      return "" ;

   }
   ////////////////////////////////////////////////////////////
   //
   // unsetting the context/environment
   //
   public static final String hh_unset_context="<contextName>" ;
   public static final String hh_unset_env    ="<environmentName>" ;
   public String ac_unset_context_$_1( Args args )throws CommandException {
      return unset_dict( args , _nucleus.getDomainContext() ) ;
   }
   public String ac_unset_env_$_1( Args args )throws CommandException {
      return unset_dict( args , _environment ) ;
   }

    private String unset_dict(Args args, Map<String,Object> dict)
           throws CommandException
    {
      String name = args.argv(0) ;
      Object o = dict.remove( name ) ;
      if( o == null ){
         throw new
         CommandException ( "Not found : "+name ) ;
      }else{
         return name+"<"+o.getClass().getName()+"> removed\n" ;
      }
   }
   ////////////////////////////////////////////////////////////
   //
   // displaying the context/environment variables
   //
   public static final String hh_ls = "[-l] [-ll] [-e] [-list]" ;
   public static final String fh_ls =
     " ls [options]\n"+
     "        Prints context/environment\n"+
     "    Options\n"+
     "       -l adds class name\n"+
     "       -ll adds first 40 chars of content\n"+
     "       -e  list environment instead of context\n"+
     "       -list  prints simple list instead of formatted one\n"+
     "\n";
   public static final String hh_show_context = "[<contextName>]" ;
   public static final String hh_show_env     = "[<environmentName>]" ;
   public static final String hh_test_context = "[<contextName>]" ;
   public static final String hh_test_env     = "[<environmentName>]" ;
   public String ac_ls_$_0_1( Args args ) throws CommandException {
      return ls_dict( args , args.hasOption("e")  ?
                             _environment :
                             _nucleus.getDomainContext()  ) ;
   }
   public String ac_show_context_$_0_1( Args args ) throws CommandException {
      return show_dict( args , _nucleus.getDomainContext() ) ;
   }
   public String ac_show_env_$_0_1( Args args ) throws CommandException {
      return show_dict( args , _environment ) ;
   }
   public String ac_test_context_$_0_1( Args args ) throws CommandException {
      return test_dict(args, _nucleus.getDomainContext()) ;
   }
   public String ac_test_env_$_0_1( Args args ) throws CommandException {
      return test_dict(args, _environment) ;
   }
    private String test_dict(Args args, Map<String,Object> dict)
        throws CommandException
    {
      String name  = args.argv(0) ;
      if( dict.get( name ) == null ){
         throw new
         CommandException( 66 , "not found : "+name );
      }return "" ;
   }
    private String show_dict(Args args, Map<String,Object> dict)
        throws CommandException
    {
      StringBuilder sb = new StringBuilder();
      if( args.argc() == 0 ){
          for (Map.Entry<String,Object> e: dict.entrySet()) {
              String name = e.getKey();
              Object o = e.getValue();
            if( o instanceof String ){
               sb.append(name).append("=") ;
               String line = (String)o ;
               int len = line.length() ;
               len = len > 40 ? 40 : len ;
               for( int i = 0 ; i < len ; i++ ) {
                   sb.append(line.charAt(i) == '\n' ? '$' : line.charAt(i));
               }
               if( len == 40 ) {
                   sb.append("...\n");
               } else {
                   sb.append("\n");
               }
            }else {
                sb.append(name).append("=<").append(o.getClass().getName())
                        .append(">\n");
            }
         }
      }else{
         String name  = args.argv(0) ;
         Object o = dict.get( name ) ;
         if( o == null ) {
             throw new
                     CommandException(23, "Context name " + name + " not found");
         }
         sb.append( o.toString() ) ;
      }
      return sb.toString() ;
   }
    private String ls_dict(Args args, Map<String,Object> dict)
        throws CommandException
    {
      StringBuilder sb = new StringBuilder();
      if( args.argc() == 0 ){
          int maxLength = 0 ;
          SortedSet<String> set = new TreeSet<>();

          for (String name: dict.keySet()) {
            maxLength = Math.max( maxLength , name.length() ) ;
            set.add(name);
          }
          boolean detail = args.hasOption("l") ;
          boolean moreDetail = args.hasOption("ll") ;
          if( moreDetail ) {
              detail = true;
          }
          boolean list   = args.hasOption("list") ;
          for (String name : set) {
              sb.append(name);
              if (detail) {
                  sb.append("   ");
                  if (!list) {
                      int diff = maxLength - name.length();
                      for (int i = 0; i < diff; i++) {
                          sb.append(".");
                      }
                  }
                  Object o = dict.get(name);
                  sb.append("  ").append(o.getClass().getName());
                  if (moreDetail) {
                      sb.append("\n          ");
                      String line = o.toString();
                      int len = line.length();
                      len = len > 40 ? 40 : len;
                      for (int i = 0; i < len; i++) {
                          sb.append(line.charAt(i) == '\n' ? '$' : line
                                  .charAt(i));
                      }
                      if (len == 40) {
                          sb.append("...");
                      }
                  }
              }
              sb.append("\n");
          }
      }else{
          throw new
          CommandSyntaxException("Not yet supported");
      }
      return sb.toString() ;
   }

   public static final String fh_test =
      "test <kind> <target>\n\n" +
      "  Check whether <target>, of type <kind>, is available in the current environment.\n" +
      "  If <target> is present then the return-code is zero, if not then a non-zero\n" +
      "  return-code is returned.\n\n" +
      "  Possible invocations are:\n" +
      "     -i <cell>   test if <cell> is running,\n" +
      "     -e <file>   test if <file> exists,\n" +
      "     -f <file>   test if <file> exists and is a normal file,\n" +
      "     -d <file>   test if <file> exists and is a directory";
   public static final String hh_test = "-i <cell> | -e <file> | -f <file> | -d <file>";
   public String ac_test_$_1(Args args) throws CommandEvaluationException {
       Tester tester = testerForArgs(args);

       if( !tester.test()){
           throw new CommandEvaluationException(1, tester.getMessage());
       }

       return "";
   }

   Tester testerForArgs( Args args) {
       if( args.argc() != 1) {
           throw new IllegalArgumentException( "Expecting exactly one argument");
       }

       if( args.hasOption("i")) {
           return new CellRunningTester(args);
       } else  if( args.hasOption( "e")) {
           return new FileExistsTester(args);
       } else  if( args.hasOption( "f")) {
           return new FileIsNormalTester(args);
       } else  if( args.hasOption( "d")) {
           return new FileIsDirectoryTester(args);
       } else {
           throw new IllegalArgumentException( "Expecting either -cell or -file");
       }
   }

   public static final String fh_exec =
      "exec [<options>] <url> [<args>]\n" +
      "exec context [<options>] <contextName> [<args>]\n" +
      "exec env [<options>] <envName> [<args>]\n"+
      "\n"+
      "   Executes the content of an env or context variable or the\n" +
      "   resource identified by the URL.\n"+
      "     -shell : opens a new shell for the execution\n"+
      "     -nooutput : discard the output of the executed commands\n"+
      "     -loop=<variableContextName> : \n"+
      "        Executes the block for each line in <varContextName> as arg\n"+
      "     -ifok[=<varName>] : run the context/env ONLY if the \n"+
      "                         specified value of <varName> is '0'\n"+
      "                         The default <varName> is 'rc'\n"+
      "     -ifnotok[=<varName>]  : negation of -ifok\n\n";
    public static final String hh_exec =
        "[-shell] [-nooutput] [-loop=<variable>] [-ifok[=<variable>]|-ifnotok[=<variable>}] <url> [<args>]";
    public String ac_exec_$_1_99(Args args)
        throws CommandException
    {
        try {
            URI uri = new URI(args.argv(0));
            args.shift();
            return run_reader(uri, args);
        } catch (URISyntaxException e) {
            throw new CommandException(43 , e.getMessage());
        }
    }

    public static final String fh_exec_env = fh_exec;
    public static final String hh_exec_env =
        "[-shell] [-nooutput] [-loop=<variable>] [-ifok[=<variable>]|-ifnotok[=<variable>}] <envName> [<args>]";
    public String ac_exec_env_$_1_99(Args args) throws CommandException
    {
        try {
            URI uri = new URI("env",  args.argv(0), null);
            args.shift();
            return run_reader(uri, args);
        } catch (URISyntaxException e) {
            throw new CommandException(43, e.getMessage());
        }
    }

    public static final String fh_exec_context = fh_exec;
    public static final String hh_exec_context =
        "[-shell] [-nooutput] [-loop=<variable>] [-ifok[=<variable>]|-ifnotok[=<variable>}] <contextName> [<args>]";
    public String ac_exec_context_$_1_99(Args args) throws CommandException
    {
        try {
            URI uri = new URI("context", args.argv(0), null);
            args.shift();
            return run_reader(uri, args);
        } catch (URISyntaxException e) {
            throw new CommandException(43 , e.getMessage());
        }
    }

    private void println(Writer out, String s)
        throws IOException
    {
        if (!s.isEmpty()) {
            out.append(s);
            if ((s.length() > 0) && (s.charAt(s.length() - 1) != '\n')) {
                out.append('\n');
            }
        }
    }

    public void execute(String source, Reader in, Args args)
        throws CommandExitException, IOException
    {
        try (Writer out = new BufferedLineWriter(new Slf4jInfoWriter(_log));
             Writer err = new BufferedLineWriter(new Slf4jErrorWriter(_log))) {
            execute(source, in, out, err, args);
        }
    }

    public void execute(String source, Reader in, Writer out, Writer err, Args args)
        throws CommandExitException, IOException
    {
        List<String> store = _argumentVector;
        int no = 1;
        try {
            _argumentVector  = new Vector<>();
            for (int i = 0; i < args.argc(); i++) {
                _argumentVector.add(args.argv(i));
            }

            String line;
            StringBuilder sb = null;
            BufferedReader input = new BufferedReader(in);
            for (; (line = input.readLine()) != null; no = no + 1) {
                /* Skip empty and comment lines.
                 */
                String s = line.trim();
                if (s.length() == 0 || s.charAt(0) == '#') {
                    continue;
                }

                /* Handle line continuation.
                 */
                int len = line.length();
                if (line.charAt(len - 1) == '\\') {
                    if (sb == null) {
                        sb = new StringBuilder();
                    }
                    sb.append(line.substring(0, len - 1)).append(' ');
                    continue;
                } else if (sb != null) {
                    sb.append(line);
                    line = sb.toString();
                    sb = null;
                }

                /* Execute command.
                 */
                Object answer = objectCommand2(line);

                /* Process result.
                 */
                if (!(answer instanceof Throwable)) {
                    println(out, answer.toString());
                } else {
                    Throwable error = (Throwable) answer;

                    if (error instanceof CommandPanicException) {
                        _log.error("Bug detected in dCache; please report this " +
                                "to <support@dcache.org> with the following " +
                                "information.", error.getCause());
                    }

                    if (_doOnExit != null) {
                        String msg =
                            String.format("%s: line %d: %s", source, no,
                                          error.getMessage());
                        if (_doOnExit.equals("shutdown")) {
                            throw new CommandExitException(msg, 666, error);
                        } else if (error instanceof CommandException) {
                            int rc = ((CommandException) error).getErrorCode();
                            throw new CommandExitException(msg, rc, error);
                        } else {
                            throw new CommandExitException(msg, 1, error);
                        }
                    }

                    /* CommandEvaluationException does not generate
                     * output since it is not really an error. Runtime
                     * exceptions other than IllegalArgumentException
                     * are logged. Other exceptions are printed to the
                     * error output.
                     */
                    if (error instanceof IllegalArgumentException) {
                        String msg =
                            String.format("%s: line %d: Illegal argument (%s)",
                                          source, no, error.getMessage());
                        println(err, msg);
                    } else if (error instanceof RuntimeException) {
                        _log.warn(error.toString(), error);
                    } else if (!(error instanceof CommandEvaluationException)) {
                        String msg =
                            Exceptions.getMessageWithCauses(error);
                        println(err, String.format("%s: line %d: Command failed (%s)",
                                                   source, no, msg));
                    }
                }
            }
        } catch (IOException e) {
            throw new IOException(String.format("%s: line %d: %s", source,
                                                no, e.getMessage()), e);
        } catch (RuntimeException e) {
            throw new RuntimeException(String.format("%s: line %d: %s", source,
                    no, e.toString()), e);
        } finally {
            _argumentVector = store;
        }
    }

    private String run_reader(URI uri, Args args)
        throws CommandException
    {
        String loopName = args.getOpt("loop");
        String var;
        if ((var = args.getOpt("ifok")) != null) {
            if (var.equals("")) {
                if (_errorCode != 0) {
                    return "";
                }
            } else {
                Object x = getDictionaryEntry(var) ;
                if ((x == null) || (!x.toString().equals("0"))) {
                    return "";
                }
            }
        }
        if ((var = args.getOpt("ifnotok")) != null) {
            if (var.equals("")) {
                if (_errorCode == 0) {
                    return "";
                }
            } else {
                Object x = getDictionaryEntry(var) ;
                if ((x != null) && (x.toString().equals("0"))) {
                    return "";
                }
            }
        }

        try {
            StringWriter out = new StringWriter();

            if (loopName == null) {
                CellShell shell =
                    (args.hasOption("shell"))
                    ? new CellShell(_nucleus)
                    : this;

                try (Reader in = open(uri)) {
                    shell.execute(uri.toString(), in, out, out, args);
                }

            } else {
                try (Reader loopReader = _nucleus
                        .getDomainContextReader(loopName)) {
                    BufferedReader reader = new BufferedReader(loopReader);
                    String line;
                    while ((line = reader.readLine()) != null) {
                        CellShell shell =
                                (args.hasOption("shell"))
                                        ? new CellShell(_nucleus)
                                        : this;

                        try (Reader in = open(uri)) {
                            shell.execute(uri.toString(), in, out, out,
                                    new Args(line));
                        }

                    }
                }

            }

            return args.hasOption("nooutput") ? "" : out.toString();
        } catch (StackOverflowError e) {
            throw new CommandExitException("Stack overflow", 2, e);
        } catch (FileNotFoundException e) {
            throw new CommandException(66, e.getMessage(), e);
        } catch (IOException e) {
            throw new CommandExitException("I/O error: " + e.getMessage(), 11);
        }
    }

   public static final String hh_eval = "upn expression" ;
   public String ac_eval_$_1_99( Args args )throws CommandException{
       Stack<String> v = new Stack<>() ;
       for( int i = 0 ; i < args.argc() ; i++ ){

          if( args.argv(i).equals("==") ){
          //                   -------------
             Object right = v.pop() ;
             Object left  = v.pop() ;
             v.push(right.equals(left) ?"0" :"1") ;

          }else if( args.argv(i).equals("!=") ){
          //                   -------------------
             Object right = v.pop() ;
             Object left  = v.pop() ;
             v.push(right.equals(left)?"1":"0") ;

          }else if( args.argv(i).equals("&&") ){
          //                   -------------------
             Object right = v.pop() ;
             Object left  = v.pop() ;
             v.push(
                 right.equals("0")&&left.equals("0")?
                 "0":"1") ;

          }else if( args.argv(i).equals("||") ){
          //                   -------------------
             Object right = v.pop() ;
             Object left  = v.pop() ;
             v.push(
                 right.equals("0")||left.equals("0")?
                 "0":"1") ;

          }else if( args.argv(i).equals("!") ){
          //                   -------------------
             Object right = v.pop() ;
             v.push(right.equals("0")?"1":"0") ;

          }else{
             v.push( args.argv(i).trim() ) ;
          }

       }
       if( v.size() != 1 ) {
          throw new
          CommandException( 2 , "Stack position violation ("+v.size()+")" ) ;
       }

       String result = v.firstElement() ;
       if( result.equals("0") ) {
           return "";
       }

       int rc;
       try{
          rc = Integer.parseInt(result) ;
       }catch(NumberFormatException nfe){
          rc = 3 ;
       }

       throw new
       CommandEvaluationException( rc , "Eval Result : "+result ) ;

   }
   public static final String hh_define_context = "<contextName> [<delimiter>]" ;
   public String ac_define_context_$_1_2( Args args ){
       _contextName      = args.argv(0) ;
       _contextDelimiter = args.argc() > 1 ? args.argv(1) : "." ;
       _contextString    = new StringBuilder() ;
       return "" ;
   }
   public static final String hh_define_env = "<environmentName>" ;
   public String ac_define_env_$_1_2( Args args ){
       _envName      = args.argv(0) ;
       _envDelimiter = args.argc() > 1 ? args.argv(1) : "." ;
       _envString    = new StringBuilder();
       return "" ;
   }
   public static final String hh_load_context = "[-b] <contextName> <fileName>" ;
   public String ac_load_context_$_2( Args args ) throws CommandException {
      String name = args.argv(0) ;
      File   file = new File( args.argv(1) ) ;

      if( ! file.canRead()  ) {
          throw new CommandException("File not found : " + args.argv(1));
      }

      if( ( args.optc() != 0 ) && ( args.optv(0).equals("-b") ) ){
         FileInputStream in = null ;
         try{
            long fileLength = file.length() ;
            byte [] buffer = new byte[(int)fileLength] ;
            in = new FileInputStream( file ) ;
            in.read( buffer ) ;
            in.close() ;
            _nucleus.getDomainContext().put( name , buffer ) ;
         }catch( IOException ioe ){

            throw new CommandException( 11 ,
                       "Problem with file : "+file+" : "+ioe ) ;
         }finally{
        	 if(in != null) {
                     try {
                         in.close();
                     } catch (IOException eeee) {
                     }
                 }
         }
      }else{
         StringBuilder sb = new StringBuilder();
         BufferedReader reader = null ;
         String         line;
         try{
            reader = new BufferedReader( new FileReader( file ) ) ;
            while( ( line = reader.readLine() ) != null ) {
                sb.append(line).append("\n");
            }
         }catch( IOException ioe ){

            throw new CommandException( 11 ,
                       "Problem with file : "+file+" : "+ioe ) ;
         }finally{
        	 if(reader != null) {
                     try {
                         reader.close();
                     } catch (IOException eeee) {
                     }
                 }
         }
         _nucleus.getDomainContext().put( name , sb.toString() ) ;
      }
      return "Loaded ... " ;
   }
   ////////////////////////////////////////////////////////////
   //
   // the incredible copy command
   //
   public static final String fh_copy =
      "   copy  <fromCellURL>  <toCellURL>\n"+
      "       <fromCellURL> : <extendedCellURL>\n"+
      "                        Protocols : env/context/cell/http/file/ftp\n"+
      "       <toCellURL>   : <env/context CellURL>\n"+
      "                        Protocols : env/context\n\n" +
      "       Protocols :\n"+
      "          env:<environmentVariable>\n"+
      "          context:<contextVariable>\n"+
      "          context://<cellPath>/<contextVariable>\n"+
      "          cell://<cellPath>/<requestString>\n" ;

   public static final String hh_copy = "<fromCellURL> <toCellURL>" ;
   public String ac_copy_$_2( Args args ) throws CommandException {
      URI from;
      URI to;
      try {
         from = new URI(args.argv(0));
         to = new URI(args.argv(1));
      } catch (URISyntaxException e) {
         throw new CommandException(43, "Invalid URL: "+ e.toString());
      }
      if (from.equals(to)) {
         throw new CommandException(43, "Source and destination URL must not be the same");
      }

      String source;
      try {
          try (BufferedReader in = new BufferedReader(open(from))) {
              String line;
              StringBuilder sb = new StringBuilder();
              while ((line = in.readLine()) != null) {
                  sb.append(line).append("\n");
              }
              source = sb.toString();
          }

      } catch (IOException e) {
          throw new CommandException(43, e.toString());
      }

      String scheme = to.getScheme();
      if (scheme == null) {
          scheme = "env";
      }

      String destination = to.getSchemeSpecificPart();
      if (destination == null) {
         throw new CommandException( 43 , "Destination missing");
      }

       switch (scheme) {
       case "env":
           _environment.put(destination, source);
           break;
       case "context":
           _nucleus.getDomainContext().put(destination, source);
           break;
       default:
           throw new CommandException(43, "Unsupported scheme for destination:" + scheme);
       }
      return "" ;
   }

   ////////////////////////////////////////////////////////////
   //
   // ----------------------------------------------
   //
   public static final String hh_exit = "[<exitCode> [<exitMessage>]]" ;
   public String ac_exit_$_0_2( Args args ) throws CommandExitException {
       String msg = "" ;
       int    code = 0 ;
       if( args.argc() > 0 ){
         try{
            code = Integer.parseInt(args.argv(0));
         }catch( Exception e ){
            code = 0 ;
         }
         if( args.argc() > 1 ){
            msg = args.argv(1) ;
         }
       }
       throw new CommandExitException( msg , code ) ;
   }

    private Reader open(URI uri)
        throws IOException
    {
        String scheme = uri.getScheme();
        String ssp = uri.getSchemeSpecificPart();

        if (scheme == null) {
            return new InputStreamReader(uri.toURL().openStream());
        } else if (scheme.equals("context")) {
            String host = uri.getHost();
            String path = uri.getPath();
            if (host == null) {
                return _nucleus.getDomainContextReader(ssp);
            } else {
                if (path == null || path.length() < 2) {
                    throw new MalformedURLException("Cell URI must be on the form: context://domainname/variable");
                }

                Object o = getRemoteData("System@" + host,
                                         "show context " + path.substring(1),
                                         4000);

                if (o instanceof Exception) {
                    throw new IOException(o.toString());
                }

                return new StringReader(o.toString());
            }
        } else if (scheme.equals("env")) {
            Object o = _environment.get(ssp);
            if (o == null) {
                throw new IOException("Variable is not defined: " + ssp);
            }
            return new StringReader(o.toString());
        } else if (scheme.equals("cell")) {
            String host = uri.getHost();
            String path = uri.getPath();
            if (host == null || path == null || path.length() < 2) {
                throw new MalformedURLException("Cell URI must be on the form: cell://cellname/command");
            }
            Object o = getRemoteData(host, path.substring(1), 4000);

            if (o instanceof Exception) {
                throw new IOException(o.toString());
            }

            return new StringReader(o.toString());
        } else {
            return new InputStreamReader(uri.toURL().openStream());
        }
    }

    private Object getRemoteData(String path, String command, long timeout)
        throws IOException
    {
        try {
            CellMessage answer =
                _nucleus.sendAndWait(new CellMessage(new CellPath(path), command), timeout);
            if (answer == null) {
                throw new IOException("Request timed out");
            }
            return answer.getMessageObject();
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.toString());
        } catch (ExecutionException | NoRouteToCellException e){
            throw new IOException("sendAndWait : " + e);
        }
    }

    private interface Tester {
        /** check for something */
        boolean test();
        /** Useful message if answer is false */
        String getMessage();
    }


    ////////////////////////////////////////////////////////////
    //
    //   domain class loader routines
    //
    @Command(name = "set classloader", hint = "obsolete", allowAnyOption = true)
    public class SetClassloaderCommand implements Callable<String>
    {
        @Argument
        String[] args;

        @Override
        public String call() throws IllegalArgumentException
        {
            return "obsolete";
        }
    }

    @Command(name = "show classloader", hint = "obsolete", allowAnyOption = true)
    public class ShowClassloaderCommand implements Callable<String>
    {
        @Argument
        String[] args;

        @Override
        public String call()
        {
            return "obsolete";
        }
    }

    /**
     * Test if a cell is running.
     */
    private class CellRunningTester implements Tester {
        private final String _name;

        CellRunningTester(Args args) {
            _name = args.argv(0);
        }

        @Override
        public boolean test() {
            return _nucleus.getCellInfo(_name) != null;
        }

        @Override
        public String getMessage() {
            return _name + " is not running";
        }
    }

    /**
     * Test presence of a file.
     */
    private class FileExistsTester implements Tester {
        private final File _file;

        FileExistsTester(Args args) {
            _file = new File(args.argv(0));
        }

        @Override
        public boolean test() {
            return _file.exists();
        }

        @Override
        public String getMessage() {
            return _file.toString() + " does not exist";
        }
    }

    /**
     * Test presence of a file and that the file is
     * not special
     */
    private class FileIsNormalTester implements Tester {
        private final File _file;
        private boolean _exists;

        FileIsNormalTester(Args args) {
            _file = new File(args.argv(0));
        }

        @Override
        public boolean test() {
            _exists = _file.exists();
            return _file.isFile();
        }

        @Override
        public String getMessage() {
            return _file.toString() + (_exists ? " is not a normal file" : " does not exist");
        }
    }

    /**
     * Test presence of a file and that the file is
     * not special
     */
    private class FileIsDirectoryTester implements Tester {
        private final File _file;
        private boolean _exists;

        FileIsDirectoryTester(Args args) {
            _file = new File(args.argv(0));
        }

        @Override
        public boolean test() {
            _exists = _file.exists();
            return _file.isDirectory();
        }

        @Override
        public String getMessage() {
            return _file.toString() + (_exists ? " is not a directory file" : " does not exist");
        }
    }
}

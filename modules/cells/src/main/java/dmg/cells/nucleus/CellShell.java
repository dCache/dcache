package dmg.cells.nucleus ;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.SortedSet;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
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
import dmg.util.command.CommandLine;
import dmg.util.command.DelayedCommand;
import dmg.util.command.Option;

import org.dcache.util.Args;
import org.dcache.util.ColumnWriter;
import org.dcache.util.Glob;

import static dmg.util.CommandException.checkCommand;

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

    enum ErrorAction
    {
        SHUTDOWN, EXIT, CONTINUE
    }

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
   private ErrorAction _doOnError = ErrorAction.CONTINUE;
   private final Map<String, Object> _environment =
           new ConcurrentHashMap<>();
   private CommandInterpreter _externalInterpreter;
   private ImmutableList<String> _argumentVector = ImmutableList.of();

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
           return String.valueOf(_errorCode);
       case "rmsg":
           return (_errorMsg == null ? "(0)" : _errorMsg);
       case "thisDomain":
           return _nucleus.getCellDomainName();
       case "thisCell":
           return _nucleus.getCellName();
       case "nextSequenceNumber":
           return String.valueOf(nextSequenceNumber());
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
         if (!str.isEmpty() && str.equals(_contextDelimiter)){

             _nucleus.getDomainContext().
                      put( _contextName , _contextString.toString() ) ;
             _contextString = null ;
             return null  ;

         }
         _contextString.append( str ).append('\n');
         return null ;
      }else if( _envString != null  ){
         //
         // are we in the define environment
         //
         if (!str.isEmpty() && str.equals(_envDelimiter)){

             _environment.put( _envName , _envString.toString() ) ;
             _envString = null ;
             return null  ;

         }
         _envString.append( str ).append('\n');
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

          switch (_doOnError) {
          case SHUTDOWN:
              throw new CommandExitException(ce.toString(), 666);
          case EXIT:
              throw new CommandExitException(ce.getErrorMessage(), ce.getErrorCode());
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
                   sb.append('\n').append(help).append('\n');
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
            sb.append( t.getClass().getName()).append(" : ").append(t.getMessage()).append('\n') ;
            return sb.toString() ;
         }else if( ce instanceof CommandPanicException ){
            CommandPanicException cpe = (CommandPanicException)ce ;
            StringBuilder sb = new StringBuilder() ;
            sb.append( "Panic : ").append(cpe.getMessage()).append('\n') ;
            Throwable t = cpe.getTargetException() ;
            sb.append( t.getClass().getName()).append(" : ").append(t.getMessage()).append('\n') ;
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
         _contextString.append( c ).append('\n');
         return "" ;
      }else {
          return super.command(c);
      }
   }

   ////////////////////////////////////////////////////////////
   //
   //  version
   //
    @Command(name = "version", hint = "query jar file metadata",
            description = "Information about the implementation-title, -vendor " +
                    "and -version, as described within some jar.  The jar file " +
                    "is the one that provides some specific Java package.  If " +
                    "the jar file implementation-vendor is 'dCache.org' then the " +
                    "implementation-version is the dCache version.")
    public class VersionCommand implements Callable<Serializable>
    {
        @Argument(required = false,
                usage = "The package used to select the jar file.")
        String packageName="dmg.cells.nucleus";

        @Override
        public Serializable call()
        {
            Package p = Package.getPackage(packageName);
            StringBuilder sb = new StringBuilder();
            if( p != null ){
                String tmp = p.getImplementationTitle();
                sb.append("ImplementationTitle:   ").append(tmp==null?"(Unknown)":tmp).append('\n');
                tmp = p.getImplementationVendor();
                sb.append("ImplementationVendor:  ").append(tmp==null?"(Unknown)":tmp).append('\n');
                tmp = p.getImplementationVersion();
                sb.append("ImplementationVersion: ").append(tmp==null?"(Unknown)":tmp).append('\n');
            }else{
                sb.append("No information found");
            }
            return sb.toString() ;
        }
    }
   ////////////////////////////////////////////////////////////
   //
   //   getroutes, getcelltunnelinfos, getcellinfos
   //

    /* Do not modify or remove this command: is used by pcells */
    @Command(name = "getroutes", hint = "list all routes",
            description = "List all message routes available in " +
                    "this domain. The returned information " +
                    "comprises of the target cell name, target domain name, " +
                    "gateway for this route and the route type (such as " +
                    "default, alias, domain, topic, etc).")
    public class GetroutesCommand implements Callable<CellRoute[]>
    {
        @Override
        public CellRoute[] call()
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
        public CellInfo call() throws CommandException
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
        public CellInfo[] call()
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
        public Serializable call() throws CommandException
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
      CellMessage answer;
      Serializable message = (command == null) ? new PingMessage() : command;

      Object o;
      boolean noRoute;
      while( true ){
          noRoute = false ;
          answer = null ;
          try{
            _log.warn( "waitForCell : Sending request" ) ;
              answer = _nucleus.sendAndWait(new CellMessage(destination , message), ((long) check) * 1000);
            _log.warn( "waitForCell : got {}", answer ) ;
         } catch (NoRouteToCellException e) {
            noRoute = true ;
         } catch (ExecutionException ignored) {
         } catch (InterruptedException e) {
            throw new CommandException(66, "sendAndWait problem : " + e, e);
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

    @Command(name="route", hint = "show routing table",
            description =
              "A cell message is sent with a destination path, consisting of "
            + "one or more 'hops', where each hop is a cell address.  The "
            + "routing table describes how messages are handled.  When given a "
            + "message to send, the domain will use the next address in the "
            + "destination path (the next-hop address) and the routing table "
            + "to decide how to handle the message."
            + "\n\n"
            + "The routine table has various routine table entries (or "
            + "'routes').  Each route has a TYPE, which provides both an "
            + "implicit priority and how the message will be handled.  "
            + "Routes have a DESTINATION address, which selects which messages "
            + "are affected by this route and many types also have a "
            + "TARGET address that has some type-specific meaning."
            + "\n\n"
            + "There are seven possible types: alias, default, domain, "
            + "dumpster, exact, queue, topic.  These have the following semantics:"
            + "\n\n"
            + "    ALIAS: if next-hop address matches the DESTINATION address "
            + "then rename next-hop address to the TARGET address and route "
            + "accordingly.  The routing table can have at most one ALIAS or "
            + "EXACT route with the same DESTINATION address."
            + "\n\n"
            + "    DEFAULT: deliver to the TARGET address.  If multiple DEFAULT "
            + "routes exist then one is choosen pseudo-randomly."
            + "\n\n"
            + "    DOMAIN: deliver to the TARGET address if the next-hop "
            + "address' domain matches the DESTINATION address domain.  The "
            + "routing table can have at most one DOMAIN route with the same "
            + "DESTINATION address domain."
            + "\n\n"
            + "    DUMPSTER: legacy type -- not supported."
            + "\n\n"
            + "    EXACT: if the next-hop address matches the DESTINATION "
            + "address then deliver to the TARGET address.  The routing table "
            + "can have at most one ALIAS or EXACT route with the same "
            + "DESTINATION address."
            + "\n\n"
            + "    QUEUE: if the next-hop address matches the DESTINATION "
            + "address then deliver to the TARGET address.  If the routing table "
            + "has multiple QUEUE routes with DESTINATION addresses that matches "
            + "a message's next-hop address then the message is delivered to the "
            + "TARGET address of one QUEUE route chosen pseudo-randomly."
            + "\n\n"
            + "    TOPIC: deliver to the TARGET address.  If the routing table "
            + "has multiple TOPIC routes with DESTINATION addresses that "
            + "matches a message next-hop address then that message is "
            + "delivered to all corresponding TARGET addresses."
            + "\n\n"
            + "Message routing is handled in the following fashion:"
            + "\n\n"
            + "    1. if the domain of the message next-hop address is this "
            + "domain then try to deliver the message to the corresponding "
            + "cell and no further action is taken.  If no such cell exists "
            + "then return an error to the sender."
            + "\n\n"
            + "    2. if the message next-hop address matches one or more "
            + "TOPIC route DESTINATION addresses then deliver the message to each "
            + "corresponding TARGET address.  No further action is taken."
            + "\n\n"
            + "    3. If the message next-hop address matches an EXACT route "
            + "DESTINATION address then the message is delivered to the "
            + "corresponding TARGET address and no further action is taken."
            + "\n\n"
            + "    4. If the message next-hop address matches an ALIAS route "
            + "DESTINATION address then change the next-hop address to the "
            + "corresponding TARGET address and route accordingly."
            + "\n\n"
            + "    5. If the message next-hop address has domain 'local' "
            + "then try matching QUEUE routes, otherwise try matching "
            + "DOMAIN routes."
            + "\n\n"
            + "    6. Deliver to the DEFAULT route TARGET address if the "
            + "routing table has one."
            + "\n\n"
            + "    7. Return an error to the sender.")
    public class RouteCommand implements Callable<String>
    {
        @Option(name="destination",
                usage="Show only routes with a matching DESTINATION address. "
                        + "This option value is a glob pattern.")
        private Glob destination = Glob.ANY;

        @Option(name="type",
                // Note: must keep values in sync with CellRoute#TYPE_NAMES
                values={"auto", "exact", "queue", "domain",
                        "default", "dumpster", "alias", "topic"},
                usage="Limit output to routing entries of this type. Default "
                        + "shows all routes.")
        private String type;

        @Option(name="target",
                usage="Show only routes with a matching TARGET address.  This "
                        + "option value is a glob pattern.")
        private Glob target = Glob.ANY;

        @Override
        public String call()
        {
            ColumnWriter writer = new ColumnWriter()
                    .header("TYPE").left("type").space()
                    .header("DESTINATION").right("dest-cell").fixed("@").left("dest-domain").space()
                    .header("TARGET").right("target-cell").fixed("@").left("target-domain");

            Arrays.stream(_nucleus.getRoutingList())
                    .filter(this::matches)
                    .forEach(r -> {
                                writer.row()
                                        .value("dest-cell", r.getCellName())
                                        .value("dest-domain", r.getDomainName())
                                        .value("type", r.getRouteTypeName().toUpperCase())
                                        .value("target-cell", r.getTarget().getCellName())
                                        .value("target-domain", r.getTarget().getCellDomainName());
                            });

            return writer.toString();
        }

        private boolean matches(CellRoute r)
        {
            return (type == null || type.equalsIgnoreCase(r.getRouteTypeName()))
                    && destination.matches(r.getCellName() + "@" + r.getDomainName())
                    && target.matches(r.getTarget().toString());
        }
    }

    private abstract class RouteSpecifyingCommand
    {
        @Option(name = "alias", usage="Specifies an ALIAS route type.")
        boolean aliasType;

        @Option(name = "auto", usage="Specifies an AUTO route type.  This is "
                + "the default if no type is specified.")
        boolean autoType;

        @Option(name = "default", usage="Specifies an DEFAULT route type.  A "
                + "DESTINATION address must not be supplied.")
        boolean defaultType;

        @Option(name = "domain", usage="Specifies an DOMAIN route type")
        boolean domainType;

        @Option(name = "dumpster", usage="Specifies an DUMPSTER route type.  A "
                + "DESTINATION address must not be supplied.")
        boolean dumpsterType;

        @Option(name = "exact", usage="Specifies an EXACT route type.")
        boolean exactType;

        @Option(name = "queue", usage="Specifies an QUEUE route type.")
        boolean queueType;

        @Option(name = "topic", usage="Specifies an TOPIC route type.")
        boolean topicType;

        @Option(name = "wellknown", usage="Specifies an QUEUE route type.  "
                + "This option exists for backwards compatibility.")
        boolean wellknownType;

        @Option(name = "zone", usage="Specifies preferred zone.")
        String zone;

        @Argument(index=-2, required=false,
                usage="The DESTINATION address for this route.  These limits "
                        + "the effect of a route to only those messages with a "
                        + "next-hop address that matches this value."
                        + "\n\n"
                        + "The exact match depends on the route type.  It must "
                        + "not be specified for DEFAULT and DUMPSTER route "
                        + "types but is required for all others.")
        String destination;

        @Argument(index=-1,
                usage="The TARGET address for this route.  This argument is "
                        + "interpreted depending on the route type.  Typically "
                        + "it is the address to which a message is sent.")
        String target;

        private int updateType(int currentType, boolean hasOption, int newType)
                throws CommandException
        {
            if (hasOption) {
                if (currentType != -1) {
                    throw new CommandException(String.format("Multiple route types specified (%s and %s)",
                            CellRoute.TYPE_NAMES[currentType], CellRoute.TYPE_NAMES[newType]));
                }
                currentType = newType;
            }

            return currentType;
        }

        private int getType() throws CommandException
        {
            int type = -1;

            type = updateType(type, aliasType, CellRoute.ALIAS);
            type = updateType(type, autoType, CellRoute.AUTO);
            type = updateType(type, defaultType, CellRoute.DEFAULT);
            type = updateType(type, domainType, CellRoute.DOMAIN);
            type = updateType(type, dumpsterType, CellRoute.DUMPSTER);
            type = updateType(type, exactType, CellRoute.EXACT);
            type = updateType(type, queueType, CellRoute.QUEUE);
            type = updateType(type, topicType, CellRoute.TOPIC);
            type = updateType(type, wellknownType, CellRoute.QUEUE);

            return type == -1 ? CellRoute.AUTO : type;
        }

        private void checkArguments(int type) throws CommandException
        {
            switch (type) {
            case CellRoute.DEFAULT:
            case CellRoute.DUMPSTER:
                checkCommand(destination == null, "Too many arguments");
                break;
            default:
                checkCommand(destination != null, "Not enough arguments");
                break;
            }
        }

        protected CellRoute getCellRoute() throws CommandException
        {
            int type = getType();
            checkArguments(type);
            return new CellRoute(destination, new CellAddressCore(target), Optional.ofNullable(zone), type);
        }
    }

    @Command(name="route add", hint = "add an entry to the routing table",
            description = "Add a new route to the routing table."
                    + "\n\n"
                    + "NOTE: dCache adds routing entries automatically; "
                    + "therefore, this command SHOULD NOT be needed under "
                    + "normal operational conditions."
                    + "\n\n"
                    + "NOTE: incorrect changes to the message routing table "
                    + "could lead to significant failures or failures that "
                    + "are difficult to diagnose.  Therefore, only those with "
                    + "detailed knowledge of dCache messaging internals should "
                    + "use this command.")
    public class RouteAddCommand extends RouteSpecifyingCommand implements Callable<String>
    {
        @Override
        public String call() throws CommandException
        {
            try {
                _nucleus.routeAdd(getCellRoute());
            } catch (IllegalArgumentException e) {
                throw new CommandException(e.getMessage());
            }
            return "Done\n";
        }
    }

    @Command(name="route delete", hint = "remove a route from the routing table",
            description = "Removes an existing route from the routing table."
                    + "\n\n"
                    + "NOTE: dCache removes routing entries automatically; "
                    + "therefore, this command SHOULD NOT be needed under "
                    + "normal operational conditions."
                    + "\n\n"
                    + "NOTE: incorrect changes to the message routing table "
                    + "could lead to significant failures or failures that "
                    + "are difficult to diagnose.  Therefore, only those with "
                    + "detailed knowledge of dCache messaging internals should "
                    + "use this command.")
    public class RouteDeleteCommand extends RouteSpecifyingCommand implements Callable<String>
    {
        @Override
        public String call() throws CommandException
        {
            try {
                _nucleus.routeDelete(getCellRoute());
            } catch (IllegalArgumentException e) {
                throw new CommandException(e.getMessage());
            }
            return "Done\n";
        }
    }

    @Command(name="route find", hint="simulate message routing",
            description = "Exercise the routing table to discover where a "
                    + "message with a specific next-hop address would be "
                    + "delivered."
                    + "\n\n"
                    + "NOTE: delivery of messages to locally running cells are "
                    + "not considered.")
    public class RouteFindCommand implements Callable<String>
    {
        @Argument(usage="The next-hop address of the simulated message.")
        CellAddressCore address;

        @Override
        public String call() throws Exception
        {
            CellRoute route = _nucleus.routeFind(address);
            if (route == null) {
                return "No route for address " + address + '\n';
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Routing details for a message with ").append(address).append(" as next-hop address:\n");
            sb.append("         type: ").append(route.getRouteTypeName().toUpperCase()).append('\n');
            sb.append("  destination: ").append(route.getCellName()).append('@').append(route.getDomainName()).append('\n');
            sb.append("       target: ").append(route.getTarget()).append('\n');
            sb.append("         zone: ").append(route.getZone().orElse("Undefined")).append('\n');

            return sb.toString();
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
                   "the number of message queues, an estimate of how " +
                   "long a message stays on the queue, the thread count, " +
                   "the class name of the cell and lastly, a short " +
                   "description of the cell itself." +
                   "\n\n" +
                   "When a particular cell is specify, a summarised " +
                   "information on the cell is returned. With the option " +
                   "'-f', all information about the cell will be return in a " +
                   "comprehensive and detailed manner.")
   public class PsCommand implements Callable<String>
   {
       private static final String INDENTATION = "   ";

       @Argument(usage = "specify a cell or list of cell names", required = false)
       String[] names;

       @Option(name="f",
               usage = "display with the full attributes" )
       boolean full;

       private final StringBuilder sb = new StringBuilder();

       private void appendWithIndentation(String indentation, String value)
       {
            for (String line : Splitter.on('\n').split(value.trim())) {
               sb.append(indentation).append(line).append('\n');
            }
       }

       @Override
       public String call()
       {
            if (names == null) {
               List<String> list = _nucleus.getCellNames();
               if (full) {
                   ColumnWriter table = new ColumnWriter().headersInColumns()
                           .header("Name").left("name").space()
                           .header("State").centre("state").space()
                           .header("Queue").right("queue-length").space()
                           .header("Q-time/ms").right("queue-time").space()
                           .header("Threads").right("thread").space()
                           .header("Class").left("class").space()
                           .header("Additional info").left("short-info");
                   for (String name: list) {
                       CellInfo info = _nucleus.getCellInfo(name);
                       if (info == null) {
                           table.row("name " + name);
                       } else {
                           // Work around cells where toString shows only
                           // the cell name.
                           String shortInfo = info.getShortInfo().equals(name) ? "" : info.getShortInfo();
                           table.row().value("name", name)
                                   .value("state", info.getStateName().substring(0,1))
                                   .value("queue-length", info.getEventQueueSize())
                                   .value("queue-time", info.getExpectedQueueTime())
                                   .value("thread", info.getThreadCount())
                                   .value("class", info.getCellSimpleClass())
                                   .value("short-info", shortInfo);
                       }
                   }
                   sb.append(table);
               } else {
                   for (String name: list) {
                       sb.append(name).append('\n');
                   }
               }
            } else {
               boolean isMultiple = names.length > 1;
               String secondIndentation = isMultiple ? (INDENTATION + INDENTATION) : INDENTATION;
               String firstIndentation = isMultiple ? INDENTATION : "";
               boolean isFirst = true;

               for (String name : names) {
                   if (isFirst) {
                       isFirst = false;
                   } else {
                       sb.append('\n');
                   }
                   CellInfo info = _nucleus.getCellInfo(name);
                   if (info == null) {
                       sb.append(name).append(" Not found\n");
                       continue;
                   }
                    if (isMultiple) {
                        sb.append("=== ").append(name).append(" ===\n");
                    }
                    if (full) {
                        ColumnWriter generalInfo = new ColumnWriter().fixed(secondIndentation)
                                .left("name").fixed(" : ").left("value");
                        generalInfo.row().value("name", "Cell").value("value", info.getCellName() + "@" + info.getDomainName());
                        generalInfo.row().value("name", "Class").value("value", info.getCellClass());
                        generalInfo.row().value("name", "State").value("value", info.getStateName());
                        generalInfo.row().value("name", "Queue length").value("value", info.getEventQueueSize());
                        generalInfo.row().value("name", "Queue time").value("value", info.getExpectedQueueTime() + " ms");
                        CellVersion version = info.getCellVersion();
                        if (version != null) {
                            generalInfo.row().value("name", "Version").value("value", version);
                        }
                        sb.append(firstIndentation).append("-- Generic information --\n").append(generalInfo);

                        ColumnWriter threadsInfo = new ColumnWriter().headersInColumns()
                                .fixed(secondIndentation)
                                .header("Name").left("name").space()
                                .header("Priority").right("priority").space()
                                .header("State").left("state");
                        Thread[] threads = _nucleus.getThreads(name);
                        for (int j = 0; j < threads.length && threads[j] != null; j++) {
                            Thread t = threads [j];
                            threadsInfo.row().value("name", t.getName())
                                    .value("priority", t.getPriority())
                                    .value("state", (t.isAlive() ? "A" : "-") + (t.isDaemon() ? "D" : "-") + (t.isInterrupted() ? "I" : "-"));
                        }

                        sb.append('\n').append(firstIndentation).append("-- Threads --\n").append(threadsInfo);

                        sb.append('\n').append(firstIndentation).append("-- Cell-specific Information --\n");
                        appendWithIndentation(secondIndentation, info.getPrivatInfo());
                    } else {
                        appendWithIndentation(firstIndentation, info.getPrivatInfo());
                    }
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
                           _nucleus.kill(cellName).get();
                       } catch (ExecutionException e) {
                           response = e.getCause();
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
               _nucleus.sendMessage(msg, !nolocal, !noremote, true, this, MoreExecutors.directExecutor(), 10000);
               return this;
           } else {
               _nucleus.sendMessage(msg, !nolocal, !noremote, true);
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

    @Command(name = "traceroute", hint = "print the domains messages take",
            description = "Prints the cell paths a cell message follows in both the outbound and inbound direction.")
    class TracerouteCommand extends DelayedReply implements Callable<Serializable>, CellMessageAnswerable
    {
        @Argument(metaVar = "address", usage = "Colon separated path of cell addresses.")
        CellPath address;

        @Option(name = "nolocal", usage = "don't deliver locally")
        boolean nolocal;

        @Option(name = "noremote", usage = "don't deliver remotely")
        boolean noremote;

        @Override
        public Serializable call()
        {
            CellMessage msg = new CellMessage(address, new PingMessage());
            _nucleus.sendMessage(msg, !nolocal, !noremote, true, this, MoreExecutors.directExecutor(), 10000);
            return this;
        }

        @Override
        public void answerArrived(CellMessage request, CellMessage answer)
        {
            reply(((PingMessage) answer.getMessageObject()).getOutboundPath() + " -> " + answer.getSourcePath());
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

    @Command(name = "sleep", hint = "wait for a period of time",
            description = "Wait for the specified duration of time then " +
                    "return 'Ready'.")
    public class SleepCommand extends DelayedCommand<String>
    {
        @Argument(metaVar = "seconds",
                usage = "The amount of time to be asleep.")
        int sleepTime;

        @Override
        public String execute() throws InterruptedException
        {
            Thread.sleep(sleepTime*1000);
            return "Ready\n";
        }
    }

    @Command(name = "ping", hint = "send a message and wait for the reply",
            description = "A ping message of a default or specified size is send to " +
                    "the destination cell. The message is processed and a reply is " +
                    "send back to the sender. This procedure is repeated based on the " +
                    "number of iterations specified. A timeout message is returned as " +
                    "a result of the termination of the command. This happens if the " +
                    "timeout duration elapsed before the command finish its execution. " +
                    "On a successful run, the number of pings with the time taken will " +
                    "be printed.\n\n" +
                    "The ping command can be use to test a connection to a cell, " +
                    "check the latency of the message system, and to verify " +
                    "if a cell is up and running.")
    public class PingCommand extends DelayedReply implements Callable<PingCommand>
    {
        @Argument(index = 0,
                usage = "Name of the cell to be pinged.")
        CellPath destinationCell;

        @Argument(index = 1, required = false, metaVar = "bytes",
                usage = "The size of the message to be sent.")
        int messageSize;

        @Argument(index = 2, required = false,
                usage = "The number of times the cell should be pinged.")
        int packets = 1;

        @Option(name = "timeout", valueSpec = "MILLISECONDS",
                usage = "The duration of time that ping waits for a reply.")
        int timeout = 1000;

        private int count;

        private final Stopwatch sw = Stopwatch.createUnstarted();

        @Override
        public PingCommand call()
        {
            sw.start();
            ping();
            return this;
        }

        private void ping()
        {
            if (count < packets) {
                count++;
                _nucleus.sendMessage(new CellMessage(destinationCell, new PingMessage(messageSize)), true, true,
                                     true, new CellMessageAnswerable()
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

       @Option(name = "async")
       boolean isAsync;

       @Override
       public String call() throws ClassNotFoundException, NoSuchMethodException,
               InstantiationException, IllegalAccessException, InvocationTargetException,
               ClassCastException, CommandException, InterruptedException
       {
           checkCommand(_nucleus.getCellInfo(cellName) == null,
                   "Cell %s already exists.", cellName);

           Constructor<? extends CellAdapter> constructor =
                   Class.forName(className).asSubclass(CellAdapter.class).getConstructor(String.class, String.class);
           try {
               CellAdapter cell = constructor.newInstance(cellName, cellArg);
               if (cell instanceof EnvironmentAware) {
                   ((EnvironmentAware) cell).setEnvironment(Collections.unmodifiableMap(_environment));
               }
               CompletableFuture<Void> startup = cell.start();
               if (!isAsync) {
                   startup.get();
               } else {
                   startup.whenComplete((r, t) -> {
                       if (t != null) {
                           _log.error("failed create {}: {}", cellName, t.toString());
                       } else {
                           _log.info("created: {}", cellName);
                       }
                   });
               }
               return "created : " + cell;
           } catch (CancellationException e) {
               throw new CommandThrowableException("Startup of " + cellName + " was cancelled.", e);
           } catch (InvocationTargetException e) {
               Throwables.throwIfUnchecked(e.getCause());
               throw new RuntimeException(e.getCause());
           } catch (ExecutionException e) {
               Throwables.throwIfInstanceOf(e.getCause(), CommandException.class);
               throw new CommandThrowableException(e.getCause().getMessage(), e.getCause());
           }
       }
   }

    ////////////////////////////////////////////////////////////
    //
    //   this and that
    //
    @Command(name = "onerror", hint = "set error action",
            description = "Defines how the command interpreter reacts to processing errors.")
    class OnErrorCommand implements Callable<String>
    {
        @Argument(valueSpec = "shutdown|exit|continue",
                usage = "shutdown:\n" +
                        "\tterminate dCache domain.\n" +
                        "exit:\n" +
                        "\tterminate interpreter.\n" +
                        "continue:\n" +
                        "\tignore error.")
        ErrorAction action;

        @Override
        public String call()
        {
            _doOnError = action;
            return "";
        }
    }

    @Command(name = "show onerror", hint = "show current error action",
            description = "Shows how the command interpreter reacts to errors. The action can " +
                          "be set using the onerror command.")
    class ShowOnErrorCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            return _doOnError.toString().toLowerCase();
        }
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

      if( ( levelString != null ) && (!levelString.isEmpty()) ){
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
      return _nucleus.getCellDomainName() + '\n';
   }

    @Command(name = "check", hint = "check if variables are defined",
            description = "Determine if all variables are defined. " +
                    "An error is return if at least one of the variables " +
                    "is not defined.")
    public class CheckCommand implements Callable<String>
    {
        @Argument(usage = "One or more variable names to check.")
        String[] varName;

        @Option(name = "strong",
                usage = "This returns an error if any of the variables " +
                        "contain only whitespace.")
        boolean strong;


        @Override
        public String call() throws CommandException
        {
            Object value;
            for (String name : varName) {
                if ((value = _environment.get(name)) == null) {
                    value = _nucleus.getDomainContext().get(name);
                }
                if (value == null) {
                    throw new
                            CommandException(1, "variable is not defined : " + name);
                }

                if (strong) {
                    String strValue = value.toString();
                    if (strValue.trim().isEmpty()) {
                        throw new
                                CommandException(2, "variable is defined but empty : " + name);
                    }
                }
            }
            return "" ;
        }
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
         return name + '<' + o.getClass().getName() + "> removed\n" ;
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
               sb.append(name).append('=') ;
               String line = (String)o ;
               int len = line.length() ;
               len = len > 40 ? 40 : len ;
               for( int i = 0 ; i < len ; i++ ) {
                   sb.append(line.charAt(i) == '\n' ? '$' : line.charAt(i));
               }
               if( len == 40 ) {
                   sb.append("...\n");
               } else {
                   sb.append('\n');
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
         sb.append(o) ;
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
                          sb.append('.');
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
              sb.append('\n');
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
            if (!s.isEmpty() && s.charAt(s.length() - 1) != '\n') {
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
        ImmutableList<String> store = _argumentVector;
        int no = 1;
        try {
            _argumentVector = args.getArguments();

            String line;
            StringBuilder sb = null;
            BufferedReader input = new BufferedReader(in);
            for (; (line = input.readLine()) != null; no = no + 1) {
                /* Skip empty and comment lines.
                 */
                String s = line.trim();
                if (s.isEmpty() || s.charAt(0) == '#') {
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

                if (answer instanceof DelayedReply) {
                    answer = ((DelayedReply)answer).take();
                }

                /* Process result.
                 */
                if (!(answer instanceof Throwable)) {
                    println(out, answer.toString());
                } else {
                    Throwable error = (Throwable) answer;

                    if (error instanceof CommandPanicException) {
                        _log.error("Bug detected in dCache; please report this " +
                                "to <support@dcache.org> with the following " +
                                "information.", error);
                    }

                    if (_doOnError != ErrorAction.CONTINUE) {
                        String msg =
                            String.format("%s: line %d: %s", source, no,
                                          error.getMessage());
                        if (_doOnError == ErrorAction.SHUTDOWN) {
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
                        println(err, String.format("%s: line %d: Command failed: %s",
                                                   source, no, msg));
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new CommandExitException(String.format("%s: line %d: interrupted", source,
                    no));
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
            if (var.isEmpty()) {
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
            if (var.isEmpty()) {
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
          CommandException( 2 , "Stack position violation (" + v.size() + ')') ;
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
         String         line;
          try (InputStreamReader isr = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
               BufferedReader bufferedReader = new BufferedReader(isr)) {
              while ((line = bufferedReader.readLine()) != null) {
                  sb.append(line).append('\n');
              }
          } catch (IOException ioe) {

              throw new CommandException(11,
                      "Problem with file : " + file + " : " + ioe);
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
         throw new CommandException(43, "Invalid URL: " + e);
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
                  sb.append(line).append('\n');
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
            return new InputStreamReader(uri.toURL().openStream(), StandardCharsets.UTF_8);
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
            return new InputStreamReader(uri.toURL().openStream(), StandardCharsets.UTF_8);
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
    @Command(name = "set classloader", hint = "obsolete")
    @Deprecated
    public class SetClassloaderCommand implements Callable<String>
    {
        @Argument
        String[] args;

        @CommandLine(allowAnyOption = true)
        Args cmd;

        @Override
        public String call() throws IllegalArgumentException
        {
            return "obsolete";
        }
    }

    @Command(name = "show classloader", hint = "obsolete")
    @Deprecated
    public class ShowClassloaderCommand implements Callable<String>
    {
        @Argument
        String[] args;

        @CommandLine(allowAnyOption = true)
        Args cmd;

        @Override
        public String call()
        {
            return "obsolete";
        }
    }

    @Command(name = "zk ls", hint = "list zookeeper node")
    public class ZooKeeperList implements Callable<String>
    {
        @Argument(required = false)
        String path = "/";

        @Override
        public String call() throws CommandException
        {
            try {
                return String.join("\n", _nucleus.getCuratorFramework().getChildren().forPath(path));
            } catch (Exception e) {
                Throwables.propagateIfPossible(e);
                throw new CommandException("Failed to list zookeeper nodes: " +
                        e.getMessage(), e);
            }
        }
    }

    @Command(name = "zk get", hint = "get zookeeper node data")
    public class ZooKeeperGet implements Callable<String>
    {
        @Argument
        String path;

        @Override
        public String call() throws CommandException
        {
            try {
                return new String(_nucleus.getCuratorFramework().getData().forPath(path), StandardCharsets.UTF_8);
            } catch (Exception e) {
                Throwables.propagateIfPossible(e);
                throw new CommandException("Failed to get zookeeper node data: "
                        + e.getMessage(), e);
            }
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
    private static class FileExistsTester implements Tester {
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
            return _file + " does not exist";
        }
    }

    /**
     * Test presence of a file and that the file is
     * not special
     */
    private static class FileIsNormalTester implements Tester {
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
            return _file + (_exists ? " is not a normal file" : " does not exist");
        }
    }

    /**
     * Test presence of a file and that the file is
     * not special
     */
    private static class FileIsDirectoryTester implements Tester {
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
            return _file + (_exists ? " is not a directory file" : " does not exist");
        }
    }
}

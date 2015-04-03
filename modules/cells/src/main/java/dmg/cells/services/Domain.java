package  dmg.cells.services ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import dmg.cells.network.LocationManagerConnector;
import dmg.cells.network.TopoCell;
import dmg.cells.nucleus.SystemCell;
import dmg.cells.services.login.LoginManager;
import dmg.cells.services.login.UserMgrCell;

/**
  *  dmg.cells.services.Domain creates a hightly configurable domain.
  *  A set of standard cells can be started by command line options
  *  and a file can be specified which is interpreted by a BatchCell
  *  using the standard CellShell environment.
  *  <pre>
  *     Usage : ... &lt;domainName&gt; [options]
  *            options :
  *                -telnet  &lt;telnetPort&gt;
  *                    A TelnetLoginManager Cell is created
  *                    listening to port &lt;telnetPort&gt;
  *                -acm  &lt;userDatabase&gt;
  *                    A dmg.cells.services.login.UserMgrCell is created
  *                    using the database &lt;userDatabase&gt;
  *                -batch  &lt;filename&gt;
  *                    The specified file is executed using
  *                    dmg.cells.serives.BatchCell.
  *                -boot   &lt;bootDomain&gt;
  *                    The bootfacility is started and the
  *                    key &lt;bootDomain&gt; is set as context
  *                    variable ${bootDomain}
  *                -routed
  *                    The RoutingManagerCell is created.
  *                    For the automatic default route facility the
  *                    keyword <strong>up0</strong> is used.
  *                -spy &lt;listenPort&gt;
  *                    Starts the topology manager and listens to
  *                    incoming spy requests.
  *                -ic &lt;interruptClass&gt;
  *                    Tries to install the specified interrupt handler.
  *                -param &lt;key&gt;=&lt;value&gt; [...]
  *                    Sets context variables prior to the start
  *                    of any Cells exept the SystemCell.
  *                -silent
  *                    CellGlue and default printout is set to none.
  *                -debug
  *                    CellGlue and default printout is set to all.
  *  </pre>
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class Domain {

  private static final Logger _log = LoggerFactory.getLogger(Domain.class);

  private static final int IDLE      = 0 ;
  private static final int ASSEMBLE  = 1 ;

  public static void main( String [] args ){

     if( args.length < 1 ){
        try{
            Package p = Package.getPackage("dmg.cells.nucleus");
            if( p != null ){
                String tmp = p.getSpecificationTitle() ;
                System.out.println("SpecificationTitle:   "+(tmp==null?"(Unknown)":tmp));
                tmp = p.getSpecificationVendor() ;
                System.out.println("SpecificationVendor:  "+(tmp==null?"(Unknown)":tmp));
                tmp = p.getSpecificationVersion() ;
                System.out.println("SpecificationVersion: "+(tmp==null?"(Unknown)":tmp));
            }
        }catch(Exception ee){}
        System.out.println( "USAGE : <domainName> [options]" ) ;
        System.out.println( "         -telnet  \"<telnetPort> [-acm=acm] "+
                                               "[-localOk] [-passwd=<passwd>]\"" ) ;
        System.out.println( "         -tunnel(2)  <tunnelPort>" ) ;
        System.out.println( "         -connect(2) <host> <port>" ) ;
        System.out.println( "         -routed" ) ;
        System.out.println( "         -batch <fileName>" ) ;
        System.out.println( "         -boot  <bootDomain>" ) ;
        System.out.println( "         -spy   <spyListenPort>" ) ;
        System.out.println( "         -ic    <interruptHandlerClass>" ) ;
        System.out.println( "         -param <key>=<value> [...]" ) ;
        System.out.println( "         -acm   <userDbRoot>" ) ;
        System.out.println( "         -connectDomain   <domainName>" ) ;
        System.out.println( "         -accept" ) ;
        System.out.println( "         -lm [<hostname>] <portNumber> [options]" ) ;
        System.out.println( "              Options : /noclient /noboot /strict=yes|no" ) ;
        System.out.println( "         -debug [full]" ) ;
        System.out.println( "         -cp <cellPrinterCellName>" ) ;
        System.exit(1);
      }
      //
      // split the rest of the arguments into rows and columns
      // according to the requests
      //
      int state   = IDLE ;
      Vector<String> columns = null ;
      Vector<String[]> rowVec  = new Vector<>() ;
      for( int pos = 1 ; pos < args.length ;  ){
         switch( state ){
            case IDLE :
               if( args[pos].charAt(0) == '-' ){
                   columns = new Vector<>() ;
                   columns.addElement( args[pos] ) ;
                   state = ASSEMBLE ;
               }
               pos++ ;
            break ;
            case ASSEMBLE :
               if( args[pos].charAt(0) == '-' ){
                  String [] col = new String[columns.size()] ;
                  columns.copyInto( col ) ;
                  rowVec.addElement( col ) ;
                  state = IDLE ;
               }else{
                  columns.addElement( args[pos++] ) ;
               }
            break ;

         }
      }
      if( state == ASSEMBLE ){
         String [] col = new String[columns.size()] ;
         columns.copyInto( col ) ;
         rowVec.addElement( col ) ;
      }
      Hashtable<String, String[]> argHash = new Hashtable<>() ;
      for( int i = 0  ; i < rowVec.size() ; i++ ){
          String [] el = rowVec.elementAt(i);
          argHash.put( el[0] , el ) ;
      }
      /*
      Enumeration e = argHash.keys() ;
      for( ; e.hasMoreElements() ; ){
         String key = (String) e.nextElement() ;
         String []ar  = (String []) argHash.get( key ) ;
         System.out.print( key+" : " ) ;
         for( int j = 0 ; j < ar.length ; j++ )
            System.out.print( ar[j]+"," ) ;
         System.out.println("");
      }
      */
      //
      //
      SystemCell systemCell;
      try{
         //
         // start the system cell
         //
         systemCell = new  SystemCell( args[0] ) ;
         String [] tmp;
         //
         if( argHash.get("-version") != null ){
            Package p = Package.getPackage("dmg.cells.nucleus");
            System.out.println(p.toString());
            System.exit(0);
         }
         //
         //
         if( ( ( tmp = argHash.get( "-param" )) != null ) &&
             ( tmp.length > 1 ) ){

              String [] [] parameters = getParameter( tmp ) ;

              Map<String,Object> dict = systemCell.getDomainContext();

             for (String[] parameter : parameters) {
                 dict.put(parameter[0], parameter[1]);
             }

         }

         if( ( tmp = argHash.get( "-debug" )) != null ){


             _log.info( "Starting DebugSequence" ) ;
             List<String> v = new ArrayList<>() ;
             if( ( tmp.length > 1 ) && ( tmp[1].equals("full") ) ){
                v.add( "set printout CellGlue all" ) ;
                v.add( "set printout default all" ) ;
             }else{
                v.add( "set printout default 3" ) ;
             }
             String [] commands = new String[v.size()] ;
             new BatchCell( "debug" , v.toArray( commands ) ) ;

         }
         if( ( tmp = argHash.get( "-cp" )) != null ){

             StringBuilder sb = new StringBuilder() ;
             for( int i = 1 ; i < tmp.length ; i++ ){
                sb.append(" ") ;
                if( tmp[i].startsWith("/") ) {
                    sb.append("-").append(tmp[i].substring(1));
                } else {
                    sb.append(tmp[i]);
                }
             }
             String a = sb.toString() ;

             _log.info( "Loading new CellPrinter "+a ) ;
             List<String> v = new ArrayList<>() ;
             v.add( "load cellprinter "+a );

             String [] commands = new String[v.size()] ;
             new BatchCell( "cellprinter" , v.toArray( commands )  ) ;

         }
         if( argHash.get("-routed") != null ){
             _log.info("Starting Routing Manager") ;
             new RoutingManager( "RoutingMgr" , "up0" ) ;
         }

         if( ( ( tmp = argHash.get( "-lm" )) != null ) &&
             ( tmp.length > 1 ) ){
             StringBuilder sb = new StringBuilder() ;
             for( int i = 1 ; i < tmp.length ; i++ ){
                sb.append(" ") ;
                if( tmp[i].startsWith("/") ) {
                    sb.append("-").append(tmp[i].substring(1));
                } else {
                    sb.append(tmp[i]);
                }
             }
             String a = sb.toString() ;
             _log.info( "Installing LocationManager '"+a+"'") ;
             new LocationManager( "lm" , a ) ;
             new RoutingManager( "RoutingMgr" , "" ) ;
         }

         if( argHash.get( "-silent" ) != null ){


             _log.info( "Starting Silent Sequence" ) ;
             List<String> v = new ArrayList<>();
             v.add( "set printout CellGlue none" ) ;
             v.add("set printout default none") ;
             String [] commands = new String[v.size()] ;
             new BatchCell( "silent" , v.toArray(commands)  ) ;

         }


         if( ( ( tmp = argHash.get( "-telnet" )) != null ) &&
             ( tmp.length > 1 ) ){

             StringBuilder sb = new StringBuilder() ;
             //
             // the port number class and protocol
             //
             sb.append( tmp[1] ).
                append( " dmg.cells.services.StreamLoginCell" ).
                append( " -prot=telnet " ) ;
             //
             // and possible options
             //
             for( int i = 3 ; i < tmp.length ; i++ ){

                 sb.append( " -" ).append( tmp[i] ) ;
             }

             _log.info( "Starting LoginManager (telnet) on "+sb.toString() ) ;
             new LoginManager( "tlm" , sb.toString() ) ;

         }
         if( ( ( tmp = argHash.get( "-connectDomain" )) != null ) &&
             ( tmp.length > 1 ) ){

             _log.info( "Starting LocationMgrTunnel on "+tmp[1] ) ;
             new LocationManagerConnector("upD", "-lm=lm " + "-domain=" + tmp[1]);
         }
         if( ( ( tmp = argHash.get( "-acm" )) != null ) &&
             ( tmp.length > 1 ) ){

             _log.info( "Starting UserMgrCell on "+tmp[1] ) ;
             new UserMgrCell( "acm" , tmp[1] ) ;

         }
         if( ( ( tmp = argHash.get( "-accept" )) != null ) &&
             ( tmp.length > 0 ) ){

             _log.info( "Starting LocationMgrTunnel(listen)" ) ;
             new LoginManager(
                   "downD" ,
                   "0 dmg.cells.network.LocationMgrTunnel "+
                   "-prot=raw -lm=lm" ) ;

         }
         if( ( ( tmp = argHash.get( "-boot" )) != null ) &&
             ( tmp.length > 1 ) ){


             _log.info( "Starting BootSequence for Domain "+tmp[1] ) ;
             List<String> v = new ArrayList<>() ;
             v.add( "onerror shutdown" ) ;
             v.add( "set context bootDomain "+tmp[1] ) ;
             v.add( "waitfor context Ready ${bootDomain}" ) ;
             v.add( "copy context://${bootDomain}/${thisDomain}Setup context:bootStrap" ) ;
             v.add( "exec context bootStrap" );
             v.add( "# exit" );
             String [] commands = new String[v.size()] ;

             new BatchCell( "boot" , v.toArray( commands ) ) ;

         }
         if( ( ( tmp = argHash.get( "-spy" )) != null ) &&
             ( tmp.length > 1 ) ){


             _log.info( "Starting TopologyManager " ) ;
             new TopoCell( "topo" , "" ) ;
             _log.info( "Starting Spy Listener on "+tmp[1] ) ;
             new LoginManager( "Spy" ,
                tmp[1]+
                " dmg.cells.services.ObjectLoginCell"+
                " -prot=raw" ) ;

         }
         if( ( ( tmp = argHash.get( "-batch" )) != null ) &&
             ( tmp.length > 1 ) ){


             _log.info( "Starting BatchCell on "+tmp[1] ) ;
             new BatchCell( "batch" , tmp[1] ) ;

         }
         if( ( ( tmp = argHash.get( "-ic" )) != null ) &&
             ( tmp.length > 1 ) ){


             _log.info( "Installing interruptHandlerClass "+tmp[1] ) ;
             systemCell.enableInterrupts( tmp[1] ) ;

         }
      }catch( Exception e ){
          _log.error(e.toString(), e);
      }
  }
  private static String [] [] getParameter( String [] args ){
      String [][] param = new String[args.length-1][] ;
      int pos ;
      for( int i = 0 ; i < args.length-1 ; i++ ){
          String p = args[i+1] ;
          param[i] = new String[2] ;
          if( ( pos = p.indexOf('=') ) < 0 ){
              param[i][0] = p ;
              param[i][1] = "" ;
          }else{
              param[i][0] = p.substring(0,pos) ;
              param[i][1] = p.length() == ( pos + 1 ) ? "" : p.substring(pos+1) ;
          }
      }
      return param ;

  }

}


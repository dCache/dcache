package  dmg.cells.services ;

import   dmg.cells.services.login.* ;
import   dmg.cells.nucleus.* ;
import   dmg.cells.network.* ;
import   dmg.util.* ;

import java.util.* ;
import java.io.* ;

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
  *                -tunnel(2)  &lt;tunnelPort&gt;
  *                    A RetryTunnel Listener Cell is created
  *                    listening to port &lt;tunnelPort&gt;
  *                -connect(2)  &lt;host> &lt;port&gt;
  *                    A RetryTunnel is created
  *                    connecting to the specified destintion.
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

  private static final int IDLE      = 0 ;
  private static final int ASSEMBLE  = 1 ;

  public static void main( String [] args ){

     if( args.length < 1 ){
        try{
            Class c = dmg.cells.nucleus.CellAdapter.class ;
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
      Vector columns = null ;
      Vector rowVec  = new Vector() ;
      for( int pos = 1 ; pos < args.length ;  ){
         switch( state ){
            case IDLE :
               if( args[pos].charAt(0) == '-' ){
                   columns = new Vector() ;
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
      Hashtable argHash = new Hashtable() ;
      for( int i = 0  ; i < rowVec.size() ; i++ ){
          String [] el = (String [] )rowVec.elementAt(i) ;
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
      SystemCell systemCell = null ;
      try{
         //
         // start the system cell
         //
         systemCell = new  SystemCell( args[0] ) ;
         String [] tmp = null ;
         //
         if( argHash.get("-version") != null ){
            Package p = Package.getPackage("dmg.cells.nucleus");
            System.out.println(p.toString());
            System.exit(0);
         }
         //
         //
         if( ( ( tmp = (String[])argHash.get( "-param" ) ) != null ) &&
             ( tmp.length > 1 ) ){

              String [] [] p = getParameter( tmp ) ;

              Map<String,Object> dict = systemCell.getDomainContext();

              for( int i = 0 ; i < p.length ; i++ )
                 dict.put( p[i][0] , p[i][1] ) ;

         }

         if( ( tmp = (String[])argHash.get( "-debug" ) ) != null ){


             System.out.println( "Starting DebugSequence" ) ;
             List<String> v = new ArrayList<String>() ;
             if( ( tmp.length > 1 ) && ( tmp[1].equals("full") ) ){
                v.add( "set printout CellGlue all" ) ;
                v.add( "set printout default all" ) ;
             }else{
                v.add( "set printout default 3" ) ;
             }
             String [] commands = new String[v.size()] ;
             new BatchCell( "debug" , v.toArray( commands ) ) ;

         }
         if( ( tmp = (String[])argHash.get( "-cp" ) ) != null ){

             StringBuffer sb = new StringBuffer() ;
             for( int i = 1 ; i < tmp.length ; i++ ){
                sb.append(" ") ;
                if( tmp[i].startsWith("/") )
                   sb.append("-").append(tmp[i].substring(1)) ;
                else
                   sb.append(tmp[i]);
             }
             String a = sb.toString() ;

             System.out.println( "Loading new CellPrinter "+a ) ;
             List<String> v = new ArrayList<String>() ;
             v.add( "load cellprinter "+a );

             String [] commands = new String[v.size()] ;
             new BatchCell( "cellprinter" , v.toArray( commands )  ) ;

         }
         if( argHash.get("-routed") != null ){
             System.out.println( "Starting Routing Manager" ) ;
             new RoutingManager( "RoutingMgr" , "up0" ) ;
         }

         if( ( ( tmp = (String[])argHash.get( "-lm" ) ) != null ) &&
             ( tmp.length > 1 ) ){
             StringBuffer sb = new StringBuffer() ;
             for( int i = 1 ; i < tmp.length ; i++ ){
                sb.append(" ") ;
                if( tmp[i].startsWith("/") )
                   sb.append("-").append(tmp[i].substring(1)) ;
                else
                   sb.append(tmp[i]);
             }
             String a = sb.toString() ;
             System.out.println( "Installing LocationManager '"+a+"'") ;
             new LocationManager( "lm" , a ) ;
             new RoutingManager( "RoutingMgr" , "" ) ;
         }

         if( argHash.get( "-silent" ) != null ){


             System.out.println( "Starting Silent Sequence" ) ;
             List<String> v = new ArrayList<String>();
             v.add( "set printout CellGlue none" ) ;
             v.add( "set printout default none" ) ;
             String [] commands = new String[v.size()] ;
             new BatchCell( "silent" , v.toArray( commands )  ) ;

         }


         if( ( ( tmp = (String[])argHash.get( "-telnet" ) ) != null ) &&
             ( tmp.length > 1 ) ){

             StringBuffer sb = new StringBuffer() ;
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

             System.out.println( "Starting LoginManager (telnet) on "+sb.toString() ) ;
             new LoginManager( "tlm" , sb.toString() ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-tunnel2" ) ) != null ) &&
             ( tmp.length > 1 ) ){

             StringBuffer sb = new StringBuffer() ;
             //
             // the port number class and protocol
             //
             sb.append( tmp[1] ).
                append( " dmg.cells.network.RetryTunnel2" ).
                append( " -prot=raw " ) ;
             //
             // and possible options
             //
             for( int i = 3 ; i < tmp.length ; i++ ){

                 sb.append( " -" ).append( tmp[i] ) ;
             }

             System.out.println( "Starting RetryTunnel2 (raw) on "+sb.toString() ) ;
             new LoginManager( "down" , sb.toString() ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-connect" ) ) != null ) &&
             ( tmp.length > 2 ) ){


             System.out.println( "Starting RetryTunnel on "+tmp[1]+" "+tmp[2] ) ;
             new RetryTunnel( "up0" , tmp[1]+" "+tmp[2] ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-connect2" ) ) != null ) &&
             ( tmp.length > 2 ) ){

             System.out.println( "Starting RetryTunnel2 on "+tmp[1]+" "+tmp[2] ) ;
             new RetryTunnel2( "up0" , tmp[1]+" "+tmp[2] ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-connectDomain" ) ) != null ) &&
             ( tmp.length > 1 ) ){

             System.out.println( "Starting LocationMgrTunnel on "+tmp[1] ) ;
             new LocationManagerConnector("upD", "-lm=lm " + "-domain=" + tmp[1]);
         }
         if( ( ( tmp = (String[])argHash.get( "-acm" ) ) != null ) &&
             ( tmp.length > 1 ) ){

             System.out.println( "Starting UserMgrCell on "+tmp[1] ) ;
             new UserMgrCell( "acm" , tmp[1] ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-tunnel" ) ) != null ) &&
             ( tmp.length > 1 ) ){

             System.out.println( "Starting RetryTunnel on "+tmp[1] ) ;
             new GNLCell( "down" , "dmg.cells.network.RetryTunnel "+tmp[1] ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-accept" ) ) != null ) &&
             ( tmp.length > 0 ) ){

             System.out.println( "Starting LocationMgrTunnel(listen)" ) ;
             new LoginManager(
                   "downD" ,
                   "0 dmg.cells.network.LocationMgrTunnel "+
                   "-prot=raw -lm=lm" ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-boot" ) ) != null ) &&
             ( tmp.length > 1 ) ){


             System.out.println( "Starting BootSequence for Domain "+tmp[1] ) ;
             List<String> v = new ArrayList<String>() ;
             v.add( "onerror shutdown" ) ;
             v.add( "set context bootDomain "+tmp[1] ) ;
             v.add( "waitfor context Ready ${bootDomain}" ) ;
             v.add( "copy context://${bootDomain}/${thisDomain}Setup context:bootStrap" ) ;
             v.add( "exec context bootStrap" );
             v.add( "# exit" );
             String [] commands = new String[v.size()] ;

             new BatchCell( "boot" , v.toArray( commands ) ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-spy" ) ) != null ) &&
             ( tmp.length > 1 ) ){


             System.out.println( "Starting TopologyManager " ) ;
             new TopoCell( "topo" , "" ) ;
             System.out.println( "Starting Spy Listener on "+tmp[1] ) ;
             new LoginManager( "Spy" ,
                tmp[1]+
                " dmg.cells.services.ObjectLoginCell"+
                " -prot=raw" ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-batch" ) ) != null ) &&
             ( tmp.length > 1 ) ){


             System.out.println( "Starting BatchCell on "+tmp[1] ) ;
             new BatchCell( "batch" , tmp[1] ) ;

         }
         if( ( ( tmp = (String[])argHash.get( "-ic" ) ) != null ) &&
             ( tmp.length > 1 ) ){


             System.out.println( "Installing interruptHandlerClass "+tmp[1] ) ;
             systemCell.enableInterrupts( tmp[1] ) ;

         }
      }catch( Exception e ){
          e.printStackTrace() ;
          System.exit(4);
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


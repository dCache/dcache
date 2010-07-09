package dmg.cells.applets ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;

import dmg.util.* ;
import dmg.cells.examples.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.network.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class      CellSpyApplet
       extends    Applet
       implements ActionListener,
                  Runnable ,
                  Cell ,
                  CellEventListener       {

  private final static Logger _log =
      LoggerFactory.getLogger(CellSpyApplet.class);

  //
  // Cell Environment Stuff
  //
  private String      _domainName        = "<" + new Date().getTime() + ">" ;
  private boolean     _systemCellCreated = false ;
  private boolean     _tunnelCreated     = false ;
  private CellNucleus _nucleus ;
  private CellRoute   _defaultRoute      = null ;
  //
  // awt stuff
  //
  private CardLayout _cards        = null ;
  private Button     _topologyButton , _worksheetButton ;
  private Panel      _cardPanel    = null ,
                     _buttonPanel  = null ;
  private LogoCanvas _logo         = null ;
  private String     _firstVisible = null ;
  private int        _startCount   = 0 ;
  public void init(){
      //
      // first things first
      // we need to create the SystemCell, if not yet done.
      //
      System.out.println( "Trying to create system cell" ) ;
      try{
          new SystemCell( _domainName ) ;
          _systemCellCreated = true ;
      }catch( Exception ex ){
         System.out.println( "Seems to have been done before" ) ;
      }
      System.out.println( "Cell Environment created" ) ;
      //
      // make us a Cell
      //
      _nucleus = new CellNucleus( this , "Applet*" ) ;
      _nucleus.addCellEventListener( this ) ;
      _nucleus.setPrintoutLevel(15) ;
      //
      // if we created the system cell we also are
      // responsible for creating the tunnel
      //
      String dest = getParameter( "Galactica" ) ;
      if( _systemCellCreated && ( dest != null ) ){

         System.out.println( "Creating tunnel" ) ;
         try{
            new RetryTunnel( "et*" , dest ) ;
            _tunnelCreated = true ;
         }catch( Exception rte ){
            _tunnelCreated = false ;
            System.out.println( "Problem creating tunnel : "+rte ) ;
         }
      }

      setLayout( new BorderLayout() ) ;
      _cardPanel    = new Panel( _cards = new CardLayout() )  ;
      _buttonPanel  = new Panel( new FlowLayout(FlowLayout.CENTER) ) ;

      add( _cardPanel    , "Center" ) ;
      add( _buttonPanel  , "North" ) ;
      _worksheetButton = new Button( "Worksheet" ) ;
      _topologyButton  = new Button( "Topology" ) ;
      _worksheetButton.addActionListener( this ) ;
      _topologyButton.addActionListener( this ) ;
      _buttonPanel.add( _worksheetButton ) ;
      _buttonPanel.add( _topologyButton ) ;

      //
      // create the general animation stuff
      //
      Dimension   d    = getSize() ;
      _logo  = new LogoCanvas("Cell Spy") ;
      _logo.setSize( d.width , d.height ) ;
      _logo.setActionListener( this ) ;
      _cardPanel.add( _logo  , "animation" ) ;
      //
      // and the optional worksheet
      //
      _cardPanel.add( new WorksheetCell( "WSC*" ) , "worksheet" ) ;
      _cardPanel.add( new TopoCanvasCell( "Topo*" ) , "topo" ) ;
      System.out.println( "CellAppletDomain started (Init ready)" ) ;

      if( ( _firstVisible = getParameter( "ShowUp" ) ) == null )
         _firstVisible = "worksheet" ;


  }
  public void start(){
      System.out.println("Starting ... " ) ;
      if( _startCount < 1 ){
         _logo.animation( LogoCanvas.GROWING ) ;
         _cards.show( _cardPanel , "animation" ) ;
         _startCount ++ ;
      }
      setVisible( true ) ;
  }
  public void actionPerformed( ActionEvent event ){
     String command = event.getActionCommand() ;
     System.out.println( " Action : " + command ) ;
     //
     // filter the animation events ( won't be too much ) ;
     //
     if( event.getSource() == _logo ){
         if( command.equals( "finished" ) ){
            _cards.show( _cardPanel , _firstVisible ) ;
         }
     }else if( event.getSource() == _worksheetButton ){
         _cards.show( _cardPanel , "worksheet" ) ;
     }else if( event.getSource() == _topologyButton ){
         _cards.show( _cardPanel , "topo" ) ;
     }

     System.out.println( " Ready "  ) ;

  }
  public void run(){
  }
  public void stop(){
     System.out.println( "Applet stopping"  ) ;
  }
  public void destroy(){
     System.out.println( "Applet destroying"  ) ;
  }
   //
   // interface from Cell
   //
   public String toString(){
      return  "Applet" ;
   }
   public String getInfo(){
      StringBuffer sb = new StringBuffer() ;
      return  sb.toString() ;
   }
   public void   messageArrived( MessageEvent me ){
     if( me instanceof LastMessageEvent ){
     }else{
        CellMessage msg  = me.getMessage() ;
        if( msg.isFinalDestination() ){
           Object      obj  = msg.getMessageObject() ;
           _log.info( "Msg arrived (f) : "+msg ) ;
        }
     }
   }
   public void   prepareRemoval( KillEvent ce ){
     // this will remove whatever was stored for us
   }

   public synchronized void  routeAdded( CellEvent ce ){
      _log.info( "routeAdded : "+ce );
      if( _defaultRoute == null  ){

         CellRoute route  = (CellRoute)ce.getSource() ;
         if( route.getRouteType() != CellRoute.DOMAIN )return ;

         Args args = new Args( "-default *@"+route.getDomainName() ) ;
         _defaultRoute =  new CellRoute( args ) ;

         _log.info( "routeAdded : adding default : "+_defaultRoute ) ;
         _nucleus.routeAdd( _defaultRoute ) ;

      }
   }
   public synchronized void  routeDeleted( CellEvent ce ){
     _log.info( "routeDeleted : "+ce ) ;
      if( _defaultRoute != null  ){

         CellRoute route  = (CellRoute)ce.getSource() ;
         if( route.getRouteType() != CellRoute.DOMAIN )return ;


         _log.info( "routeDeleted : removing default : "+_defaultRoute ) ;
         _nucleus.routeDelete( _defaultRoute ) ;

         _defaultRoute = null ;

      }
   }
   public void   exceptionArrived( ExceptionEvent ce ){
//     _log.info( " exceptionArrived "+ce ) ;
   }
   //
   // interface from CellEventListener
   //
   public void  cellCreated( CellEvent  ce ){
//     _log.info( " cellCreated "+ce ) ;
   }
   public void  cellDied( CellEvent ce ){
//     _log.info( " cellDied "+ce ) ;
   }
   public void  cellExported( CellEvent ce ){
//     _log.info( " cellExported "+ce ) ;
   }

}

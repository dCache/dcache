package dmg.cells.applets.spy ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.network.* ;



class RoutingPanel 
      extends Panel 
      implements ActionListener, FrameArrivable {

   private DomainConnection _connection ;
   private Button   _updateButton ;
   private Label    _topLabel  ;
   private TextArea _routeText ;
   private Font   _bigFont   = new Font( "SansSerif" , Font.ITALIC , 18 )  ;
   private Font   _smallFont = new Font( "SansSerif" , Font.ITALIC|Font.BOLD , 14 )  ;
   private Font   _textFont  = new Font( "Monospaced" , Font.ITALIC   , 14 )  ;
   private CellDomainNode _domainNode = null ;
   private boolean        _useColor = false ;
      
   RoutingPanel( DomainConnection connection ){
      _connection = connection ; 
      _useColor   = System.getProperty( "bw" ) == null ;
      if( _useColor ) {
          setBackground(Color.orange);
      }
      setLayout( new BorderLayout() ) ;

      _topLabel = new Label( "Routing" , Label.CENTER )  ;
      _topLabel.setFont( _bigFont ) ;
      add( _topLabel , "North" ) ;
      _updateButton = new Button( "Update Routing" ) ;
      _updateButton.addActionListener( this ) ;
      add( _updateButton , "South" ) ;
      _routeText = new TextArea() ;
      _routeText.setFont( _textFont ) ;
      if( _useColor ) {
          _routeText.setBackground(Color.magenta);
      }
      add( _routeText , "Center" ) ;
   }
   @Override
   public Insets getInsets(){ return new Insets( 20 , 20 ,20 , 20 ) ; }
   public void showDomain( CellDomainNode domainNode ){
      _topLabel.setText( " Routing Table of "+domainNode.getName()) ;
      _domainNode = domainNode ;
      updateDomain() ;
   }
   private void updateDomain(){
      if( _domainNode == null ) {
          return;
      }
      _connection.send( _domainNode.getAddress() , "getroutes" , this ) ;
   
   }
   @Override
   public void actionPerformed( ActionEvent event ){
       String command = event.getActionCommand() ;
//       System.out.println( " Action : " + command ) ;
       Object o = event.getSource() ;
       if( o == _updateButton ){
          updateDomain() ;
       }
   }
   public void clear(){
      _topLabel.setText( "<Routing Table>" ) ;
      _routeText.setText("") ;
   }
   @Override
   public void frameArrived( MessageObjectFrame frame ){
       Object obj = frame.getObject() ;
//       System.out.println( "Class arrived : "+obj.getClass().getName() ) ;
       if( obj instanceof String ){
         _routeText.setText( obj.toString() ) ;
       }else if( obj instanceof Object [] ){
          Object [] array = (Object [])obj ;
         _routeText.setText( "Routing : \n" ) ;
         for( int i = 0 ; i < array.length ; i++ ) {
             _routeText.append(array[i].toString() + "\n");
         }
       }
   }

} 

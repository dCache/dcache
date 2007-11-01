package dmg.apps.osm  ;


import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.applets.spy.* ;

public class      OsmSpy 
       extends    Frame 
       implements WindowListener, 
                  ActionListener ,
                  DomainConnectionListener {

   private DomainConnection _connection = null ;
   private Button _connectButton    = null ;
   private Button _closeButton      = null ;
   private Label  _messageText      = null ;
   private Panel  [] _libraryPanel       = null ;
   private class SStatusCanvasx extends Label {
      
      public  SStatusCanvasx(){ setText("Connecting" ) ; }
      public  void setMode( int mode ){
         switch( mode ){
            case 0 : setText("Connecting") ; break ;
            case 1 : setText("Waiting") ; break ;
            case 2 : setText("Connected") ; break ;
         }
      }
   
   }
   private SStatusCanvas _status    = new SStatusCanvas() ;
   private class BorderPanel extends Panel {
      public BorderPanel( Component c ){
         super( new BorderLayout() ) ;
         add( c , "Center" ) ;
      }
     
      public Insets getInsets(){ return new Insets( 10 , 10 ,10 , 10 ) ; }
   
   }
   
  public OsmSpy( Args args ){
      super( "Osm Spy" ) ;
      
      if( args.argc() < 1 )
          throw new 
          IllegalArgumentException(
          "Usage : ... [-host=<hostname>] [-port=<portNumber>] <libs> [...]" );
          
      String host       = null ;
      String portString = null ;
      int    port       = 0 ;
      
      _status.setPreferredSize( new Dimension(20 , 20 ) ) ;
      _status.setBackground(Color.green);
      _status.setForeground(Color.green);
      if( ( host = args.getOpt("host") ) == null ){
          host = "localhost" ;
      }
      if( ( portString = args.getOpt("port") ) == null ){
          portString = "22222" ;
      }
      port = Integer.parseInt( portString ) ;
      
      _connection = new DomainConnection( host , port ) ;
      
      
      addWindowListener( this ) ;
      _connection.addConnectionListener( this ) ;
      
      Panel masterPanel = new Panel( new BorderLayout() ) ;
      Panel buttonPanel = new Panel( new FlowLayout( FlowLayout.CENTER ) ) ;
      Panel actionPanel = new Panel( new GridLayout(1,0) ) ;
      actionPanel.setBackground( Color.blue ) ;
      
      _libraryPanel = new LibraryPanel[args.argc()] ;
      for( int i = 0 ; i < _libraryPanel.length ; i++ ){       
        _libraryPanel[i] = new LibraryPanel( args.argv(i) , _connection ) ;
        actionPanel.add( _libraryPanel[i]  );
      }
             
      _connectButton = new Button( "Connect" ) ;
      _closeButton   = new Button( "Close Connection" ) ;
      _messageText   = new Label("Not Connected") ;
      
      _connectButton.addActionListener( this ) ;
      _closeButton.addActionListener( this ) ;
      
      buttonPanel.add( _connectButton ) ;
      buttonPanel.add( _closeButton ) ;
      
      masterPanel.add( _status , "North" ) ;
      masterPanel.add( actionPanel , "Center" ) ;
      masterPanel.add( _messageText , "South" ) ;
      
      add( masterPanel ) ;
      
      connectionActive( false ) ;
      setLocation( 100  , 100 ) ;
//      setSize( 400 , 600 ) ;
      pack() ;
//      setSize( 400 , 600 ) ;
      setVisible( true ) ;
      try{
         _connection.connect() ;
      }catch(Exception ee ){
         System.err.println( "PANIC : "+ee ) ;
         System.exit(4);
      }
  }
  //
  // action interface
  //
  public void actionPerformed( ActionEvent event ) {
    Object source = event.getSource() ;
    if( source == _connectButton ){
       try{
          _connection.connect() ;
       }catch( Exception e ){
          _messageText.setText( "Not Connected : "+e.getMessage() ) ;
          return ;
       }
    
    }else if( source == _closeButton ){
       _connection.close() ;
    }
  
  }
  //
  // domain connection interfase
  //
  public void connectionActivated( DomainConnectionEvent event ){
      connectionActive( true ) ;
      _messageText.setText( "Connected" ) ;
      _status.setMode(2) ;
  }
  public void connectionDeactivated( DomainConnectionEvent event ){
      System.out.println( "Listener : "+event.getMessage()) ;
      connectionActive( false ) ;
      _messageText.setText( "Connection Closed : "+event.getMessage() ) ;
      new Thread( new Runnable(){
                      public void run(){
                          try{
                             _status.setMode(1) ;
                             Thread.sleep(4000) ;
                          }catch(InterruptedException e){
                             return ;
                          }
                          try{
                             _status.setMode(0) ;
                             _connection.connect() ;
                          }catch(Exception e){
                             System.err.println("Panic : "+e) ;
                          }
                      }
                  }
                ).start() ;
  }
  public void connectionFailed( DomainConnectionEvent event ){
      _messageText.setText( "Connection Failed : "+event.getMessage() ) ;
      connectionActive( false ) ;
      final DomainConnection dc = _connection ;
      new Thread( new Runnable(){
                      public void run(){
                          try{
                             _status.setMode(1) ;
                             Thread.sleep(10000) ;
                          }catch(InterruptedException e){
                             return ;
                          }
                          try{
                             _status.setMode(0) ;
                             _connection.connect() ;
                          }catch(Exception e){
                             System.err.println("Panic : "+e) ;
                          }
                      }
                  }
                ).start() ;
  }
  private void connectionActive( boolean enabled ){
      _connectButton.setEnabled( ! enabled ) ;
      _closeButton.setEnabled( enabled ) ;
      _messageText.setBackground( enabled ? Color.green : Color.red ) ;
//      _domainListPanel.setEnabled(enabled);
  }
  //
  // window interface
  //
  public void windowOpened( WindowEvent event ){}
  public void windowClosed( WindowEvent event ){
      System.exit(0);
  }
  public void windowClosing( WindowEvent event ){
      System.exit(0);
  }
  public void windowActivated( WindowEvent event ){}
  public void windowDeactivated( WindowEvent event ){}
  public void windowIconified( WindowEvent event ){}
  public void windowDeiconified( WindowEvent event ){}
   public static void main( String [] argString ){
      try{
         Args args = new Args( argString ) ;
         new OsmSpy( args ) ;
      
      }catch( IllegalArgumentException e ){
         System.err.println( e.getMessage() ) ;
         System.exit(5);
      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }
      
   }

}

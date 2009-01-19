package dmg.cells.applets.alias ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;

public class      SingleRequestPanel 
       extends    Panel 
       implements CellMessageListener,
                  ActionListener             {
       
   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 
   private Label  _errorLabel  ;
   private Label  _halloLabel ;
   private RequestDesc _requestPanel = new RequestDesc() ;
   private Object      _request = null ;
   private CellMessage _message = null ;
   private Button      _forwardButton = new Button( "Forward" ) ;
   private Button      _replyButton   = new Button( "Reply" ) ;
   private AliasDomainConnection _connection = null ;
   public SingleRequestPanel( String name , AliasDomainConnection connection ){
      super( new BorderLayout() ) ;
      setBackground( Color.green ) ;
      System.out.println( "Created : "+name ) ;
      System.out.println( "Connection : "+connection ) ;
      _connection = connection ;
      _errorLabel = new Label( "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm" ) ;
      _halloLabel = new Label( "Cell : "+name , Label.CENTER ) ;
      _halloLabel.setFont( _bigFont ) ;
      
      add( _halloLabel   , "North" ) ;
      add( _requestPanel , "Center" ) ;
      
      _forwardButton.addActionListener( this ) ;
      _forwardButton.setEnabled(false);
      _replyButton.addActionListener( this ) ;
      _replyButton.setEnabled(false);
      Panel menu = new Panel(new FlowLayout(FlowLayout.LEFT)) ;
      menu.add( _forwardButton ) ;
      menu.add( _replyButton ) ;
      menu.add( _errorLabel ) ;
      add( menu   , "South" ) ;
      
      
   }
   public void actionPerformed( ActionEvent event ){
      Object source = event.getSource() ;
      _forwardButton.setEnabled(false);
      _replyButton.setEnabled(false);
      _requestPanel.reset() ;
      try{
         if( source == _replyButton ){
             _message.revertDirection() ;
             _errorLabel.setText("replying ... ") ;
         }
         if( source == _forwardButton ){
             _message.nextDestination() ;
             _errorLabel.setText("forwarding ... ") ;
         }
         _connection.sendMessage( _message ) ;
      }catch(Exception ee ){
         ee.printStackTrace() ;
         System.err.println( "Problem sending message : "+ee ) ;
      }
      _message = null ;
      _request = null ;
      
   }
   public Insets getInsets(){ return new Insets(20,20,20,20) ; }
   public void messageArrived( CellMessage msg ){
      _errorLabel.setText( msg.getMessageObject().toString() ) ;
      if( _request == null ){
         _message = msg ;
         _replyButton.setEnabled(true) ;
         if( ! _message.isFinalDestination() )
             _forwardButton.setEnabled(true) ;
         _request = msg.getMessageObject() ;
         _requestPanel.setRequest( _request ) ;
      }
   }
}

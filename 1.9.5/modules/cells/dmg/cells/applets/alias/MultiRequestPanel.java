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

public class      MultiRequestPanel 
       extends    Panel 
       implements CellMessageListener,
                  ActionListener,
                  ItemListener             {
       
   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 14 )  ; 
   private Label  _errorLabel ;
   private Label  _halloLabel ;
   private Label  _fromLabel ;
   private Label  _toLabel ;
   private RequestDesc _requestPanel = new RequestDesc() ;
   private Object      _request = null ;
   private CellMessage _message = null ;
   private Button      _forwardButton = new Button( "Forward" ) ;
   private Button      _replyButton   = new Button( "Reply" ) ;
   private Button      _deleteButton  = new Button( "Delete" ) ;
   private java.awt.List _requestList = new java.awt.List() ;
   private int         _messageCounter = 1000 ;
   private Hashtable   _messageHash    = new Hashtable() ;
   private String      _messageName    = null ;
   private ScrollPane  _scroll = 
            new ScrollPane( ScrollPane.SCROLLBARS_AS_NEEDED) ;

   private AliasDomainConnection _connection = null ;
   
   public MultiRequestPanel( String name , AliasDomainConnection connection ){
      BorderLayout bl = new BorderLayout() ;
      bl.setHgap(5) ;
      bl.setVgap(5) ;
      setLayout( bl ) ;
//      setBackground( Color.green ) ;
      System.out.println( "Created : "+name ) ;
      System.out.println( "Connection : "+connection ) ;
      _connection = connection ;
      _errorLabel = new Label( "" ) ;
      _requestList.addItemListener( this ) ;
      _halloLabel = new Label( "Cell : "+name , Label.CENTER ) ;
      _halloLabel.setFont( _bigFont ) ;
      _fromLabel = new Label( "From : " , Label.LEFT ) ;
      _fromLabel.setFont( _bigFont ) ;
      _toLabel = new Label( "To : " , Label.LEFT ) ;
      _toLabel.setFont( _bigFont ) ;
      
      Panel labelPanel = new Panel( new GridLayout(0,1) ) ;
      labelPanel.add( _halloLabel ) ;
      labelPanel.add( _fromLabel ) ;
      labelPanel.add( _toLabel ) ;
      
      add( labelPanel   , "North" ) ;
      add( _requestList , "West" ) ;
      
      _scroll.add( _requestPanel ) ;
      add( _scroll , "Center" ) ;
      
      _forwardButton.addActionListener( this ) ;
      _forwardButton.setEnabled(false);
      _replyButton.addActionListener( this ) ;
      _replyButton.setEnabled(false);
      _deleteButton.addActionListener( this ) ;
      _deleteButton.setEnabled(true);
      Panel menu = new Panel(new BorderLayout() ) ;
      Panel menuButtons = new Panel( new GridLayout(1,0) ) ;
      menuButtons.add( _forwardButton ) ;
      menuButtons.add( _replyButton ) ;
      menuButtons.add( _deleteButton ) ;
      menu.add( menuButtons , "West" ) ;
      menu.add( _errorLabel , "Center" ) ;
      
      add( menu   , "South" ) ;
      
      
   }
   public void actionPerformed( ActionEvent event ){
      Object source = event.getSource() ;
      if( _message == null ){
         _errorLabel.setText( "No Message selected" ) ;
         return ;
      }
      _forwardButton.setEnabled(false);
      _replyButton.setEnabled(false);
      _requestPanel.reset() ;
      try{
         if( source == _replyButton ){
             _message.revertDirection() ;
             _connection.sendMessage( _message ) ;
             _errorLabel.setText("Done") ;
         }else if( source == _forwardButton ){
             _message.nextDestination() ;
             _connection.sendMessage( _message ) ;
             _errorLabel.setText("Done") ;
         }else if( source == _deleteButton ){
              _errorLabel.setText("Done") ;
         }else return ;
         
         _messageHash.remove(_messageName) ;
         _requestList.remove(_messageName) ;
      }catch(Exception ee ){
//         ee.printStackTrace() ;
         _errorLabel.setText( "Problem sending message : "+ee ) ;
      }
      _message = null ;
      _request = null ;
      _messageName = null ;
      setPaths( "" ,"" ) ;
      
   }
   public Insets getInsets(){ return new Insets(20,20,20,20) ; }
   private void setPaths( String source , String destination ){
      _toLabel.setText( "Destination : "+destination ) ;
      _fromLabel.setText( "Source : "+source ) ;
   }
   public void itemStateChanged( ItemEvent event ){
       ItemSelectable sel = event.getItemSelectable() ;
       Object [] obj = sel.getSelectedObjects() ;
       if( ( obj == null ) || ( obj.length == 0 ) )return ;
       _messageName = obj[0].toString() ;
       _message = (CellMessage)_messageHash.get(_messageName) ;
       _replyButton.setEnabled(true) ;
       if( ! _message.isFinalDestination() )
             _forwardButton.setEnabled(true) ;
       _request = _message.getMessageObject() ;
       _requestPanel.setRequest( _request ) ;
       setPaths( _message.getSourcePath().toString() ,
                 _message.getDestinationPath().toString() ) ;
                         
       _scroll.doLayout() ;
   }
   public void messageArrived( CellMessage msg ){
      _errorLabel.setText( msg.getMessageObject().toString() ) ;
      _messageCounter ++ ;
      String msgName = "MSG-"+(++_messageCounter) ;
      _messageHash.put( msgName , msg ) ;
      _requestList.add( msgName ) ;
   }
}

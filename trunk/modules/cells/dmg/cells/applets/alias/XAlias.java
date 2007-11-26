package dmg.cells.applets.alias ;

import java.lang.reflect.* ;
import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.applets.login.CenterLayout ;

public class      XAlias 
       extends    Frame 
       implements WindowListener, 
                  ActionListener    {
                  
   private class BorderPanel extends Panel {
      public BorderPanel( Component c ){
         super( new BorderLayout() ) ;
         add( c , "Center" ) ;
      }
     
      public Insets getInsets(){ return new Insets( 10 , 10 ,10 , 10 ) ; }
   
   }
   private class Dummy extends Canvas {
       int _count = 5 ;
       public Dummy( int count ){ _count = count ; }
       public Dummy(){
           _count = 5 ;
       }
       public void paint( Graphics g ){
           Dimension d = getSize() ;
           int dx = d.width / 2 ;
           int dy = d.height / 2 ;
           g.setColor( Color.red ) ;
           int off = 0 ;
           for( int i = 0 ;i < _count ; i++ ){
              g.drawRect( off  , off , 
                          d.width - 1 - 2*off , 
                          d.height - 1 - 2 * off ) ;
              off += 5 ;
           }
       }
   }
   private class SwitchPanel extends Panel {
   
      private CardLayout _cards = new CardLayout() ;
     
      public SwitchPanel(){
         setLayout( _cards ) ;
         add( new Dummy() , "dummy" ) ;
      }
      public void addPanel( String name , Component panel ){
         add( panel , name ) ;
      }
      public void showIt( String name ){ _cards.show(this,name) ; }
      public void setEnabled( boolean en ){
         if( ! en ){
            _cards.show( this , "dummy" ) ; 
         }
      }
   }
   private SwitchPanel _switchPanel ;
   private AliasDomainConnection _aliasConnection  ;
   private Label     _messageText   = new Label() ;
   private Button    _connectButton = new Button( "Connect" ) ;
   private Button    _aliasButton   = new Button( "Alias Editor" ) ;
   private Button    _dummyButton   = new Button( "Alias Requests" ) ;
   private Button    _privateButton = new Button( "Private Requests" ) ;
   private AliasEditorPanel _aliasEditor   ;
   private NewRequestPanel  _privatePanel ;
   private CellSwitchPanel  _csp = new CellSwitchPanel() ;
   private Label     _topLabel      = 
          new Label( "Cell Alias Controller" ,Label.CENTER ) ;
   private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 
             
             
  public XAlias( String host , int port  ){
      super( "CellAlias" ) ;
      BorderLayout bl = new BorderLayout() ;
      bl.setVgap(10) ;
      bl.setHgap(10) ;
      setLayout( bl ) ;
      
      addWindowListener( this ) ;
      setLocation( 60 , 60) ;
      
      _aliasConnection = new AliasDomainConnection( host , port ) ;
      _aliasConnection.addActionListener(this);
      
      _privatePanel = new NewRequestPanel( _aliasConnection ) ;
      _privatePanel.addActionListener(this);
      
      _aliasEditor = new AliasEditorPanel() ;
      _aliasEditor.addActionListener(this);
      
      _topLabel.setFont( _bigFont ) ;
      _topLabel.setForeground( Color.blue ) ;
      
      _switchPanel = new SwitchPanel() ;
      _switchPanel.setEnabled(false);
      _switchPanel.addPanel( "editor" , _aliasEditor ) ;      
      _switchPanel.addPanel( "d" , _csp ) ;
      _switchPanel.addPanel( "private" , _privatePanel ) ;
      
      _connectButton.addActionListener( this ) ;
      _aliasButton.addActionListener( this ) ;
      _dummyButton.addActionListener( this ) ;
      _privateButton.addActionListener( this ) ;
      
      
      _messageText.setBackground( Color.red ) ;
      
      
      Panel leftPanel   = new Panel( new BorderLayout() ) ;
      GridLayout gl = new GridLayout(0,1) ;
      gl.setHgap(10) ;
      gl.setVgap(10) ;
      Panel buttonPanel = new Panel( gl ) ;
      buttonPanel.add( _connectButton ) ;
      buttonPanel.add( _aliasButton ) ;
      buttonPanel.add( _dummyButton ) ;
      buttonPanel.add( _privateButton ) ;
      leftPanel.add( buttonPanel , "North" ) ;
      
      buttonsEnable(false) ;   
      
      createTrash() ;
      add( _topLabel    , "North" ) ;
      add( _switchPanel , "Center" ) ;
      add( leftPanel    , "West" ) ;
      add( _messageText , "South" ) ;
      
      setSize( 750 , 500 ) ;
      pack() ;
      setSize( 750 , 500 ) ;
      setVisible( true ) ;
  
  }
  private static final Class [] __panelArgs = {
     java.lang.String.class ,
     dmg.cells.applets.alias.AliasDomainConnection.class
  } ;
  //
  // action interface
  //
  public class CellMessageListenerImpl 
         implements CellMessageListener {
         
      public CellMessageListenerImpl(){}
      
      public void messageArrived( CellMessage msg ){
         System.out.println( "Message arrived from : "+
                 msg.getSourcePath() ) ;
      }      
  }
  private void buttonsEnable( boolean t ){
     _dummyButton.setEnabled(t) ;
     _aliasButton.setEnabled(t) ;
     _privateButton.setEnabled(t) ;
  }
           
  private CellMessageListener createPanel( String className , String cellName )
          throws Exception {
     Class       c   = null ;
     Constructor con = null ;
     Object      o   = null ;
     Object [] argList = new Object[2] ;
     c   = Class.forName( className ) ;
     con = c.getConstructor( __panelArgs ) ;
     argList[0] = cellName ;
     argList[1] = _aliasConnection ;
     o = con.newInstance( argList ) ;
     if(  ! ( ( o instanceof CellMessageListener ) &&
              ( o instanceof Component           )     ) )
        throw new 
        IllegalArgumentException( cellName +" not an AL" ) ;
     return (CellMessageListener)o ;
  
  }
  public void actionPerformed( ActionEvent event ) {
    Object source = event.getSource() ;
    String command = event.getActionCommand() ;
    if( source == _connectButton ){
        try{
           if( _connectButton.getLabel().equals("Connect") ){
              _aliasConnection.connect() ;
           }else{
              _aliasConnection.disconnect() ;
           }
        }catch( Exception e ){
           _messageText.setText( "connection problem : "+e ) ;
           return ;
        }
        _messageText.setText("O.K.") ;
    }else if( source ==  _aliasEditor ){
       System.out.println( "Action : "+command ) ;
       if( command.startsWith("a:" ) ){
          int p = command.lastIndexOf(":") ;
          if( p == 1 )return ;
          String cellName  = command.substring(2,p) ;
          String className = command.substring(p+1) ;
          CellMessageListener cml = null ;
          try{
             cml = createPanel( className , cellName ) ;
          }catch( Exception ie ){
             _messageText.setText( "Couldn't create ("+className+") : "+ie ) ;
             return ;
          }
          _aliasConnection.addAlias( cellName , cml ) ;
          _csp.addPanel( cellName , (Component)cml ) ;
          doLayout() ;
       }else if( command.startsWith( "r:" ) ){
          String name = command.substring(2) ;
          _aliasConnection.removeAlias( name ) ;
          _csp.removePanel( name ) ;
          doLayout() ;
       }
    }else if( source ==  _dummyButton ){
              _switchPanel.showIt("d") ;
    }else if( source ==  _aliasButton ){
              _switchPanel.showIt("editor") ;
    }else if( source ==  _privateButton ){
              _switchPanel.showIt("private") ;
    }else if( source == _aliasConnection ){
       if( command.equals("connected") ){
              _connectButton.setLabel( "DisConnect" ) ;
              buttonsEnable(true) ;
              _switchPanel.setEnabled(true) ;
              _switchPanel.showIt("editor") ;       
       }else if( command.equals("disconnected") ){
              _connectButton.setLabel( "Connect" ) ;
              _switchPanel.setEnabled(false) ;    
              buttonsEnable(false) ;   
       }
    }
    
  
  }
  private void createTrash(){
      String cellName  = "Trash" ;
      String className = "dmg.cells.applets.alias.MultiRequestPanel" ;
      CellMessageListener cml = null ;
      try{
         cml = createPanel( className , cellName ) ;
      }catch( Exception ie ){
         _messageText.setText( "Couldn't create ("+className+") : "+ie ) ;
         return ;
      }
      _aliasConnection.addTrashListener( cml ) ;
      _csp.addPanel( cellName , (Component)cml ) ;
      doLayout() ;
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
   public static void main( String [] args ){
      if( args.length < 2 ){
         System.err.println( "Usage : ... <domainHostName> <domainPort>" ) ;
         System.exit(4) ;
      }
      int port = Integer.parseInt( args[1] ) ;
      try{
            
         new XAlias( args[0] , port ) ;
      
      }catch( Exception e ){
         e.printStackTrace() ;
         System.err.println( "Connection failed : "+e.toString() ) ;
         System.exit(4);
      }
      
   }

}

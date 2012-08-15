package dmg.cells.applets.login ;

import dmg.cells.applets.login.CenterLayout ;
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

public class AdminSwitchPanel 
       extends SshActionPanel 
       implements ActionListener   {

   private static final long serialVersionUID = -4725884520345101419L;
   private Font   _bigFont =
             new Font( "SansSerif" , Font.BOLD , 24 )  ; 
   private Font   _bigFont2 = 
             new Font( "SansSerif" , Font.BOLD , 16 )  ; 
  
  private Label      _topLabel   = new Label( "Cell Choise" , Label.CENTER ) ;
  private CardLayout _cards = new CardLayout() ;
  private Panel      _switchPanel = new Panel( _cards ) ;

  private int _b = 5 ;
  @Override
  public Insets getInsets(){ return new Insets(_b , _b ,_b , _b) ; }
  @Override
  public void paint( Graphics g ){

     Dimension   d    = getSize() ;
     g.setColor( Color.white ) ;
     g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
  }
  private Panel _topRow = new Panel( new BorderLayout() ) ;
  private final static String __header = "Cell Choice" ;
  private Label _errorLabel = new Label() ;
  private Button _userAdminButton = new Button("UserAdmin") ;
  private Button _gateAdminButton = new Button("Volume Admin" ) ;
  private Button _backButton      = new Button("Back" ) ;
  
  public AdminSwitchPanel(DomainConnection dc ){
      setLayout( new BorderLayout() ) ;
      
      Panel leftPanel   = new Panel( new BorderLayout() ) ;
      GridLayout gl = new GridLayout(0,1) ;
      gl.setHgap(10) ;
      gl.setVgap(10) ;
      Panel buttonPanel = new Panel( gl ) ;
      buttonPanel.add( _userAdminButton ) ;
      buttonPanel.add( _gateAdminButton ) ;
      buttonPanel.add( _backButton ) ;
      leftPanel.add( buttonPanel , "North" ) ;
      add( leftPanel , "West" ) ;
      add( _switchPanel , "Center" ) ;
      add( _errorLabel , "South" ) ;
      
      _userAdminButton.addActionListener(this);
      _gateAdminButton.addActionListener(this);
      _backButton.addActionListener(this);

     _switchPanel.add( new Dummy(5) , "dummy" ) ; 
     _switchPanel.add( new UserAdminPanel(dc)  , "user" ) ; 
     _switchPanel.add( new UserAdminPanel(dc)  , "tape" ) ; 
     
     _cards.show( _switchPanel , "dummy" ) ;

  }
  
  @Override
  public void actionPerformed( ActionEvent event ){
      Object source = event.getSource() ;
      if( source == _userAdminButton ){
        _cards.show( _switchPanel , "user" ) ;
      }else if( source == _gateAdminButton ){
        _cards.show( _switchPanel , "tape" ) ;
      }else if( source == _backButton ){
          informActionListeners( "exit" ) ;
      }
  }
   private class Dummy extends Canvas {
       private static final long serialVersionUID = 3398266695459128584L;
       int _count = 5 ;
       public Dummy( int count ){ _count = count ; }
       public Dummy(){
           _count = 5 ;
       }
       @Override
       public void paint( Graphics g ){
           Dimension d = getSize() ;
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
       
}
                  
 

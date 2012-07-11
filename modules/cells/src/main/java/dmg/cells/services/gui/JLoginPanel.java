// $Id: JLoginPanel.java,v 1.3 2004-04-26 05:43:59 patrick Exp $
//
package dmg.cells.services.gui ;
//
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;
import java.util.*;
import java.io.* ;
import java.net.* ;
/**
 */
public class JLoginPanel extends JPanel {

   private Object  AntiAlias    = RenderingHints.VALUE_ANTIALIAS_ON;
   private JLabel  _logoString  = new JLabel("Cell Login", JLabel.CENTER) ;
   private Font    _bigFont     = new Font( "Times" , Font.BOLD | Font.ITALIC , 26 ) ;
   private boolean _isLogin     = true ;
   private JButton _setupButton = new JButton("Setup") ;
   private JButton _loginButton = new JButton("Login") ;
   private String  _password    = "" ;
   private UserPasswordPanel _loginPanel   = new UserPasswordPanel() ;
   private SetupPanel        _setupPanel   = new SetupPanel() ;
   private MessagePanel      _messagePanel = new MessagePanel() ;
   private CardLayout        _cards        = new CardLayout() ;
   private JPanel            _cardPanel    = new JPanel() ;
   
   private class MessagePanel extends JPanel {
   
      private JLabel _message = new JLabel("",JLabel.CENTER) ;
      private MessagePanel(){
         setLayout( new GridBagLayout() ) ;
         _message.setForeground(Color.red);
         GridBagConstraints c = new GridBagConstraints()  ;
         c.gridwidth  = 1 ;
         c.gridheight = 1 ;
         c.fill = GridBagConstraints.HORIZONTAL ;
         add( _message , c ) ;
      }
      public void setText( String message ){ _message.setText(message) ; }
      
   }
   
   private class SetupPanel extends JPanel {
   
      private JLabel _hostLabel   = new JLabel( "Hostname" , JLabel.RIGHT ) ;
      private JLabel _portLabel   = new JLabel( "Portnumber" , JLabel.RIGHT ) ;
      private JLabel _schemaLabel = new JLabel( "Schema" , JLabel.RIGHT ) ;
      private JTextField _host    = new JTextField(20) ;
      private JTextField _port    = new JTextField(20) ;
      private JTextField _schema  = new JTextField(10) ;
      private JCheckBox  _lm      = new JCheckBox("Use LM" , false ) ;
      
      public Insets getInsets(){ return new Insets(20,20,20,20) ; }
    
      private SetupPanel(){
         setLayout( new GridBagLayout() ) ;
//         setBorder( new BevelBorder(BevelBorder.RAISED));
         setBorder(
            BorderFactory.createTitledBorder(" Cell Server Setup ") 
         );
         GridBagConstraints c = new GridBagConstraints()  ;
         c.gridheight = 1 ;
         c.insets     = new Insets(4,4,4,4) ;
      
         c.gridwidth  = 1 ; c.gridx = 0 ; c.gridy = 0 ;
         add( _hostLabel , c ) ; 
         c.gridwidth  = 2 ; c.gridx = 1 ; c.gridy = 0 ;
         add( _host , c ) ;
         
         c.gridwidth  = 1 ; c.gridx = 0 ; c.gridy = 1 ;
         add( _portLabel , c ) ; 
         c.gridwidth  = 2 ; c.gridx = 1 ; c.gridy = 1 ;
         add( _port , c ) ;
         
         c.gridwidth  = 1 ; c.gridx = 0 ; c.gridy = 2 ;
         add( _schemaLabel , c ) ; 
         c.gridwidth  = 1 ; c.gridx = 1 ; c.gridy = 2 ;
         add( _schema , c ) ;
         
         c.gridwidth  = 1 ; c.gridx = 2 ; c.gridy = 2 ;
         add( _lm , c ) ;

         boolean selected = _lm.isSelected() ;
         _schema.setEnabled( selected ) ;
         _schemaLabel.setEnabled( selected ) ;
         
         _lm.addActionListener( 
            new ActionListener(){
                public void actionPerformed( ActionEvent event ){
                   boolean selected = _lm.isSelected() ;
                   _schema.setEnabled( selected ) ;
                   _schemaLabel.setEnabled( selected ) ;
                }
            }
         ) ;
      }
   }
   private class UserPasswordPanel extends JPanel {
   
      private JLabel _loginLabel     = new JLabel( "Login Name" , JLabel.RIGHT ) ;
      private JLabel _passwordLabel  = new JLabel( "Password" , JLabel.RIGHT ) ;
      private JTextField     _login  = new JTextField(25) ;
      private JPasswordField _passwd = new JPasswordField(25) ;
      private JLabel    _statusLabel = new JLabel("",JLabel.CENTER) ;
      
      public Insets getInsets(){ return new Insets(10,10,10,10);}
      
      private UserPasswordPanel(){
         setLayout( new GridBagLayout() ) ;
         GridBagConstraints c = new GridBagConstraints()  ;
         c.gridwidth  = 1 ;
         c.gridheight = 1 ;
         c.insets     = new Insets(2,2,2,2) ;

         c.gridx = 0 ; c.gridy = 0 ;
         add( _loginLabel , c ) ; 
         c.gridx = 1 ; c.gridy = 0 ;
         add( _login , c ) ;
         c.gridx = 0 ; c.gridy = 1 ;
         add( _passwordLabel , c ) ;
         c.gridx = 1 ; c.gridy = 1 ;
         add( _passwd , c ) ;
         c.gridx = 0 ; c.gridy = 2 ;
         c.gridwidth = 2 ;
         c.fill = GridBagConstraints.HORIZONTAL ;
         _statusLabel.setForeground(Color.red) ;
         add( _statusLabel , c ) ;
         
         _passwd.setEchoChar( '*' ) ;
      }
   }
   private class IconDisplayPanel extends JPanel {
       private Icon _icon = null ;
       public IconDisplayPanel( Icon icon ){ 
          _icon = icon ;
       }
       public Dimension getPreferredSize(){
         return new Dimension( _icon.getIconWidth() , _icon.getIconHeight() );
       }
       public void paintComponent( Graphics g ){
          Dimension d = getSize() ;
          int x = ( d.width  - _icon.getIconWidth() ) / 2 ;
          int y = ( d.height - _icon.getIconHeight()) / 2 ;
          _icon.paintIcon( this , g , x , y ) ;
       }
   }
   private class CellIcon implements Icon {
      private int _height = 0 ;
      private int _width  = 0 ;
      private CellIcon( int width , int height ){
         _height = height ;
         _width  = width ;
      }
      public void paintIcon( Component c , Graphics gin , int xi , int yi ){
         Graphics2D g = (Graphics2D) gin ;

         g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, AntiAlias);
         g.setColor( c.getBackground() ) ;
         g.fillRect(  xi , yi , _width - 1 , _height - 1 ) ;
         int x = xi + 4 ;
         int y = yi + 4 ;
         int width = _width - 8 ;
         int height = _height - 8 ;
         
         Color col = new Color( 0 , 0 , 255 ) ;
         
         while( width > 0 ){
            g.setColor( col ) ;
            width = width / 2 ; height = height / 2 ;
            g.fillOval( x , y , width , height ) ;
            x = x + width  ; y = y + height   ;
            col = col.brighter() ;
         }
       }
      public int getIconWidth(){ return _height ; }
      public int getIconHeight(){ return _width ; }
   }
   public Dimension getMinimumSize(){ return getPreferredSize() ; }
   public Dimension getMaximumSize(){ return getPreferredSize() ; }
   public Insets    getInsets(){ return new Insets(10,10,10,10);}
   
   public void setMessage( String message ){ 
      displayMessagePanel(message) ;
   }
   public void setErrorMessage( String message ){
      if( message.length() > 45 ) {
          message = message.substring(0, 34);
      }
      _loginPanel._statusLabel.setText(message);
   }
   private JPanel _currentPanel = _loginPanel ;
   
   public void displayMessagePanel(String message){ 
     _setupButton.setEnabled(false);
     _loginButton.setEnabled(false) ;
     if( ( message != null ) && ( ! message.equals("") ) ) {
         _messagePanel.setText(message);
     }
        _cards.show( _cardPanel ,  "message" ) ;
   }
   public void displayLoginPanel(){ 
      _setupButton.setEnabled(true);
      _loginButton.setEnabled(true) ;
      _setupButton.setText("Setup");
      _cards.show(_cardPanel ,  "login");
   }
   public void displaySetupPanel(){ 
      _setupButton.setEnabled(true);
      _loginButton.setEnabled(false) ;
      _setupButton.setText("Back");
      _cards.show( _cardPanel ,  "setup" ) ;
   }
   public void paintComponent( Graphics gin ){
       Graphics2D g = (Graphics2D) gin ;
       g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, AntiAlias);
      super.paintComponent( g ) ;
   }
   public JLoginPanel(){
   
      BorderLayout bl = new BorderLayout() ;
      bl.setVgap(10) ;
      bl.setHgap(10);
      setLayout(bl) ;
      setBorder( new BevelBorder(BevelBorder.LOWERED));
      _logoString.setFont( _bigFont ) ;
      
      add( _logoString , "North" ) ;

      _cardPanel.setLayout( _cards ) ;
      _cardPanel.add( _loginPanel , "login" ) ;
      _cardPanel.add( _setupPanel , "setup" ) ;
      _cardPanel.add( _messagePanel , "message" ) ;
      
      add( _cardPanel , "Center" ) ;
      
      URL imageUrl = getClass().getResource("/images/cells-logo.jpg") ;
      Icon icon = imageUrl == null ?
                  (Icon)new CellIcon( 80 , 80 ) :
                  (Icon)new ImageIcon(imageUrl) ;
            
      JPanel c = new IconDisplayPanel( icon ) ;

      add(   c , "West") ;
      JPanel b = new JPanel(new BorderLayout()) ;
      b.add( _setupButton , "West" ) ;
      b.add( _loginButton , "East" ) ;
      
      add( "South" , b ) ;
      
      _setupButton.addActionListener(
         new ActionListener(){
            public void actionPerformed( ActionEvent event ){
               if( _isLogin ){
                  displaySetupPanel() ;
               }else{
                  displayLoginPanel() ;
               }
               _isLogin = ! _isLogin ;
            }
         }
       ) ;
       _loginButton.addActionListener(
          new ActionListener(){
             public void actionPerformed( ActionEvent event ){
                _password = new String( _loginPanel._passwd.getPassword() ) ;
                _loginPanel._passwd.setText("") ;
                processEvent(event);
             }
          }
       
       ) ;
       setLogin( System.getProperty( "user.name" ) ) ;
       setSchema( "default" ) ;
       setPortnumber( "22124" ) ;
       try{
         String thisHost = InetAddress.getLocalHost().getHostName() ;
         int pos = thisHost.indexOf(".") ;
         if( pos > -1 ){
            thisHost = thisHost.substring(pos) ;
            setHostname( "dcache"+thisHost ) ;
         }else{
            setHostname( "dcache" ) ;
         }
       }catch(Exception e){
         setHostname( "dcache" ) ;
       }
   }
   public void setLocationManager( boolean lm ){ _setupPanel._lm.setSelected(lm) ; }
   public void setHostname( String hostname ){ _setupPanel._host.setText( hostname ) ; }
   public void setPortnumber( String portnumber ){ _setupPanel._port.setText( portnumber ) ; }
   public void setSchema( String schema ){ _setupPanel._schema.setText( schema ) ; }
   public void setLogin( String login ){ _loginPanel._login.setText( login ) ; }
   public void setPassword( String passwd ){ _loginPanel._passwd.setText( passwd ) ; }
   
   public boolean isLocationManager(){ return _setupPanel._lm.isSelected() ; }
   public String getHostname(){ return _setupPanel._host.getText() ; }
   public String getPortnumber(){ return _setupPanel._port.getText() ; }
   public String getSchema(){ return _setupPanel._schema.getText() ; }
   public String getLogin(){ return _loginPanel._login.getText() ; }
   public String getPassword(){ return _password ; }
   
   private ActionListener _actionListener = null;

   public synchronized void addActionListener(ActionListener l) {
      _actionListener = AWTEventMulticaster.add( _actionListener, l);
   }
   public synchronized void removeActionListener(ActionListener l) {
      _actionListener = AWTEventMulticaster.remove( _actionListener, l);
   }
   public void processEvent( ActionEvent e) {
      if( _actionListener != null) {
          _actionListener.actionPerformed(e);
      }
   }         
   
   
}

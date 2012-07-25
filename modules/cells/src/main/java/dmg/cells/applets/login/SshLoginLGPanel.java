package dmg.cells.applets.login ;

import java.awt.* ;
import java.awt.event.* ;
import java.net.* ;
import java.io.*;
import dmg.util.* ;
import dmg.protocols.ssh.* ;

public class      SshLoginLGPanel 
       extends    SshActionPanel 
       implements ActionListener, TextListener {
       
   private TextField _hostText ;
   private TextField _portText ;
   private TextField _userText ;
   private TextField _passText ;
   private Button    _goButton ;
   private Button    _exitButton ;
   private Label     _message ;
   private String    _title;
   private Font      _font  = new Font( "TimesRoman" , 0 , 24 ) ;
   private Font      _fontx = new Font( "TimesRoman" , 0 , 14 ) ;
   private String  _hostName,
                   _portName,
                   _userName;
   private boolean _hostVis    = true , 
                   _portVis    = true , 
                   _userVis    = true ;
   private boolean _hostChange = true , 
                   _portChange = true , 
                   _userChange = true ;
   private String _remoteHost,
                  _remotePort,
                  _remoteUser,
                  _remotePassword;
   
   
   @Override
   public Insets getInsets(){ return new Insets(10,10,10,10) ; }
   public void ok(){
   
       setBackground( Color.green ) ;
       BorderLayout bl = new BorderLayout() ;
       bl.setHgap(10) ;
       bl.setVgap(10) ;
       Panel q = new Panel( bl ) ;
       
       Label top = new Label( _title == null ? 
                              "Login" : 
                              _title , Label.CENTER ) ;
       top.setFont( _font ) ;
       
//       GridLayout gl = new GridLayout( 0 , 2 ) ;
//       gl.setHgap(10) ;
//       gl.setVgap(10) ;
//       Panel p = new Panel( gl ) ;
       Panel p = new Panel( new KeyValueLayout() ) ;
//       p.setBackground( Color.yellow ) ;
       if( _hostVis ){
          p.add( new Label( "Hostname" ) ) ;
          p.add( _hostText = new TextField( _hostName==null?"":_hostName ) ) ;
       }
       if( _portVis ){
          p.add( new Label( "PortNumber" ) ) ;
          p.add( _portText = new TextField( _portName==null?"":_portName ) ) ;
       }
       p.add( new Label( "UserName" ) ) ;
       p.add( _userText = new TextField( _userName==null?"":_userName ) ) ;
       p.add( new Label( "Password" ) ) ;
       p.add( _passText = new TextField( "" ) ) ;
       p.add( _goButton = new Button( "Connect" ) ) ;
       p.add( _exitButton = new Button( "Exit" ) ) ;
       
       _passText.setEchoChar( '*' ) ;
       _goButton.addActionListener( this ) ;
       _exitButton.addActionListener( this ) ;
       


       _message = new Label("",Label.CENTER) ;
//       _message.setBackground( Color.red ) ;
       
       q.add( "North" , top ) ;
       q.add( "Center" , p ) ;
       q.add( "South" ,  _message ) ;
       
       add( q ) ;
       
   }
   public void setHost( String host , boolean visible , boolean changable ){
      _hostName = host ; _hostVis = visible ; _hostChange = changable ;
   }
   public void setPort( String port , boolean visible , boolean changable ){
      _portName = port ; _portVis = visible ; _portChange = changable ;
   }
   public void setUser( String user , boolean visible , boolean changable ){
      _userName = user ;
   }
   public void setTitle( String title ){ _title = title ; }
   @Override
   public void setEnabled( boolean e ){
      _goButton.setEnabled( e ) ;
      _exitButton.setEnabled( e ) ;
      _passText.setEnabled( e ) ;
   }
   public void setText( String msg ){
     if( msg.length() > 0 ) {
         _message.setBackground(Color.red);
     } else {
         _message.setBackground(getBackground());
     }
     _message.setText( msg ) ;
   }
   @Override
   public void textValueChanged( TextEvent event ){
   
   
   }
   @Override
   public void actionPerformed( ActionEvent event ){

      String command = event.getActionCommand() ;
      System.out.println( "Action SshLoginLGPanel : "+command ) ;
      Object obj = event.getSource() ;

      if( obj == _goButton ){
         setText("") ;
         if( _userText.getText().equals("") ||
             _passText.getText().equals("") ||
             _hostText.getText().equals("") ||
             _portText.getText().equals("")  ){
             setText( "Information missing" ) ;
             return ;  
          }
          _remoteHost = _hostText.getText() ;
          _remotePort = _portText.getText() ;
          _remoteUser = _userText.getText() ;
          _remotePassword = _passText.getText() ;
          _passText.setText("");
          informActionListeners( "go" ) ;
      }else if( obj == _exitButton ){
         informActionListeners( "exit" ) ;
      }
   }
   public String getUser(){ return _remoteUser ; }
   public String getPassword(){ return _remotePassword ; }
   public String getHost(){ return _remoteHost ; }
   public String getPort(){ return _remotePort ; }
   @Override
   public Dimension getMinimumSize(){
      Dimension d = super.getMinimumSize() ;
      System.out.println( "getMin : "+d ) ;
      return  d ;
   }
   @Override
   public Dimension getPreferredSize(){
      Dimension d = getMinimumSize() ;
      System.out.println( "getPr : "+d ) ;
      return  d ;
   }
   @Override
   public Dimension getMaximumSize(){
       Dimension d = getMinimumSize() ;
      System.out.println( "getPr : "+d ) ;
       return  d ;
   }


} 

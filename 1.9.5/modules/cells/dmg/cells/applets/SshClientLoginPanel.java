package dmg.cells.applets ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.net.* ;
import java.io.*;
import dmg.util.* ;
import dmg.protocols.ssh.* ;

public class      SshClientLoginPanel 
       extends    SshClientActionPanel 
       implements ActionListener {
       
   private TextField _hostName ;
   private TextField _portNumber ;
   private TextField _userName ;
   private TextField _userPassword ;
   private Button    _goButton ;
   private Button    _exitButton ;
   private Label     _message ;
   private Font      _font = new Font( "TimesRoman" , Font.BOLD , 20 ) ;
   
   SshClientLoginPanel(){
   
       setLayout( new CenterLayout() ) ;
       setBackground( Color.green ) ;
       Panel q = new Panel( new BorderLayout() ) ;
       
       Label top = new Label( "Login" , Label.CENTER ) ;
       top.setFont( _font ) ;
       top.setBackground( Color.blue ) ;
       top.setForeground( Color.red ) ;
       
       Panel p = new Panel( new GridLayout( 0 , 2  ) ) ;
       p.setBackground( Color.yellow ) ;
       p.add( new Label( "Hostname" ) ) ;
       p.add( _hostName = new TextField( "localhost" ) ) ;
       p.add( new Label( "PortNumber" ) ) ;
       p.add( _portNumber = new TextField( "22" ) ) ;
       p.add( new Label( "UserName" ) ) ;
       p.add( _userName = new TextField( "patrick" ) ) ;
       p.add( new Label( "Password" ) ) ;
       p.add( _userPassword = new TextField( "" ) ) ;
       p.add( _goButton = new Button( "Connect" ) ) ;
       p.add( _exitButton = new Button( "Exit" ) ) ;
       
       _userPassword.setEchoChar( '*' ) ;
       _goButton.addActionListener( this ) ;
       _exitButton.addActionListener( this ) ;
       
       q.add( "North" , top ) ;
       q.add( "South" , p ) ;


       add( new dmg.cells.applets.spy.BorderPanel( q ) ) ;
       _message = new Label("widiwidiwi",Label.CENTER) ;
       _message.setBackground( Color.red ) ;
       add( new dmg.cells.applets.spy.BorderPanel( _message )  ) ;
       
       
   }
   public void setEnabled( boolean e ){
      _goButton.setEnabled( e ) ;
      _exitButton.setEnabled( e ) ;
      _userPassword.setEnabled( e ) ;
   }
   public void setText( String msg ){
     _message.setText( msg ) ;
   }
   public void actionPerformed( ActionEvent event ){

      String command = event.getActionCommand() ;
      System.out.println( "Action : "+command ) ;
      Object obj = event.getSource() ;

      if( obj == _goButton ){
         _message.setText("") ;
         if( _userName.getText().equals("") ||
             _userPassword.getText().equals("") ||
             _hostName.getText().equals("") ||
             _portNumber.getText().equals("")  ){
             _message.setText( "Information missing" ) ;
             return ;  
          }
         informActionListeners( "go" ) ;
      }else if( obj == _exitButton ){
         informActionListeners( "exit" ) ;
      }
   }
   public String getUser(){ return _userName.getText() ; }
   public String getPassword(){ return _userPassword.getText() ; }
   public String getHost(){ return _hostName.getText() ; }
   public int    getPort(){ 
      try{
         return new Integer( _portNumber.getText() ).intValue() ;
      }catch( Exception e ){
         return 0 ;
      }  
   }


} 

package dmg.cells.applets ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.net.* ;
import java.io.*;
import dmg.util.* ;
import dmg.protocols.ssh.* ;

public class      SshClientApplet2 
       extends    Applet 
       implements ActionListener, 
                  Runnable ,
                  SshClientAuthentication       {
       
  //
  // remember to change the following variable to your needs 
  //  
  private String    _remoteHost = "eshome" ;
  private int       _remotePort = 22124 ;
  private String    _remoteUser = "manfred" ;
  private String    _remotePassword = "manfred" ;
  private Font      _font = new Font( "TimesRoman" , 0 , 16 ) ;
  
  private boolean   _binary  = true ;
  
  private SshClientIoPanel       _ioPanel ;
  private SshClientIoBinaryPanel _ioBinaryPanel ;
  private SshClientLoginPanel  _loginPanel ;
  private SshClientFPPanel     _fpPanel ;
  private CardLayout           _cardsLayout ;
  private Thread  _connectionThread ;
  private Gate    _fpGate  = new Gate(false) ;
  private boolean _hostOk  = false ;
  private int     _requestCounter = 0 ;
  
  public synchronized void actionPerformed( ActionEvent event ){
     String command = event.getActionCommand() ;
     System.out.println( "Action x : "+command ) ;
     Object obj = event.getSource() ;
     if( obj == _ioPanel ){
        _loginPanel.setEnabled(true) ;
        _loginPanel.setText("");
        _cardsLayout.show( this , "login" ) ;
     }else if( obj == _ioBinaryPanel ){
        _loginPanel.setEnabled(true) ;
        _loginPanel.setText("");
        _cardsLayout.show( this , "login" ) ;
     }else if( obj == _loginPanel ){
       if( command.equals(  "go" ) ){
           _loginPanel.setEnabled(false) ;
           _remoteUser     = _loginPanel.getUser() ;
           _remotePassword = _loginPanel.getPassword() ;
           _remoteHost     = _loginPanel.getHost() ;
           _remotePort     = _loginPanel.getPort() ;
           _connectionThread = new Thread( this ) ;
           _fpGate.close() ;
           _connectionThread.start() ;
        }else if( command.equals( "exit" ) ){
           System.exit(4);
        }
     }else if( obj == _fpPanel ){
        if( command.equals("accept" ) ){
           _hostOk = true ;
           _cardsLayout.show( this , "io" ) ;
        }else if( command.equals("reject") ){
           _hostOk = false ;
           _cardsLayout.show( this , "login" ) ;
        }
        
        _fpGate.open() ;
     }
  }
  public void run(){
     if( Thread.currentThread() == _connectionThread ){
       try{
          Socket socket = new Socket( _remoteHost , _remotePort ) ;
          SshStreamEngine engine = new SshStreamEngine( socket , this ) ;
          if( _binary ){
             _ioBinaryPanel.startIO( 
                     engine.getInputStream() , 
                     engine.getOutputStream() ,
                     engine.getReader() ,
                     engine.getWriter()            ) ;
          }else{
             _ioPanel.startIO( engine.getReader() , engine.getWriter() ) ;
          }
       }catch( Error er ){
          er.printStackTrace() ;
       }catch( Exception e ){
          e.printStackTrace() ;
          System.out.println( "ConnectionThread : "+e ) ;
          _cardsLayout.show( this , "login" ) ;
          _loginPanel.setEnabled(true);
          _loginPanel.setText( "Not connected : "+e.getMessage() ) ;
//          e.printStackTrace() ;
//          System.exit(4);
       }
     }
     
  }
  public void init(){
      System.out.println( "init ..." ) ;
      Dimension   d  = getSize() ;
//      setFont( _font ) ;
      setLayout( _cardsLayout = new CardLayout() ) ;
      
      add( _loginPanel = new SshClientLoginPanel() , "login" )   ;
      if( ! _binary ){
          add( _ioPanel       = new SshClientIoPanel()    , "io" ) ;
          _ioPanel.addActionListener(this );
      }else{
          add( _ioBinaryPanel = new SshClientIoBinaryPanel()    , "io" ) ;
          _ioBinaryPanel.addActionListener(this );
      }
      add( _fpPanel    = new SshClientFPPanel()    , "fp" ) ;
            
      _loginPanel.addActionListener(this );
      _fpPanel.addActionListener(this );

      
      _cardsLayout.show( this , "login" ) ;
      setVisible( true ) ;
  }
  public void start(){
      System.out.println("start ..." ) ;
      //
      // get the parameter from the html file
      //
  }
  public void stop(){
      System.out.println("stop ..." ) ;
  }
  public void destroy(){
      System.out.println("destroy ..." ) ;
      try{
          _connectionThread.interrupt();
      }catch( Exception e ){}
  }      
  //
  //   Client Authentication interface 
  //   
  public boolean isHostKey( InetAddress host , SshRsaKey keyModulus ) {


      System.out.println( "Host key Fingerprint\n   -->"+
                      keyModulus.getFingerPrint()+"<--\n"   ) ;
      _fpGate.close();
      _fpPanel.setFingerPrint( host.getHostName() , 
                               keyModulus.getFingerPrint() ) ;
      _cardsLayout.show( this , "fp" ) ;
      _fpGate.check() ;
      return _hostOk ; 

  }
  public String getUser( ){
     _requestCounter = 0 ;
     return _remoteUser ;
  }
  public SshSharedKey  getSharedKey( InetAddress host ){ return null ; }

  public SshAuthMethod getAuthMethod(){  
      return _requestCounter++ > 2 ? 
             null : 
             new SshAuthPassword( _remotePassword ) ;
  }
       
}

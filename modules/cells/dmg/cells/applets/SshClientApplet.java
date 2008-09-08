package dmg.cells.applets ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.net.* ;
import java.io.*;
import dmg.util.* ;
import dmg.protocols.ssh.* ;

public class      SshClientApplet 
       extends    Applet 
       implements ActionListener, Runnable, SshClientAuthentication       {
       
  //
  // remember to change the following variable to your needs 
  //  
  private String    _remoteHost = "eshome" ;
  private int       _remotePort = 22124 ;
  private String    _remoteUser = "manfred" ;
  private String    _remotePassword = "manfred" ;

  private TextArea  _output ;
  private TextField _input ;
  private Button    _connectButton ; 
  private Button    _psButton ;
  private Button    _exitButton ;
  private Button    _clearButton ;
  private BufferedReader _reader ;
  private PrintWriter    _writer ;
  
  private Thread         _printerThread  ;
  
  public void actionPerformed( ActionEvent event ){
     String command = event.getActionCommand() ;
     System.out.println( "Action : "+command ) ;
     Object obj = event.getSource() ;
     if( obj == _connectButton ){
       try{
          _connectButton.setEnabled(false) ;
          Socket socket = new Socket( _remoteHost , _remotePort ) ;
          _output.append( "Socket connected : "+
                          socket.getInetAddress().getHostName()+"\n" ) ;
          SshStreamEngine engine = new SshStreamEngine( socket , this ) ;
          _output.append( "Engine finished\n") ;
          _reader = new BufferedReader( engine.getReader() ) ;
          _writer = new PrintWriter( engine.getWriter() ) ;
          _printerThread = new Thread( this ) ;
          _printerThread.start() ;
          weAreConnected(true) ;
       }catch( Exception e ){
          e.printStackTrace() ;
          System.exit(4);
       }
     }else if( obj == _psButton ){
       _writer.println( "ps -a" ) ; _writer.flush() ;
     }else if( obj == _exitButton ){
       _writer.println( "exit" ) ; _writer.flush() ;
     }else if( obj == _input ){
       _writer.println( _input.getText() ) ; _writer.flush() ;
     }else if( obj == _clearButton ){
       _output.setText("") ;
     }
     
  }
  private void weAreConnected( boolean connected ){
      _psButton.setEnabled( connected ) ;
      _exitButton.setEnabled( connected ) ;
      _connectButton.setEnabled(! connected) ;
  }
  public void run(){
     if( Thread.currentThread() == _printerThread ){
        try{
          String line ;
          while( ( line = _reader.readLine() ) != null ){
//             System.out.println( "got : "+line ) ;
             _output.append( line + "\n" ) ;          
          }
          weAreConnected(false) ;
        }catch( IOException ioe ){
           ioe.printStackTrace() ;
        }
     
     }
  }
  public void init(){
      System.out.println( "init ..." ) ;
      Dimension   d  = getSize() ;
      setLayout( new BorderLayout() ) ;
      
      _output = new TextArea( 10 , 10 ) ;
      add( _output , "Center" ) ;
      
      _input = new TextField(80 ) ;
      add( _input , "South" ) ;
      
      Panel buttonPanel  = new Panel( new GridLayout(1,0) ) ;
      
      buttonPanel.add( _connectButton = new Button( "Connect") ) ;
      buttonPanel.add( _psButton      = new Button( "Process List" ) ) ;
      buttonPanel.add( _exitButton    = new Button( "Exit" ) ) ;
      buttonPanel.add( _clearButton   = new Button( "ClearScreen" ) ) ;
      
      _connectButton.addActionListener(this );
      _psButton.addActionListener(this );
      _exitButton.addActionListener(this );
      _clearButton.addActionListener(this );
      _input.addActionListener(this );
       
      weAreConnected(false) ;
      
      add( buttonPanel , "North" ) ;
      
      setVisible( true ) ;
  }
  public void start(){
      System.out.println("start ..." ) ;
      //
      // get the parameter from the html file
      //
      if( ( _remoteHost = getParameter("host") ) == null ){
         _output.append( "Sorry, can't find host in parameter list\n" ) ;
         return ;
      } 
      if( ( _remoteUser = getParameter("user") ) == null ){
         _output.append( "Sorry, can't find user in parameter list\n" ) ;
         return ;
      } 
      if( ( _remotePassword = getParameter("password") ) == null ){
         _output.append( "Sorry, can't find password in parameter list\n" ) ;
         return ;
      } 
      String tmp ;
      if( ( tmp = getParameter("port") ) == null ){
         _output.append( "Sorry, can't find port in parameter list\n" ) ;
         return ;
      }
      _remotePort = new Integer( tmp ).intValue() ; 
      _output.append( "Connecting ... \n" );
      _output.append( "Host   "+_remoteHost+"\n" );
      _output.append( "Port   "+_remotePort+"\n" );
      _output.append( "User   "+_remoteUser+"\n" );
      _output.append( "Pswd   "+"****"+"\n" );
  }
  public void stop(){
  
  }
  public void destroy(){
  
  }      
   //
   //   Client Authentication interface 
   //   
   public boolean isHostKey( InetAddress host , SshRsaKey keyModulus ) {
   
   
       _output.append( "Host key Fingerprint\n   -->"+
                       keyModulus.getFingerPrint()+"<--\n"   ) ;
                       
       return true ; // whatever happens , we agree 

   }
   public String getUser( ){
      return _remoteUser ;
   }
   public SshSharedKey  getSharedKey( InetAddress host ){ return null ; }

   public SshAuthMethod getAuthMethod(){   
       return new SshAuthPassword( _remotePassword ) ;
   }
       
}

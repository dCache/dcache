package dmg.cells.applets.login ;

import java.awt.* ;
import java.awt.event.* ;
import java.io.*;
import java.net.* ;
import java.util.* ;
import dmg.util.* ;
import dmg.protocols.ssh.* ;


public class      SshLoginPanel
       extends    SshActionPanel
       implements ActionListener,
                  Runnable,
                  SshClientAuthentication,
                  DomainConnection          {


    private static final long serialVersionUID = 5230265767298840740L;
    private CardLayout  _cardsLayout ;
    private SshLoginFPPanel _fpPanel ;
//    private SshLoginLGPanel _lgPanel ;
    private HelloPanel      _lgPanel ;
    private SshLoginOKPanel _okPanel ;
    private Thread          _connectionThread;
    private Thread          _timeoutThread;
    private Thread          _receiverThread;
    private boolean         _hostKeyOk        = true ;
    private String _remoteHost, _remotePort,
                   _remoteUser, _remotePassword;
    private int _b = 10 ;
    private int     _requestCounter;
    private long    _timeout       = 20000 ;
    private ObjectInputStream   _objIn;
    private ObjectOutputStream  _objOut;
    private final Object              _threadLock = new Object() ;
    private Socket              _socket;
    private static final String [] __panels = { "login" , "fp" , "ok" } ;
    private static final int ST_IDLE   = 0 ;
    private static final int ST_ACCEPT = 1 ;
    private static final int ST_REJECT = 2 ;
    private static final int ST_WAITING_FOR_OK = 3 ;
    private static final int ST_OK = 4 ;
    private static final int ST_TIMEOUT = 6 ;

    private int  _status = ST_IDLE ;


    public SshLoginPanel(){
       setLayout( _cardsLayout = new CardLayout() ) ;
       setBackground( Color.white ) ;

//       add( _lgPanel = new SshLoginLGPanel() , "login" ) ;
       add( _lgPanel = new HelloPanel("Welcome to EuroStore") , "login" ) ;
       add( _fpPanel = new SshLoginFPPanel() , "fp" ) ;
       add( _okPanel = new SshLoginOKPanel() , "ok" ) ;

       _lgPanel.addActionListener(this) ;
       _fpPanel.addActionListener(this) ;
       _okPanel.addActionListener(this) ;

       _cardsLayout.show( this , "login" ) ;

       setVisible( true ) ;
    }

    @Override
    public void run(){
       Thread current = Thread.currentThread() ;
       if( current == _connectionThread ){
          try{

             runConnectionProtocol() ;

             synchronized( _threadLock ){
                 System.out.println( "ST_OK -> notify" ) ;
                 _status = ST_OK ;
                 _threadLock.notifyAll() ;
             }
             _receiverThread = new Thread( this ) ;
             _receiverThread.start() ;

             informActionListeners( "connected" ) ;
             informListenersOpened() ;
          }catch( Exception rcpe ){
             System.out.println( "Trying to close socket" ) ;
             try{ _socket.close() ; }catch(Exception eeee){
                System.out.println( "Socket close : "+eeee ) ;
             }

             rcpe.printStackTrace() ;
             System.err.println( "runConnectionProtocol failed : "+rcpe ) ;
             _cardsLayout.show( this , "login" ) ;
             _lgPanel.setEnabled(true);
//             _lgPanel.setText( "Not connected : "+rcpe.getMessage() ) ;
             _lgPanel.setText( "Not connected : Login Failed") ;

             synchronized( _threadLock ){
                 System.out.println( "ST_IDLE -> notify" ) ;
                 _status = ST_IDLE ;
                 _threadLock.notifyAll() ;
             }
          }
          System.out.println( "!!! Connection Thread finished" ) ;
       }else if( current == _timeoutThread ){
          synchronized( _threadLock ){

             _connectionThread.start() ;

             long start = System.currentTimeMillis() ;
             long rest  = _timeout ;
             while( ( rest > 0 ) && ( _status != ST_OK ) ){
                try{
                    _threadLock.wait(rest) ;
                }catch( InterruptedException ie ){
                   System.out.println( "isHostKey was interrupted");
                   break ;
                }
                rest = _timeout - (System.currentTimeMillis()-start);
             }

             if( _status != ST_OK ){

                 _connectionThread.interrupt() ;
                 _cardsLayout.show( this , "login" ) ;
                 _lgPanel.setEnabled( true ) ;
                 _status = ST_IDLE ;
                 _threadLock.notifyAll() ;
//                 _status = ST_TIMEOUT ;
                 System.out.println( "ST_TIMEOUT -> notify" ) ;
             }
             _timeoutThread = null ;
          }
          System.out.println( "!!! Timeout Thread finished" ) ;
       }else if( current == _receiverThread ){
          runReceiver() ;
          System.out.println( "!!! Receiver Thread finished" ) ;
          _cardsLayout.show( this , "login" ) ;
          _lgPanel.setEnabled( true ) ;
       }
    }
    private void runConnectionProtocol() throws Exception {

        int             port    = Integer.parseInt( _remotePort ) ;
                        _socket = new Socket( _remoteHost , port ) ;
        SshStreamEngine engine  = new SshStreamEngine( _socket , this ) ;
        PrintWriter     writer  = new PrintWriter( engine.getWriter() ) ;
        BufferedReader  reader = new BufferedReader( engine.getReader() ) ;
        System.out.println( "sending binary" ) ;
        writer.println( "$BINARY$" ) ;
        writer.flush() ;
        String  check;
        do{
           check = reader.readLine()   ;
        }while( ! check.equals( "$BINARY$" ) ) ;
//        if( check.equals( "$BINARY$" ) ){
//           System.out.println( "Switch to binary ack." ) ;
//        }else{
//           System.err.println( "Switch to binary failed" ) ;
//           throw new Exception( "can't switch to binary" ) ;
//        }
        //
        System.out.println("Binary ack.");
        _objOut = new ObjectOutputStream( engine.getOutputStream() ) ;
        _objIn  = new ObjectInputStream( engine.getInputStream() ) ;

    }
    @Override
    public synchronized void actionPerformed( ActionEvent event ){
       String command = event.getActionCommand() ;
       System.out.println( "Action x : "+command ) ;
       Object obj = event.getSource() ;
       if( obj == _lgPanel ){
          if( command.equals(  "go" ) ){
              _lgPanel.setEnabled(false) ;
              _remoteUser     = _lgPanel.getUser() ;
              _remotePassword = _lgPanel.getPassword() ;
              _remoteHost     = _lgPanel.getHost() ;
              _remotePort     = _lgPanel.getPort() ;
              synchronized( _threadLock ){
                 _connectionThread = new Thread( this ) ;
                 _timeoutThread    = new Thread( this ) ;
                 _timeoutThread.start() ;
              }
           }else if( command.equals( "exit" ) ){
              System.exit(4);
           }
       }else if( obj == _okPanel ){
           try{ _socket.close() ; }catch(IOException ee ){}
           _cardsLayout.show( this , "login" ) ;
           _lgPanel.setEnabled( true ) ;
       }else if( obj == _fpPanel ){
           if( command.equals("accept" ) ){
              _cardsLayout.show( this , "ok" ) ;
              synchronized(_threadLock){
                  System.out.println( "ST_ACCEPT -> notify" ) ;
                  _status = ST_ACCEPT ;
                  _threadLock.notifyAll() ;
              }
           }else if( command.equals("reject") ){
              _cardsLayout.show( this , "login" ) ;
              synchronized(_threadLock){
                  System.out.println( "ST_REJECT -> notify" ) ;
                  _status = ST_REJECT ;
                  _threadLock.notifyAll() ;
              }
           }
      }
    }
   @Override
   public Dimension getMinimumSize(){
      Dimension d = super.getMinimumSize() ;
//      System.out.println( "getMin : "+d ) ;
      return  d ;
   }
   @Override
   public Dimension getPreferredSize(){
      Dimension d = getMinimumSize() ;
//      System.out.println( "getPr : "+d ) ;
      return  d ;
   }
   @Override
   public Dimension getMaximumSize(){
       Dimension d = getMinimumSize() ;
//      System.out.println( "getPr : "+d ) ;
       return  d ;
   }
    public void ok(){ _lgPanel.ok() ; }
    public void logout(){
    /* *
       *   not allowed
       *
        try{ _objOut.close() ; }catch(Exception e){
           System.err.println( "Closing _objOut : "+e ) ;
           e.printStackTrace() ;
        }
        try{ _objIn.close() ; }catch(Exception e){
           System.err.println( "Closing _objIn : "+e ) ;
           e.printStackTrace() ;
        }
    */
       try{
          _objOut.write( 4 ) ; // control D
          _objOut.flush() ;
       }catch(IOException e){
           System.err.println( "_objOut.write( 4 ) : "+e ) ;
           e.printStackTrace() ;
       }
        try{ if(_timeoutThread!=null) {
            _timeoutThread.interrupt();
        }
        }catch(Exception e){
           System.err.println( "_timeoutThread : "+e ) ;
           e.printStackTrace() ;
        }
        try{ if(_receiverThread!=null) {
            _receiverThread.interrupt();
        }
        }catch(Exception e){
           System.err.println( "_receiverThread : "+e ) ;
           e.printStackTrace() ;
        }
        try{ if(_connectionThread!=null) {
            _connectionThread.interrupt();
        }
        }catch(Exception e){
           System.err.println( "_connectionThread : "+e ) ;
           e.printStackTrace() ;
        }
        _lgPanel.setEnabled(true) ;
        System.out.println("Logout requested" ) ;
    }
    public void setHost( String host , boolean visible , boolean changable ){
       _lgPanel.setHost( host , visible , changable ) ;
    }
    public void setPort( String port , boolean visible , boolean changable ){
       _lgPanel.setPort( port , visible , changable ) ;
    }
    public void setUser( String user , boolean visible , boolean changable ){
       _lgPanel.setUser( user , visible , changable ) ;
    }
    public void setTitle( String title ){
       _lgPanel.setTitle(title);
    }
    @Override
    public Insets getInsets(){ return new Insets( _b , _b ,_b , _b ) ; }
    /*
    public void paint( Graphics g ){
       Dimension d = getSize() ;
       int h = _b / 2 ;
       g.setColor( new Color(0,255,255) ) ;
       int [] xs = new int[4] ;
       int [] ys = new int[4] ;
       xs[0] = h           ; ys[0] = h ;
       xs[1] = d.width-h-1 ; ys[1] = h ;
       xs[2] = d.width-h-1 ; ys[2] = d.height - h - 1 ;
       xs[3] = h           ; ys[3] = d.height - h - 1 ;
       g.drawPolygon( xs , ys , xs.length  ) ;
    }
    */
  //===============================================================================
  //
  //   domain connection interface
  //
  private Hashtable _packetHash = new Hashtable() ;
  private final Object    _ioLock     = new Object() ;
  private int       _ioCounter  = 100 ;
  private Vector    _listener   = new Vector() ;
  private boolean   _connected;
  @Override
  public String getAuthenticatedUser(){ return _remoteUser ; }
  @Override
  public int sendObject( Serializable obj ,
                         DomainConnectionListener listener ,
                         int id
                                              ) throws IOException {
      synchronized( _ioLock ){
          DomainObjectFrame frame =
                  new DomainObjectFrame( obj , ++_ioCounter , id ) ;
          _objOut.writeObject( frame ) ;
          _objOut.reset() ;
          _packetHash.put( frame , listener ) ;
          return _ioCounter ;
      }
  }
  @Override
  public int sendObject( String destination ,
                         Serializable obj ,
                         DomainConnectionListener listener ,
                         int id
                                              ) throws IOException {
      synchronized( _ioLock ){
          DomainObjectFrame frame =
                  new DomainObjectFrame( destination , obj , ++_ioCounter , id ) ;
          _objOut.writeObject( frame ) ;
          _objOut.reset() ;
          _packetHash.put( frame , listener ) ;
          return _ioCounter ;
      }
  }
  @Override
  public void addDomainEventListener( DomainEventListener listener ){
     synchronized( _ioLock ){
       _listener.addElement(listener) ;
       if( _connected ){
           try{  listener.connectionOpened( this ) ;
           }catch( Throwable t ){
              t.printStackTrace() ;
           }
       }
     }
  }
  @Override
  public void removeDomainEventListener( DomainEventListener listener ){
     synchronized( _ioLock ){
       _listener.removeElement(listener);
     }
  }
  private void informListenersOpened(){
     synchronized( _ioLock ){
        _connected = true ;
         for (DomainEventListener listener : new ArrayList<DomainEventListener>(_listener)) {
             try {
                 listener.connectionOpened(this);
             } catch (Throwable t) {
                 t.printStackTrace();
             }
         }
     }
  }
  private void informListenersClosed(){
     synchronized( _ioLock ){
        _connected = false ;
         for (DomainEventListener listener : new ArrayList<DomainEventListener>(_listener)) {
             try {
                 listener.connectionClosed(this);
             } catch (Throwable t) {
                 t.printStackTrace();
             }
         }
     }
  }
  //
  //
  /////////////////////////////////////////////////////////////////////////////////
  private void runReceiver(){

     Object            obj;
     DomainObjectFrame frame;
     DomainConnectionListener listener;
     while( true ){
        try{
            if( ( obj = _objIn.readObject()  ) == null ) {
                break;
            }
        }catch(Exception ioe ){
            System.err.println( "readObject failed : "+ioe ) ;
            break ;
        }
        if( ! ( obj instanceof DomainObjectFrame ) ) {
            continue;
        }
        frame    = (DomainObjectFrame) obj ;
        listener = (DomainConnectionListener)_packetHash.remove( frame ) ;
        if( listener == null ) {
            continue;
        }

        try{

            listener.domainAnswerArrived( frame.getPayload() , frame.getSubId() ) ;

        }catch(Exception eee ){
            System.out.println( "Problem in domainAnswerArrived : "+eee ) ;
        }
     }
     informActionListeners( "disconnected" ) ;
     informListenersClosed() ;
     try{
         System.out.println( "Closing connection" ) ;
         _socket.close() ;
     }catch( Exception ce ){

     }
  }
  ////////////////////////////////////////////////////////////////////////////////////////
  //
  //   Client Authentication interface
  //
  @Override
  public boolean isHostKey( InetAddress host , SshRsaKey keyModulus ) {


//      System.out.println( "Host key Fingerprint\n   -->"+
//                      keyModulus.getFingerPrint()+"<--\n"   ) ;

      if( ! _hostKeyOk ){
          _fpPanel.setFingerPrint( host.getHostName() ,
                                   keyModulus.getFingerPrint() ) ;

          synchronized( _threadLock ){
              _cardsLayout.show( this , "fp" ) ;
              _status = ST_WAITING_FOR_OK ;
              while( _status == ST_WAITING_FOR_OK ){
                 try{ _threadLock.wait() ;
                 }catch( InterruptedException ie ){
                    System.out.println( "isHostKey was interrupted(will continue)");
                 }
              }
          }
      }else{
          _status = ST_ACCEPT ;
      }
      return _status == ST_ACCEPT ;

  }
  @Override
  public String getUser( ){
     _requestCounter = 0 ;
     System.out.println( "getUser : "+_remoteUser ) ;
     return _remoteUser ;
  }
  @Override
  public SshSharedKey  getSharedKey( InetAddress host ){ return null ; }

  @Override
  public SshAuthMethod getAuthMethod(){
      return _requestCounter++ > 2 ?
             null :
             new SshAuthPassword( _remotePassword ) ;
  }


}

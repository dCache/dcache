package dmg.protocols.snmp ;

import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.Choice;
import java.awt.Color;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.Label;
import java.awt.Menu;
import java.awt.MenuBar;
import java.awt.MenuItem;
import java.awt.Panel;
import java.awt.TextArea;
import java.awt.TextField;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      Snmp
       extends    Frame
       implements ActionListener, Runnable {

    private static final long serialVersionUID = 3331772305941422667L;
    private MenuBar         _menuBar ;
    private Menu            _editMenu , _fileMenu , _widthMenu ;
    private MenuItem        _editEditItem ;
    private MenuItem        _fileExitItem ;

    private TextField _hostText ;
    private TextField _communityText ;
    private TextField _portText ;
    private TextField _oidText ;
    private TextField _valueText ;
    private TextField _messageText ;
    private Choice    _typeChoice ;
    private TextArea  _outputText ;
    private Button    _nextButton ;
    private Button    _getButton ;
    private Button    _clearButton ;
    private Button    _walkButton ;
    private Button    _stopButton ;
    private Button    _resetButton ;
    private Button    _systemButton ;

    private Thread         _receiverThread , _senderThread ;
    private DatagramSocket _socket;

    private boolean        _isNext;
    private int            _requestId = 100 ;

    private SnmpOID     _argOID ;
    private InetAddress _argHost ;
    private int         _argPort ;
    private SnmpOctetString _argCommunity ;

    private int         _walk;

    private int         _id;
    private int         _special;
    private final Object      _sendLock    = new Object() ;
    private final Object      _receiveLock = new Object() ;
    private SnmpRequest _request;
    private SnmpRequest _result;
    private boolean     _sending;

    public Snmp( String [] args ){
      String frameName = args.length < 1 ? "Snmp" : args[0] ;
      setTitle( frameName ) ;
      installMenu();


      _hostText      = new TextField("oho") ;
      _portText      = new TextField("161" ) ;
      _communityText = new TextField("public") ;
      _oidText       = new TextField("1.3.6.1.2.1.1.1" ) ;
      _oidText.setBackground( Color.orange ) ;
      _valueText       = new TextField("" ) ;
      _valueText.setBackground( Color.blue ) ;
      _valueText.setForeground( Color.yellow ) ;
      _messageText     = new TextField("") ;
      _messageText.setForeground(Color.red);
      _typeChoice      = new Choice() ;
      _typeChoice.add( "TimeTicks" ) ;
      _typeChoice.add( "OctetString" ) ;
      _typeChoice.add( "Integer" ) ;
      _typeChoice.add( "IpNumber" ) ;
      _typeChoice.add( "OID" ) ;
      _outputText    = new TextArea( 15 , 100 ) ;
      _outputText.setFont( new Font("Monospaced" , Font.BOLD , 12 ) ) ;
      _nextButton    = new Button( "Next" ) ;
      _getButton     = new Button( "Get" ) ;
      _clearButton    = new Button( "Clear" ) ;
      _walkButton     = new Button( "Walk" ) ;
      _stopButton     = new Button( "Stop" ) ;
      _resetButton     = new Button( "Reset OID" ) ;
      _systemButton     = new Button( "System" ) ;
      _nextButton.addActionListener( this ) ;
      _getButton.addActionListener( this ) ;
      _clearButton.addActionListener( this ) ;
      _walkButton.addActionListener( this ) ;
      _stopButton.addActionListener( this ) ;
      _resetButton.addActionListener( this ) ;
      _systemButton.addActionListener( this ) ;

      setLayout( new BorderLayout(0,1) ) ;

      Panel buttonPanel = new Panel( new GridLayout(0,1) ) ;
      buttonPanel.add( _clearButton ) ;
      buttonPanel.add( _resetButton ) ;
      buttonPanel.add( _getButton ) ;
      buttonPanel.add( _nextButton ) ;
      buttonPanel.add( _walkButton ) ;
      buttonPanel.add( _stopButton ) ;
      buttonPanel.add( _systemButton ) ;

      Panel actionPanel = new Panel( new GridLayout(0,3) ) ;
      add( actionPanel , "North" ) ;
      add( _outputText , "Center" ) ;
      add( buttonPanel , "West" ) ;
      add( _messageText , "South" ) ;

      Label hostLabel      = new Label("Host") ;
      Label portLabel      = new Label("Port") ;
      Label communityLabel = new Label("Community");
      Label oidLabel       = new Label("ObjectIdentifier");
      Label valueLabel     = new Label("ObjectValue");
      Label typeLabel      = new Label("ObjectType");

      actionPanel.add( hostLabel ) ;
      actionPanel.add( portLabel ) ;
      actionPanel.add( communityLabel ) ;
      actionPanel.add( _hostText ) ;
      actionPanel.add( _portText ) ;
      actionPanel.add( _communityText ) ;

      actionPanel.add( oidLabel ) ;
      actionPanel.add( valueLabel ) ;
      actionPanel.add( typeLabel ) ;
      actionPanel.add( _oidText ) ;
      actionPanel.add( _valueText ) ;
      actionPanel.add( _typeChoice ) ;


      pack();
      setVisible(true);


      try{
        _socket = new DatagramSocket() ;
      }catch( SocketException e ){}
      _receiverThread = new Thread(this);
      _receiverThread.start();
      _senderThread = new Thread(this);
      _senderThread.start();

 }
 private void installMenu(){
      _menuBar      = new MenuBar() ;

      _fileMenu     = new Menu( "File" ) ;

      _fileMenu.add( _fileExitItem = new MenuItem( "Exit" ) );
      _fileExitItem.addActionListener( this ) ;
      _fileExitItem.setActionCommand( "exit" ) ;

      _editMenu     = new Menu( "Edit" ) ;

      _editMenu.add( _editEditItem = new MenuItem( "Edit Topology" ) );
      _editEditItem.addActionListener( this ) ;
      _editEditItem.setActionCommand( "edit" ) ;

      _widthMenu     = new Menu( "Options" ) ;

      _menuBar.add( _fileMenu ) ;
//      _menuBar.add( _editMenu ) ;
      _menuBar.add( _widthMenu ) ;
      setMenuBar( _menuBar ) ;

 }
 public String _cut( String in ){
    int x = in.lastIndexOf('.') ;
    if( x  < 0 ) {
        return in;
    }
    if( x == (in.length()-1) ) {
        return in;
    }
    String s = in.substring(x+1) ;
    if( ! s.startsWith("Snmp") ) {
        return s;
    }
    return s.substring(4) ;
 }
 @Override
 public void actionPerformed( ActionEvent event ){
      String s      = event.getActionCommand() ;
      _messageText.setText("");
      _special  = 0 ;
     switch (s) {
     case "exit":
         System.exit(0);
     case "System":
         _isNext = false;
         _special = 1;
         _walk = 0;
         _next();
         break;
     case "Get":
         _isNext = false;
         _walk = 0;
         _next();
         break;
     case "Next":
         _isNext = true;
         _walk = 0;
         _next();
         break;
     case "Walk":
         _isNext = true;
         _walk = 100000;
         _next();

         break;
     case "Stop":
         stopSending();
         break;
     case "Reset OID":
         _oidText.setText("1.3.6.1.2.1.1.1.0");
         break;
     case "Clear":
         _outputText.setText("");

         break;
     }
 }
 @Override
 public void run(){
   if( Thread.currentThread() == _senderThread ){
     while(true){
       synchronized( _sendLock ){
         while(!_sending) {
             try {
                 _sendLock.wait();
             } catch (InterruptedException e) {
             }
         }
       }
       say("Transmission initialized\n");
       while(true){  // just to do internal walk
          byte []        b      = _request.getSnmpBytes() ;
          SnmpRequest    result;
          DatagramPacket packet = new DatagramPacket(
                                    b , b.length , _argHost , _argPort ) ;
          synchronized( _receiveLock ){
             _result = null ;
             _id     = _request.getRequestID().intValue() ;
             for( int i = 0 ; (i<3) && ( _result == null ) ; i++ ){
                try{
                  say("Sending ..." + packet + '\n');
                  _socket.send( packet ) ;
                  say("Send Ready; Waiting\n");
                  _receiveLock.wait(3000) ;
                  say("Wait Ready\n");
                }catch(Exception iei){
                   esay("Exception in send : "+iei ) ;
                }
                if( ! _sending ) {
                    break;
                }
             }
             if( ( _result == null ) || ( ! _sending ) ){
                _id = 0 ;
                snmpAnswer( null ) ;
                break;
             }
             result  = _result ;
             _result = null ;
             _id     = 0 ;
          }
          _request = snmpAnswer( result ) ;
          if( _request == null ) {
              break;
          }
       }
       _sending = false ;
     }
   }else if( Thread.currentThread() == _receiverThread ){
      while( true ){
        try{
           byte [] b = new byte[2048] ;
           DatagramPacket p =  new DatagramPacket( b , b.length ) ;
           _socket.receive( p ) ;
           synchronized( _receiveLock ){
              say("Data received\n");
              if( _id == 0 ) {
                  continue;
              }
              SnmpObject snmp = SnmpObject.generate(
                                    p.getData(),0,
                                    p.getLength());

              SnmpRequest request = new SnmpRequest( snmp ) ;
//              say( "Request\n"+request ) ;
              if( _id != request.getRequestID().intValue() ) {
                  continue;
              }
              _result = request ;
              _receiveLock.notifyAll() ;
           }
        }catch( Exception ee ){
           esay("Exception \n" + ee) ;
        }
      }
   }
 }
       static final String [] __sysAll = {
           "System Description" ,
           "System Object ID  " ,
           "System Uptime     " ,
           "System Contact    " ,
           "System Name       " ,
           "System Location   " ,
           "System Services   "    } ;
  private SnmpRequest snmpAnswer( SnmpRequest request ){
    if( request == null ) {
        return null;
    }
    SnmpOID    oid   = request.varBindOIDAt(0) ;
    SnmpObject value = request.varBindValueAt(0);
    String     cls   = _cut( value.getClass().getName() ) ;

    int errorStatus = request.getErrorStatus().intValue() ;
    if( errorStatus != 0 ){
       _outputText.append(oid + ": Error status = " + errorStatus +
                          " Error index = " +
                          request.getErrorIndex().intValue() + '\n') ;
       return null ;
    }
    if( _special == 0 ){
       _typeChoice.select( cls ) ;
       _valueText.setText( value.toString() ) ;
       _outputText.append(oid + ": (" + cls + ") :" + value + '\n') ;
       _oidText.setText( oid.toString() ) ;
    }else if( _special == 1 ){
       if( request.varBindListSize() < 7 ){
          _outputText.setText(
               "not enought system infos received : "+
               request.varBindListSize()  ) ;
          return null ;
       }

       StringBuilder sb = new StringBuilder() ;
       for( int  i= 0 ; i < 7 ; i++ ) {
           sb.append(__sysAll[i]).append(" : ")
                   .append(request.varBindValueAt(i)).append('\n');
       }

       _outputText.setText( sb.toString() ) ;
       return null ;

    }

     int type = _isNext ? SnmpObjectHeader.GetNextRequest :
                          SnmpObjectHeader.GetRequest ;
     request =
         new SnmpRequest(
                _argCommunity ,
                new SnmpInteger( _requestId++ ) ,
                type ) ;
     request.addVarBind( oid , new SnmpNull() ) ;
     _walk-- ;
//    say( request.toString() ) ;
    return _walk > 0 ? request : null  ;
 }
 private boolean _getArguments(){
   try{
      _argOID  = new SnmpOID( _oidText.getText() ) ;
      _argHost = InetAddress.getByName( _hostText.getText() ) ;
      _argPort = Integer.parseInt(_portText.getText());
      _argCommunity = new SnmpOctetString( _communityText.getText() ) ;
      return true ;
   }catch( Exception ie ){
      _outputText.append( "Exception : "+ie ) ;
      return false ;
   }
 }
 private boolean sendRequest( SnmpRequest request ){
   synchronized( _sendLock ){
     if( _sending ) {
         return false;
     }
     _sending = true ;
     _request = request ;
     say("Notifying sender\n");
     _sendLock.notifyAll() ;
   }
   return true ;
 }
 private void stopSending(){
    synchronized( _sendLock ){
       if( ! _sending ) {
           return;
       }
       _sending = false ;
       _sendLock.notifyAll();
    }
 }
 private void _next(){
     say("Preparing Request\n") ;
     if( ! _getArguments() ){
        esay( "Argument error" ) ;
        return ;
     }
     say("Host : " + _argHost + '\n') ;
     say("Port : " + _argPort + '\n') ;
     say("OID  : " + _argOID + '\n') ;
     say("Community  : " + _argCommunity + '\n') ;
     int type = _isNext ? SnmpObjectHeader.GetNextRequest :
                          SnmpObjectHeader.GetRequest ;
     SnmpRequest request ;
     if( _special == 1 ){
        request =
           new SnmpRequest(
                   _argCommunity ,
                   new SnmpInteger( _requestId++ ) ,
                   SnmpObjectHeader.GetRequest     ) ;
       request.addVarBind( new SnmpOID("1.3.6.1.2.1.1.1.0") , new SnmpNull() ) ;
       request.addVarBind( new SnmpOID("1.3.6.1.2.1.1.2.0") , new SnmpNull() ) ;
       request.addVarBind( new SnmpOID("1.3.6.1.2.1.1.3.0") , new SnmpNull() ) ;
       request.addVarBind( new SnmpOID("1.3.6.1.2.1.1.4.0") , new SnmpNull() ) ;
       request.addVarBind( new SnmpOID("1.3.6.1.2.1.1.5.0") , new SnmpNull() ) ;
       request.addVarBind( new SnmpOID("1.3.6.1.2.1.1.6.0") , new SnmpNull() ) ;
       request.addVarBind( new SnmpOID("1.3.6.1.2.1.1.7.0") , new SnmpNull() ) ;

     }else{
        request =
           new SnmpRequest(
                   _argCommunity ,
                   new SnmpInteger( _requestId++ ) ,
                   type ) ;
        request.addVarBind( _argOID , new SnmpNull() ) ;
     }
//     say( "Sending\n"+request.toString() ) ;
     if( ! sendRequest( request ) ){
       _messageText.setText( "Send Action still Locked" ) ;
     }

 }
 private void say( String text ){
      System.out.print( text ) ;
 }
 private void esay( String text ){
      _messageText.setText( text ) ;
 }
 public static void main( String [] args ){


      new Snmp( args ) ;

 }

}

package dmg.cells.applets ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.net.* ;
import java.io.*;
import dmg.util.* ;
import dmg.protocols.ssh.* ;

public class      SshClientIoBinaryPanel
       extends    SshClientActionPanel 
       implements ActionListener, Runnable {
       
     private TextArea       _output ;
     private TextField      _input ;
     private Button         _psButton ;
     private Button         _exitButton ;
     private Button         _clearButton ;

     private BufferedReader     _reader ;
     private PrintWriter        _writer ;
     private ObjectOutputStream _objOut ;
     private ObjectInputStream  _objIn ;
     private Thread             _printerThread  ;
  
     private boolean        _isActive = false ;
       
     SshClientIoBinaryPanel( ){
     
        setLayout( new BorderLayout() ) ;

        _output = new TextArea( 10 , 10 ) ;
        add( _output , "Center" ) ;

        _input = new TextField(80 ) ;
        add( _input , "South" ) ;

        Panel buttonPanel  = new Panel( new GridLayout(1,0) ) ;

        buttonPanel.add( _psButton      = new Button( "Process List" ) ) ;
        buttonPanel.add( _exitButton    = new Button( "Exit" ) ) ;
        buttonPanel.add( _clearButton   = new Button( "ClearScreen" ) ) ;

        _psButton.addActionListener(this );
        _exitButton.addActionListener(this );
        _clearButton.addActionListener(this );
        _input.addActionListener(this );

        add( buttonPanel , "North" ) ;
        
     }
     public synchronized void startIO( 
              InputStream in , OutputStream out ,
              Reader reader  , Writer writer             ){
              
        if( _isActive )return ;
        _writer = new PrintWriter( writer ) ;
        _writer.println( "$BINARY$" ) ;
        _writer.flush() ;
        _reader = new BufferedReader( reader ) ;
        try{
           String checkString = _reader.readLine() ;
           if( checkString.equals( "$BINARY$" ) ){
              System.out.println( "Switch to binary ack." ) ;
           }else{
              System.err.println( "Swtich to binary failed" ) ;
           }
           _objOut = new ObjectOutputStream( out ) ;
           _objIn  = new ObjectInputStream( in ) ;
        }catch(Exception ee ){
            System.err.println( "Error : "+ee ) ;
        }
        _printerThread = new Thread( this ) ;
        _printerThread.start() ;
        _isActive  = true ;
        _input.setBackground( Color.red ) ;
     }
     public void run(){
        if( Thread.currentThread() == _printerThread ){
           try{
             Object obj ;
             while( ( obj = _objIn.readObject() ) != null ){
                _output.append( obj.toString() + "\n" ) ;          
             }
           }catch( Exception ioe ){
              ioe.printStackTrace() ;
           }
           _input.setBackground( Color.white ) ;
           _output.setText("");
           informActionListeners("ioFinished") ;
        }
        _isActive =false ;
     }
     public synchronized void actionPerformed( ActionEvent event ){

        String command = event.getActionCommand() ;
        System.out.println( "Action : "+command ) ;
        Object obj = event.getSource() ;

        if( obj == _psButton ){
          _writer.println( "ps -a" ) ; _writer.flush() ;
        }else if( obj == _exitButton ){
          _writer.println( "exit" ) ; _writer.flush() ;
        }else if( obj == _input ){
           try{
              _objOut.writeObject( _input.getText() ) ; 
              _objOut.flush() ;
           }catch( Exception ee ){
               System.err.println( "error : "+ee ) ;
           }
        }else if( obj == _clearButton ){
          _output.setText("") ;
        }
     }     
       
       
}
 

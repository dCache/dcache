package dmg.cells.applets ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;

import dmg.util.* ;
import dmg.cells.nucleus.* ;

public class      GuiCellApplet 
       extends    Applet 
       implements ActionListener, Runnable       {
       
  LogoCanvas _logo  ;
  int        _state = 0 ;
  int        _counter = 0 ;
  Thread     _timer   ;
  
  private  void engine( boolean force ){
       _counter -- ;
       showStatus( " Counter "+_counter+" state "+_state+" Mode "+force ) ;
       System.out.println( " Counter "+_counter+" state "+_state+" Mode "+force ) ;
       if( ( _counter <= 0 ) || force ){
          switch( _state ){
            case 0 : System.out.println( " Going into wait " ) ;
                     _logo.setString( "Wait" ) ;
                     _counter = 4 ;
                     _state   = 1 ;
                     System.out.println( " leaving wait " ) ;
            break ;                     
            case 1 : _logo.animation( LogoCanvas.SNOW ) ;
                     _counter = 20 ;
                     _state   = 2 ;
            break ;
            case 2 : _logo.animation( LogoCanvas.SHRINKING ) ;
                     _counter = 100000 ;
                     _state   = 3 ;
            break ; 
            case 3 : _logo.setString( "Connected" ) ;
                     _counter = 100000 ;
                     _state   = 4 ;
            break ;  
          }       
       }    
  
  }
  public void actionPerformed( ActionEvent event ){
     String command = event.getActionCommand() ;
     System.out.println( " Action : " + command ) ;
     engine(true);
     System.out.println( " Ready "  ) ;
     
  }
  public void run(){
    if( Thread.currentThread() == _timer ){
     while(true){
       try{ Thread.sleep(1000) ; }
       catch( Exception e ){}
       engine(false);
     }
    }
  }
  public void init(){
      Dimension   d  = getSize() ;
      setLayout( new FlowLayout() ) ;
      
      add( _logo = new LogoCanvas("Hallo") ) ;
      _logo.setSize(200,200);
      _logo.setActionListener( this ) ;
      setSize(200,200);
      setVisible( true ) ;
      _timer = new Thread( this ) ;
  }
  public void start(){
    System.out.println("start ..." ) ;
    _logo.animation( LogoCanvas.GROWING ) ;
    _state = 0 ;
    _counter = 2000000 ;
    _timer.start() ;
  }
  public void stop(){
  
  }
  public void destroy(){
  
  }      
       
}

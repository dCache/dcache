package dmg.apps.libraryServer ;

import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import dmg.util.* ;
import dmg.cells.applets.login.* ;

public class OverviewPanel extends SimpleBorderPanel implements Runnable{

   private LSProtocolHandler _control  = null ;
   private int      _sectionCount = -1 ;
   private Label [] _owner = null , _command = null , 
                    _detail = null , _name = null ;
   private Panel           _labelPanel = null ;
   private RowColumnLayout _rcl = null ;
   private Font            _headerFont    = new Font( "SansSerif" , 0 , 18 ) ;
   private String       [] _header = { "Name" , "Owner" , "Command" , "Detail" } ;
   private Thread          _workerThread = null ;
   public OverviewPanel( LSProtocolHandler control ){
      _control = control ;
      BorderLayout bl = new BorderLayout() ;
      bl.setVgap(10);
      bl.setHgap(10);
      setLayout(bl) ;
      
      _rcl = new RowColumnLayout(_header.length,RowColumnLayout.LAST) ;
      _rcl.setVgap(10);
      _rcl.setHgap(10);
      
      _workerThread = new Thread(this) ;
      _workerThread.start() ;
   }
   public void run(){
      while( ! Thread.interrupted() ){
          _refresh() ;
          try{
             Thread.sleep(5000) ;
          }catch(InterruptedException ie ){
             break ;
          }
      
      }
   }
//
//
   public synchronized void _refresh(){
      try{
         //
         // are we already online
         //
         if( _sectionCount < 0 ){
            _labelPanel = new Panel( _rcl ) ;
            int count = _control.getSectionCount() ;
            _name    = new Label[count] ;
            _owner   = new Label[count] ;
            _command = new Label[count] ;
            _detail  = new Label[count] ;
            for( int i = 0 ; i < _header.length ; i++ ){
               Label l = new  Label( _header[i] ) ;
               l.setFont( _headerFont ) ;
               l.setBackground( Color.blue ) ;
               _labelPanel.add( l ) ;
            }
            for( int i = 0 ; i < count ; i++ ){
               _labelPanel.add(_name[i]    = new Label("") ) ;
               _labelPanel.add(_owner[i]   = new Label("") ) ;
               _labelPanel.add(_command[i] = new Label("") ) ;
               _labelPanel.add(_detail[i]  = new Label("") ) ;
            }
            _sectionCount = count ;
            add( _labelPanel , "Center" ) ;
            validate() ;
         }
         for( int i = 0 ; i < _sectionCount ; i++ ){
            LSProtocolHandler.Section s = _control.getSection(i) ;
            char [] c = new char[1] ;
            c[0] = s.readSemaphore() ;
            _owner[i].setText( new String(c) ) ;
            _name[i].setText( s.getName() ) ;
            String command = s.getCommandString() ;
            Args args = new Args( command ) ;
            
         }
      }catch(Exception ee ){
         ee.printStackTrace() ;
      }
   }
   

}

package dmg.apps.osm ;


import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.applets.spy.* ;

public class      LibraryPanel 
       extends    Panel   
       implements FrameArrivable , Runnable {
       
     private DomainConnection _connection = null ;
     private Font    _headerFont = 
                       new Font( "SansSerif" ,Font.ITALIC , 24 )  ;
     private int     _basicFontHeight = 18 ;
     private Font    _basicFont = 
                       new Font( "SansSerif" ,Font.ITALIC , _basicFontHeight )  ;
     private Toolkit _toolkit = Toolkit.getDefaultToolkit() ;             
     private String  _destination = null ;
     private String  _command     = null ;
     private long    _last        = System.currentTimeMillis() ;
     private boolean _isOk        = false ;
     private BarCanvas  _barCanvas       = null ;
     private SHistogram _histogramCanvas = null ;
     private SDrives    _drives          = null ;
     private long       _update          = 10 ;
     private boolean    _dataReceived    = false ;
     private VectorHistory _queueHistory = null ;
     private Label _header , _numOfDrives ,
                   _numOfDisabled , _numOfDp , _queueSize ,
                   _queueDiff , _queueTotal ;
     public Insets getInsets(){ return new Insets( 10 , 10 ,10 , 10 ) ; }
     
     public LibraryPanel( String label , DomainConnection dc ){
         super( new BorderLayout() ) ;
         _connection  = dc ;
         _destination = label ;
         _command     = "get info" ;
          setFont( _basicFont ) ;

         _queueSize     = new Label( "QS=?" )  ; 
         _queueDiff     = new Label( "QD=?" )  ; 
         _queueTotal    = new Label( "QT=?" )  ; 
         _numOfDrives   = new Label( "D's=?" ) ; 
         _numOfDisabled = new Label( "Dis=?" )  ; 
         _numOfDp       = new Label( "DP=?" )  ;
      
         
         _header = new Label(label,Label.CENTER) ;
         _header.setFont(_headerFont);
         
         _drives = new SDrives()  ;
         _drives.setPreferredSize(new Dimension(30,30));
         
         SBorder drivePanel = new SBorder(_drives,"Drives",20) ;
          
         
         _barCanvas = new BarCanvas(_basicFontHeight+2) ;
         SBorder barPanel = new SBorder(_barCanvas,"Queue Size",20) ;
         
         
         _histogramCanvas = new SHistogram(new Dimension(100,100) ) ;
         _histogramCanvas.setForeground(new Color(150,150,150) ) ;
         _histogramCanvas.setTickColor(Color.red);
         _histogramCanvas.setXTick(3600) ;
         _histogramCanvas.setYTick(20) ;
         
         SBorder histPanel = new SBorder(_histogramCanvas,"Queue",20 ) ;
      
         
         _queueHistory = new VectorHistory(50) ;
         _queueHistory.setTick(50) ;
         _queueHistory.setTickColor(Color.red) ;
         _queueHistory.setForeground(new Color(150,150,150) ) ;
         
         SBorder historyPanel = new SBorder(_queueHistory,"History",20) ;
         
         Panel displayPanel = new Panel( new BorderLayout() ) ;
         
         displayPanel.add( barPanel     , "North" ) ;
         displayPanel.add( historyPanel , "Center" ) ;
         displayPanel.add( histPanel    , "West" ) ;
         displayPanel.add( drivePanel   , "South" ) ;
         
//         add( _header      , "North" ) ;
         add( new SBorder( displayPanel , label , 30 ) , "Center" ) ;
         
         new Thread(this).start() ;
         System.out.println( "Ping label started : "+label ) ;
     }
     private void setStatus( boolean ok ){
        if( ok  && ! _isOk ){
           setBackground( Color.green ) ;
        }else if( ! ok ){
           if( _isOk )setBackground( Color.red ) ;
        }
        _isOk = ok ;
        return ;
     }
     public void run(){
        while(true){
            synchronized( _connection ){
                setStatus( _dataReceived ) ;
                if( ! _dataReceived ) _queueHistory.addValue(0);
                _dataReceived = false ;
                _connection.send( _destination , _command , this ) ;
            }
	    try{ 
                long update  = _update * 1000 ;
                long rest    = update ;
                long start   = System.currentTimeMillis() ;
                while( rest > 0 ){ 
                   Thread.sleep( rest ) ;
                   rest = update - ( System.currentTimeMillis() - start ) ;             
                }
	    }catch(InterruptedException ie ){
	        break ;
	    }
	}      
     }
     public void frameArrived( MessageObjectFrame frame ){
         Object obj = frame.getObject() ;
         if( obj instanceof Exception){
            System.out.println( "Exception arrived : "+obj.toString() ) ;
            return ;
         }
         _last = System.currentTimeMillis() ;
         if( obj instanceof LibraryInfo ){
            LibraryInfo info = (LibraryInfo)obj ;
            int update = info.getUpdate() ;
            if( update > 0 )_update = update ;
            try{
               displayLibraryInfo( info ) ;
               _dataReceived = true ;
            }catch(Exception ee ){
               ee.printStackTrace() ;
            }
         }
     }
     private LibraryInfo _lastInfo = null ;
     private void displayLibraryInfo( LibraryInfo info ){
         DriveInfo [] drives = info.getDrives() ;
         QueueInfo queue = info.getQueueInfo() ;
         boolean isEqual = false ;
         if( _lastInfo != null ){
            DriveInfo [] last = _lastInfo.getDrives() ;
            if( last.length == drives.length ){
               isEqual = true ;
               for( int i = 0 ; i < drives.length ; i++ ){
                  if( ! drives[i].equals(last[i] ) ){
                     isEqual = false ;
                     break ;
                  }
               }
            }           
         }
         if( ! isEqual ){
            int dis = 0 ;
            int dp  = 0 ;
            for( int i = 0 ; i < drives.length ; i++ ){
               if( drives[i].getStatus().equals("DP") )dp++ ;
               if( drives[i].getStatus().indexOf("D") > -1 )dis++ ;
            }
            _numOfDrives.setText("D's="+drives.length ) ;
            _numOfDisabled.setText("DIS="+dis ) ;
            _numOfDp.setText("DP="+dp ) ;
            _drives.setDriveInfos( drives ) ;
         }
         int queueSize = queue.getQueueSize() ;
         _queueHistory.addValue(queueSize);
         _queueSize.setText( "QSize = "+queueSize ) ;
         _queueSize.setForeground(queueSize>50?Color.red:Color.black) ;
         int seconds = queue.getQueueStart();
         _queueDiff.setText( "QStart = "+toHours(seconds) ) ;
         _queueDiff.setForeground(seconds>3600?Color.red:Color.black) ;
         seconds = queue.getQueueEnd() ;
         _queueTotal.setText( "QEnd="+toHours(seconds)) ;
         _queueTotal.setForeground(seconds>3600?Color.red:Color.black) ;
         if( queueSize < 100 ){
            _barCanvas.setMarker(false) ;
            _barCanvas.setOutfit( queueSize , 100 , 10 ) ; 
         }else{
            _barCanvas.setMarker(true) ;
            _barCanvas.setOutfit( queueSize , 500 , 100 ) ; 
         }
         int [] hist = queue.getHistogram() ;
         if( ( hist == null ) || ( hist.length < 2 ) || ( hist[0] != 1 ) ){
            System.err.println( "Histogram is empty" ) ;
         }else{
            _histogramCanvas.setBinWidth( hist[1] ) ;
            _histogramCanvas.setValues( hist , 2 , hist.length - 2 ) ;
            int min = ( hist[1] * (hist.length-2) ) / 60 ;
            _histogramCanvas.setLabel(""+min);
         }
         _lastInfo = info ;
     }
     private String toHours( int t ){
        int min = t / 60 ;
        int hour = min / 60 ;
        min = min % 60 ;
        return ""+hour+":"+(min < 10 ? ( "0"+min ) : ""+min) ;
     }

}
 
 

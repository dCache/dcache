package dmg.apps.osm ;

import java.io.* ;
import java.util.* ;

public class MapStatus {
/* for the mode */
   private static final int MS_PUT   = 1 ;
   private static final int MS_GET   = 2 ;
   private static final int MS_IDLE  = 3;

/* for the xState */
   private static final int MS_UNDEF   = 0;
   private static final int MS_OPEN    = 1;
   private static final int MS_LOCATE  = 2;
   private static final int MS_READ    = 3;
   private static final int MS_WRITE   = 4;
   private static final int MS_WRFMK   = 5;
   private static final int MS_CLOSE   = 6;
   private static final int MS_CLACK   = 7;
   private static final int MS_WRDONE  = 8;
   private static final int MS_RDDONE  = 9;
   private static final int MS_MOUNT   = 10;
   private static final int MS_UNMOUNT  =  11;
   private static final int MS_LOAD    = 12;
   private static final int MS_RLABEL  = 13;
   private static final int MS_WLABEL  = 14;
   private static final int MS_LOPEN   = 15;
   private static final int MS_LCLOSE  = 16;
   private static final int MS_UNLOAD  = 17;
   private static final int MS_EMPTY   = 18;

   private String _device = null ;
   private String _volume = null ;
   private int  _mode = 0 ;
   private int  _tapeState  = 0 ,  _tapeIoBytes  = 0 ;
   private long _tapeIoTime = 0 ,  _tapeLastTime = 0 ;
   private int  _netState   = 0 ,  _netIoBytes   = 0 ;
   private long _netIoTime  = 0 ,  _netLastTime  = 0 ;
   private int  _connectTime = 0 , _openTime     = 0 , _locateTime = 0 ;
   private int  _mountTime  = 0 ,  _umountTime   = 0 , _shortNetReads = 0 ;
   
   public String toString(){
      StringBuffer sb = new StringBuffer() ;
      sb.append( "Device = " ).append(_device).append("\n") ;
      sb.append( "Volume = " ).append(_volume).append("\n") ;
      sb.append( "Mode = " ).append(modeToString(_mode)).append("\n");
      sb.append( "Tape : state=").append(stateToString(_tapeState)).
         append( ";ioBytes=").append(_tapeIoBytes).
         append( ";ioTime=").append(_tapeIoTime).
         append( ";lastTime=").append(_tapeLastTime).append("\n") ;
      sb.append( "Net  : state=").append(stateToString(_netState)).
         append( ";ioBytes=").append(_netIoBytes).
         append( ";ioTime=").append(_netIoTime).
         append( ";lastTime=").append(_netLastTime).append("\n") ;
      return sb.toString() ;
   }
   public MapStatus( DataInputStream data ) throws IOException {
   
       byte [] x = new byte[128] ;
       data.read( x , 0 , 128 ) ;
       _device = byteToString(x) ;
       data.read( x , 0 , 16 ) ;
       _volume = byteToString(x) ;
       _mode   = data.readInt() ;
       
       _tapeState    = data.readInt() ;
       _tapeIoBytes  = data.readInt() ;
       _tapeIoTime   = data.readLong() ;
       _tapeLastTime = data.readLong() ;
       
       _netState    = data.readInt() ;
       _netIoBytes  = data.readInt() ;
       _netIoTime   = data.readLong() ;
       _netLastTime = data.readLong() ;
       
       _connectTime = data.readInt() ;
       _openTime    = data.readInt() ;
       _locateTime  = data.readInt() ;
       _mountTime   = data.readInt() ;
       _umountTime  = data.readInt() ;
       
       _shortNetReads = data.readInt() ;
   }
   private String byteToString( byte [] data ){
      int i=  0 ;
      for( i = 0 ; 
           ( i < data.length ) &&
           ( data[i] != 0    )   ; i++ ) ; 
      return new String( data , 0 , i  ) ;
   
   }
   private String modeToString( int mode ){
      return mode==MS_PUT?"PUT":
             mode==MS_GET?"GET":
             mode==MS_IDLE?"IDLE":"?";
   }
   private String stateToString( int state ){
     String x = null ;
     switch( state ){
       case MS_UNDEF : x = "Undefined" ; break ;
       case MS_OPEN : x = "Open" ; break ;
       case MS_LOCATE : x = "Locate" ; break ;
       case MS_READ : x = "Read" ; break ;
       case MS_WRITE : x = "Write" ; break ;
       case MS_WRFMK : x = "WriteFMK" ; break ;
       case MS_CLOSE : x = "Close" ; break ;
       case MS_CLACK : x = "CloseACK" ; break ;
       case MS_WRDONE : x = "WriteDone" ; break ;
       case MS_RDDONE : x = "ReadDone" ; break ;
       case MS_MOUNT : x = "Mount" ; break ;
       case MS_UNMOUNT : x = "Unmount" ; break ;
       case  MS_LOAD : x = "Load" ; break ;
       case MS_RLABEL : x = "ReadLabel" ; break ;
       case MS_WLABEL : x = "WriteLabel" ; break ;
       case MS_LOPEN : x = "LOpen" ; break ;
       case MS_LCLOSE  : x = "LClose" ; break ;
       case MS_UNLOAD  : x = "Unload" ; break ;
       case MS_EMPTY  : x = "Empty" ; break ;
       default : x = "Unknown" ; break ;
     }
     return x+"("+state+")" ;
   }
   public static void main( String [] args )throws Exception {
     if( args.length < 1 ){
         System.err.println( "Usage : ... <mapFileName>" ) ;
         System.exit(4) ;
     }
     File f = new File( args[0] ) ;
     DataInputStream data = 
         new DataInputStream(
              new FileInputStream( f ) ) ;
     MapStatus map = new MapStatus( data ) ;
     System.out.println( map.toString() ) ;
     data.close() ;
   
   
   }
}

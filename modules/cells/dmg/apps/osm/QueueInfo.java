package dmg.apps.osm ;

import java.util.* ;
import java.io.* ;
import java.text.* ;


public class QueueInfo implements Serializable {

    private int _length     = 0 , 
                _queueStart = 0 , 
                _queueEnd   = 0  ;
    private int [] _histogram = null ;            
    QueueInfo( int queueLength , int queueStart , int queueEnd ){
       _length     = queueLength ;
       _queueStart = queueStart ;
       _queueEnd   = queueEnd ;
    }
    void setHistogram( int [] histogram ){
       _histogram = histogram ;
    }
    public int [] getHistogram(){ return _histogram ; }
    public int getQueueSize(){ return _length ; }
    public int getQueueStart(){ return _queueStart; }
    public int getQueueEnd(){ return _queueEnd ; }
    public String toString(){
      StringBuffer sb = new StringBuffer() ;
      sb.append("q="+_length).
         append(";s="+_queueStart).
         append(";a="+_queueEnd) ;
      if( ( _histogram != null ) && ( _histogram.length > 1 ) ){
         sb.append(";h={(").append(_histogram[1]).append(")") ;
         for( int i = 2 ; i < _histogram.length ; i++ ){
            sb.append( _histogram[i] ).append(",") ;
         }
         sb.append( "}" ) ;
      }
      return sb.toString() ;
    }
}

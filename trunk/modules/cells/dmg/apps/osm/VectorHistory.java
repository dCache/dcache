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

public class VectorHistory extends Canvas {
   private int     _marks  = 10 ;
   private int     _preferredHeight = 10 ;
   private boolean _marker        = false ;
   private Color   _markerColor   = null ;
   private Font    _basicFont = null   ;
   private int     _mode      = ALL ;
   private static final int ALL      = 0 ;
   private static final int OneAdded = 1; 
   private int     _latestWidth = 1 ;
   private int     _currentCount = 0 ;
   private int []  _history      = new int[_latestWidth] ;
   private int     _autoMax      = 0 ;
   private Random  _r = new Random() ;
   private int     _maxValue     = 0 ;
   
   public VectorHistory( int preferredHeight ){
   
      _preferredHeight = preferredHeight ;
      addMouseListener( new MouseAction() ) ;
      _basicFont = new Font( "SansSerif" ,
                             Font.ITALIC , 
                             _preferredHeight-6 )  ;
                             
     /*
      _history = new int[300] ;
      _currentCount = 200 ;
      for( int i = 0 ; i < _currentCount ; i++ ){
         byte [] x = new byte[1] ;
         _r.nextBytes(x) ;
         _history[i] = x[0] < 0 ? (-x[0]) : x[0] ;
      }
      calculateMax() ;
      System.out.println( "autoMax : "+_autoMax ) ;
      Thread x = new Thread(
           new Runnable(){
            public void run(){
              while(true){ 
                 byte [] xx = new byte[1] ;
                 _r.nextBytes(xx) ;
                 addValue(xx[0] < 0 ? (-xx[0]) : xx[0]) ;
                 try{
                    Thread.sleep(1000) ;
                 }catch(Exception e ){ break ; }
              }
            }
           }
      ) ;
      x.start() ;
      */
   }
   public class MouseAction extends MouseAdapter {
     
         public void mouseClicked( MouseEvent me ){
         }
   }
   public void setTickColor( Color markerColor ){
      _markerColor = markerColor ;
   }
   public void setTick( int tick ){ _marks = tick ; }
   public void setMax( int maxValue ){
      _maxValue = maxValue ;
   }
   private boolean calculateMax(){
      if( _maxValue > 0 ){ _autoMax = _maxValue ; return false ; }
      int autoMax = 0 ;
      for( int i = 0 ; i < _currentCount ; i++ )
          autoMax = autoMax >= _history[i] ? autoMax : _history[i] ;
      boolean grow = autoMax > _autoMax ;
      _autoMax = autoMax + 2 ;
      return grow ;
   }
   public synchronized void addValue( int value ){
      boolean total = false ;
      if( _currentCount >= _history.length ){
          shiftAll() ;
          total = true ;
      }
      _history[_currentCount++] = value ;
      total = calculateMax() || total ;
      _mode = total ? ALL : OneAdded ;
      repaint() ;
   }
   private void shiftAll(){
      int newCount = _currentCount / 2 ;
      System.arraycopy(_history,_history.length-newCount,
                       _history,0,newCount ) ;
      _currentCount = newCount ;
      calculateMax() ;
   }
   public void paint( Graphics g ){
      _mode = ALL ;
      update(g) ;
   }
   public synchronized void update( Graphics g ){
      Dimension d = getSize() ;
      int mode    = _mode ;
      int height  = d.height ;
      if( ( d.width <= 5 ) || ( d.height <= 5 ) )return ;
      if( _latestWidth != d.width ){
         int [] x = new int[d.width] ;
         mode = ALL ;
         if( _latestWidth < d.width ){
            System.arraycopy(_history,0,x,0,_currentCount) ;
         }else if( _latestWidth > d.width ){
            if( _currentCount <= d.width ){
               System.arraycopy(_history,0,x,0,_currentCount) ;
            }else{
               System.arraycopy(_history,_currentCount-d.width,x,0,d.width) ;
               _currentCount = d.width ;
               calculateMax() ;
            }
         }
         _latestWidth = d.width ;
         _history = x ;
      }
      if( mode == ALL ){
         g.setColor(getBackground()) ;
         g.fillRect(0,0,d.width,d.height) ;
      }
      g.setColor(getForeground()) ;
      if( _autoMax == 0 )return ;
      for( int i = 0 ; i < _currentCount ; i++ ){
         int v = _history[i]  * height / _autoMax ;
         g.drawLine(i,d.height-1,i,d.height-v-1) ;
      }
      g.setColor(_markerColor==null?getForeground():_markerColor) ;
      int v = _marks  * height / _autoMax ;
      for( int p = v ; p < height ; p+= v )
         g.drawLine( 0 , d.height-p-1  , d.width-1 , d.height-p-1 ) ; 
      
   }
   public void setMarker( boolean marker ){ _marker = marker ; }
   public Dimension getPreferredSize(){
      return new Dimension( 100 , _preferredHeight ) ;
   }
} 
 

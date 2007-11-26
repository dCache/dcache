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

public class SHistogram extends Canvas {
   private int     _xMarks  = 10 ;
   private int     _yMarks  = 10 ;
   private Dimension _preferredSize = new Dimension(100,100) ;
   private boolean _marker        = false ;
   private Color   _markerColor   = null ;
   private int     _autoMax       = 0 ;
   private int  [] _values        = new int[0] ;
   private Random  _r = new Random() ;
   private int     _maxValue     = 0 ;
   private int     _binWidth     = 1 ;
   private String  _label        = null ;
   private Font    _basicFont    = new Font( "SansSerif" ,
                                       Font.ITALIC , 
                                       18 )  ;
   
   public SHistogram( Dimension preferredSize ){
   
      _preferredSize = preferredSize ;
      addMouseListener( new MouseAction() ) ;
//      _basicFont = new Font( "SansSerif" ,
//                             Font.ITALIC , 
//                             _preferredSize.heigth-6 )  ;
//      
   }
   public SHistogram( Dimension preferredSize , boolean sim ){
     this( preferredSize ) ; 
      Thread t = new Thread(
           new Runnable(){
            public void run(){
              while(true){ 
                 int count = _r.nextInt() % 20 ;
                 count = count < 0 ? (-count) : count ;
                 int [] values = new int[count] ;                      
                 for( int i = 0 ; i < values.length ; i++ ){
                     byte [] x = new byte[count] ;
                     _r.nextBytes(x) ;
                     values[i] = x[i] < 0 ? (-x[i]) : x[i] ;
                 }
                 setValues(values);
                 try{
                    Thread.sleep(1000) ;
                 }catch(Exception e ){ break ; }
              }
            }
           }
      ) ;
      t.start() ;
      
   }
   public class MouseAction extends MouseAdapter {
     
         public void mouseClicked( MouseEvent me ){
         }
   }
   public void setBinWidth( int binWidth ){ _binWidth = binWidth ; }
   public void setTickColor( Color markerColor ){
      _markerColor = markerColor ;
   }
   public void setXTick( int tick ){ _xMarks = tick ; }
   public void setYTick( int tick ){ _yMarks = tick ; }
   public void setMax( int maxValue ){
      _maxValue = maxValue ;
   }
   public synchronized void setValues( int [] values ){
      setValues( values , 0 , values.length ) ;
   }
   public synchronized void setValues( int [] values , int off , int size ){
      int [] _newValue = new int[size] ;
      int     autoMax  = 0 ;
      boolean modified = false ;
      if( _newValue.length == _values.length ){
         for( int i = 0 ; i < size ; i++ ){
            _newValue[i] = values[off+i] ;
            modified = modified || (_newValue[i] != _values[i] );
            autoMax  = Math.max( autoMax , _newValue[i] ) ;
         }
      }else{
         modified = true ;
         for( int i = 0 ; i < size ; i++ ){
            _newValue[i] = values[i+off] ;
            autoMax = Math.max( autoMax , _newValue[i] ) ;
         }
      }
      if( modified ){
         _values  = _newValue ;
         _autoMax = autoMax ;
         repaint() ;
      }
   }
   public void setLabel( String label ){
      _label = label ;
   }
   public void paint( Graphics g ){
      Dimension d = getSize() ;
      g.setColor(getBackground()) ;
      g.fillRect(0,0,d.width,d.height) ;
      int height  = d.height ;
      int width   = d.width ;
      if( ( height <= 5 ) || ( width <= 5 ) )return ;
      if( ( _autoMax == 0 ) || ( _values.length == 0 ) )return ;
      g.setColor(getForeground()) ;
      
      int tick = d.width / _values.length ;
      for( int i = 0 ; i < _values.length ; i++ ){
         int v = _values[i]  * height / _autoMax ;
         g.fillRect(i*tick,d.height-v,tick,v) ;
      }
      
      
      g.setColor(_markerColor==null?getForeground():_markerColor) ;
      
      g.drawLine(0,d.height-1,d.width-1,d.height-1) ;
      g.drawLine(0,0,0,d.height-1) ;
      
      int v = _yMarks  * d.height / _autoMax ;
      for( int p = v ; p < height ; p+= v )
         g.drawLine( 0 , d.height-p-1  , d.width-1 , d.height-p-1 ) ;
       
      v = _xMarks * d.width / ( _values.length * _binWidth ) ;
      for( int p = v ; p < width ; p+= v )
         g.drawLine( p , 0  , p , d.height-1 ) ; 
         
      if( _label != null ){
         setFont( _basicFont ) ;
         FontMetrics fm = g.getFontMetrics() ;
         int w = fm.stringWidth(_label) ;
         int h = fm.getAscent()+fm.getDescent() ;
         g.drawString(_label,(d.width-w)/2,d.height/2);
      }
   }
   public void setMarker( boolean marker ){ _marker = marker ; }
   public Dimension getPreferredSize(){
      return _preferredSize ;
   }
} 
 

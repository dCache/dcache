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

public class SStatusCanvas extends Canvas implements Runnable {

   private Dimension _preferredSize = new Dimension(100,100) ;
   private Random    _r = new Random() ;
   private int       _mode = 0 ;
   private int       _maxModes = 3 ;
   private int       _pos      = 0 ;
   private int       _seq      = 0 ;
   private boolean   _sim  = true ;
   private Rectangle [] _recs = null ;
   public static final int CONNECTING = 0 ;
   public static final int WAITING    = 1 ;
   public static final int CONNECTED  = 2 ;
   
   public SStatusCanvas(){
      setPreferredSize( new Dimension(100,30) ) ; 
      addMouseListener( new MouseAction() ) ;
      new Thread(this).start() ;
   }
   public SStatusCanvas( boolean sim ){
      super() ;
      _sim = sim ;      
   }
   public void setPreferredSize( Dimension d ){
     _preferredSize = d ;
   }
   public void run(){
      while( true ){
      
         if( _recs != null ){
           _pos = ( _pos + 1 ) % _recs.length ;
           _seq = ( _seq + 1 ) % 2 ;
           repaint() ;
         }
         try{
            Thread.sleep(1000) ;
         }catch(InterruptedException ee ){
            break ;
         }
      }
   
   }
   public class MouseAction extends MouseAdapter {
     
         public void mouseClicked( MouseEvent me ){
            if( _sim ){
              _mode = ( _mode + 1 ) % _maxModes ;
//              System.out.println("Mode -> "+_mode);
              repaint() ;
            }
         }
   }
   public synchronized void setMode( int mode ){
      if( _mode == mode )return ;
      _mode = mode ;
      repaint() ;
   }
   private int _bound = 5 ;
   public void paint( Graphics g ){
      Dimension d = getSize() ;
      g.setColor(getBackground()) ;
      if( ( d.height <= 0 ) || ( d.width <= 0 ) )return ;
      g.fillRect(0,0,d.width,d.height) ;
      //
      // 
      int q = d.height - 2 * _bound ;
      if( q <= 0 )return ;
      int n = d.width / 2 / q ;
      int off = ( d.width - 2 * n * q + q ) / 2 ;
      _recs = new Rectangle[n] ;
      for( int i = 0 ; i < _recs.length ; i++ ){
         _recs[i] = 
             new Rectangle( off , _bound , q , q ) ;
         off += 2 * q ;
      }
      update( g , d ) ;
      
   }
   public void update( Graphics g ){
       update( g , getSize() ) ;
   }
   public void update( Graphics g , Dimension d ){
      if( _recs == null )return ;
      if( _mode == CONNECTING ){
         g.setColor(getForeground()) ;
         for( int i = 0 ; i < _recs.length ; i++ ){
            g.fillRect(_recs[i].x,_recs[i].y,
                       _recs[i].width,_recs[i].height) ;
         }
         g.setColor(Color.red);
         g.fillRect(_recs[_pos].x,_recs[_pos].y,
                    _recs[_pos].width,_recs[_pos].height) ;
      }else if( _mode == CONNECTED ){
         g.setColor(getForeground()) ;
         for( int i = 0 ; i < _recs.length ; i++ ){
            g.fillRect(_recs[i].x,_recs[i].y,
                       _recs[i].width,_recs[i].height) ;
         }
         g.setColor(Color.red);
         g.fillRect(_recs[_pos].x,_recs[_pos].y,
                    _recs[_pos].width,_recs[_pos].height) ;
      
      }else if( _mode == WAITING ){
         g.setColor(getForeground()) ;
         int seq = _seq ;
         for( int i = seq % 2 ; i < _recs.length ; i+=2 )
            g.fillRect(_recs[i].x,_recs[i].y,
                       _recs[i].width,_recs[i].height) ;
         
         g.setColor(Color.red);
         for( int i = ( seq + 1 )% 2 ; i < _recs.length ; i+=2 )
         g.fillRect(_recs[i].x,_recs[i].y,
                    _recs[i].width,_recs[i].height) ;
      }
      
   }
   public Dimension getPreferredSize(){
      return _preferredSize ;
   }
} 
 

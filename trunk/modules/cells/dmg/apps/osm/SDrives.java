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

public class SDrives extends Canvas {

   private Dimension _preferredSize = new Dimension(100,100) ;
   private Random    _r            = new Random() ;
   private Rectangle [] _recs      = null ;
   private int       [] _driveInfo = null ;
   private DriveInfo [] _driveDetail = null ;
   private int          _detailed  = -1 ;   
   private Color        _gray      = new Color(150,150,150) ;  
   private Font    _basicFont    = new Font( "SansSerif" ,
                                       Font.ITALIC , 
                                       14 )  ;
   public SDrives(){
      setPreferredSize( new Dimension(100,30) ) ; 
      addMouseListener( new MouseAction() ) ;
   }
   public void setPreferredSize( Dimension d ){
     _preferredSize = d ;
   }
   public class MouseAction extends MouseAdapter {
     
         public void mouseClicked( MouseEvent me ){
            if( _detailed != -1 ){ 
               _detailed = -1 ; 
            }else{
               Rectangle [] recs = _recs ;
               if( recs != null ){
                  Point p = me.getPoint() ;
                  int i = 0 ;
                  for( i = 0 ; 
                       ( i < recs.length ) && 
                       ( ! recs[i].contains(p) ) ; i++ );
                  if( i == recs.length )return ;
                  _detailed = i ;
               }
            }
            repaint() ;
         }
   }
   private final static int ENABLED  = 0 ;
   private final static int DISABLED = 1 ;
   private final static int DP       = 2 ;
   private final static int IS_OWNED = 4 ;
   private final static int HAS_TAPE = 8 ;
   
   public synchronized void setDriveInfos( DriveInfo [] info ){
      int [] driveInfo = new int[info.length] ;
      
      for( int i = 0  ; i < info.length ; i++ ){
         String s = info[i].getStatus() ;
         if( s.indexOf("P" ) > -1  )driveInfo[i] |= DP ;
         if( s.indexOf("D") > -1 )  driveInfo[i] |= DISABLED ;
         if( (( s = info[i].getTape() ) != null )  &&
             ! s.equals("-")   )  driveInfo[i] |= HAS_TAPE ;
         if( (( s = info[i].getOwner() ) != null ) &&
             ! s.equals("-")   )  driveInfo[i] |= IS_OWNED ;
      }
      _driveInfo   = driveInfo ;
      _driveDetail = info ;
      repaint() ;
   }
   private int _bound = 5 ;
   public synchronized void paint( Graphics g ){
      Dimension d = getSize() ;
      g.setColor(getBackground()) ;
      if( ( d.height <= 0 ) || ( d.width <= 0 ) )return ;
      if( _driveInfo == null )return ;
      g.fillRect(0,0,d.width,d.height) ;
      //
      //      
      int q = d.height - 2 * _bound ;
      if( q <= 0 )return ;
      int n   = _driveInfo.length ;
      int del = d.width / n - q ;     
      int off = ( d.width - ( q + del ) * n  + del ) / 2 ;
      _recs   = new Rectangle[n] ;     
      for( int i = 0 ; i < _recs.length ; i++ ){
         _recs[i] = new Rectangle( off , _bound , q , q ) ;
         off += ( q + del ) ;
      }
      update( g , d ) ;
      
   }
   public void update( Graphics g ){
//       update( g , getSize() ) ;
       paint( g ) ;
   }
   public void update( Graphics g , Dimension d ){
      if( _recs == null )return ;
      if( ( _detailed > - 1 ) && ( _detailed < _recs.length ) ){
         g.setFont( _basicFont ) ;
         DriveInfo info = _driveDetail[_detailed] ;
         String label = info.getDriveName()+"  "+
                        info.getStatus()+"  "+
                        info.getTape()+"  "+
                        info.getOwner() ;
         FontMetrics fm = g.getFontMetrics() ;
         int w = fm.stringWidth(label) ;
         g.setColor(Color.blue);
         g.drawString(label,
                      (d.width-w)/2,
                      (d.height+fm.getAscent()-fm.getDescent())/2);
      
      }else{
         for( int i = 0 ; i < _recs.length ; i++ ){
             g.setColor( ( (_driveInfo[i]&DP)       > 0 ) ? Color.red :
                         ( (_driveInfo[i]&DISABLED) > 0 ) ? _gray :
                         Color.white ) ;
             g.fillRect(_recs[i].x,
                        _recs[i].y,
                        _recs[i].width,
                        _recs[i].height) ;
             g.setColor( Color.blue ) ;
             if( (_driveInfo[i]&IS_OWNED) > 0 ){
                g.drawRect(_recs[i].x,
                           _recs[i].y,
                           _recs[i].width-1,
                           _recs[i].height-1) ;
             }
             if( (_driveInfo[i]&HAS_TAPE) > 0 ){
                g.fillRect(_recs[i].x+_recs[i].width/4,
                           _recs[i].y+_recs[i].height/4,
                           _recs[i].width/2,
                           _recs[i].height/2) ;
             }

         } 
      }     
   }
   public Dimension getPreferredSize(){
      return _preferredSize ;
   }
} 
 

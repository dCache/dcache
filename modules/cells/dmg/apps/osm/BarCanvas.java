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

public class BarCanvas extends Canvas {
   private int _max    = 100 ;
   private int _cursor = 50 ;
   private int _marks  = 10 ;
   private Color _frame = new Color(10,10,10) ;
   private Color _foreground = new Color(130,130,130) ;
   private Color _background = new Color(190,190,190) ;
   private Color _bars       = new Color(255,255,255) ;
   private int   _preferredHeight = 10 ;
   private boolean _marker     = false ;
   private boolean _drawNumber = true ;
   private Font    _basicFont = null   ;
   public BarCanvas( int preferredHeight ){
      _preferredHeight = preferredHeight ;
         addMouseListener( new MouseAction() ) ;
      _basicFont = new Font( "SansSerif" ,
                             Font.ITALIC , 
                             _preferredHeight-6 )  ;
   }
   public class MouseAction extends MouseAdapter {
     
         public void mouseClicked( MouseEvent me ){
            _drawNumber = ! _drawNumber ;
            repaint() ;
         }
   }
   public void paint( Graphics g ){
       int bound = 2 ;
       Dimension d = getSize() ;
       g.setColor(_marker?Color.red:_frame) ;
       g.fillRect(0,0,d.width-1,d.height-1);
       Rectangle r = new Rectangle(bound,bound,
                                       d.width-2*bound,
                                       d.height-2*bound);
       g.setColor(_background);
       g.fillRect(r.x,r.y,r.width-1,r.height-1) ;
       g.setColor(_foreground);
       int width = _cursor * (r.width-1) / _max  ;
       g.fillRect(r.x,r.y,width,r.height-1) ;
       g.setColor(_marker?Color.red:_bars);
       int markPosition = 0 ;
       for( int i = _marks ; i < _max ; i+= _marks ){
          markPosition = i * r.width / _max ;
          g.drawLine( r.x+markPosition , r.y , r.x+markPosition , r.height ) ;
       }
       if( _drawNumber ){
         g.setFont(_basicFont) ;
         g.setColor(Color.red) ;
         FontMetrics fm = g.getFontMetrics() ;
         String num = ""+_cursor ;
         width = fm.stringWidth(num) ;
         int height = fm.getAscent()+fm.getDescent() ;
         height= (int) ( ((float)(r.height*fm.getAscent())) /
                             ((float)height) ) ;
         g.drawString(num,
                      r.x+markPosition+bound,r.y+height);
       }
   }
   public void setMarker( boolean marker ){ _marker = marker ; }
   public Dimension getPreferredSize(){
      return new Dimension( 100 , _preferredHeight ) ;
   }
   public void setOutfit( int cursor , int max , int marks ){
      if( ( _cursor == cursor ) &&
          ( _max    == max    ) &&
          ( _marks  == marks  )    )return ;
      _cursor = cursor ;
      _marks  = marks ;
      _max    = max ;
      repaint() ;
   
   }
} 

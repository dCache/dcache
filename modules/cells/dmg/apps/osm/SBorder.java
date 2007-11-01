package dmg.apps.osm  ;


import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

public class SBorder extends Panel {
   private int    _border = 15 ;
   private String _title  = "" ;
   private Font   _basicFont = new Font( "SansSerif" ,
                                0 , 
                                _border-6 )  ;
   public SBorder( Component c , String title , int border ){
      super( new BorderLayout() ) ;
      _border    = border ;
      _title     = title ;
      _basicFont = new Font( "SansSerif" ,0 , _border-6 )  ;
      add( c , "Center" ) ;
   }

   public Insets getInsets(){ 
       return new Insets( _border,_border,_border,_border ) ;
   }
   public void paint( Graphics g ){
       Dimension d = getSize() ;
       g.setFont(_basicFont);
       FontMetrics fm = g.getFontMetrics() ;
       int offset = ( _border - (fm.getAscent()+fm.getDescent()))/2 ;
       offset = Math.max( 0 , offset ) ;
       g.drawString(_title,_border,offset+fm.getAscent());
       int o = _border/2 ;
       int w = fm.stringWidth(_title) ;
       g.drawLine(o,o,_border-3,o) ;
       g.drawLine(_border+w+3,o,d.width-1-o,o) ;
       g.drawLine(d.width-1-o,o,d.width-1-o,d.height-1-o) ;
       g.drawLine(d.width-1-o,d.height-1-o,o,d.height-1-o) ;
       g.drawLine(o,o,o,d.height-1-o) ;
   }

}
 

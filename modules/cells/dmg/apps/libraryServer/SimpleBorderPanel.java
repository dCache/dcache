package dmg.apps.libraryServer ;

import java.awt.* ;

public class SimpleBorderPanel extends Panel {
   private int   _b     = 5 ;
   private Color _color = Color.black ;
   public SimpleBorderPanel(){}
   public SimpleBorderPanel( LayoutManager lo ){ super(lo) ; }
   public SimpleBorderPanel( LayoutManager lo , int borderSize){ 
      super(lo) ; 
      _b = borderSize ;
   }
   public SimpleBorderPanel( LayoutManager lo , int borderSize , Color color ){ 
      super(lo) ; 
      _b = borderSize ;
      _color = color ;
   }
   public Insets getInsets(){ return new Insets(_b , _b ,_b , _b) ; }
   public void setBorderSize( int border ){ _b = border ; }
   public void setBorderColor( Color color ){ _color = color ; }
   public void paint( Graphics g ){

      Dimension   d    = getSize() ;
      Color base = getBackground() ;
      g.setColor( _color ) ;
      g.drawRect( _b/2 , _b/2 , d.width-_b , d.height-_b ) ;
   }


}

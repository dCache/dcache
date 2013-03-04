package dmg.cells.applets.login ;

import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Insets;
import java.awt.LayoutManager;
import java.io.Serializable;

public class KeyValueLayout implements LayoutManager, Serializable {

    public static final int HORIZONTAL 	= 1;
    public static final int VERTICAL 	= 2;
    public static final int BOTH 	= 3;
    private static final long serialVersionUID = 1409674306718003476L;
    private int _vGap = 10 , _hGap = 10 ;
    private boolean _fitsAllSizes = true ;
    int align;
    public KeyValueLayout() {
	this(BOTH);
    }
    public KeyValueLayout(int align) {
	this.align = align;
    }
    public int getAlignment() {
	return align;
    }
    public void setAlignment(int align) {
	this.align = align;
    }
    public void setFitsAllSizes( boolean fits ){ _fitsAllSizes = fits ; }
    public void setHgap( int hGap ){ _hGap = hGap ; }
    public void setVgap( int vGap ){ _vGap = vGap ; }
    @Override
    public void addLayoutComponent(String name, Component comp) {
    }
    @Override
    public void removeLayoutComponent(Component comp) {
    }
    @Override
    public Dimension preferredLayoutSize(Container target) {
       return minimumLayoutSize( target ) ;
    }
    @Override
    public Dimension minimumLayoutSize(Container target) {
      synchronized (target.getTreeLock()) {
         Dimension []    m = getMinimumDimensions(target) ;
         Insets    insets  = target.getInsets();
         int     maxwidth  = m[0].width  + m[1].width + _hGap
                             - (insets.left + insets.right );
         int     maxheight = m[0].height + m[1].height + _vGap
                             - (insets.top  + insets.bottom );
//         int     maxwidth  = m[0].width  + m[1].width  + _hGap +
//                             insets.left + insets.right  ;
//         int     maxheight = m[0].height + m[1].height + _vGap +
//                             insets.top  + insets.bottom ;

//         if( _fitsAllSizes ){
//           Dimension d = target.getSize() ;
//           maxwidth = d.width ;
//         }
         return new Dimension( maxwidth , maxheight ) ;
      }
    }
    private Dimension [] getMinimumDimensions(Container target ){
       synchronized (target.getTreeLock()) {
 	  int components = target.getComponentCount() / 2 * 2 ;
          if( components == 0 ) {
              return new Dimension[0];
          }
          Dimension d1 = new Dimension() ;
          Dimension d2 = new Dimension() ;
          for( int i = 0 ; i < components ; i+=2 ){
             Component c1  = target.getComponent(i) ;
             Component c2  = target.getComponent(i+1) ;
             Dimension dd1 = c1.getMinimumSize() ;
             Dimension dd2 = c2.getMinimumSize() ;

             d1.width  = Math.max( d1.width , dd1.width ) ;
             d1.height = Math.max( d1.height , dd1.height ) ;
             d2.width  = Math.max( d2.width , dd2.width ) ;
             d2.height = Math.max( d2.height , dd2.height ) ;

          }
          d1.height = Math.max( d1.height , d2.height ) ;
          d2.height = d1.height ;
          Dimension [] dd = new Dimension[2] ;
          dd[0] = d1 ;
          dd[1] = d2 ;
//          System.out.println( "Minimum : "+d1+";"+d2 ) ;
          return dd ;
       }
    }
    @Override
    public void layoutContainer(Container target) {
//      System.out.println( "layoutContainer" ) ;
      synchronized (target.getTreeLock()) {
	Insets    insets  = target.getInsets();
        Dimension t_dim   = target.getSize() ;
	int     maxwidth  = t_dim.width  - (insets.left + insets.right );
	int     maxheight = t_dim.height - (insets.top  + insets.bottom );
	int components = target.getComponentCount();
        if( components < 1 ) {
            return;
        }
        Dimension [] dd = getMinimumDimensions(target );
        components = components / 2 * 2 ;
        int x , y , width , height ;
        for( int i = 0 ; i < components/2 ; i ++ ){
           Component m  = target.getComponent(2*i) ;
	   if (m.isVisible()) {
              m.validate() ;
              m.setSize( dd[0].width , dd[0].height );
              m.setLocation( insets.left  ,
                             insets.top + i * ( dd[0].height + _vGap ) ) ;
           }
           m  = target.getComponent(2*i+1) ;
	   if (m.isVisible()) {
              m.validate() ;
              if( _fitsAllSizes ){
                 Dimension d = target.getSize() ;
                 int w = d.width - ( insets.left + insets.right ) -
                        _hGap - dd[0].width  ;
                 m.setSize( w , dd[1].height );
              }else{
                 m.setSize( dd[1].width , dd[1].height );
              }
              m.setLocation( insets.left + dd[0].width + _hGap ,
                             insets.top + i * ( dd[0].height + _vGap ) ) ;
           }
        }

      }
    }

    public String toString() {
	return getClass().getName() ;
    }
}

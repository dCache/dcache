package dmg.cells.applets.login ;
import java.awt.* ;

public class CenterLayout implements LayoutManager, java.io.Serializable {

    public static final int HORIZONTAL 	= 1;
    public static final int VERTICAL 	= 2;
    public static final int BOTH 	= 3;

    int align;
    public CenterLayout() {
	this(BOTH);
    }
    public CenterLayout(int align) {
	this.align = align;
    }
    public int getAlignment() {
	return align;
    }
    public void setAlignment(int align) {
	this.align = align;
    }
    @Override
    public void addLayoutComponent(String name, Component comp) {
    }
    @Override
    public void removeLayoutComponent(Component comp) {
    }
    @Override
    public Dimension preferredLayoutSize(Container target) {
      synchronized (target.getTreeLock()) {
	int nmembers = target.getComponentCount();
        if( nmembers < 1 ) {
            return target.getSize();
        }
	Component m = target.getComponent(0);
	Dimension dim = m.getMinimumSize();
	return dim  ;

      }
    }
    @Override
    public Dimension minimumLayoutSize(Container target) {
      synchronized (target.getTreeLock()) {
	int nmembers = target.getComponentCount();
        if( nmembers < 1 ) {
            return target.getSize();
        }
	Component m = target.getComponent(0);
	return m.getMinimumSize() ;

      }
    }

    @Override
    public void layoutContainer(Container target) {
      synchronized (target.getTreeLock()) {
	Insets insets = target.getInsets();
        Dimension t_dim = target.getSize() ;
	int maxwidth  = t_dim.width  - (insets.left + insets.right );
	int maxheight = t_dim.height - (insets.top + insets.bottom );
	int nmembers = target.getComponentCount();

        if( nmembers < 1 ) {
            return;
        }
        
	Component m = target.getComponent(0);
	Dimension d = m.getMinimumSize();
        
        m.setSize(d.width , d.height);
	m.setLocation( ( t_dim.width - d.width ) / 2 ,
                       ( t_dim.height - d.height ) / 2  ) ;
        
      }
    }  
    /*
    public void layoutContainer(Container target) {
      synchronized (target.getTreeLock()) {
	Insets insets = target.getInsets();
        Dimension t_dim = target.getSize() ;
	int maxwidth  = t_dim.width  - (insets.left + insets.right );
	int maxheight = t_dim.height - (insets.top + insets.bottom );
	int nmembers = target.getComponentCount();

        if( nmembers < 1 )return ;
        
	Component m = target.getComponent(0);
        
	if (m.isVisible()) {
            m.validate() ;
//            System.out.println( "m="+m);
            Dimension d ;
            d = m.getMinimumSize();
//            System.out.println( "=minW="+d.width+";minH"+d.height);
            d = m.getMaximumSize();
//            System.out.println( "=maxW="+d.width+";maxH"+d.height);
            d = m.getSize();
//            System.out.println( "=sizeW="+d.width+";sizeH"+d.height);
            d = m.getPreferredSize();
//            System.out.println( "=preW="+d.width+";preH"+d.height);
            if( ( d.width == 0 ) || ( d.height == 0 ) ){
               
               d = m.getMinimumSize();
               System.out.println( "Sorry, no preferred size using min : "+d ) ;
            }
//            System.out.println( "t_dim : "+t_dim ) ;
//            d.width  = d.width == 0 ? t_dim.width : Math.min( d.width , t_dim.width ) ;
//            d.height = d.height == 0 ? t_dim.height : Math.min( d.height , t_dim.height ) ;
            int width , height , x , y ;
            if( align == HORIZONTAL ){
               width  = maxwidth ;
               height = d.height ;
               x = 0 ;
               y = ( maxheight - d.height ) / 2 ;
            }else{
               width  = d.width ;
               height = d.height ;
               x = ( maxwidth  - d.width  ) / 2 ;
               y = ( maxheight - d.height ) / 2 ;
            }
//            System.out.println("x="+x+";y="+y+";w="+width+";h="+height) ;
            m.setSize( width , height ); 
            m.setLocation( x , y ) ;
        }
        
        if( nmembers < 2 )return ;

	m = target.getComponent(1);
        
	if (m.isVisible()) {
            Dimension d = m.getPreferredSize();
            d.width = Math.min( d.width , t_dim.width ) ;
            d.height = Math.min( d.height , t_dim.height ) ;
            m.setSize(maxwidth, d.height);
	    m.setLocation( 0 , maxheight - d.height  ) ;
        }
        
      }
    }
    */
    public String toString() {
	String str = "";
	switch (align) {
	  case HORIZONTAL: str = ",align=horizontal"; break;
	  case VERTICAL:   str = ",align=vertical"; break;
	  case BOTH:       str = ",align=both"; break;
	}
	return getClass().getName() + "["+str + "]";
    }
}

package dmg.util.graphics ;

import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;


public class    TreeNodePanel 
       extends  Panel 
       implements ComponentListener, AdjustmentListener {

    private Scrollbar  _right;
    private Scrollbar  _bottom;
    private TreeCanvas _tree;
    public TreeNodePanel(){
       setLayout( new ScrollLayout() ) ;
       _right  = new Scrollbar( Scrollbar.VERTICAL ,
                               0 , 50 , 0 , 101     ) ;
       _bottom = new Scrollbar( Scrollbar.HORIZONTAL ,
                               0 , 50 , 0 , 101     ) ;

       _tree = new TreeCanvas() ;
       add( _bottom  ) ;
       add( _right   ) ;
       add( _tree ) ;
       
       _tree.addComponentListener(this) ;
       _bottom.addAdjustmentListener(this);
       _right.addAdjustmentListener(this);
       _tree.setOffset( new Point( 20 , 20  ) ) ;

    }
    @Override
    public void adjustmentValueChanged( AdjustmentEvent event ){
       System.out.println( "Adjustment event : "+event ) ;
       if( event.getSource() == _right ){
//           _tree.setOffset( new Point( 10, - event.getValue()  ) ) ;
            _tree.setLocation( new Point( 10 , - event.getValue() ) ) ;
       }   
    }
    @Override
    public void componentMoved( ComponentEvent event ){
       System.out.println( "ComponentEvent : "+event ) ;
    } 
    @Override
    public void componentHidden( ComponentEvent event ){
       System.out.println( "ComponentEvent : "+event ) ;
    } 
    @Override
    public void componentShown( ComponentEvent event ){
       System.out.println( "ComponentEvent : "+event ) ;
    } 
    @Override
    public void componentResized( ComponentEvent event ){
       System.out.println( "ComponentEvent : "+event ) ;
       Component c = event.getComponent() ;
       Dimension ourDim = getSize() ;
       Dimension botDim = _bottom.getSize() ;
       Dimension rigDim = _right.getSize() ;
       Dimension viewPort = 
              new Dimension( ourDim.width  - rigDim.width ,
                             ourDim.height - botDim.height ) ;
       
       Dimension dim = c.getSize() ;
       
       int vis = ourDim.height ;
       _right.setVisibleAmount( vis ) ;
       _right.setMaximum( dim.height - vis ) ;
       System.out.println( " dim : "+vis+ "  "+dim.width ) ;
       
    } 
    
    public void setTree( TreeNodeable node ){
        _tree.setTree( node ) ;
    }
    @Override
    public Dimension getPreferredSize(){ return new Dimension(300,300) ; }
    @Override
    public Dimension getSize(){ return getPreferredSize() ; }
    //
    // our layout manager
    //
    public class ScrollLayout implements LayoutManager {
        private int _scrollWidth = 10 ;
        public ScrollLayout() {
        }
        @Override
        public void addLayoutComponent(String name, Component comp) {
        }
        @Override
        public void removeLayoutComponent(Component comp) {
        }
        @Override
        public Dimension preferredLayoutSize(Container target) {
    //      System.out.println( "Caclulating preferredLayoutSize" ) ;
          synchronized (target.getTreeLock()) {
	    int nmembers = target.getComponentCount();
            if( nmembers < 3 ) {
                return target.getSize();
            }
            Dimension dim = target.getComponent(2).getSize() ;
            return new Dimension( dim.width + _scrollWidth ,
                                  dim.height + _scrollWidth ) ;
          }
        }
        @Override
        public Dimension minimumLayoutSize(Container target) {
          return preferredLayoutSize( target ) ;
        }

        @Override
        public void layoutContainer(Container target) {
           synchronized (target.getTreeLock()) {

	      int nmembers = target.getComponentCount();
              if( nmembers < 3 ) {
                  return;
              }
              Dimension dim = target.getSize() ;
              Component c = target.getComponent(0) ;
              c.setLocation( 0 , dim.height - _scrollWidth ) ;
              c.setSize( dim.width - _scrollWidth , _scrollWidth ) ;
              
              c = target.getComponent(1) ;
              c.setLocation( dim.width - _scrollWidth , 0 ) ;
              c.setSize( _scrollWidth , dim.height - _scrollWidth ) ;
              
              c = target.getComponent(2) ;
              c.setLocation( 0 , 0 ) ;
              c.setSize( dim.width - _scrollWidth , dim.height - _scrollWidth ) ;
               
              
           }
        }

    }

} 

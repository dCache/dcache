package dmg.util.graphics ;
import java.awt.* ;

public class TableLayout implements LayoutManager, java.io.Serializable {

    private int _columns = 1 ;
    private int _vGap;
    private int _hGap;
    public TableLayout( int columns ) {
        
        _columns = columns ;
    }
    public void setHgap( int gap ){ _hGap = gap ; }
    public void setVgap( int gap ){ _vGap = gap ; }
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
        if( nmembers < 1 ) {
            return target.getSize();
        }
        int    rows   = nmembers / _columns + 1 ;
        int [] width  = new int[_columns] ;
        int [] height = new int[rows] ;
        for( int i = 0 ; i < nmembers ; i++ ){
	   Component m   = target.getComponent(i);
           Dimension dim = m.getPreferredSize();
           int column  = i % _columns ;
           int row     = i / _columns ;
//           System.out.println( "Sizeof["+i+"]="+dim ) ;
           width[column] = Math.max( width[column] , dim.width ) ; 
           height[row]   = Math.max( height[row]   , dim.height ) ;
        }
        int preHeight = 0 ;
        for( int i= 0 ;i < height.length ; i++ ){
           preHeight += height[i] ;
//           System.out.println( "heigth["+i+"]="+height[i]) ;
        }
        int preWidth = 0 ;
        for( int i = 0 ; i < width.length ; i++ ){
//           System.out.println( "width["+i+"]="+width[i]) ;
           preWidth += width[i] ;
        }
        Insets insets = target.getInsets() ;
        Dimension dd = 
           new Dimension( preWidth + 
                          insets.right + insets.left + 
                          _columns*_hGap,
                          preHeight + 
                          insets.top + insets.bottom + 
                          rows*_vGap)   ;
//        System.out.println( "Preferred Dim : "+dd ) ;
	return dd   ;

      }
    }
    @Override
    public Dimension minimumLayoutSize(Container target) {
      return preferredLayoutSize( target ) ;
    }

    @Override
    public void layoutContainer(Container target) {
      Insets insets = target.getInsets() ;
      int [] widthSum;
      int [] heightSum;
      int [] width;
      int [] height;
      synchronized (target.getTreeLock()) {
        
	int nmembers = target.getComponentCount();
        if( nmembers < 1 ) {
            return;
        }
        int    rows   = nmembers / _columns + 1 ;
        width  = new int[_columns] ;
        height = new int[rows] ;
        for( int i = 0 ; i < nmembers ; i++ ){
	   Component m   = target.getComponent(i);
           Dimension dim = m.getPreferredSize();
           int column  = i % _columns ;
           int row     = i / _columns ;
//           System.out.println( "Sizeof["+i+"]="+dim ) ;
           width[column] = Math.max( width[column] , dim.width ) ; 
           height[row]   = Math.max( height[row]   , dim.height ) ;
        }
        widthSum  = new int[_columns] ;
        heightSum = new int[rows] ;
        
        heightSum[0] = insets.top ;
        for( int i= 1 ;i < height.length ; i++ ) {
            heightSum[i] = heightSum[i - 1] + height[i - 1] + _vGap;
        }
        
        widthSum[0] = insets.left ;
        for( int i = 1 ; i < width.length ; i++ ) {
            widthSum[i] = widthSum[i - 1] + width[i - 1] + _hGap;
        }
        
        for( int i = 0 ; i < nmembers ; i++ ){
	   Component m   = target.getComponent(i);
	   if (m.isVisible()) {
               m.validate() ;
               int row    = i / _columns ;
               int column = i % _columns ;
               m.setSize( width[column] , height[row] ); 
//               System.out.println( "size["+i+"]="+width[column]+":"+height[row] ) ;
               m.setLocation( widthSum[column] , heightSum[row] ) ;
           }
        }
      }
      target.setSize(
          widthSum[widthSum.length-1]+width[width.length-1]+insets.right,
          heightSum[heightSum.length-1]+height[height.length-1]+insets.bottom);
    }
    
    public String toString() {
	String str = "";
	return getClass().getName() + "["+str + "]";
    }
}

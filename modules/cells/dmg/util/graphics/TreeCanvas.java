package dmg.util.graphics ;

import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;

public class TreeCanvas extends  Canvas implements MouseListener {

     private TreeNodeable _tree = null , _currentTree = null ;
     private Toolkit      _toolkit = Toolkit.getDefaultToolkit() ;
     private Font   _bigFont = 
             new Font( "SansSerif" , Font.BOLD , 10 )  ; 
     private Font   _font = null ;
     private FontMetrics _fontMetrics = null ;    
     private int  _height = 0 , _width = 0 , _move = 0 , _descent , _ascent ;
     private Vector _recs = null ;
     public TreeCanvas(){
        _font = _bigFont ;
        _fontMetrics = getFontMetrics( _font ) ;
        _ascent =  _fontMetrics.getAscent() ;
        _descent = _fontMetrics.getDescent() ;
        _height = _ascent + _descent  ;
        _width  = _fontMetrics.getMaxAdvance() ;
        _move   = (_ascent-_descent)/2 ;
        addMouseListener(this);
     }
     private void _deselectAll(){
       TreeNodeable tree = _tree ;
       if( tree == null )return ;
       _deselect( tree ) ;
     }
     private void _deselect( TreeNodeable tree ){
        TreeNodeable sub = null ;
        if( tree == null )return ;
        for( TreeNodeable c = tree ; c != null ; c = c.getNext() ){
           c.setSelected(false);
           if( ( ! c.isFolded() ) &&
               ( sub = c.getSub() ) != null )_deselect( sub ) ;        
        }
        
     }
     public void mouseClicked( MouseEvent event ){
         if( event.isControlDown() ){ 
            //
            // reset everything
            //
            _currentTree = _tree ; 
            _deselectAll() ;
            repaint() ;
            return  ;
         }
         Point       p = event.getPoint() ;
         Enumeration e = _recs.elements() ;
         while( e.hasMoreElements() ){
            RecFrame frame = (RecFrame)e.nextElement() ;
            if( frame.rectangle.contains(p) ){
                if( frame.sw ){
                   if( event.isShiftDown() ){
                      _currentTree = frame.node ;
                   }else{
                      frame.node.switchFold() ;
                   }
                }else{
                   if( ! event.isShiftDown() )_deselectAll() ;
                   frame.node.setSelected(true) ;
                }
                break ;
            }
         }
//         if( parent instanceof ScrollPane ){
//            Point pp = ((ScrollPane)parent).getScrollPosition() ;
//            System.out.println( "Is scroll pane position "+pp ) ;
//            repaint() ;
//            ((ScrollPane)parent).doLayout() ;
//            ((ScrollPane)parent).setScrollPosition(pp) ;
//         }else{
            repaint() ;
//         }
     }
     public void mouseExited( MouseEvent event ){
     
     }
     public void mouseReleased( MouseEvent event ){
     
     }
     public void mousePressed( MouseEvent event ){
     
     }
     public void mouseEntered( MouseEvent event ){
     
     }
//     public Dimension getPreferredSize(){
//        return new Dimension(300,300) ;
//     }
     public void setTree( TreeNodeable node ){
        _currentTree = _tree = node ;
        repaint() ;
     }
     private int _b = 10 ;
     private Point _offset = null ;
     public void setOffset( Point offset ){
       _offset = offset ;
       repaint();
     }
     public void paint( Graphics g ){
        Dimension   d    = getSize() ;
        System.out.println( " new size : "+d ) ;
        Color base = getBackground() ;
        g.setColor( Color.blue ) ;
        g.setFont( _font ) ;
        if( _currentTree != null ){
            _recs = new Vector() ;
            Point p = null ;
            if( _offset == null ){
               p = new Point( 10 , 10 ) ;
            }else{
               p = _offset  ;
            }
            Dimension z = drawNode( g , p , _currentTree ) ;
            setSize( z.width + p.x  , z.height + p.y ) ;
        }


     }
     private class RecFrame {
        public Rectangle rectangle ;
        public TreeNodeable node ;
        public boolean      sw = false ;
        public RecFrame( Rectangle rec , TreeNodeable node ){
           this.rectangle = rec ;
           this.node      = node ;
        }
        public RecFrame( Rectangle rec , TreeNodeable node , boolean sw ){
           this.rectangle = rec ;
           this.node      = node ;
           this.sw        = sw ;
        }
     }
     private Dimension drawNode( Graphics g , Point offIn , TreeNodeable node ){
        Point     off  = new Point( offIn ) ;
        Rectangle rec  = null ;
        Dimension area = new Dimension( 0 , 0 ) ;
        Point     m    = null ;
        while( node != null ){
        
           Point n = new Point( off ) ;
           n.translate( 0 , 2 * _height ) ;
           g.drawLine( off.x , off.y , n.x , n.y ) ;
           m = new Point( n ) ;
           m.translate( 1 * _width , 0 ) ;
           g.drawLine( n.x , n.y , m.x , m.y ) ;
           m.translate( _width , 0 ) ;
           if( node.isSelected() ){
              Color x = g.getColor() ;
              g.setColor( Color.red ) ;
              g.drawString( node.getName() , m.x , m.y + _move ) ;
              g.setColor( x ) ;
           }else{
              g.drawString( node.getName() , m.x , m.y + _move ) ;
           }
           int nameLength = _fontMetrics.stringWidth(node.getName()) ;
           area.width = Math.max( area.width , nameLength+ 2 * _width ) ;
           rec = 
                new Rectangle(
                         m.x ,
                         m.y + _move - _ascent , 
                         nameLength,
                         _height
                    ) ;
           _recs.addElement(  new RecFrame( rec  , node ) ) ;
           
           m.translate( _width , _height ) ;
           Point k = new Point(n) ;
           if( node.isContainerNode() ){
              rec = 
                  new Rectangle( n.x - _height / 4 ,
                                 n.y - _height / 4 ,
                                 _height / 2, _height / 2  ) ;
              g.fillRect( rec.x , rec.y , rec.width , rec.height  ) ;
              _recs.addElement( new RecFrame ( rec , node , true ) ) ;
              if( ! node.isFolded() ){
                 TreeNodeable sub = node.getSub() ;
                 if( sub != null ){
                    Dimension z = drawNode( g , m , node.getSub() ) ;
                    k.translate( 0 , z.height + 2 * _height) ;
                    area.width = Math.max( area.width , m.x + z.width ) ;
                 }
              }
           }
           off = k ;
           if( ( node = node.getNext() ) != null )
               g.drawLine( n.x , n.y , k.x , k.y ) ;
        }
        area.height = off.y - offIn.y + _height ;
        return area ;
     }


}

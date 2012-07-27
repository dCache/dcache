// $Id: MovingPigs.java,v 1.5 2006-12-15 13:23:37 tigran Exp $
//
package dmg.cells.services.gui ;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.util.*;
import java.io.*;

public class      MovingPigs
       extends    JPanel
       implements MouseListener, MouseMotionListener {

    private  HashMap   _list = new HashMap() ;
    private  Item      _cursor;
    private  Point     _offset = new Point() ;
    private  String   _currentFontType = "Times" ;
    private  int      _currentFontMode = Font.BOLD | Font.ITALIC ;
    private  int      _currentFontSize = 16 ;
    private  Font      _font   = new Font( "Times" , Font.BOLD | Font.ITALIC , 16 ) ;
    private  boolean   _mode   = true ;
    private  JPopupMenu _popup;
    private  JMenu      _edit;
    private  Color      _backgroundColor = Color.white ;
    private  Color      _itemColor = blue ;
    private  Color      _textColor = white ;
    private  Color      _linkColor = red ;
    private  JPopupMenu _itemPopup;
    private  Dimension  _dimension;
    private  Rectangle  _shutdown;
    private  Random     _random    = new Random( new Date().getTime() ) ;
    private  static Color black = new Color(20, 20, 20);
    private  static Color white = new Color(240, 240, 255);
    private  static Color red = new Color(149, 43, 42);
    private  static Color blue = new Color(94, 105, 176);
    private  static Color yellow = new Color(255, 255, 140);

    public class Item {

       private Rectangle _r;
       private Color     _c;
       private String    _s;
       private double    _nextX = -1.0 , _nextY = -1.0 ;
       private int       _frame = 8 ;

       private boolean   _doesListen;
       private HashMap   _links        = new HashMap() ;
       private Item      _defaultRoute;

       private Item( String string , Color c ){
          _s = string ;
          _c = c ;
          if( _dimension != null ){
             _r = new Rectangle( _random.nextInt( _dimension.width - 50 ) ,
                                 _random.nextInt( _dimension.height - 50 ) ,
                                 0 , 0 ) ;
          }else{
             _r = new Rectangle( 10 , 10 , 0 , 0 ) ;
          }
       }
       public synchronized void setListener( boolean listen ){
           _doesListen = listen ;
           repaint() ;
       }
       public boolean isListener(){ return _doesListen ; }

       public synchronized void setDefaultRoute( Item item ){
          _defaultRoute = item ;
          repaint() ;
       }
       public synchronized Item getDefaultRoute(){ return _defaultRoute ; }

       private synchronized void setPosition( Point p ){
          _r.x = p.x  ; _r.y = p.y ;
          repaint() ;
       }
       private synchronized void setPosition( double x , double y ){
          _nextX = x ; _nextY = y ;
          repaint();
       }
       private synchronized void newPosition( Dimension oldDim , Dimension newDim ){
           _r.x = (int)( (double)_r.x / (double)oldDim.width * (double)newDim.width );
           _r.y = (int)( (double)_r.y / (double)oldDim.height * (double)newDim.height );
       }
       private Point getCenter(){
          Point p = new Point( _r.x + _r.width/2 , _r.y + _r.height/2 ) ;
//          System.out.println(" center of "+_s+" : "+p ) ;
          return p ;
       }
       private boolean hasLink( Item item ){
          return _links.get( item.getName() ) != null ;
       }
       public synchronized void addLink( Item item ){
          _links.put( item.getName() , item ) ;
          item.setListener(true);
       }
       public synchronized void removeLink( String link ){
          Item item = (Item)_links.remove(link);
          if( item == null ) {
              return;
          }
          if( item == _defaultRoute ) {
              _defaultRoute = null;
          }
          if( item._defaultRoute == this ) {
              item._defaultRoute = null;
          }
       }
       public Iterator links(){ return _links.values().iterator() ; }
       public String getName(){ return _s ; }
       //
       //               display part
       //
       private synchronized void recalculate( Graphics g ){
          if( _dimension == null ) {
              return;
          }
          Rectangle r = (Rectangle)_r.clone() ;
          if( _nextX > -1.0 ){
             _r.x = (int)( _nextX * (double)_dimension.width ) ;
             _r.y = (int)( _nextY * (double)_dimension.height ) ;
             _nextX = -1.0 ;
          }
          g.setFont( _font ) ;
          FontMetrics fm = g.getFontMetrics() ;

          _r.width  = fm.stringWidth(_s) + 2 * _frame ;
          _r.height = fm.getMaxAscent() + fm.getMaxDescent() + 2 * _frame ;
//          System.out.println("recalculate : "+_s+" "+r+" -> "+_r);
       }
       private synchronized void draw( Graphics g ){

          recalculate( g ) ;

          g.setColor( _c ) ;
          g.fill3DRect( _r.x , _r.y , _r.width , _r.height , _mode ) ;

          g.setColor(_textColor);
          if( _doesListen ){
             int [] x = new int[3] ;
             int [] y = new int[3] ;
             x[0] = _r.x + 3 ;
             x[1] = x[0] + 8 ;
             x[2] = x[0] ;
             y[0] = _r.y + 3 ;
             y[1] = y[0] ;
             y[2] = y[0] + 8 ;
             g.fillPolygon( x , y , 3 ) ;
          }
          FontMetrics fm = g.getFontMetrics() ;
          g.drawString(_s, _r.x + _frame ,
                           _r.y + ( fm.getMaxAscent() + _frame )   );
       }
       public String toString(){
          return _s ;
       }
       public boolean equals( Object obj ){
          return (obj != null) && obj.toString().equals(_s) ;
       }
       public int hashCode(){
          int i = _s.hashCode() ;
          return i ;
       }
    }
    private void createPopup(){
        JMenuItem      mi;
        ActionListener al;

        _popup = new JPopupMenu("Edit") ;
        _edit  = new JMenu("Edit") ;

        _popup.setBorderPainted(true);

        al= new ActionListener(){
              @Override
              public void actionPerformed(ActionEvent e){
                 String string =
                     JOptionPane.showInputDialog(
                          MovingPigs.this,"Name of new Domain") ;
                 if( string != null ){
                    MovingPigs.this.addItem(string) ;
                 }
              }
           } ;
        mi = _popup.add( new JMenuItem("New Domain") ) ;
        mi.addActionListener(al) ;

        mi = _edit.add(new JMenuItem("New Domain")) ;
        mi.addActionListener(al) ;
        mi.setAccelerator(
               KeyStroke.getKeyStroke(KeyEvent.VK_D,InputEvent.CTRL_MASK,false) ) ;

        _edit.addSeparator() ;
        _popup.addSeparator() ;

        al = new ActionListener(){
              @Override
              public void actionPerformed(ActionEvent e){
                 _backgroundColor =
                     JColorChooser.showDialog(MovingPigs.this,"Choose the bg color",Color.red) ;
                 if( _backgroundColor != null ){
                    MovingPigs.this.setBackground(_backgroundColor) ;
                    MovingPigs.this.repaint() ;
                 }
              }
            };

        mi = _popup.add( new JMenuItem("Background Color") ) ;
        mi.addActionListener( al ) ;
        mi = _edit.add(new JMenuItem("Background Color")) ;
        mi.addActionListener( al ) ;

        al = new ActionListener(){
              @Override
              public void actionPerformed(ActionEvent e){
                 Color color =
                     JColorChooser.showDialog(MovingPigs.this,"Choose the item color",Color.red) ;
                 if( color != null ){
                    _itemColor = color ;
                    MovingPigs.this.repaint() ;
                 }
              }
           } ;

        mi = _popup.add( new JMenuItem("Item Color") ) ;
        mi.addActionListener(al) ;
        mi = _edit.add( new JMenuItem("Item Color")) ;
        mi.addActionListener(al) ;

        al = new ActionListener(){
              @Override
              public void actionPerformed(ActionEvent e){
                 _textColor =
                     JColorChooser.showDialog(MovingPigs.this,"Choose the text color",Color.red) ;
                 if( _textColor != null ){
                    MovingPigs.this.repaint() ;
                 }
              }
           } ;

        mi = _popup.add( new JMenuItem("Text Color") ) ;
        mi.addActionListener( al ) ;
        mi = _edit.add( new JMenuItem("Text Color") ) ;
        mi.addActionListener( al ) ;

        al = new ActionListener(){
              @Override
              public void actionPerformed(ActionEvent e){
                 _linkColor =
                     JColorChooser.showDialog(MovingPigs.this,"Choose the link color",Color.red) ;
                 if( _linkColor != null ){
                    MovingPigs.this.repaint() ;
                 }
              }
           } ;

        mi = _popup.add( new JMenuItem("Link Color") ) ;
        mi.addActionListener(al) ;
        mi = _edit.add( new JMenuItem("Link Color") ) ;
        mi.addActionListener(al) ;

        _edit.addSeparator() ;
        _popup.addSeparator() ;


        JMenu submenu;

        al = new ActionListener(){
           @Override
           public void actionPerformed( ActionEvent event ){
               String ac = event.getActionCommand() ;
               _currentFontType = ac ;
               _font = new Font( _currentFontType , _currentFontMode , _currentFontSize ) ;
               repaint() ;
           }
        } ;
        String [] fontTypes = { "Times" , "Courier" , "LucidaBright" } ;
        submenu = new JMenu("Font Type") ;
        for( int i = 0 ; i < fontTypes.length ; i++ ){
           mi = submenu.add( new JMenuItem( fontTypes[i] ) ) ;
           mi.addActionListener(al) ;
           submenu.add(mi) ;
        }
        _edit.add( submenu ) ;

        al = new ActionListener(){
           @Override
           public void actionPerformed( ActionEvent event ){
               String ac = event.getActionCommand() ;
               try{
                  _currentFontSize = Integer.parseInt( ac ) ;
               }catch(NumberFormatException eee){}
               _font = new Font( _currentFontType , _currentFontMode , _currentFontSize ) ;
               repaint() ;
           }
        } ;

        String [] fontSizes = { "8" , "10" , "12" , "14" , "16" } ;
        submenu = new JMenu("Font Size") ;
        for( int i = 0 ; i < fontSizes.length ; i++ ){
           mi = submenu.add( new JMenuItem( fontSizes[i] ) ) ;
           mi.addActionListener(al) ;
           submenu.add(mi) ;
        }
        _edit.add( submenu ) ;

        al = new ActionListener(){
           @Override
           public void actionPerformed( ActionEvent event ){
               String ac = event.getActionCommand() ;
               if( ac.equals("Plain") ) {
                   _currentFontMode = 0;
               } else if( ac.equals("Italic") ) {
                   _currentFontMode = Font.ITALIC;
               } else if( ac.equals("Bold") ) {
                   _currentFontMode = Font.BOLD;
               } else if( ac.equals("BoldItalic") ) {
                   _currentFontMode = Font.BOLD | Font.ITALIC;
               }
               _font = new Font( _currentFontType , _currentFontMode , _currentFontSize ) ;
               repaint() ;
           }
        } ;
        String [] fontModes = { "Plain" , "Italic" , "Bold" , "BoldItalic"  } ;
        submenu = new JMenu("Font Mode") ;
        for( int i = 0 ; i < fontModes.length ; i++ ){
           mi = submenu.add( new JMenuItem( fontModes[i] ) ) ;
           mi.addActionListener(al) ;
           submenu.add(mi) ;
        }
        _edit.add( submenu ) ;

        _edit.addSeparator() ;
        al = new ActionListener(){
           @Override
           public void actionPerformed( ActionEvent event ){
              Iterator i = items() ;
              while( i.hasNext() ){
                 Item item = (Item)i.next() ;
                 moveToRevert(item);
              }
           }
        } ;
        mi = _edit.add( new JMenuItem("Revert Position") ) ;
        mi.addActionListener(al) ;
        mi.setAccelerator(
               KeyStroke.getKeyStroke(KeyEvent.VK_X,InputEvent.CTRL_MASK,false) ) ;

        al = new ActionListener(){
           @Override
           public void actionPerformed( ActionEvent event ){
              shutdown();
           }
        } ;
        mi = _edit.add( new JMenuItem("Shutdown") ) ;
        mi.addActionListener(al) ;
        mi.setAccelerator(
               KeyStroke.getKeyStroke(KeyEvent.VK_Z,InputEvent.CTRL_MASK,false) ) ;



    }
    public void shutdown(){
       new Thread(
          new Runnable(){
             @Override
             public void run(){
                _shutdown = new Rectangle() ;
                try{
                  for( double gamma = 0.0 ; gamma < 1.0 ; gamma += .06 ){
                   _shutdown.x = _dimension.width/2 - (int)(gamma * (double)(_dimension.width/2)) ;
                   _shutdown.y = _dimension.height/2 - (int)(gamma * (double)(_dimension.height/2)) ;
                   _shutdown.width  = (int)( gamma * (double)_dimension.width) ;
                   _shutdown.height = (int)( gamma * (double)_dimension.height) ;
                   Thread.sleep(100) ;
                   repaint() ;
                  }
                  _shutdown.x = 0 ;
                  _shutdown.y = 0 ;
                  _shutdown.width  = _dimension.width ;
                  _shutdown.height = _dimension.height ;
                   repaint() ;
                   Thread.sleep(1000) ;
                }catch(Exception ee){}
                System.exit(0) ;
             }
          }
       ).start() ;
    }
    public class DeleteMenuItem extends JMenuItem {
       private Item _item;
       private DeleteMenuItem( Item item ){
          super( "Delete "+item.getName());
          _item = item ;
          addActionListener(
              new ActionListener(){
                 @Override
                 public void actionPerformed( ActionEvent event ){
                    MovingPigs.this.remove( _item.getName() ) ;
                 }
              }
          ) ;
       }
       public Item getItem(){ return _item ;}
    }
    public class SetColorMenuItem extends JMenuItem {
       private Item _item;
       private SetColorMenuItem( Item item ){
          super( "Set color of "+item.getName());
          _item = item ;
          addActionListener(
              new ActionListener(){
                 @Override
                 public void actionPerformed( ActionEvent event ){

                   Color color =
                     JColorChooser.showDialog(
                               MovingPigs.this,
                               "Choose Color",
                               Color.red) ;
                   if(color!=null) {
                       _item._c = color;
                   }
                   MovingPigs.this.repaint() ;
                 }
              }
          ) ;
       }
    }
    public class CreateLinkMenuItem extends JMenuItem {
       private Item _from, _to;
       boolean _create = true ;
       private CreateLinkMenuItem( Item from , Item to , boolean create ){
          super( to.getName());
          _from = from ;
          _to   = to ;
          _create = create ;
          addActionListener(
              new ActionListener(){
                 @Override
                 public void actionPerformed( ActionEvent event ){
                   if( _create ) {
                       _from.addLink(_to);
                   } else {
                       _from.removeLink(_to.getName());
                   }
                   MovingPigs.this.repaint() ;
                 }
              }
          ) ;
       }
    }
    public JComponent getEditMenu(){ return _edit ; }
    private JPopupMenu createPrivateMenu( final Item item ){
        JPopupMenu popup = new JPopupMenu( item.getName() ) ;


        JMenu submenu;
        Iterator i;
        i = items() ;
        int counter = 0 ;
        while( i.hasNext() ){
           Item toLink = (Item)i.next() ;
           if( toLink.hasLink(item) ) {
               counter++;
           }
        }
        JMenuItem mi = new JMenuItem( item.isListener() ? "Don't Listen" : "Listen" ) ;
        mi.setEnabled( counter == 0 ) ;
        popup.add( mi ) ;
        mi.addActionListener(
           new ActionListener(){
              @Override
              public void actionPerformed(ActionEvent e){
                 item.setListener( ! item.isListener() ) ;
                 MovingPigs.this.repaint() ;
              }
           }
        ) ;
        popup.addSeparator() ;
        submenu = new JMenu( "Add Link To" ) ;
        i = items()  ;
        while( i.hasNext() ){
           Item linkItem = (Item)i.next() ;
           if( linkItem.getName().equals( item.getName() ) ) {
               continue;
           }
           submenu.add(
              new CreateLinkMenuItem(item,linkItem,true)
           );
        }
        popup.add( submenu ) ;

        submenu = new JMenu( "Remove Link" ) ;
        i = item.links() ;
        while( i.hasNext() ){
           Item linkItem = (Item)i.next() ;
           submenu.add(
              new CreateLinkMenuItem(item,linkItem,false)
           );
        }
        popup.add( submenu ) ;

        popup.addSeparator() ;

        popup.add( new DeleteMenuItem(item));

        popup.addSeparator() ;

        popup.add( new SetColorMenuItem(item));

        return popup ;
    }
    private JPopupMenu createPrivateLineMenu( final Item from ,
                                              final Item to     ){
        JPopupMenu popup = new JPopupMenu( from.getName() ) ;
        JMenuItem mi;

        mi = popup.add( new JMenuItem("Remove This Link") ) ;
        mi.addActionListener(
           new ActionListener(){
              @Override
              public void actionPerformed(ActionEvent e){
                 from.removeLink(to.getName()) ;
                 to.removeLink(from.getName()) ;
                 MovingPigs.this.repaint() ;
              }
           }
        ) ;

        popup.addSeparator() ;

        mi = popup.add( new JMenuItem("Set Default Route") ) ;
        mi.setEnabled( from.getDefaultRoute() != to ) ;
        mi.addActionListener(
           new ActionListener(){
              @Override
              public void actionPerformed(ActionEvent e){
                 from.setDefaultRoute(to) ;
                 MovingPigs.this.repaint() ;
              }
           }
        ) ;

        mi = popup.add( new JMenuItem("Remove Default Route") ) ;
        mi.setEnabled( from.getDefaultRoute() == to ) ;
        mi.addActionListener(
           new ActionListener(){
              @Override
              public void actionPerformed(ActionEvent e){
                 from.setDefaultRoute(null) ;
                 MovingPigs.this.repaint() ;
              }
           }
        ) ;
        return popup ;
    }
    public MovingPigs(){
        addMouseListener(this);
        addMouseMotionListener(this);
        setBackground( Color.white ) ;
        createPopup() ;
    }
    public Item getItem( String name ){ return getItem( name , false ) ; }

    public Item getItem( String name , boolean create ){
       Item item = (Item)_list.get(name) ;
       if( item == null ){
          if( ! create ) {
              return null;
          }
          item = new Item( name , _itemColor ) ;
          _list.put( name , item ) ;
       }
       repaint() ; // DON'T REMOVE
       return item ;
    }
    public Iterator items(){ return _list.values().iterator() ; }
    public synchronized void addItem( String name ){

       if( _list.get(name) == null ) {
           getItem(name, true);
       }

    }
    public synchronized void remove( String name ){
       Iterator i = items() ;
       while( i.hasNext() ) {
           ((Item) i.next()).removeLink(name);
       }
       _list.remove( name ) ;
       repaint() ;
    }
    public void clear(){ _list.clear() ; repaint() ; }
    public void connect( String from , String to ){
       Item x = getItem( from ) ;
       Item y = getItem( to ) ;
       if( ( x == null ) || ( y == null ) ) {
           return;
       }
       x.addLink(y) ;
       repaint();
    }


    private Polygon getTriangle( Point from , Point to ){
        double  nu = 20 ;
        double  mu = 10 ;

        Polygon triangle = new Polygon() ;

        Point N   = new Point( to.x - from.x , to.y - from.y ) ;
        double NN =  ((double)N.x) *((double)N.x)+((double)N.y) *((double)N.y) ;
        NN        = Math.sqrt(NN) ;

        Point  M  = new Point( N.y , - N.x ) ;
        double MM =  ((double)M.x) *((double)M.x)+((double)M.y) *((double)M.y) ;

        double tmp = 0.5 + nu / NN ;

        triangle.addPoint( (int)( tmp * (double)N.x )  +  from.x ,
                           (int)( tmp * (double)N.y )  +  from.y  ) ;
        tmp = 0.5 - nu / NN ;
        MM  = Math.sqrt(MM) ;

        triangle.addPoint( (int)( tmp * (double)N.x + mu * M.x / MM ) + from.x ,
                           (int)( tmp * (double)N.y + mu * M.y / MM ) + from.y  ) ;

        triangle.addPoint( (int)( 0.5 * (double)N.x )  +  from.x ,
                           (int)( 0.5 * (double)N.y )  +  from.y  ) ;

        triangle.addPoint( (int)( tmp * (double)N.x - mu * M.x / MM ) + from.x ,
                           (int)( tmp * (double)N.y - mu * M.y / MM ) + from.y  ) ;

        return triangle ;
    }
    private Point getDistance( Point from , Point to , Point p ){
        Point N  = new Point( to.x - from.x , to.y - from.y ) ;
        Point P  = new Point( p.x - from.x , p.y - from.y ) ;

        double NN =  ((double)N.x) *((double)N.x)+((double)N.y) *((double)N.y) ;

        double NP = ((double)N.x)*((double)P.x)+((double)N.y)*((double)P.y) ;

        double mu =   NP / NN  ;
        if( ( mu < 0.0 ) || ( mu > 1.0 ) ) {
            return null;
        }

        double PP = ((double)P.x) *((double)P.x)+((double)P.y) *((double)P.y) ;

        double dd = PP + NP*NP/NN - 2.0 * NP * NP / NN ;

        return new Point( mu < 0.5 ? -1 : 1 , (int)dd ) ;
    }
    @Override
    public void paintComponent( Graphics g ){
       super.paintComponent(g);
       Dimension d = getSize(null) ;
       if( _dimension == null ){
           _dimension = d ;
       }else if( ! d.equals( _dimension ) ){
           resizeItems( d ) ;
           _dimension = d ;
       }
       Iterator iter = _list.values().iterator() ;
       Color c = g.getColor() ;
       g.setColor(_linkColor) ;
       while( iter.hasNext() ){

          Item     item = (Item)iter.next() ;
          item.recalculate(g) ; // nasty but we need it
          Item     def  = item.getDefaultRoute() ;
          Point    t    = item.getCenter() ;
          Iterator n    = item.links() ;

          while( n.hasNext() ){
              Item  linkTo  = (Item)n.next() ;
              linkTo.recalculate(g) ;
              Point pointTo = linkTo.getCenter() ;
              g.drawLine( t.x  , t.y , pointTo.x , pointTo.y ) ;
              if( def == linkTo ) {
                  drawCircle(g, t, pointTo);
              }
              if( linkTo.getDefaultRoute() == item ) {
                  drawCircle(g, pointTo, t);
              }

              g.fillPolygon( getTriangle( t , pointTo ) ) ;
          }
       }
       g.setColor(c) ;
       iter = _list.values().iterator() ;
       while( iter.hasNext() ){
          Item item = (Item)iter.next() ;
          if( ( _cursor != null ) && ( _cursor == item ) ) {
              continue;
          }
          item.draw( g ) ;
       }
       if( _cursor != null ){
          g.setXORMode( Color.green ) ;
          _cursor.draw( g ) ;
          g.setPaintMode() ;
       }
       if( _shutdown != null ) {
           g.fillRect(_shutdown.x,
                   _shutdown.y,
                   _shutdown.width,
                   _shutdown.height);
       }
    }
    private void drawCircle( Graphics g  , Point from , Point to ){
       double mu = 0.25 ;
       int x = (int)((double)from.x + mu*((double)to.x - (double)from.x) );
       int y = (int)((double)from.y + mu*((double)to.y - (double)from.y) );
       g.fillOval( x - 10 , y - 10 , 20 , 20 ) ;
    }
    @Override
    public Dimension getPreferredSize(){ return new Dimension(500,500);}
    @Override
    public Dimension getMinimumSize(){ return new Dimension(100,100);}
    @Override
    public void mouseClicked( MouseEvent e ){
       if( e.isPopupTrigger() ){
          _popup.show(this,e.getPoint().x,e.getPoint().y);
//          System.out.println("Click");
       }
    }
    @Override
    public void mouseReleased( MouseEvent e ){
       if( e.isPopupTrigger() ){
          _popup.show(this,e.getPoint().x,e.getPoint().y);
       }
      _cursor = null ;
      repaint();
    }
    private Item getItemByPosition( Point p ){
      Iterator iter = _list.values().iterator() ;
      while( iter.hasNext() ){
         Item item = (Item)iter.next() ;
         if( item._r.contains(p) ) {
             return item;
         }
      }
      return null ;
    }
    private void resizeItems( Dimension d ){
      Iterator iter = _list.values().iterator() ;
      while( iter.hasNext() ){
         ((Item)iter.next()).newPosition( _dimension , d ) ;
      }
    }
    @Override
    public void mousePressed( MouseEvent e ){

      Point p = e.getPoint() ;
      //
      // first check if we hit an item
      //
      Iterator iter = _list.values().iterator() ;
      while( iter.hasNext() ){
         Item item = (Item)iter.next() ;
         if( item._r.contains(p) ){
            if( e.isPopupTrigger() ){
               createPrivateMenu(item).show(this,p.x,p.y) ;
               return ;
            }
            _cursor = item ;
            _offset.x = p.x - item._r.x  ;
            _offset.y = p.y - item._r.y  ;
            repaint() ;
            return ;
         }
      }
      _cursor = null ;
      //
      // are somewhere next to a line ?
      //
      iter = _list.values().iterator() ;
      while( iter.hasNext() ){
         Item item = (Item)iter.next() ;
         Iterator links = item.links() ;
         while( links.hasNext() ){
            Item toItem = (Item)links.next() ;
            Point diff = getDistance( item.getCenter() , toItem.getCenter() , p ) ;
//            System.out.println(item.getName()+" "+toItem.getName()+" "+diff);
            if( ( diff != null ) && ( diff.y < 100 ) ){
                if( e.isPopupTrigger() ){
                   createPrivateLineMenu(
                      diff.x < 0 ? item   : toItem ,
                      diff.x < 0 ? toItem : item    ).show(this,p.x,p.y) ;
                   return ;
                }
            }
         }
      }
      //
      // otherwise we open the background menu.
      //
      if( e.isPopupTrigger() ){
         _popup.show(this,p.x,p.y) ;
         return ;
      }
      repaint();
    }
    @Override
    public void mouseMoved( MouseEvent e ){}
    @Override
    public void mouseEntered( MouseEvent e ){ _mode = true ; repaint() ;}
    @Override
    public void mouseExited( MouseEvent e ){ _mode = true ; repaint() ;}
    @Override
    public void mouseDragged( MouseEvent e ){
       if( _cursor != null  ){
           Dimension d = getSize(null) ;
           Item item = _cursor ;
           Point p = e.getPoint() ;
           if( ( p.x < 0 ) || ( p.y < 0 ) ||
               ( p.x >= d.width ) || ( p.y >= d.height ) ) {
               return;
           }
           item._r.x = p.x - _offset.x ;
           item._r.y = p.y - _offset.y ;
           repaint() ;
       }
    }
    public void writeSetup( PrintWriter pw ){
       Iterator i = items() ;
       while( i.hasNext() ){
           Item item = (Item)i.next() ;
           pw.println( "define "+item.getName() ) ;
           Item defRoute = item.getDefaultRoute() ;
           if( item.isListener() ) {
               pw.println("listen " + item.getName());
           }
           Iterator links = item.links() ;
           while( links.hasNext() ){
              Item link = (Item)links.next() ;
              pw.println( "connect "+item.getName()+" "+link.getName());
           }
           if( defRoute != null ) {
               pw.println("defaultroute " + item.getName() + " " + defRoute
                       .getName());
           }
       }
       pw.println( "# color * background "+
                   _backgroundColor.getRed()+" "+
                   _backgroundColor.getGreen()+" "+
                   _backgroundColor.getBlue()+" "  ) ;
       pw.println( "# color * item "+
                   _itemColor.getRed()+" "+
                   _itemColor.getGreen()+" "+
                   _itemColor.getBlue()+" "  ) ;
       pw.println( "# color * text "+
                   _textColor.getRed()+" "+
                   _textColor.getGreen()+" "+
                   _textColor.getBlue()+" "  ) ;
       pw.println( "# color * link "+
                   _linkColor.getRed()+" "+
                   _linkColor.getGreen()+" "+
                   _linkColor.getBlue()+" "  ) ;
       i = items() ;
       while( i.hasNext() ){
           Item item = (Item)i.next() ;
           pw.println( "# move "+item.getName()+" "+
                       (float)( (double)item._r.x / (double)_dimension.width )  + " " +
                       (float)( (double)item._r.y / (double)_dimension.height )  ) ;
           pw.println( "# color "+item.getName()+" background "+
                          item._c.getRed()+" "+item._c.getGreen()+" "+item._c.getBlue() ) ;
       }
    }
    private void moveToRevert( Item item ){
       moveTo( item ,
       (double)( _dimension.width  - item._r.x -item._r.width  ) / (double)_dimension.width ,
       (double)( _dimension.height - item._r.y -item._r.height ) / (double)_dimension.height  ) ;
    }
    private void moveTo( final Item item , final double newX , final double newY ){
     new Thread(
           new Runnable(){
               @Override
               public void run(){
       double x = (double)item._r.x  / (double) _dimension.width ;
       double y = (double)item._r.y  / (double) _dimension.height ;
       double diffX = newX - x ;
       double diffY = newY - y ;

       try{
          for( double gamma = 0.0 ; gamma < 1.0 ; gamma += 0.02 ){
             item.setPosition( gamma * diffX + x , gamma * diffY + y ) ;
             Thread.sleep(50) ;
             repaint() ;
          }
       }catch(Exception ee){}
       item.setPosition( newX , newY ) ;

               }
            }
      ).start() ;
    }
    public String command( String command ){
       if( command.length() < 2 ) {
           return null;
       }
       if( command.charAt(0) == '#' ) {
           return command(command.substring(1));
       }

       StringTokenizer st = new StringTokenizer( command ) ;
       int count = st.countTokens() ;
       String [] c = new String[count] ;
       for( int i = 0 ; i < count ; i++) {
           c[i] = st.nextToken();
       }
       return command( c ) ;
   }
   private static Color colorByString( String string ){
       StringTokenizer st = new StringTokenizer(string,",") ;
       try{
          return new Color(
                       Integer.parseInt(st.nextToken()) ,
                       Integer.parseInt(st.nextToken()) ,
                       Integer.parseInt(st.nextToken())  ) ;
       }catch(Exception ee){
          return Color.black ;
       }
   }
   public String command( String [] c ){
       if( c.length < 1 ) {
           return null;
       }
       if( c[0].equals( "define" ) ){
          if( c.length < 2 ) {
              return null;
          }
          for( int i = 1 ; i < c.length ; i++ ) {
              addItem(c[i]);
          }
       }else if( c[0].equals( "listen" ) ){
          if( c.length < 2 ) {
              return null;
          }
          Item item;
          for( int i = 1 ; i < c.length ; i++ ){
             item = getItem(c[i],true) ;
             item.setListener(true) ;
          }
       }else if( c[0].equals( "defaultroute" ) ){
          if( c.length != 3 ) {
              return null;
          }
          Item item = getItem(c[1],true) ;
          Item routeItem = getItem(c[2] , true ) ;
          item.setDefaultRoute(routeItem);
       }else if( c[0].equals( "position" ) ){
          if( c.length != 4 ) {
              return null;
          }
          try{
             double x = Double.parseDouble(c[2]) ;
             double y = Double.parseDouble(c[3]);
             Item item = getItem(c[1],true) ;
             item.setPosition( x , y ) ;
          }catch(Exception ii){
             System.out.println(ii.toString());
             return null ;
          }
       }else if( c[0].equals( "move" ) ){
          if( c.length != 4 ) {
              return null;
          }
          try{
             double x = Double.parseDouble(c[2]) ;
             double y = Double.parseDouble(c[3]);
             Item item = getItem(c[1],true) ;
             moveTo( item , x , y ) ;
          }catch(Exception ii){
             System.out.println(ii.toString());
             return null ;
          }
       }else if( c[0].equals( "color" ) ){
          if( c.length < 6 ) {
              return null;
          }
          try{
             Color cc = new Color(
                              Integer.parseInt(c[3]) ,
                              Integer.parseInt(c[4]) ,
                              Integer.parseInt(c[5])  ) ;

             if( c[1].equals("*") ){
                if( c[2].equals("background") ){
                   _backgroundColor = cc ;
                   setBackground(_backgroundColor) ;
                }else if( c[2].equals("item") ){
                   _itemColor = cc ;
                }else if( c[2].equals("text") ){
                   _textColor = cc ;
                }else if( c[2].equals("link") ){
                   _linkColor = cc ;
                }
             }else{
                Item item = getItem(c[1],true) ;
                if( c[2].equals("background") ){
                   item._c = cc ;
                }
             }
             repaint() ;
          }catch(Exception ii){
             System.out.println(ii.toString());
             return null ;
          }
       }else if( c[0].equals( "clear" ) ){
          clear() ;
       }else if( c[0].equals( "exit" ) ){
          System.exit(0);
       }else if( c[0].equals( "connect" ) ){
          if( c.length < 3 ) {
              return null;
          }
          Item item = getItem( c[1] , true ) ;
          for( int i = 2 ; i < c.length ; i++ ) {
              item.addLink(getItem(c[i], true));
          }
       }else if( c[0].equals( "remove" ) ){
          if( c.length < 2 ) {
              return null;
          }
          remove( c[1] ) ;
       }
       return null ;
    }
}

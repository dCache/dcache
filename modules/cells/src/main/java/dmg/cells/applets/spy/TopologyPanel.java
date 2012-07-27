package dmg.cells.applets.spy ;

import java.applet.*;
import java.awt.* ;
import java.awt.event.* ;
import java.util.* ;
import java.io.* ;
import java.net.* ;

import dmg.util.* ;
import dmg.cells.services.* ;
import dmg.cells.nucleus.* ;
import dmg.cells.network.* ;



class TopologyPanel extends Panel  {

    private class TopoCanvas extends Canvas implements MouseListener {
    
        private Hashtable  _domainPositions   = new Hashtable() ;                          
        private Font _font = new Font( "SansSerif" , Font.ITALIC , 18 )  ;
        private CanonTopo  _canonical;
        private double     _baseA;
        public TopoCanvas(){
            addMouseListener( this ) ;
        }
        
        public void setCanonical( CanonTopo canonical ){
           _canonical = canonical ;
           repaint() ;
           _baseA = 0.0 ;
        }
        @Override
        public void paint( Graphics g ){
           Dimension   d    = getSize() ;
           Color base = Color.red ;
           for( int i = 0 ; i < 4 ; i++ ){
              g.setColor( base ) ;
              g.drawRect( i , i , d.width-2*i-1 , d.height-2*i-1 ) ;
              base = base.darker() ;
           }
           if( _canonical == null ) {
               return;
           }
           drawTopology( _canonical , g ) ;
        }
        private void drawTopology( CanonTopo topo , Graphics g ){
           Dimension d = getSize() ;
           int domains = topo.domains() ;
           if( domains <= 0 ) {
               return;
           }
           FontMetrics fm ;
           String [] domainNames = new String[domains] ;
           for( int i = 0 ; i < domainNames.length ; i++ ) {
               domainNames[i] = topo.getDomain(i);
           }
           if( domains == 1 ){
               g.setFont( _font ) ;
               fm = g.getFontMetrics() ;
               int h = fm.getHeight() ;
               int l = fm.stringWidth( domainNames[0] ) ;
               int x = ( d.width - l ) /2 ;
               int y = ( d.height + h) /2 ;
               g.drawString( domainNames[0] , x , y ) ;
               return ;
           }
           g.setFont( _font ) ;
           fm = g.getFontMetrics() ;
           int desc = fm.getMaxDescent() ;
           int acs  = fm.getMaxAscent() ;
           int h    = fm.getHeight() ;
           int l;

           int [] dX = new int[domains] ;
           int [] dY = new int[domains] ;
           //
           // calculation of dX, dY
           //
           calculatePositions( d , dX , dY , topo ) ;
           //
           // end
           //
           g.setColor( Color.white ) ;
           for( int i = 0 ; i < topo.links() ; i++ ){
              LinkPair pair = topo.getLinkPair(i) ;
              int left  = pair.getBottom() ;
              int right = pair.getTop() ;

              g.drawLine( dX[left] , dY[left] , dX[right] , dY[right] ) ;
           }
           _domainPositions.clear() ;
           for( int i = 0 ; i < domains ; i++ ){
              l = fm.stringWidth( domainNames[i] ) ;
              h = acs + desc ;
              //
              // smallest box around the text
              //
              int x = dX[i] - l / 2 ;
              int y = dY[i] - acs ;

              Color col = Color.green ;
              g.setColor( col ) ;

              _domainPositions.put( 
                    new Rectangle( x-2 , y-2 , l+4 , h+4 ) ,
                    domainNames[i] ) ;

              col = Color.red ;
              g.fillRect( x-2 , y-2 , l+4 , h+4 ) ;
              g.setColor( col = col.darker() ) ;
              g.drawRect( x-3 , y-3 , l+5 , h+5 ) ;
              g.setColor( col = col.darker() ) ;
              g.drawRect( x-4 , y-4 , l+7 , h+7 ) ;
              g.setColor( Color.red ) ;
              g.drawString( domainNames[i] , x , dY[i] ) ;
           }
        
        }
        private void calculatePositions( Dimension d , 
                                         int [] dX , int [] dY ,
                                         CanonTopo topo           ){
           int domains = dX.length ;                            
           int r = d.height > d.width ? d.width : d.height ;
           r = (int)(  (float)r / 2.0 * 0.8) ;
           int x0 = d.width / 2 ;
           int y0 = d.height / 2 ;
           double dAngle = 2. * Math.PI / (double)domains ;
           double A0     = 2. * Math.PI * _baseA ;
           for( int i = 0 ; i < domains ; i++ ){
              dX[i] = (int)( x0 + r * Math.sin( A0 + dAngle * (double) i ) ) ;
              dY[i] = (int)( y0 - r * Math.cos( A0 + dAngle * (double) i ) ) ;
           }
        }
        @Override
        public void mouseClicked( MouseEvent ev ){
            Point       p = ev.getPoint() ;
            Enumeration e = _domainPositions.keys() ;
            Rectangle   r;
            String domain = null ;
            for( ; e.hasMoreElements() ; ){
               r = (Rectangle)e.nextElement() ;
               if( r.contains( p ) ){
                  domain = (String)_domainPositions.get( r ) ;
                  break ;
               }
            
            }
            if( domain == null ){
               _baseA += 0.05 ;
               repaint() ;
               return ;
            }
            if( _callback != null ) {
                _callback.actionPerformed(
                        new ActionEvent(TopologyPanel.this, 0, domain));
            }
         }   
         @Override
         public void mouseExited( MouseEvent e ){}
         @Override
         public void mouseEntered( MouseEvent e ){}
         @Override
         public void mousePressed( MouseEvent e ){}
         @Override
         public void mouseReleased( MouseEvent e ){}
    }
    
    private TopoCanvas     _topo;
    private ActionListener _callback ;
    
    public TopologyPanel(){
        super( new BorderLayout() ) ;
        _topo = new TopoCanvas() ;
        add( _topo , "Center" ) ;
        setBackground( Color.blue ) ;
    
    }
    public void addActionListener( ActionListener l ){
       _callback = l ;
    }
    @Override
    public Insets getInsets(){ return new Insets( 10 , 10 , 10 ,  10 ) ; }
    public void setTopology( CellDomainNode [] in ){
       CanonTopo topo = new CanonTopo( in );
//       System.out.println( "Topology : \n" + topo ) ;
       _topo.setCanonical( topo ) ;
    }

}
      
 

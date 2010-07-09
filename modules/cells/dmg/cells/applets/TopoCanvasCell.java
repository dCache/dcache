package dmg.cells.applets ;

import java.io.*;
import java.util.* ;
import java.awt.* ;
import java.awt.event.* ;

import dmg.cells.nucleus.* ;
import dmg.cells.network.* ;
import dmg.cells.services.* ;
import dmg.util.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class      TopoCanvasCell
       extends    Canvas
       implements Cell,
                  Runnable ,
                  MouseListener
{
    private final static Logger _log =
        LoggerFactory.getLogger(TopoCanvasCell.class);

     private CellNucleus _nucleus ;
     private CellShell   _shell ;
     private Gate        _finalGate     = new Gate( false ) ;
     private Thread      _requestThread = new Thread( this ) ;
     private Thread      _psThread      = null ;
     private String      _psAddress     = null ;

     private CellDomainNode [] _domainNode = null ;

     private CanonTopo         _topology          = null ,
                               _displayedTopology = null ;
     private Hashtable         _domainPositions   = new Hashtable() ;
     private Date              _updateDate = new Date() ;
     private Object            _recvLock   = new Object() ;

     private Font       _font   , _smallFont  ;

     private String [] _stayTuned   = { "Stay" , "Tuned" } ;
     private String [] _displayInfo        = null ;
     private Font      _displayFont        = null ;
     private Color     _displayColor       = null ;
     private Color     _displayBackground  = null ;
     private Object    _displayLock        = new Object() ;
     private CellInfo [] _displayCellInfo    = null ;

     private final static int DISPLAY_TOPOLOGY = 1 ;
     private final static int DISPLAY_INFO     = 2 ;
     private final static int DISPLAY_CELLS    = 3 ;

     private int   _displayMode     = DISPLAY_TOPOLOGY ;
     private int   _displayLastMode = DISPLAY_TOPOLOGY ;
     private static final String [] _control = {
        "Update" , "Up" , "Next" , "Previous" } ;

     public TopoCanvasCell( String name ){

        _nucleus = new CellNucleus( this , name+"*" ) ;
        _nucleus.setPrintoutLevel(15) ;

        _requestThread.start() ;

        setBackground( Color.blue ) ;
        addMouseListener( this ) ;

        _font      = new Font( "TimesRoman" , Font.BOLD , 20 ) ;
        _smallFont = new Font( "TimesRoman" , Font.ITALIC , 15 ) ;

     }

     public void paint( Graphics g ){
      synchronized( _displayLock ){
        switch( _displayMode ){
          case DISPLAY_TOPOLOGY :
            drawTopology( g ) ;
          break ;
          case DISPLAY_INFO :
            drawInfo( g ) ;
          break ;
          case DISPLAY_CELLS :
            drawCells( g ) ;
          break ;
          default : drawInfo( g ) ;
        }
      }
     }

     private void drawCells( Graphics g ){
        Dimension   d    = getSize() ;
        synchronized( _displayLock ){
           if( _displayCellInfo == null )return ;
           CellInfo [] ci = _displayCellInfo ;
           _displayFont = _smallFont ;
           g.setFont( _font ) ;
           FontMetrics fm = g.getFontMetrics() ;
           int total = fm.getMaxAscent() ;
           int base1 = total ;
           total += fm.getMaxDescent() ;
           total += 5 ;
           g.setFont( _smallFont ) ;
           fm = g.getFontMetrics() ;
           total += fm.getMaxAscent() ;
           int base2 = total ;
           total += fm.getMaxDescent()  ;
           total += 5 ;
           //
           int nameLength = 0 ;
           int typeLength = 0 ;
           g.setFont( _font ) ;
           fm = g.getFontMetrics() ;
           for( int i = 0 ; i < ci.length ; i++ ){
              nameLength = Math.max( nameLength ,
                                     fm.stringWidth( ci[i].getCellName() ) ) ;
              typeLength = Math.max( typeLength ,
                                     fm.stringWidth(
                                       CellInfo.cutClass(
                                         ci[i].getCellClass() ) ) ) ;
           }
           int namePosition  = 5 ;
           int typePosition  = ( typeLength + nameLength + 10 ) > d.width ?
                               -1 : nameLength+20  ;
           int countPosition = d.width - 50 ;
           //
           // now draw
           //
           int y = 0 ;
           _domainPositions.clear() ;
           //
           // some controll buttons
           //
           int xDiff = d.width / ( _control.length + 1 ) ;
           int x     = 0 ;
           y += 5 ;
           for( int i = 0 ; i < _control.length ; i++ ){
              x += xDiff ;
              _domainPositions.put(
                 new Rectangle( x - fm.stringWidth(_control[i])/2 ,
                                y , fm.stringWidth(_control[i]) , total ) ,
                 _control[i] ) ;
              g.drawString( _control[i] ,
                            x - fm.stringWidth(_control[i])/2 ,
                            y + fm.getMaxAscent() ) ;
           }
           y += total ;
           for( int i = 0 ; i < ci.length ; i++ ){
              y += 5 ;
              g.setColor( Color.yellow ) ;
              _domainPositions.put(
                 new Rectangle( 3 , y , d.width-6 , total ) ,
                 ci[i].getCellName() ) ;
              g.fillRect( 3 , y , d.width-6 , total ) ;
              g.setColor( Color.red ) ;
              g.setFont( _font ) ;
              g.drawString( ci[i].getCellName() , namePosition ,  y+base1 ) ;
              if( typePosition > -1 )
              g.drawString( CellInfo.cutClass( ci[i].getCellClass() ),
                            typePosition, y+base1  ) ;
              g.drawString( ""+ci[i].getEventQueueSize() ,
                             countPosition , y+base1  ) ;
              g.setColor( Color.blue ) ;
              g.setFont( _smallFont ) ;
              g.drawString( ci[i].getShortInfo() , namePosition ,  y+base2 ) ;
              y += total ;
           }
        }
     }
     private void drawInfo( Graphics g ){
        Dimension   d    = getSize() ;
        synchronized( _displayLock ){

        if( ( _displayInfo == null ) || ( _displayInfo.length == 0 ) ){
           g.setColor( Color.red ) ;
           String str     = "EuroStore" ;
           Font font      = new Font( "TimesRoman" , Font.BOLD , 50 ) ;
           g.setFont( font ) ;
           FontMetrics fm = g.getFontMetrics() ;
           int         l  = fm.stringWidth( str ) ;
           g.drawString( str , (d.width-l)/2 , d.height/2 ) ;
           return ;
        }
        setBackground( _displayBackground ) ;
        g.setColor( _displayColor ) ;
        g.setFont( _displayFont ) ;
        FontMetrics fm = g.getFontMetrics() ;
        int diff = d.height / ( _displayInfo.length + 1 ) ;
        int y    = 0 ;
        if( _displayInfo.length < 4 ){
            for( int i = 0 ; i < _displayInfo.length ; i++ ){
               int l  = fm.stringWidth( _displayInfo[i] ) ;
               y     += diff ;
               g.drawString( _displayInfo[i]  , (d.width-l)/2 , y ) ;
            }
        }else{
            for( int i = 0 ; i < _displayInfo.length ; i++ ){
               y     += diff ;
               g.drawString( _displayInfo[i]  , 10 , y ) ;
            }
        }
        }
     }
     private void say( String str ){

        StringTokenizer st = new StringTokenizer( str , "\n" ) ;
        int c = st.countTokens() ;
        String [] x = new String[c] ;
        for( int i = 0 ; st.hasMoreTokens() ; i++ )
           x[i] = st.nextToken() ;
        say( x ) ;
     }
     private void say(){ say( new String[0] ) ; }
     private void say( String [] x ){
        synchronized( _displayLock ){
           _displayInfo       = x.length == 0 ? null : x ;
           if( _displayInfo == null ){  say("EuroStore") ; return ; }
           if( _displayInfo.length > 3 )_displayFont = _smallFont ;
           else       _displayFont = _font ;
           _displayColor      = Color.red ;
           _displayBackground = Color.blue ;
           setDisplayMode ( DISPLAY_INFO ) ;

        }
        repaint() ;
     }
     private void drawTopology( Graphics g ){

        Dimension   d    = getSize() ;

        CanonTopo   topo ;
        Date        update ;

        Color c = Color.red ;
        g.setColor( c ) ;
        g.fillRect( 0 , 0 , d.width , d.height ) ;
        for( int i = 4 ; i > 0 ; i-- ){
           g.setColor( c = c.darker() ) ;
           g.drawRect( i , i , d.width - 2*i - 1 , d.height - 2*i - 1 ) ;
        }
        synchronized( _recvLock ){
              topo   = _topology ;
              update = _updateDate ;
        }
        if( topo == null ){
           say(  "No Topology Informations available" ) ;
           return ;
        }

        g.setColor( Color.red ) ;
        long diff = new Date().getTime() - update.getTime() ;
        if( diff > 10000 ){
            synchronized( _recvLock ){
               _topology = null ;
           }
           repaint() ;
           return ;
        }

        int domains = topo.domains() ;
        if( domains <= 0 )return ;
        FontMetrics fm ;
        String [] domainNames = new String[domains] ;
        for( int i = 0 ; i < domainNames.length ; i++ )
           domainNames[i] = topo.getDomain(i) ;
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
        int l    = 0 ;

        int [] dX = new int[domains] ;
        int [] dY = new int[domains] ;
        int r = d.height > d.width ? d.width : d.height ;
        r = (int)(  (float)r / 2.0 * 0.8) ;
        int x0 = d.width / 2 ;
        int y0 = d.height / 2 ;
        double dAngle = 2. * Math.PI / (double)domains ;
        for( int i = 0 ; i < domains ; i++ ){
           dX[i] = (int)( x0 + r * Math.sin( dAngle * (double) i ) ) ;
           dY[i] = (int)( y0 - r * Math.cos( dAngle * (double) i ) ) ;
        }
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

           g.fillRect( x-2 , y-2 , l+4 , h+4 ) ;
           g.setColor( col = col.darker() ) ;
           g.drawRect( x-3 , y-3 , l+5 , h+5 ) ;
           g.setColor( col = col.darker() ) ;
           g.drawRect( x-4 , y-4 , l+7 , h+7 ) ;
           g.setColor( Color.red ) ;
           g.drawString( domainNames[i] , x , dY[i] ) ;
        }

     }
     /*
     public void update( Graphics g ){
        Dimension   d  = getSize() ;
        g.setColor( Color.yellow ) ;
        g.fillRect( 0 , 0 , d.width , d.height ) ;
        paint( g ) ;
     }
     */
     public void run(){
        if( Thread.currentThread() == _requestThread ){
           while(true){
              try{
                 _nucleus.sendMessage(
                    new CellMessage(
                      new CellPath( "topo" ) ,"gettopomap" ) ) ;
                 Thread.sleep(5000) ;

              }catch(Exception e){
                 _log.warn( "Problem sending request : "+e) ;
                 try{Thread.sleep(5000) ;
                 }catch(Exception ex){}
              }
          }
        }else if( Thread.currentThread() == _psThread ){
            say( "Please Wait" ) ;
            CellMessage msg  = null ;
            try{
                msg = _nucleus.sendAndWait(
                          new CellMessage(
                               new CellPath( _psAddress ) ,
                               "getcellinfos" ) ,
                          5000 ) ;
            }catch( Exception ee ){
               say( ee.toString() ) ;
               return ;
            }
            _log.info( "getcellinfos arrived "+msg ) ;
            if( msg == null ){
               say( "Timeout, Sorry" ) ;
               return ;
            }
            Object obj = msg.getMessageObject() ;

            if( ( obj == null ) ||
              ! ( obj instanceof CellInfo [] ) ){

               say( "Something very weird arrived" ) ;
               return ;
            }
            synchronized( _displayLock ){
               _displayCellInfo = (CellInfo [] )obj ;
               setDisplayMode( DISPLAY_CELLS ) ;
//                String [] s = new String[_displayCellInfo.length] ;
//                for( int i= 0 ; i < s.length ; i++ ){
//                   s[i] = _displayCellInfo[i].toString() ;
//                   _log.info( " -> "+s[i] ) ;
//                }
//                say( s ) ;
            }

        }
     }

   //
   // and the cell interface
   //
   public String toString(){
       return "Servlet Cell : " +_nucleus.getCellName();
   }

   public String getInfo(){
     StringBuffer sb = new StringBuffer() ;
     sb.append( toString()+"\n" ) ;
     return sb.toString()  ;
   }
   public void   messageArrived( MessageEvent me ){

     if( me instanceof LastMessageEvent ){
        _log.info( "Last message received; releasing lock" ) ;
        _finalGate.open();
     }else{
        CellMessage msg   = me.getMessage() ;
        Object      obj   = msg.getMessageObject() ;

        if( obj instanceof CellDomainNode [] ){
           boolean doRepaint = false ;
           synchronized( _recvLock ){
              _domainNode    = (CellDomainNode [])obj ;
              _updateDate    = new Date() ;
              CanonTopo topo = new CanonTopo( _domainNode ) ;
              if( (_topology == null ) ||
                  ! topo.equals( _topology ) ){
                   _topology  = topo ;
                   _log.info( "Change in topogy registered" ) ;
                   doRepaint  = true ;
              }
           }
           if( doRepaint ){
              repaint() ;
//              _toolkit.sync() ;
              _log.info( "Repaint called" ) ;
           }
        }
     }

   }
   public void   prepareRemoval( KillEvent ce ){
     _log.info( "prepareRemoval : waiting to enter final gate" ) ;
     _finalGate.check() ;

     _log.info( "finished" ) ;
     // returning from this routing
     // means that the system will stop
     // all thread connected to us
     //
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _log.info( "exceptionArrived : "+ce ) ;
   }
   private void setDisplayMode( int mode ){
      synchronized( _displayLock ){
        _displayLastMode = _displayMode ;
        _displayMode     = mode ;
      }
      repaint() ;
   }
   public void mouseClicked( MouseEvent ev ){
        switch( _displayMode ){
          case DISPLAY_TOPOLOGY : {
            Point       p = ev.getPoint() ;
            Enumeration e = _domainPositions.keys() ;
            Rectangle   r = null ;
            String domain = null ;
            for( ; e.hasMoreElements() ; ){
               r = (Rectangle)e.nextElement() ;
               if( r.contains( p ) ){
                  domain = (String)_domainPositions.get( r ) ;
                  break ;
               }

            }
            if( domain == null )return ;
            CellDomainNode [] dn ;
            synchronized( _recvLock ){ dn = _domainNode ; }
            if( dn == null )return ;
            int i = 0 ;
            for( i = 0 ; ( i < dn.length ) &&
                         ( ! ( dn[i].getName().equals( domain ) ) ) ; i++ );
            if( i == dn.length ){
               say( "Nothing found\nAbout\n"+domain ) ;
            }else{
               _psAddress = dn[i].getAddress() ;
               _psThread = new Thread( this ) ;
               _psThread.start() ;
            }
          }
          break ;
          case DISPLAY_INFO :
            synchronized( _displayLock ){
                setDisplayMode( _displayLastMode ) ;
            }
          break ;
          case DISPLAY_CELLS :
            synchronized( _displayLock ){
                Point       p = ev.getPoint() ;
                Enumeration e = _domainPositions.keys() ;
                Rectangle   r = null ;
                String domain = null ;
                for( ; e.hasMoreElements() ; ){
                   r = (Rectangle)e.nextElement() ;
                   if( r.contains( p ) ){
                      domain = (String)_domainPositions.get( r ) ;
                      break ;
                   }

                }
                if( domain == null ){
                   _log.info( "Nothing klicked" ) ;
                   return ;
                }else{
                   _log.info( "Found : "+domain ) ;
                }
                if( domain.equals( "Update" ) ){
                    _psThread = new Thread( this ) ;
                    _psThread.start() ;
                    return ;
                }else if( domain.equals( "Up" ) ){
                    setDisplayMode( DISPLAY_TOPOLOGY );
                }
                int i = 0 ;
                for( i = 0 ;
                     ( i < _displayCellInfo.length ) &&
                     ! _displayCellInfo[i].getCellName().equals( domain ) ;
                     i++ );
                if( i == _displayCellInfo.length )return ;
                say( _displayCellInfo[i].getPrivatInfo() ) ;
            }
          break ;
          default :
            synchronized( _displayLock ){
                setDisplayMode ( DISPLAY_TOPOLOGY ) ;
            }


        }
        repaint();
   }
   public void mouseExited( MouseEvent e ){
   }
   public void mouseEntered( MouseEvent e ){
   }
   public void mousePressed( MouseEvent e ){
   }
   public void mouseReleased( MouseEvent e ){

   }
}

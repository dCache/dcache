// $Id: CellInfoContainer.java,v 1.1 2006-06-08 15:23:27 patrick Exp $Cg

package diskCacheV111.services.web ;

import java.util.* ;
import java.util.regex.Pattern ;
import java.text.* ;
import java.io.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;


import diskCacheV111.pools.* ;


    public class CellInfoContainer {
       
       private Map _poolHash           = new HashMap() ;
       private Map _patternHash        = new HashMap() ;
       private Map _poolGroupClassHash = new HashMap() ;
       
       public synchronized void addInfo( String poolName , Object payload ){
           Map link = (Map)_poolHash.get(poolName);
           if( link != null ){
               for( Iterator list = link.values().iterator() ; list.hasNext() ; ){
                   Map table = (Map)list.next() ;
                   table.put( poolName , payload );
               }
           }
           for( Iterator patterns = _patternHash.values().iterator() ; patterns.hasNext() ; ){
               PatternEntry patternEntry = (PatternEntry)patterns.next() ;
               if( patternEntry.pattern.matcher(poolName).matches() ){
                   link = patternEntry.linkMap ;
                   for( Iterator list = link.values().iterator() ; list.hasNext() ; ){
                       Map table = (Map)list.next() ;
                       table.put( poolName , payload );
                   }
               }
           }
       }
       public synchronized void addPool( String groupClass , String group , String poolName ){
           Map poolGroupMap = (Map)_poolGroupClassHash.get( groupClass ) ;
           if( poolGroupMap == null )_poolGroupClassHash.put( groupClass , poolGroupMap = new HashMap() ) ;
           
           Map table = (Map)poolGroupMap.get( group ) ;
           if( table == null )poolGroupMap.put( group , table = new HashMap() ) ;
           
           Map link = (Map)_poolHash.get( poolName ) ;
           if( link == null )_poolHash.put( poolName , link = new HashMap() ) ;
           
           link.put( groupClass+":"+group , table ) ;
           
       }
       public synchronized void removePool( String groupClass , String group , String poolName ) 
               throws NoSuchElementException , IllegalStateException {
           Map poolGroupMap = (Map)_poolGroupClassHash.get( groupClass ) ;
           if( poolGroupMap == null )
               throw new
               NoSuchElementException("groupClass not found : "+groupClass);
           Map tableMap = (Map)poolGroupMap.get( group ) ;
           if( tableMap == null )
               throw new
               NoSuchElementException("group not found : "+group);
           //
           //
           // now get the table map from the poolHash side
           //
           Map link = (Map)_poolHash.get( poolName ) ;
           if( link == null )
               throw new
               NoSuchElementException("pool not found : "+poolName ) ;

           tableMap = (Map)link.remove( groupClass+":"+group ) ;
           if( tableMap == null )
               throw new
               IllegalStateException("not found in link map : "+groupClass+":"+group);
           //
           // here we should check if both table maps are the same. But we wouldn't know
           // what to do if not.
           //
           // clear the possible content 
           //
           tableMap.remove( poolName ) ;
           
           return ;
       }
       //
       //   NOT FINISHED YET
       //
       public synchronized void removePoolGroup( String className , String groupName ){
           //
           // first remove pool group from poolGroupClass hash
           //
           Map groupMap = (Map)_poolGroupClassHash.get(className);
           if( groupMap == null )
               throw new
               NoSuchElementException("not found : "+className ) ;
           
           Map tableMap = (Map)groupMap.remove(groupName);
           if( tableMap == null )
               throw new
               NoSuchElementException("not found : "+groupName ) ;
           
           String d = className+":"+groupName ;
           
           for( Iterator pools = _poolHash.entrySet().iterator() ;pools.hasNext() ; ){
               
               Map.Entry entry    = (Map.Entry)pools.next() ;
               String    poolName = entry.getKey().toString() ;
               Map       link     = (Map)entry.getValue() ;

               for( Iterator domains = link.entrySet().iterator() ; domains.hasNext() ; ){
                   
                   Map.Entry domain     = (Map.Entry)domains.next() ;
                   String    domainName = (String)domain.getKey() ;
                   Map       table      = (Map)domain.getValue() ;
                   
               }
           }
           
       }
       private class PatternEntry {
           private Map linkMap     = new HashMap() ;
           private Pattern pattern = null ;
           private PatternEntry( Pattern pattern ){ 
               this.pattern = pattern ; 
           }
           public String toString(){
               return pattern.pattern()+" "+linkMap.toString() ;
           }
       }
       public synchronized void addPattern( String groupClass , String group , String patternName , String pattern ){
           
           Map poolGroupMap = (Map)_poolGroupClassHash.get( groupClass ) ;
           if( poolGroupMap == null )_poolGroupClassHash.put( groupClass , poolGroupMap = new HashMap() ) ;
           
           Map table = (Map)poolGroupMap.get( group ) ;
           if( table == null )poolGroupMap.put( group , table = new HashMap() ) ;
           
           PatternEntry patternEntry = (PatternEntry)_patternHash.get( patternName ) ;
        
           if( patternEntry == null ){
               if( pattern == null )
                   throw new
                   IllegalArgumentException("patterName is new, so we need pattern" ) ;
               
               _patternHash.put( patternName , patternEntry = new PatternEntry( Pattern.compile(pattern) ) ) ;
           }else{
               if( pattern != null ){
                   if( ! patternEntry.pattern.pattern().equals(pattern) )       
                       throw new
                       IllegalArgumentException("Conflict in pattern (name in use with different pattern)");
               }
           }
           
           Map link = patternEntry.linkMap ;
                      
           link.put( groupClass+":"+group , table ) ;
           
       }
       public synchronized void removePattern( String groupClass , String group , String patternName ){
           Map poolGroupMap = (Map)_poolGroupClassHash.get( groupClass ) ;
           if( poolGroupMap == null )
               throw new
               NoSuchElementException("groupClass not found : "+groupClass);
           Map tableMap = (Map)poolGroupMap.get( group ) ;
           if( tableMap == null )
               throw new
               NoSuchElementException("group not found : "+group);
           //
           //
           // now get the table map from the poolHash side
           //
           PatternEntry patternEntry = (PatternEntry)_patternHash.get( patternName ) ;
           if( patternEntry == null )
               throw new
               NoSuchElementException("patternName not found : "+patternName ) ;

           Map link = (Map)patternEntry.linkMap ;
           
           tableMap = (Map)link.remove( groupClass+":"+group ) ;
           if( tableMap == null )
               throw new
               IllegalStateException("not found in link map : "+groupClass+":"+group);
           
           // if( link.size() == 0 )_patternHash.remove( patternName ) ;
           //
           // here we should check if both table maps are the same. But we wouldn't know
           // what to do if not.
           //
           // clear the possible content 
           //
           List toBeRemoved = new ArrayList() ;
           Iterator it = tableMap.keySet().iterator() ;
           while( it.hasNext() ){
               String poolName = (String)it.next() ;
               if( patternEntry.pattern.matcher( poolName ).matches() )toBeRemoved.add(poolName) ;
           }
           for( it = toBeRemoved.iterator() ; it.hasNext() ; )tableMap.remove( it.next() ) ;
           
           return ;
       }
       public synchronized String getInfo(){
           StringBuffer sb = new StringBuffer() ;
           
           for( Iterator classes = _poolGroupClassHash.entrySet().iterator()  ; classes.hasNext() ; ){
               Map.Entry entry = (Map.Entry)classes.next() ;
               
               String className = (String)entry.getKey() ;
               Map    groupMap  = (Map)entry.getValue() ;
               
               sb.append("Class : ").append(className).append("\n");

               for( Iterator groups = groupMap.entrySet().iterator( ) ; groups.hasNext() ; ){
                   
                   
                   Map.Entry groupEntry = (Map.Entry)groups.next() ;
                   String groupName = (String)groupEntry.getKey() ;
                   Map    tableMap  = (Map)groupEntry.getValue() ;
                   
                   sb.append("   Group : ").append(groupName).append("\n");

                   printTable(sb,"            ",tableMap) ;
               }
           }
           
           sb.append("PoolHash :\n");
           for( Iterator pools = _poolHash.entrySet().iterator() ;pools.hasNext() ; ){
               
               Map.Entry entry    = (Map.Entry)pools.next() ;
               String    poolName = entry.getKey().toString() ;
               Map       link     = (Map)entry.getValue() ;

               sb.append("  ").append(poolName).append("\n");
               
               for( Iterator domains = link.entrySet().iterator() ; domains.hasNext() ; ){
                   
                   Map.Entry domain     = (Map.Entry)domains.next() ;
                   String    domainName = (String)domain.getKey() ;
                   Map       table      = (Map)domain.getValue() ;
                   
                   sb.append("     ").append(domainName).append("\n");

                   printTable( sb , "           " , table ) ;
               }
           }
           sb.append("Pattern List :\n");
           for( Iterator pools = _patternHash.entrySet().iterator() ;pools.hasNext() ; ){
               
               Map.Entry    entry        = (Map.Entry)pools.next() ;
               String       patternName  = entry.getKey().toString() ;
               PatternEntry patternEntry = (PatternEntry)entry.getValue() ;
               Pattern      pattern      = patternEntry.pattern ;
               Map          link         = patternEntry.linkMap ;

               sb.append("  ").append(patternName).append("(").append(pattern.pattern()).append(")").append("\n");
               
               for( Iterator domains = link.entrySet().iterator() ; domains.hasNext() ; ){
                   
                   Map.Entry domain = (Map.Entry)domains.next() ;
                   String domainName = (String)domain.getKey() ;
                   Map    table      = (Map)domain.getValue() ;
                   
                   sb.append("     ").append(domainName).append("\n");

                   printTable( sb , "           " , table ) ;
               }
           }
           return sb.toString();
       }
       private void printTable( StringBuffer sb , String prefix , Map table ){
           for( Iterator tableEntries = table.entrySet().iterator() ; tableEntries.hasNext() ; ){

               Map.Entry tableEntry = (Map.Entry)tableEntries.next() ;
               String pn = (String)tableEntry.getKey() ;
               String tc = tableEntry.getValue().toString() ;

               sb.append(prefix).append(pn).append(" -> ").append(tc).append("\n");
           }
       }
       public synchronized Map createExternalTopologyMap(){
       
          Map allClasses   = new HashMap() ;
          Map currentClass = null ;
          Map currentGroup = null ;

          for( Iterator classes = this._poolGroupClassHash.entrySet().iterator()  ; classes.hasNext() ; ){

              Map.Entry entry = (Map.Entry)classes.next() ;

              String className = (String)entry.getKey() ;
              Map    groupMap  = (Map)entry.getValue() ;

              allClasses.put( className , currentClass = new HashMap() ) ;


              for( Iterator groups = groupMap.entrySet().iterator( ) ; groups.hasNext() ; ){


                  Map.Entry groupEntry = (Map.Entry)groups.next() ;
                  String    groupName  = (String)groupEntry.getKey() ;
                  Map       tableMap   = (Map)groupEntry.getValue() ;

                  currentClass.put( groupName , currentGroup = new HashMap() ) ;

                  for( Iterator poolNames = tableMap.keySet().iterator() ; poolNames.hasNext() ; ){
                       String poolName = poolNames.next().toString() ;
                       currentGroup.put( poolName , null ) ;
                  }

              }
          }
          return allClasses ;
       }
   }


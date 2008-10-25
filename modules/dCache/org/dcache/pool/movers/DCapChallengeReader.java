/*
 * $Id: DCapChallengeReader.java,v 1.1 2006-07-18 09:08:22 tigran Exp $
 */
package org.dcache.pool.movers;

import  java.io.*;
import  java.nio.*;
import  java.nio.channels.*;
import  java.net.*;

import diskCacheV111.util.DCapProrocolChallenge;

class DCapChallengeReader implements org.dcache.net.ChallengeReader 
{    
    public Object getChallenge(SocketChannel socketChannel) 
    {
        DCapProrocolChallenge challenge = null;
        
        /*
         *  verify connection
         *  4 byte session id
         *  4 bytes challange len
         *  challenge 
         * 
         */
        
        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024); 
            
            buffer.rewind();                
            buffer.limit(8);
            socketChannel.read(buffer);
            buffer.rewind();
            int sessionId = buffer.getInt();                
            int challangeLen = buffer.getInt();
            buffer.rewind();
            buffer.limit(challangeLen);
            socketChannel.read(buffer);
            buffer.rewind();
            byte[] recivedChallengeBase64 = new byte[challangeLen];                
            buffer.get(recivedChallengeBase64);        
                            
            byte[] recivedChallengeBase = diskCacheV111.util.Base64.base64ToByteArray(new String(recivedChallengeBase64));
                        
            challenge = new DCapProrocolChallenge(sessionId, recivedChallengeBase);
            
        }catch(Exception e) {
            // e.printStackTrace();
        }
                
        return challenge;
    }    
}


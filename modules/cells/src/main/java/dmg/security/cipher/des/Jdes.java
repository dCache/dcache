package dmg.security.cipher.des ;

import dmg.security.cipher.BlockCipher;

public class Jdes implements BlockCipher {

  private static byte bK_C[] = {
	57, 49, 41, 33, 25, 17,  9,
	 1, 58, 50, 42, 34, 26, 18,
	10,  2, 59, 51, 43, 35, 27,
	19, 11,  3, 60, 52, 44, 36,
  };
  private static byte bK_D[] = {
	63, 55, 47, 39, 31, 23, 15,
	 7, 62, 54, 46, 38, 30, 22,
	14,  6, 61, 53, 45, 37, 29,
	21, 13,  5, 28, 20, 12, 4,
  };
  private static byte preshift[] = {
	1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1,
  };
  private static byte bCD_KS[] = {
	14, 17, 11, 24,  1,  5,
	3,  28, 15,  6, 21, 10,
	23, 19, 12,  4, 26,  8,
	16,  7, 27, 20, 13,  2,
	41, 52, 31, 37, 47, 55,
	30, 40, 51, 45, 33, 48,
	44, 49, 39, 56, 34, 53,
	46, 42, 50, 36, 29, 32,
  };
  private static byte P[] = {
	16,  7, 20, 21,
	29, 12, 28, 17,
	 1, 15, 23, 26,
	 5, 18, 31, 10,
	 2,  8, 24, 14,
	32, 27,  3,  9,
	19, 13, 30,  6,
	22, 11,  4, 25,
  };
  private static byte S[][] ;

  private static byte St[] = {

	14, 4,13, 1, 2,15,11, 8, 3,10, 6,12, 5, 9, 0, 7,
	 0,15, 7, 4,14, 2,13, 1,10, 6,12,11, 9, 5, 3, 8,
	 4, 1,14, 8,13, 6, 2,11,15,12, 9, 7, 3,10, 5, 0,
	15,12, 8, 2, 4, 9, 1, 7, 5,11, 3,14,10, 0, 6,13,

	15, 1, 8,14, 6,11, 3, 4, 9, 7, 2,13,12, 0, 5,10,
	 3,13, 4, 7,15, 2, 8,14,12, 0, 1,10, 6, 9,11, 5,
	 0,14, 7,11,10, 4,13, 1, 5, 8,12, 6, 9, 3, 2,15,
	13, 8,10, 1, 3,15, 4, 2,11, 6, 7,12, 0, 5,14, 9,

	10, 0, 9,14, 6, 3,15, 5, 1,13,12, 7,11, 4, 2, 8,
	13, 7, 0, 9, 3, 4, 6,10, 2, 8, 5,14,12,11,15, 1,
	13, 6, 4, 9, 8,15, 3, 0,11, 1, 2,12, 5,10,14, 7,
	 1,10,13, 0, 6, 9, 8, 7, 4,15,14, 3,11, 5, 2,12,

	 7,13,14, 3, 0, 6, 9,10, 1, 2, 8, 5,11,12, 4,15,
	13, 8,11, 5, 6,15, 0, 3, 4, 7, 2,12, 1,10,14, 9,
	10, 6, 9, 0,12,11, 7,13,15, 1, 3,14, 5, 2, 8, 4,
	 3,15, 0, 6,10, 1,13, 8, 9, 4, 5,11,12, 7, 2,14,

	 2,12, 4, 1, 7,10,11, 6, 8, 5, 3,15,13, 0,14, 9,
	14,11, 2,12, 4, 7,13, 1, 5, 0,15,10, 3, 9, 8, 6,
	 4, 2, 1,11,10,13, 7, 8,15, 9,12, 5, 6, 3, 0,14,
	11, 8,12, 7, 1,14, 2,13, 6,15, 0, 9,10, 4, 5, 3,

	12, 1,10,15, 9, 2, 6, 8, 0,13, 3, 4,14, 7, 5,11,
	10,15, 4, 2, 7,12, 9, 5, 6, 1,13,14, 0,11, 3, 8,
	 9,14,15, 5, 2, 8,12, 3, 7, 0, 4,10, 1,13,11, 6,
	 4, 3, 2,12, 9, 5,15,10,11,14, 1, 7, 6, 0, 8,13,

	 4,11, 2,14,15, 0, 8,13, 3,12, 9, 7, 5,10, 6, 1,
	13, 0,11, 7, 4, 9, 1,10,14, 3, 5,12, 2,15, 8, 6,
	 1, 4,11,13,12, 3, 7,14,10,15, 6, 8, 0, 5, 9, 2,
	 6,11,13, 8, 1, 4,10, 7, 9, 5, 0,15,14, 2, 3,12,

	13, 2, 8, 4, 6,15,11, 1,10, 9, 3,14, 5, 0,12, 7,
	 1,15,13, 8,10, 3, 7, 4,12, 5, 6,11, 0,14, 9, 2,
	 7,11, 4, 1, 9,12,14, 2, 0, 6,10,13,15, 3, 5, 8,
	 2, 1,14, 7, 4,10, 8,13,15,12, 9, 0, 3, 5, 6,11,
  };
  private static int wC_K4[][], wC_K3[][];
  private static int wD_K4[][], wD_K3[][];
  private static int hKS_C4[][];
  private static int lKS_D4[][];
  private static int wL_I8[];
  private static int wO_L4[];
  private static int wPS[][];
  static {
     S = new byte [8] [] ;
     for( int i = 0 ; i < 8 ; i++ ) {
         S[i] = new byte[64];
     }

     for( int i = 0 ; i < 8 ; i++ ) {
         System.arraycopy(St, i * 64, S[i], 0, 64);
     }

     wC_K4  = new int [8] [] ;
     wC_K3  = new int [8] [] ;
     wD_K4  = new int [8] [] ;
     wD_K3  = new int [8] [] ;
     hKS_C4 = new int [7] [] ;
     lKS_D4 = new int [7] [] ;
     wPS    = new int [8] [] ;
     wL_I8  = new int [0x55 + 1] ;
     wO_L4  = new int [16] ;
     for( int i = 0 ; i < 8 ; i++ ){
        wC_K4[i]  = new int [16] ;
        wC_K3[i]  = new int [8] ;
        wD_K4[i]  = new int [16] ;
        wD_K3[i]  = new int [8] ;
        wPS[i]    = new int [64] ;
     }
     for( int i = 0 ; i < 7 ; i++ ){
        hKS_C4[i]  = new int [16] ;
        lKS_D4[i]  = new int [16] ;
     }

    int wC_K[]  = new int[64] ;
    int wD_K[]  = new int[64] ;
    int hKS_C[] = new int[28];
    int lKS_D[] = new int[28];
    int Smap[]  = new int[64];
    int wP[]    = new int[32];

     int v = 1;
     for( int j = 28; --j >= 0; ) {

         wC_K[ bK_C[j] - 1 ] = wD_K[ bK_D[j] - 1 ] = v;
         v += v;      /* (i.e. v <<= 1) */
      }

      for( int i = 0; i < 64; i++ ){
         int t = 8 >> (i & 3);
         for( int j = 0 ; j < 16 ; j++) {

            if( ( j & t) != 0 ) {
                 wC_K4[i >> 3][j] |= wC_K[i];
                 wD_K4[i >> 3][j] |= wD_K[i];
                 if( j < 8 ){
                     wC_K3[i >> 3][j] |= wC_K[i + 3];
                     wD_K3[i >> 3][j] |= wD_K[i + 3];
                 }
            }
          }
          /* Generate the sequence 0,1,2,3, 8,9,10,11, ..., 56,57,58,59. */
          if(t == 1) {
              i += 4;
          }
      }
//     System.out.println( " Array filled " + S[5][3] ) ;
     /* Invert permuted-choice-2 */

     v = 1;
     for( int i = 24; (i -= 6) >= 0; ) {
         int j = i+5;
//         do {
//		hKS_C[ bCD_KS[j] - 1 ] = lKS_D[ bCD_KS[j+24] - 28 - 1 ] = v;
//		v += v; 	/* Like v <<= 1 but may be faster */
//         } while(--j >= i);
         while( true ){
            hKS_C[ bCD_KS[j] - 1 ] = lKS_D[ bCD_KS[j+24] - 28 - 1 ] = v;
            v += v;        /* Like v <<= 1 but may be faster */
            if( -- j < i ) {
                break;
            }
         }
         v <<= 2;		/* Keep byte aligned */
     }

     for( int i = 0; i < 28; i++) {
        v = 8 >> (i & 3);
        for(int j = 0; j < 16; j++) {
           if( ( j & v ) != 0 ) {
              hKS_C4[i >> 2][j] |= hKS_C[i];
              lKS_D4[i >> 2][j] |= lKS_D[i];
           }
        }
     }

     /* Initial permutation */

     for( int i = 0; i <= 0x55; i++) {
         v = 0;
         if( ( i & 64) != 0 ) {
             v = 1 << 24;
         }
         if( ( i & 16) != 0 ) {
             v |= 1 << 16;
         }
         if( ( i & 4) != 0 ) {
             v |= 1 << 8;
         }
         if( ( i & 1) != 0 ) {
             v |= 1;
         }
         wL_I8[i] = v;
     }

     /* Final permutation */

     for( int i = 0; i < 16; i++) {
         v = 0;
         if( ( i & 1) != 0 ) {
             v = 1 << 24;
         }
         if( ( i & 2) != 0 ) {
             v |= 1 << 16;
         }
         if( ( i & 4) != 0 ) {
             v |= 1 << 8;
         }
         if( ( i & 8) != 0 ) {
             v |= 1;
         }
         wO_L4[i] = v;
     }

     /* Funny bit rearrangement on second index into S tables */

     for( int i = 0; i < 64; i++) {
         Smap[i] = (i & 0x20) | (i & 1) << 4 | (i & 0x1e) >> 1;
     }

     /* Invert permutation P into mask indexed by R bit number */

     v = 1;
     for( int i = 32; --i >= 0; ) {
         wP[ P[i] - 1 ] = v;
         v += v;
     }

     /* Build bit-mask versions of S tables, indexed in natural bit order */

     for( int i = 0; i < 8; i++) {
         for( int j = 0; j < 64; j++) {

              int t = S[i][ Smap[j] ];
              for( int k = 0; k < 4; k++) {
                  if( ( t & 8) != 0 ) {
                      wPS[i][j] |= wP[4 * i + k];
                  }
                  t += t;
              }
         }
     }

  }
  //
  //   ks[0-15][0-1]
  //    0 = l
  //    1 = h
  //
  private void fsetkey( byte [] key, long [][] ks){

    int C = 0 ;
    int D = 0;
    for( int i = 0; i < 8; i++) {
       int v  = key[i] >>> 1;	/* Discard "parity" bit */
       v = v < 0 ? 256 + v : v ;
//       System.out.println( " Key "+i+" -> "+key[i]+" "+v ) ;
       C |= wC_K4[i][(v>>3) & 15] | wC_K3[i][v & 7];
       D |= wD_K4[i][(v>>3) & 15] | wD_K3[i][v & 7];
    }
//    System.out.println( " C : "+C+" ; D : "+D ) ;
    //
    // C and D now hold the suitably right-justified
    //* 28 permuted key bits each.
    //
    for( int i = 0; i < 16; i++) {

         /* 28-bit left circular shift */
         C <<= preshift[i];
         C = ((C >> 28) & 3) | (C & ((1<<28) - 1));
         ks[i][1] =
             hKS_C4[6][(C&15)] |
             hKS_C4[5][((C>>4)&15)]  | hKS_C4[4][((C>>8)&15)]  |
             hKS_C4[3][((C>>12)&15)] | hKS_C4[2][((C>>16)&15)] |
             hKS_C4[1][((C>>20)&15)] | hKS_C4[0][((C>>24)&15)]  ;

//         System.out.println( " ks[i][1] : "+ks[i][1] ) ;
         D <<= preshift[i];
         D = ((D >> 28) & 3) | (D & ((1<<28) - 1));
         ks[i][0] =
             lKS_D4[6][(D&15)] |
             lKS_D4[5][((D>>4)&15)]  | lKS_D4[4][((D>>8)&15)]  |
             lKS_D4[3][((D>>12)&15)] | lKS_D4[2][((D>>16)&15)] |
             lKS_D4[1][((D>>20)&15)] | lKS_D4[0][((D>>24)&15)]  ;
//         System.out.println( " ks[i][0] : "+ks[i][0] ) ;
     }
//     for( int i = 0 ; i < ks.length ; i++ ){
//        for( int j = 0 ; j < ks[i].length ; j++ ){
//            System.out.println( "ks["+i+"]["+j+"]="+Long.toHexString( ks[i][j] ) + " ") ;
//       }
//        System.out.println("");
//     }
  }
  public static void say( String str ){
     System.out.println( str ) ;
  }
  private void fencrypt(byte [] block , boolean decrypt, long [][] ks){
     fencrypt( block , 0 , block , 0 , decrypt , ks ) ;
  }
  private void fencrypt(byte [] block  , int blockOff ,
                        byte [] out    , int outOff ,
                        boolean decrypt, long [][] ks     ){

     long   L , R;
     long  [] ksp;

     /* Initial permutation */

     L  = R = 0;

     for( int i = 7+blockOff ; i >= blockOff ; i-- ){

        int v = block[i]  ;
        v = v < 0 ? 256 + v : v ;
        L = wL_I8[v & 0x55]        | (L << 1);
        R = wL_I8[(v >> 1) & 0x55] | (R << 1);

     }
     R &= 0xffffffffL ;
     L &= 0xffffffffL ;

//     say( " initial R : "+Long.toHexString( R ) + " ; L : "+
//          Long.toHexString( L ) ) ;

     ksp = decrypt ? ks[15] : ks[0] ;

     for( int i = 15 ; i >= 0 ; i-- ){
        long k, tR;

        ksp = decrypt ? ks[i] : ks[15-i] ;

        tR = (R >> 15) | (R << 17);

        k = ksp[1];
        L ^=   wPS[0][(int)((tR >>> 12) ^ (k >>> 24)) & 63]
             | wPS[1][(int)((tR >>> 8)  ^ (k >>> 16)) & 63]
             | wPS[2][(int)((tR >>> 4)  ^ (k >>> 8))  & 63]
             | wPS[3][(int)(tR ^ k)                 & 63];

        k = ksp[0];
        L ^=   wPS[4][(int)((R >>> 11)  ^ (k >>> 24)) & 63]
             | wPS[5][(int)((R >>> 7)   ^ (k >>> 16)) & 63]
             | wPS[6][(int)((R >>> 3)   ^ (k >>> 8))  & 63]
             | wPS[7][(int)((tR >>> 16) ^ k)         & 63];

        tR = L;
        L  = R  & 0xffffffffL ;
        R  = tR & 0xffffffffL ;

//        say( " round "+i+" R : "+Long.toHexString( R ) + " ; L : "+
//          Long.toHexString( L ) ) ;

     }
      long  t;

      t =  (wO_L4[(int)(L >> (0)) & 15 ] << 1 | wO_L4[(int)(R >> (0)) & 15 ]) |
          ((wO_L4[(int)(L >> (8)) & 15 ] << 1 | wO_L4[(int)(R >> (8)) & 15 ]) |
          ((wO_L4[(int)(L >> (16)) & 15 ] << 1 | wO_L4[(int)(R >> (16)) & 15 ]) |
          ((wO_L4[(int)(L >> (24)) & 15 ] << 1 | wO_L4[(int)(R >> (24)) & 15 ])
          << 2)) << 2) << 2;

      R =  (wO_L4[(int)(L >> (4)) & 15 ] << 1 | wO_L4[(int)(R >> (4)) & 15 ]) |
          ((wO_L4[(int)(L >> (12)) & 15 ] << 1 | wO_L4[(int)(R >> (12)) & 15 ]) |
          ((wO_L4[(int)(L >> (20)) & 15 ] << 1 | wO_L4[(int)(R >> (20)) & 15 ]) |
          ((wO_L4[(int)(L >> (28)) & 15 ] << 1 | wO_L4[(int)(R >> (28)) & 15 ])
          << 2)) << 2) << 2;

      L = t & 0xffffffffL ;
      t = R & 0xffffffffL ;


      long lt ;

      lt = t & 255 ;
      out[outOff+7] = (byte)( lt > 127 ? lt - 256 : lt ) ;
      lt = (t >>= 8) & 255;
      out[outOff+6] = (byte)( lt > 127 ? lt - 256 : lt ) ;
      lt = (t >>= 8) & 255;
      out[outOff+5] = (byte)( lt > 127 ? lt - 256 : lt ) ;
      lt = (t >>  8) & 255;
      out[outOff+4] = (byte)( lt > 127 ? lt - 256 : lt ) ;
      t = L;
      lt = t & 255 ;
      out[outOff+3] = (byte)( lt > 127 ? lt - 256 : lt ) ;
      lt = (t >>= 8) & 255;
      out[outOff+2] = (byte)( lt > 127 ? lt - 256 : lt ) ;
      lt = (t >>= 8) & 255;
      out[outOff+1] = (byte)( lt > 127 ? lt - 256 : lt ) ;
      lt = (t >>  8) & 255;
      out[outOff+0] = (byte)( lt > 127 ? lt - 256 : lt ) ;

  }
  //
  //    object part
  //
  private long [][] _keySchedule ;
  private byte []   _keyBytes ;

  public Jdes( byte [] key ){
     if( key.length < 8 ) {
         throw new IllegalArgumentException("key too short");
     }

     _keyBytes = new byte[8] ;
     System.arraycopy( key , 0 , _keyBytes , 0 , 8 ) ;

     _keySchedule = getKeySchedule( key ) ;

  }
  private long [][] getKeySchedule( byte [] key ){

     long [] [] ks = new long [16] [] ;
     for( int i = 0 ; i < 16 ; i++ ) {
         ks[i] = new long[2];
     }

     fsetkey( key , ks ) ;
     return ks ;

  }
  //
  // the BlockCipher interface definition
  //
  @Override
  public int getBlockLength(){  return 8*8 ; }

  @Override
  public byte [] getKeyBytes(){ return _keyBytes ; }

  @Override
  public void encrypt( byte [] inBlock , int inOff , byte [] outBlock , int outOff ){
     fencrypt( inBlock , inOff , outBlock , outOff , false, _keySchedule ) ;
  }
  @Override
  public void decrypt( byte [] inBlock , int inOff , byte [] outBlock , int outOff ){
     fencrypt( inBlock , inOff , outBlock , outOff , true, _keySchedule ) ;
  }
  public void encrypt( byte [] inBlock , byte [] outBlock ){
     fencrypt( inBlock , 0 , outBlock , 0 , false, _keySchedule ) ;
  }
  public void decrypt( byte [] inBlock , byte [] outBlock ){
     fencrypt( inBlock , 0 , outBlock , 0 , true, _keySchedule ) ;
  }
  public void encrypt( byte [] block ){
     fencrypt( block , 0 , block , 0 , false, _keySchedule ) ;
  }

  public void decrypt( byte [] block ){
     fencrypt( block , 0 ,  block ,  0 , true, _keySchedule ) ;
  }

  private void printKeySchedule(){
     byte  [] out = new byte[8] ;

     for( int i = 0 ; i < 16 ; i++ ){
        twoIntsToEightBytes( _keySchedule[i][1] ,
                             _keySchedule[i][0] , out ) ;
        System.out.println( "keys["+i+"]-> "+byteToHexString( out ) ) ;
     }
  }


  static long [] _x1 = {
    0, 0x00000000 , 0x00000000 , 0x00000000 , 0x00000000 , 0x8ca64de9 , 0xc1b123a7 };
  static long [] _x2 = {
    0, 0x11111111 , 0x11111111 , 0x00000000 , 0x00000000 , 0x82e13665 , 0xb4624df5 };
  static long [] _x3 = {
    0, 0x11111111 , 0x11111111 , 0x00000000 , 0x00000000 , 0x82e13665 , 0xb4624df5 };
  static long [] _x4 = {
    1, 0x12486248 , 0x62486248 , 0xf0e1d2c3 , 0xb4a59687 , 0xdf597e0f , 0x84fd994f };
  static long [] _x5 = {
    0, 0x1bac8107 , 0x6a39042d , 0x812801da , 0xcbe98103 , 0xd8883b2c , 0x4a7c61dd };
  static long [] _x6 = {
    1, 0x1bac8107 , 0x6a39042d , 0xd8883b2c , 0x4a7c61dd , 0x812801da , 0xcbe98103 };
  static long [] _x7 = {
    1, 0xfedcba98 , 0x76543210 , 0xa68cdca9 , 0x0c9021f9 , 0x00000000 , 0x00000000 };
  static long [] _x8 = {
    0, 0xeca86420 , 0x13579bdf , 0x01234567 , 0x89abcdef , 0xa8418a54 , 0xff97a505 };

  static long [] [] _x = { _x1 , _x2, _x3, _x4, _x5, _x6, _x7, _x8 } ;

  public static void main( String [] args ){

     byte [] out = new byte[8] ;
     byte [] key = new byte[8] ;
     byte [] in  = new byte[8] ;

      for (long[] ar : _x) {
          boolean decrypt = ar[0] > 0;
          twoIntsToEightBytes(ar[1], ar[2], key);
          twoIntsToEightBytes(ar[3], ar[4], in);
          twoIntsToEightBytes(ar[5], ar[6], out);

          Jdes des = new Jdes(key);

          System.out.println("\n\n Key " + byteToHexString(key));
          System.out.println(" In  " + byteToHexString(in));
          if (decrypt) {
              des.decrypt(in);
          } else {
              des.encrypt(in);
          }
          System.out.println(" Out " + byteToHexString(in));
          System.out.println(" Exp " + byteToHexString(out));

      }
  }
  private static void twoIntsToEightBytes( long high , long low , byte [] out ){
      int s = 24 ;
      for( int i = 0 ; i < 4 ; i++ , s-=8 ){
         out[i] = (byte)( ( high >>> s ) & 0xff ) ;
      }
      s = 24 ;
      for( int i = 4 ; i < 8 ; i++ , s-=8 ){
         out[i] = (byte)( ( low  >>> s ) & 0xff ) ;
      }
//      System.out.println( " Converted : "+byteToHexString( out ) ) ;
  }
  static public String byteToHexString( byte b ) {
       String s = Integer.toHexString( ( b < 0 ) ? ( 256 + (int)b ) : (int)b  ) ;
       if( s.length() == 1 ) {
           return "0" + s;
       } else {
           return s;
       }
  }
  static public String byteToHexString( byte [] bytes ) {

	  StringBuilder sb = new StringBuilder(bytes.length +1);

      for (byte aByte : bytes) {
          sb.append(byteToHexString(aByte)).append(" ");
      }
       return sb.toString() ;
  }
}

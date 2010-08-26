#!/bin/sh

awk '

BEGIN {
  max=0;
  min=0;
  count=0;
  sum=0;
}

{ 

 a=$1/$4;
 if(a > max) {
    max = a;
 }


 if(count == 0) {
    min = a;
 }

 if( a < min ) {
   min = a;
 }

 count++;
 sum = sum + a;

}

END {
  printf("\n");
  printf("\tNumber of concurent processes %d\n", count);
  printf("\tMax = %dK\n", max/1024);
  printf("\tMin = %dK\n", min/1024);
  printf("\tAvarage = %dK\n", (sum/count)/1024);
  printf("\n");

}

'

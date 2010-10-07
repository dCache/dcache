/*
 *   DCAP - dCache Access Protocol client interface
 *
 *   Copyright (C) 2000,2004 DESY Hamburg DMG-Division.
 *
 *   AUTHOR: Paul Millar <paul.millar@desy.de>
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 *
 */


/*
 * $Id: dccp.c,v 1.77 2007-02-22 10:19:46 tigran Exp $
 */

#include <math.h>
#include <stdio.h>

#include "sigfig.h"

/* Largest difference between two numbers that are considered equal */
#define TOLERANCE 1e-10

/* Check whether two floating-point numbers only differ by some small amount */
#define TOLERABLY_EQUAL(A,B,EPSILON) (fabs(A-B) < EPSILON)

/* True if A > B or A and B differ by less than EPSILON */
#define GREATER_THAN_OR_TOLERABLY_EQUAL(A,B,EPSILON) ((A) > (B) || \
					 TOLERABLY_EQUAL(A,B,EPSILON))

typedef enum {
  value_greater_or_equal_to_one,
  value_greater_than_zero_less_than_one,
  value_zero
} value_class_t;

static double calc_round( double value, int sigfigs);
static int whole_digit_count( double value);
static int zeros_after_dp( double value);
static value_class_t identify_value_class( double value);

/**
 *  Print, into the supplied character array, the floating-point
 *  number value to sigfigs number of significant figures (s.f.).  A
 *  significant figure is a digit greater than 0.  The count starts
 *  from the left-most (most significant) digit, so leading zeros and
 *  zeros immediately after the decimal point are not significant.
 *
 *  Some examples:
 *               1 s.f.,  2 s.f.,   3 s.f.,    4 s.f.
 *    1.234     "1"       "1.2"     "1.23"     "1.234"
 *    0.001234  "0.001"   "0.0012"  "0.00123"  "0.001234"
 *    123.4     "100"     "120"     "123"      "123.4"
 *
 *  The number is rounded in the usual fashion: if the part of the
 *  number not printed is greater than half of the least-significant
 *  significant figure's place value then the least-significant digit
 *  is increased; for example, to 2 s.f, 1.24 is "1.2", 1.28 is "1.3"
 *  and 9.95 is "10".
 *
 *  Zero is treated as a special case.  It is printed as "0.0",
 *  independently of the value of sigfigs.
 *
 *  The overall size of the number may be limited by buffer_size.  The
 *  function is careful never to overrun the buffer and strlen(buffer)
 *  after calling this method is never more than (buffer_size-1).
 *
 *  The limited buffer size may prevent the result from containing all
 *  output needed to represent the floating-point number to the
 *  desired number of significant figures.  If this happens, the
 *  output is truncated so the initial symbols are present.
 *  Truncation can happen if the floating-point number is too small
 *  (underrun) or if the number is too large (overrun).
 *
 *  If truncation occures due to underrun then the output format
 *  degrades gracefully: the output will loose precision until there
 *  are only zeros (e.g. "0.00123", "0.0012", "0.001", "0.00").
 *
 *  If there is truncation due to overrun then the output may be
 *  grossly incorrect (e.g. "1234", "123", "12").  Therefore, it is
 *  important that buffer size is chosen so it can hold the largest
 *  number (with the desired number of significant figures).
 *
 *  Supplied arguments must satisfy the following constraints:
 *    value >= 0,
 *    sigfigs >= 1.
 *
 *  Example usage is:
 *
 *    char buffer[10];
 *    dc_print_with_sigfig( buffer, sizeof(buffer), 3, value);
 */
void dc_print_with_sigfig( char *buffer, size_t buffer_size, int sigfigs,
			   double value)
{
  int desired_width, trunc_desired_width;
  int whole_digits, trunc_whole_digits;
  int i;
  size_t max_index = buffer_size-1;
  value_class_t value_class;

  value += calc_round( value, sigfigs);

  value_class = identify_value_class(value);

  switch( value_class) {
  case value_greater_or_equal_to_one:
    whole_digits = whole_digit_count(value);
    desired_width = (whole_digits >= sigfigs) ? whole_digits : sigfigs+1;
    break;

  case value_greater_than_zero_less_than_one:
    desired_width = 2 + zeros_after_dp(value) + sigfigs;
    break;

  default:
  case value_zero:
    desired_width = 3;
  }

  snprintf( buffer, buffer_size, "%f", value);

  trunc_desired_width = (desired_width < max_index) ? desired_width : max_index;
  buffer[trunc_desired_width] = '\0';

  if( value_class == value_greater_or_equal_to_one) {
    trunc_whole_digits = (whole_digits < max_index) ? whole_digits : max_index;
    for( i = sigfigs; i < trunc_whole_digits; i++) {
      buffer[i] = '0';
    }
  }
}


/* The sig-fig algorithm depends on what kind of number we've been
 * given to process.  There are three classes: 1 <= x, 0 < x < 1, and
 * x == 0.  This function classifies a number into one of these three
 * classes. */
value_class_t identify_value_class( double value)
{
  value_class_t identified_class;

  if( GREATER_THAN_OR_TOLERABLY_EQUAL(value,1,TOLERANCE)) {
    identified_class = value_greater_or_equal_to_one;
  } else if( value > 0) {
    identified_class = value_greater_than_zero_less_than_one;
  } else {
    identified_class = value_zero;
  }

  return identified_class;
}


/* Calculate what number to add to value to correctly round-up */
double calc_round( double value, int sigfigs)
{
  int i, whole_digits, count;
  double round=0.5;

  switch( identify_value_class(value)) {
  case value_greater_or_equal_to_one:
    whole_digits = whole_digit_count(value);

    for( i = 0; i < whole_digits-sigfigs; i++) {
      round *= 10;
    }

    for( i = 0; i < sigfigs-whole_digits; i++) {
      round /= 10;
    }
    break;

  case value_greater_than_zero_less_than_one:
    count = zeros_after_dp(value)+sigfigs;

    for( i = 0; i < count; i++) {
      round /= 10;
    }
    break;

  default:
  case value_zero:
    round = 0;
    break;
  }

  return round;
}


/* Return number of digits making up the whole part of the number;
 * e.g., 1.301 --> 1, 3.2 --> 1, 40.2 --> 2, etc.  Only call this
 * function if value has class value_greater_or_equal_to_one.
 */
int whole_digit_count( double value)
{
  int digits = 1;
  double dummy;

  for(dummy = value;
      GREATER_THAN_OR_TOLERABLY_EQUAL(dummy,10,TOLERANCE);
      dummy /= 10) {
    digits++;
  }

  return digits;
}


/* Return number of zeros after the decimal point.  Only call this
 * function if value has class value_greater_than_zero_less_than_one
 */
int zeros_after_dp( double value)
{
  int digits = 0;
  double dummy;

  for( dummy = value; dummy < 0.1; dummy *= 10) {
    digits++;
  }

  return digits;
}

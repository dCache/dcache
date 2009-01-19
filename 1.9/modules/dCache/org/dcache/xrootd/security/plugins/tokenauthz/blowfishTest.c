/*
 * 
 *	OpenSSL Blowfish Test
 *  
 *  This is a blowfish demo (using CBC mode, standard PCKS5 Padding, generated key with speceified length 
 *  and an init vector). It does a full encrypt/decrypt cycle and binary-compares the resulting data 
 *  with the input data. Plaintext, key, iv and encrypted buffers are all written to disk.
 *  
 *  compile: 'gcc -lcrypto -o blowfishTest blowfishTest.c' 
 * 
 */ 

#include <stdio.h>
#include <time.h>

#include <openssl/evp.h>

#define	RAND_MAX	2147483647
#define ENCRYPT 1
#define DECRYPT 0

static int keylength = 56; // blowfish keylength in byte
static int ivlength = 8;	//blowfish init vector length
static int datalength = 80;	//length of the plaintext data to be encrypted


int openssl_bf_cbc(const unsigned char* in, unsigned char* out, const int datalength, const unsigned char* key, const unsigned char* iv, int enc);
void fillRandom(unsigned char* arr, int len);
void printArray(const char* name, const unsigned char* arry, int offset, int len);
void write2File(const char* filename, const unsigned char* data, int offset, int len);


int main ( int argc, char *argv[] )
{
	
	time_t ltime;
	time(&ltime);

	srand(ltime);
	
	unsigned char* key = (unsigned char*)malloc(keylength);;
	fillRandom(key,keylength);
	printArray("\nkey",key,0,keylength);
	write2File("BF_KEY",key,0,keylength);
  	
  	unsigned char* iv = (unsigned char*) malloc(ivlength);
	fillRandom(iv,ivlength);
  	printArray ("iv",iv,0,ivlength);
	write2File("BF_IV",iv,0,ivlength);
  	
	unsigned char* data = (unsigned char*) malloc(datalength);
  	fillRandom(data,datalength);
  	printArray("plain text",data,0,datalength);
	write2File("BF_PLAINTEXT",data,0,datalength);
	
	
	unsigned char* temp = (unsigned char*) malloc(datalength+EVP_MAX_BLOCK_LENGTH);
	int encryptedLength = openssl_bf_cbc(data, temp, datalength, key, iv, ENCRYPT);
	
	if (encryptedLength < datalength) {
		printf("error encrypted only %d bytes\n",encryptedLength); 	
	} else {
		printArray("\n\nencrypted data",temp,0,encryptedLength);
  		write2File("BF_ENCRYPTED",temp,0,encryptedLength);
			
	}
	
  	unsigned char* result = (unsigned char*) malloc(encryptedLength);  
  	int decryptedLength = openssl_bf_cbc(temp, result, encryptedLength, key, iv, DECRYPT); 
		
	if (decryptedLength < datalength) {
		printf("error decrypted only %d bytes\n",decryptedLength); 	
	} else {
		printArray("\ndecrypted data",result,0,datalength);
		
		int position;
		if (position = memcmp (data, result, datalength) == 0) {
			printf("\n\t!!! encyption-cycle successful !!!\n\n");
		} else {
			printf("\n\t!!! decrypted data differs from original data !!!\n\n");
		}	
	}	
}

// returns the number of actually transformed (encrypted or decrypted, respectively) bytes
int openssl_bf_cbc(const unsigned char* in, unsigned char* out, const int datalength, const unsigned char* key, const unsigned char* iv, int enc)
{
  	EVP_CIPHER_CTX ectx;

//	init cipher context
	EVP_CIPHER_CTX_init(&ectx);
//	set up cipher for blowfish except key and iv
	EVP_CipherInit_ex(&ectx, EVP_bf_cbc(), NULL, NULL, NULL, enc);
	EVP_CIPHER_CTX_set_key_length(&ectx, keylength);
//	now, after the keylength is known to the cipher context, set up key and iv
	EVP_CipherInit_ex(&ectx, NULL, NULL, key, iv, enc);
  	
  	int outbuflen1, outbuflen2;
  	
  	enc ? printf("\nencrypting..") :  printf("\ndecrypting..");
  	
  	EVP_CipherUpdate(&ectx, out, &outbuflen1, in, datalength);
   	out += outbuflen1;
   	printf("update: %d\n",outbuflen1);
   	
  	EVP_CipherFinal(&ectx, out, &outbuflen2);
  	printf("final: %d\n",outbuflen2);
  	
  	EVP_CIPHER_CTX_cleanup(&ectx);
  	
  	
  	
  	return outbuflen1 + outbuflen2;

}

void fillRandom(unsigned char* arr, int len)
{
	int i;
	for (i=0; i < len; i++) {
    	*(arr+i) = 1+(int) (255.0*rand()/(RAND_MAX+1.0));
	}
}

void printArray(const char* name, const unsigned char* arry, int offset, int len)
{
	printf("%s: ",name);
	int i;
	for (i = 0; i < len; i++) {
			int j = (int) (arry[offset + i] & 0xff);
			if (j < 16) {
				printf("0");
			}
			printf("%X",(int) (arry[offset + i] & 0xff));
	}
	printf(" (total: %d bytes)\n",len);
}

void write2File(const char* filename, const unsigned char* data, int offset, int len)
{
	FILE* fp = fopen(filename,"wb");
	printf("wrote %s to disk (%d bytes).\n", filename, fwrite(data,1,len,fp));
	fclose(fp);
}

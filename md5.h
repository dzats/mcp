#ifndef MD5_H
#define MD5_H
#include <stdio.h> /* for FILE */
#include <stdint.h> /* for uint32_t */

/*  The following tests optimise behaviour on little-endian
    machines, where there is no need to reverse the byte order
    of 32 bit words in the MD5 computation.  By default,
    HIGHFIRST is defined, which indicates we're running on a
    big-endian (most significant byte first) machine, on which
    the byteReverse function in md5.c must be invoked. However,
    byteReverse is coded in such a way that it is an identity
    function when run on a little-endian machine, so calling it
    on such a platform causes no harm apart from wasting time. 
    If the platform is known to be little-endian, we speed
    things up by undefining HIGHFIRST, which defines
    byteReverse as a null macro.  Doing things in this manner
    insures we work on new platforms regardless of their byte
    order.  */

#define HIGHFIRST

#ifdef __i386__
#undef HIGHFIRST
#endif

class MD5sum
{
	struct MD5Context
	{
    uint32_t buf[4];
    uint32_t bits[2];
    unsigned char in[64];
	};
	struct MD5Context ctx;
	void transform(uint32_t buf[4], uint32_t in[16]);
public:
	unsigned char signature[16];
	static void display_signature(FILE *stream,
		const unsigned char *signature);
	MD5sum();
	void update(void *buff, unsigned len);
	void final();
};


/*
 * This is needed to make RSAREF happy on some MS-DOS compilers.
 */
typedef struct MD5Context MD5_CTX;

/*  Define CHECK_HARDWARE_PROPERTIES to have main,c verify
    byte order and uint32_t settings.  */
#define CHECK_HARDWARE_PROPERTIES

#endif /* !MD5_H */


#define VER(a) #a " " __DATE__ " " __TIME__ ;
#define VERLONG(a) #a " " __DATE__ " " __TIME__ " CFLAGS=\"" __CFLAGS__ "\"";

const char *getDcapVersion()
{
	return VER( 1.9.3-5rc );
}

const char *getDcapVersionLong()
{
	return VERLONG( 1.9.3-5rc );
}


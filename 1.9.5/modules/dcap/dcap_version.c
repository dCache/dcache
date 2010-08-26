
#define VER(a) #a " " __DATE__ " " __TIME__ ;
#define VERLONG(a) #a " " __DATE__ " " __TIME__ " CFLAGS=\"" __CFLAGS__ "\"";

const char *getDcapVersion()
{
	return VER( version-1-2-42 );
}

const char *getDcapVersionLong()
{
	return VERLONG( version-1-2-42 );
}


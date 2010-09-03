 
/*
 * $Id: win32_libdl.c,v 1.2 2004-11-01 19:33:30 tigran Exp $
 */

#include <windows.h>
#include <stdio.h>

static char errbuf[512];

void *dlopen(const char *name, int mode)
{
  HINSTANCE hdll;

  hdll = LoadLibrary(name);
#ifdef _WIN32
  if (! hdll) {
    sprintf(errbuf, "error code %d loading library %s", GetLastError(), name);
    return NULL;
  }
#else
  if ((UINT) hdll < 32) {
    sprintf(errbuf, "error code %d loading library %s", (UINT) hdll, name);
    return NULL;
  }
#endif
  return (void *) hdll;
}

void *dlsym(void *lib, const char *name)
{
  HMODULE hdll = (HMODULE) lib;
  void *symAddr;
  symAddr = (void *) GetProcAddress(hdll, name);
  if (symAddr == NULL)
    sprintf(errbuf, "can't find symbol %s", name);
  return symAddr;
}

int dlclose(void *lib)
{
  HMODULE hdll = (HMODULE) lib;

#ifdef _WIN32
  if (FreeLibrary(hdll))
    return 0;
  else {
    sprintf(errbuf, "error code %d closing library", GetLastError());
    return -1;
  }
#else
  FreeLibrary(hdll);
  return 0;
#endif
}

char *dlerror()
{
  return errbuf;
}

/*
 * src/port/pthread-win32.h
 */
#ifndef __PTHREAD_H
#define __PTHREAD_H
#include <windows.h>
typedef ULONG pthread_key_t;
typedef CRITICAL_SECTION *pthread_mutex_t;
typedef int pthread_once_t;
typedef DWORD pthread_t;
typedef DWORD pthread_attr_t;
typedef DWORD pthread_mutexattr_t;

#define PTHREAD_MUTEX_INITIALIZER 0
#define PTHREAD_STACK_MIN 1024
#define PTHREAD_MUTEX_ERRORCHECK 0

extern pthread_t main_tid;

DWORD		pthread_self(void);

void		pthread_setspecific(pthread_key_t, void *);
void	   *pthread_getspecific(pthread_key_t);

int			pthread_mutex_init(pthread_mutex_t *, void *attr);
int			pthread_mutex_lock(pthread_mutex_t *);

int pthread_attr_init(pthread_attr_t*);
int pthread_attr_destroy(pthread_attr_t*);
int pthread_attr_setstacksize(pthread_attr_t*, int);

int pthread_mutexattr_init(pthread_mutex_t*);
int pthread_mutexattr_settype(pthread_mutex_t*, int);
/* blocking */
int			pthread_mutex_unlock(pthread_mutex_t *);

#endif

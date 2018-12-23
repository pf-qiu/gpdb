#ifndef __PTHREAD_H
#define __PTHREAD_H

#include <winsock2.h>
#include <windows.h>

#define PTHREAD_STACK_MIN 0

typedef DWORD pthread_t;
typedef DWORD pthread_attr_t;
int pthread_create(pthread_t *thread, const pthread_attr_t *attr,
	void *(*start_routine) (void *), void *arg);
int pthread_join(pthread_t thread, void **retval);
pthread_t pthread_self();
int pthread_kill(pthread_t thread, int sig);
void pthread_exit(void *retval);
void pthread_cleanup_push(void(*routine)(void *), void *arg);
void pthread_cleanup_pop(int execute);
int pthread_equal(pthread_t a, pthread_t b);

int pthread_attr_init(pthread_attr_t* attr);
int pthread_attr_destroy(pthread_attr_t* attr );
int pthread_attr_setstacksize(pthread_attr_t* attr, int v);

typedef SRWLOCK pthread_mutex_t;

#define PTHREAD_MUTEX_INITIALIZER {0}

int	pthread_mutex_init(pthread_mutex_t *, void *attr);
int	pthread_mutex_lock(pthread_mutex_t *);
int	pthread_mutex_unlock(pthread_mutex_t *);
int pthread_mutex_trylock(pthread_mutex_t *mutex);

typedef int pthread_mutexattr_t;

int pthread_mutexattr_init(pthread_mutexattr_t *attr);
int pthread_mutexattr_destroy(pthread_mutexattr_t *attr);
int pthread_mutexattr_gettype(const pthread_mutexattr_t *attr, int *type);
int pthread_mutexattr_settype(pthread_mutexattr_t *attr, int type);

#define PTHREAD_MUTEX_ERRORCHECK 0
#endif
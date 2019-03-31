/*
 * src/port/pthread-win32.h
 */

#include "pthread-gpdb-win32.h"

pthread_t		pthread_self(void)
{
	return GetCurrentThread();
}

int pthread_equal(pthread_t t1, pthread_t t2)
{
	if (GetThreadId(t1) == GetThreadId(t2))
		return 1;
	else
		return 0;
}

int pthread_create(pthread_t *thread, const pthread_attr_t *attr,
	void *(__stdcall *start_routine) (void *), void *arg)
{
	HANDLE h = CreateThread(0, 0, start_routine, arg, 0, 0);
	if (h == INVALID_HANDLE_VALUE)
		return 1;
	*thread = h;
	return 0;
}

int pthread_join(pthread_t thread, void **retval)
{
	WaitForSingleObject(thread, -1);
	GetExitCodeThread(thread, retval);
	return 0;
}

void		pthread_setspecific(pthread_key_t k, void *v)
{
}
void	   *pthread_getspecific(pthread_key_t k)
{
}

int			pthread_mutex_init(pthread_mutex_t *m, void *attr)
{
	InitializeCriticalSection(m);
	return 0;
}
int			pthread_mutex_lock(pthread_mutex_t *m)
{
	EnterCriticalSection(m);
	return 0;
}

/* blocking */
int			pthread_mutex_unlock(pthread_mutex_t *m)
{
	LeaveCriticalSection(m);
	return 0;
}

int			pthread_mutex_trylock(pthread_mutex_t *m)
{
	TryEnterCriticalSection(m);
	return 0;
}

int pthread_attr_init(pthread_attr_t* att)
{
	return 0;
}
int pthread_attr_destroy(pthread_attr_t* att)
{
	return 0;
}
int pthread_attr_setstacksize(pthread_attr_t* att, int s)
{
	return 0;
}

int pthread_mutexattr_init(pthread_mutexattr_t* att)
{
	return 0;
}

int pthread_mutexattr_settype(pthread_mutexattr_t* att, int s)
{
	return 0;
}
#include "pthread.h"
int pthread_create(pthread_t *thread, const pthread_attr_t *attr,
	void *(*start_routine) (void *), void *arg)
{
	LPTHREAD_START_ROUTINE routine = (LPTHREAD_START_ROUTINE)start_routine;
	HANDLE hThread = CreateThread(0, 0, routine, arg, 0, thread);
	CloseHandle(hThread);
	return 0;
}

int pthread_join(pthread_t thread, void **retval)
{
	HANDLE hThread = OpenThread(THREAD_ALL_ACCESS, 0, thread);
	if (hThread == INVALID_HANDLE_VALUE)
	{
		return EINVAL;
	}
	WaitForSingleObject(hThread, -1);
	LPDWORD p = (LPDWORD)retval;
	GetExitCodeThread(hThread, p);
	return 0;
}

int pthread_equal(pthread_t a, pthread_t b)
{
	if (a == b) return 1;
	return 0;
}

pthread_t pthread_self()
{
	return GetCurrentThreadId();
}

void pthread_exit(void *retval)
{

}

int pthread_kill(pthread_t thread, int sig)
{
	return 0;
}

void pthread_cleanup_push(void(*routine)(void *), void *arg)
{

}

void pthread_cleanup_pop(int execute)
{

}

int pthread_attr_init(pthread_attr_t* attr)
{
	return 0;
}

int pthread_attr_destroy(pthread_attr_t* attr)
{
	return 0;
}

int pthread_attr_setstacksize(pthread_attr_t* attr, int s)
{
	return 0;
}

int	pthread_mutex_init(pthread_mutex_t *m, void *attr)
{
	InitializeSRWLock(m);
	return 0;
}

int	pthread_mutex_lock(pthread_mutex_t *m)
{
	AcquireSRWLockExclusive(m);
	return 0;
}

int pthread_mutex_trylock(pthread_mutex_t *m)
{
	if (TryAcquireSRWLockExclusive(m))
	{
		return 0;
	}
	return 1;
}

int	pthread_mutex_unlock(pthread_mutex_t *m)
{
	ReleaseSRWLockExclusive(m);
	return 0;
}

int pthread_mutexattr_init(pthread_mutexattr_t *attr)
{
	return 0;
}
int pthread_mutexattr_destroy(pthread_mutexattr_t *attr)
{
	return 0;
}
int pthread_mutexattr_gettype(const pthread_mutexattr_t *attr, int *type)
{
	return 0;
}
int pthread_mutexattr_settype(pthread_mutexattr_t *attr, int type)
{
	return 0;
}
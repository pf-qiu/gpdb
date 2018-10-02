#ifndef __POLL_H
#define __POLL_H

#ifdef WIN32
#include <winsock2.h>
#define poll(fds, nfds, timeout) WSAPoll(fds, nfds, timeout)
#endif


#endif /* __POLL_H */
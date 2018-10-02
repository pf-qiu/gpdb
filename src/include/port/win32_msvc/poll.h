/* sys/poll.h

   Copyright 2000, 2001 Red Hat, Inc.

   This file is part of Cygwin.

   This software is a copyrighted work licensed under the terms of the
   Cygwin license.  Please consult the file "CYGWIN_LICENSE" for
   details. */

#ifndef __POLL_H
#define __POLL_H

#ifdef WIN32
#include <winsock2.h>
#define poll(fds, nfds, timeout) WSAPoll(fds, nfds, timeout)
#endif


#endif /* _SYS_POLL_H */
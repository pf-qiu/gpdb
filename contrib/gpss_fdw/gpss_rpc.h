#ifndef GPSS_RPC_H
#define GPSS_RPC_H

extern "C"
{
#include "c.h"
#include "lib/stringinfo.h"
}


void* create_gpss_stub(const char* address);
void delete_gpss_stub(void* p);
int64 gpssfdw_estimate_size(void* stub, const char* id);
bool gpssfdw_stream_data(void *p, const char *id, StringInfo str);

#endif
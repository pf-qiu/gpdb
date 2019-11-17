#ifndef GPSS_RPC_H
#define GPSS_RPC_H

void* create_gpss_stub(const char* address);
void delete_gpss_stub(void* p);
int64 gpssfdw_estimate_size(void* stub, const char* id);

#endif
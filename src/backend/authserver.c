#include <unistd.h>
#include <stdio.h>

#include "postgres.h"
#include "libpq/auth.h"
#include "libpq/hba.h"
#include "libpq/libpq.h"
#include "libpq/libpq-be.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#define MAXLISTEN 64
static int ListenSocket[MAXLISTEN];
const char *progname;

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        printf("Usage: %s listen_port\n", argv[0]);
        exit(1);
    }
    const char *datadir = getenv("MASTER_DATA_DIRECTORY");
    if (datadir == NULL)
    {
        printf("MASTER_DATA_DIRECTORY not set\n");
        exit(1);
    }
    progname = get_progname(argv[0]);
    int port;
    if (1 != sscanf(argv[1], "%d", &port))
    {
        printf("Invalid port: %s\n", argv[1]);
        exit(1);
    }
    MemoryContextInit();
    InitializeGUCOptions();
    SelectConfigFiles(datadir, progname);
    ChangeToDataDir();
    if (!load_hba())
    {
        printf("could not load pg_hba.conf\n");
        exit(1);        
    }
    int status = StreamServerPort(AF_UNSPEC, NULL, port, "/tmp", ListenSocket, MAXLISTEN);
    if (status != STATUS_OK)
    {
        printf("could not create listen socket\n");
        exit(1);
    }
    Port p;
    StreamConnection(ListenSocket[0], &p);
    ClientAuthentication(&p);
    return 0;
}
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
void BackendInitialize(Port *port);

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
    int portnum;
    if (1 != sscanf(argv[1], "%d", &portnum))
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
    for (int i = 0; i < MAXLISTEN; i++)
        ListenSocket[i] = -1;

    int status = StreamServerPort(AF_UNSPEC, NULL, portnum, "/tmp", ListenSocket, MAXLISTEN);
    if (status != STATUS_OK)
    {
        printf("could not create listen socket\n");
        exit(1);
    }
    Port *port = calloc(1, sizeof(Port));
    MyProcPid = getpid();
    MyStartTime = time(NULL);
    StreamConnection(ListenSocket[0], port);
    BackendInitialize(port);
    ClientAuthentication(port);
    ReadyForQuery(DestRemote);
    return 0;
}
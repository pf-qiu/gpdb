#include <curl/curl.h>
#include <string.h>
#include <stdlib.h>

static char curl_Error_Buffer[CURL_ERROR_SIZE];
static CURLM *multi_handle = 0;
static int in_multi_handle = 0;
const char *DataDir = ".";
const static int extssl_protocol = CURL_SSLVERSION_DEFAULT;
const static int extssl_verifycert = 1;
const static int extssl_verifyhost = 2;
const static int extssl_no_verifycert = 0;
const static int extssl_no_verifyhost = 0;
const char *extssl_cert = "gpfdists/client.crt";
const char *extssl_key = "gpfdists/client.key";
const char *extssl_ca = "gpfdists/root.crt";
const char *extssl_pass = NULL;
const char *extssl_crl = NULL;
static int extssl_libcurldebug = 1;

static int verify_gpfdists_cert = 0;
#define MAXPGPATH 256

char extssl_key_full[MAXPGPATH] = {0};
char extssl_cer_full[MAXPGPATH] = {0};
char extssl_cas_full[MAXPGPATH] = {0};

int readable_external_table_timeout = 0;

#define ERROR 1
#define LOG 2
#define INFO 3
static void elog(int c, const char *f, ...)
{
	if (c == ERROR)
	{
		exit(1);
	}
}

#define CURL_EASY_SETOPT(h, opt, val)                                       \
	do                                                                      \
	{                                                                       \
		int e;                                                              \
                                                                            \
		if ((e = curl_easy_setopt(h, opt, val)) != CURLE_OK)                \
			elog(ERROR, "internal error: curl_easy_setopt error (%d - %s)", \
				 e, curl_easy_strerror(e));                                 \
	} while (0)

char *pstrdup(char *str)
{
	return strdup(str);
}

static size_t
header_callback(void *ptr_, size_t size, size_t nmemb, void *userp)
{
	return size * nmemb;
}

static size_t
write_callback(char *buffer, size_t size, size_t nitems, void *userp)
{
	const int nbytes = size * nitems;
	return nbytes;
}

static int still_running = 1;

static int
fill_buffer(void *x, int want)
{
	fd_set fdread;
	fd_set fdwrite;
	fd_set fdexcep;
	int maxfd = 0;
	struct timeval timeout;
	int nfds = 0, e = 0;
	int timeout_count = 0;

	/* elog(NOTICE, "= still_running %d, bot %d, top %d, want %d",
	   file->u.curl.still_running, curl->in.bot, curl->in.top, want);
	*/

	/* attempt to fill buffer */
	while (still_running)
	{
		FD_ZERO(&fdread);
		FD_ZERO(&fdwrite);
		FD_ZERO(&fdexcep);

		/* set a suitable timeout to fail on */
		timeout.tv_sec = 5;
		timeout.tv_usec = 0;

		/* get file descriptors from the transfers */
		if (0 != (e = curl_multi_fdset(multi_handle, &fdread, &fdwrite, &fdexcep, &maxfd)))
		{
			elog(ERROR, "internal error: curl_multi_fdset failed (%d - %s)",
				 e, curl_easy_strerror(e));
		}

		if (maxfd <= 0)
		{
			elog(LOG, "curl_multi_fdset set maxfd = %d", maxfd);
			still_running = 0;
			break;
		}
		nfds = select(maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout);
	}

	if (still_running == 0)
	{
		elog(LOG, "quit fill_buffer due to still_running = 0");
	}

	return 0;
}

static int
check_response(CURL *curl, int *rc, char **response_string)
{
	long response_code;
	char *effective_url = NULL;
	char buffer[30];

	/* get the response code from curl */
	if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code) != CURLE_OK)
	{
		*rc = 500;
		*response_string = pstrdup("curl_easy_getinfo failed");
		return -1;
	}
	*rc = response_code;
	snprintf(buffer, sizeof buffer, "Response Code=%d", (int)response_code);
	*response_string = pstrdup(buffer);

	if (curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &effective_url) != CURLE_OK)
		return -1;
	if (effective_url == NULL)
		effective_url = "";

	if (!(200 <= response_code && response_code < 300))
	{
		if (response_code == 0)
		{
			long oserrno = 0;
			static char connmsg[64];

			/* get the os level errno, and string representation of it */
			if (curl_easy_getinfo(curl, CURLINFO_OS_ERRNO, &oserrno) == CURLE_OK)
			{
				if (oserrno != 0)
					snprintf(connmsg, sizeof connmsg, "error code = %d (%s)",
							 (int)oserrno, strerror((int)oserrno));
			}

			printf("connection with gpfdist failed\n");
		}
	}

	return 0;
}

static void
url_curl_fopen(char *url)
{

	int sz;
	int ip_mode;
	int e;
	CURL *handle;

	/* Reset curl_Error_Buffer */
	curl_Error_Buffer[0] = '\0';

	/* initialize a curl session and get a libcurl handle for it */
	if (!(handle = curl_easy_init()))
		elog(ERROR, "internal error: curl_easy_init failed");

	CURL_EASY_SETOPT(handle, CURLOPT_URL, url);

	CURL_EASY_SETOPT(handle, CURLOPT_VERBOSE, 0L /* FALSE */);

	/* set callback for each header received from server */
	CURL_EASY_SETOPT(handle, CURLOPT_HEADERFUNCTION, header_callback);

	/* 'file' is the application variable that gets passed to header_callback */
	//CURL_EASY_SETOPT(handle, CURLOPT_WRITEHEADER, 0);

	/* set callback for each data block arriving from server to be written to application */
	CURL_EASY_SETOPT(handle, CURLOPT_WRITEFUNCTION, write_callback);

	/* 'file' is the application variable that gets passed to write_callback */
	//CURL_EASY_SETOPT(handle, CURLOPT_WRITEDATA, file);

	CURL_EASY_SETOPT(handle, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);

	if (!multi_handle)
	{
		if (!(multi_handle = curl_multi_init()))
			elog(ERROR, "internal error: curl_multi_init failed");
	}

	elog(LOG, "trying to load certificates from %s", DataDir);

	/* curl will save its last error in curlErrorBuffer */
	CURL_EASY_SETOPT(handle, CURLOPT_ERRORBUFFER, curl_Error_Buffer);

	/* cert is stored PEM coded in file... */
	CURL_EASY_SETOPT(handle, CURLOPT_SSLCERTTYPE, "PEM");

	/* set the cert for client authentication */
	if (extssl_cert != NULL)
	{
		memset(extssl_cer_full, 0, MAXPGPATH);
		snprintf(extssl_cer_full, MAXPGPATH, "%s/%s", DataDir, extssl_cert);
		CURL_EASY_SETOPT(handle, CURLOPT_SSLCERT, extssl_cer_full);
	}

	/* set the key passphrase */
	if (extssl_pass != NULL)
		CURL_EASY_SETOPT(handle, CURLOPT_KEYPASSWD, extssl_pass);

	CURL_EASY_SETOPT(handle, CURLOPT_SSLKEYTYPE, "PEM");

	/* set the private key (file or ID in engine) */
	if (extssl_key != NULL)
	{
		memset(extssl_key_full, 0, MAXPGPATH);
		snprintf(extssl_key_full, MAXPGPATH, "%s/%s", DataDir, extssl_key);
		CURL_EASY_SETOPT(handle, CURLOPT_SSLKEY, extssl_key_full);
	}

	/* set the file with the CA certificates, for validating the server */
	if (extssl_ca != NULL)
	{
		memset(extssl_cas_full, 0, MAXPGPATH);
		snprintf(extssl_cas_full, MAXPGPATH, "%s/%s", DataDir, extssl_ca);

		CURL_EASY_SETOPT(handle, CURLOPT_CAINFO, extssl_cas_full);
	}

	/* set cert verification */
	CURL_EASY_SETOPT(handle, CURLOPT_SSL_VERIFYPEER,
					 (long)(verify_gpfdists_cert ? extssl_verifycert : extssl_no_verifycert));

	/* set host verification */
	CURL_EASY_SETOPT(handle, CURLOPT_SSL_VERIFYHOST,
					 (long)(verify_gpfdists_cert ? extssl_verifyhost : extssl_no_verifyhost));

	/* set protocol */
	CURL_EASY_SETOPT(handle, CURLOPT_SSLVERSION, extssl_protocol);

	/* disable session ID cache */
	CURL_EASY_SETOPT(handle, CURLOPT_SSL_SESSIONID_CACHE, 0);

	/* set debug */
	if (CURLE_OK != (e = curl_easy_setopt(handle, CURLOPT_VERBOSE, (long)extssl_libcurldebug)))
	{
		if (extssl_libcurldebug)
		{
			elog(INFO, "internal error: curl_easy_setopt CURLOPT_VERBOSE error (%d - %s)",
				 e, curl_easy_strerror(e));
		}
	}

	/*
	 * lets check our connection.
	 * start the fetch if we're SELECTing (GET request), or write an
	 * empty message if we're INSERTing (POST request)
	 */

	int response_code;
	char *response_string;

	if (CURLE_OK != (e = curl_multi_add_handle(multi_handle, handle)))
	{
		if (CURLM_CALL_MULTI_PERFORM != e)
			elog(ERROR, "internal error: curl_multi_add_handle failed (%d - %s)",
				 e, curl_easy_strerror(e));
	}
	in_multi_handle = 1;

	while (CURLM_CALL_MULTI_PERFORM ==
		   (e = curl_multi_perform(multi_handle, &still_running)))
		;

	if (e != CURLE_OK)
		elog(ERROR, "internal error: curl_multi_perform failed (%d - %s)",
			 e, curl_easy_strerror(e));

	/* read some bytes to make sure the connection is established */
	fill_buffer(0, 1);

	/* check the connection for GET request */
	if (check_response(handle, &response_code, &response_string))
	{
		printf("%d, %s\n", response_code, response_string);
	}
}

int main(int argc, char **argv)
{
	DataDir = argv[1];
	url_curl_fopen(argv[2]);
	return 0;
}
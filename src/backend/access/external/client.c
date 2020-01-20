/***************************************************************************
 *                                  _   _ ____  _
 *  Project                     ___| | | |  _ \| |
 *                             / __| | | | |_) | |
 *                            | (__| |_| |  _ <| |___
 *                             \___|\___/|_| \_\_____|
 *
 * Copyright (C) 1998 - 2018, Daniel Stenberg, <daniel@haxx.se>, et al.
 *
 * This software is licensed as described in the file COPYING, which
 * you should have received as part of this distribution. The terms
 * are also available at https://curl.haxx.se/docs/copyright.html.
 *
 * You may opt to use, copy, modify, merge, publish, distribute and/or sell
 * copies of the Software, and permit persons to whom the Software is
 * furnished to do so, under the terms of the COPYING file.
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
 * KIND, either express or implied.
 *
 ***************************************************************************/
/* <DESC>
 * using the multi interface to do a single download
 * </DESC>
 */

#include <stdio.h>
#include <string.h>

/* somewhat unix-specific */
#include <sys/time.h>
#include <unistd.h>

/* curl stuff */
#include <curl/curl.h>

#ifdef _WIN32
#define WAITMS(x) Sleep(x)
#else
/* Portable sleep for platforms other than Windows. */
#define WAITMS(x)                        \
	struct timeval wait = {0, (x)*1000}; \
	(void)select(0, NULL, NULL, NULL, &wait);
#endif

const static int extssl_protocol = CURL_SSLVERSION_TLSv1;
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
static int verify_gpfdists_cert = 1;
#define MAXPGPATH 256
char extssl_key_full[MAXPGPATH] = {0};
char extssl_cer_full[MAXPGPATH] = {0};
char extssl_cas_full[MAXPGPATH] = {0};
static char curl_Error_Buffer[CURL_ERROR_SIZE];
char* DataDir;

#define CURL_EASY_SETOPT(h, opt, val) \
	do { \
		int			e; \
\
		if ((e = curl_easy_setopt(h, opt, val)) != CURLE_OK) \
			printf("internal error: curl_easy_setopt error (%d - %s)", \
				e, curl_easy_strerror(e)); \
	} while(0)

/*
 * Simply download a HTTP file.
 */
int main(int argc, char **argv)
{
	CURL *http_handle;
	CURLM *multi_handle;

	int still_running = 0; /* keep number of running handles */
	int repeats = 0;

	curl_global_init(CURL_GLOBAL_DEFAULT);

	http_handle = curl_easy_init();

	const char *url = argv[1];
	/* set the options (I left out a few, you'll get the point anyway) */
	curl_easy_setopt(http_handle, CURLOPT_URL, url);

	/* curl will save its last error in curlErrorBuffer */
	CURL_EASY_SETOPT(http_handle, CURLOPT_ERRORBUFFER, curl_Error_Buffer);

	/* cert is stored PEM coded in file... */
	CURL_EASY_SETOPT(http_handle, CURLOPT_SSLCERTTYPE, "PEM");

	/* set the cert for client authentication */
	if (extssl_cert != NULL)
	{
		memset(extssl_cer_full, 0, MAXPGPATH);
		snprintf(extssl_cer_full, MAXPGPATH, "%s/%s", DataDir, extssl_cert);
		CURL_EASY_SETOPT(http_handle, CURLOPT_SSLCERT, extssl_cer_full);
	}

	/* set the key passphrase */
	if (extssl_pass != NULL)
		CURL_EASY_SETOPT(http_handle, CURLOPT_KEYPASSWD, extssl_pass);

	CURL_EASY_SETOPT(http_handle, CURLOPT_SSLKEYTYPE, "PEM");

	/* set the private key (file or ID in engine) */
	if (extssl_key != NULL)
	{
		memset(extssl_key_full, 0, MAXPGPATH);
		snprintf(extssl_key_full, MAXPGPATH, "%s/%s", DataDir, extssl_key);
		CURL_EASY_SETOPT(http_handle, CURLOPT_SSLKEY, extssl_key_full);
	}

	/* set the file with the CA certificates, for validating the server */
	if (extssl_ca != NULL)
	{
		memset(extssl_cas_full, 0, MAXPGPATH);
		snprintf(extssl_cas_full, MAXPGPATH, "%s/%s", DataDir, extssl_ca);
		CURL_EASY_SETOPT(http_handle, CURLOPT_CAINFO, extssl_cas_full);
	}

	/* set cert verification */
	CURL_EASY_SETOPT(http_handle, CURLOPT_SSL_VERIFYPEER,
					 (long)(verify_gpfdists_cert ? extssl_verifycert : extssl_no_verifycert));

	/* set host verification */
	CURL_EASY_SETOPT(http_handle, CURLOPT_SSL_VERIFYHOST,
					 (long)(verify_gpfdists_cert ? extssl_verifyhost : extssl_no_verifyhost));

	/* set protocol */
	CURL_EASY_SETOPT(http_handle, CURLOPT_SSLVERSION, extssl_protocol);

	/* disable session ID cache */
	CURL_EASY_SETOPT(http_handle, CURLOPT_SSL_SESSIONID_CACHE, 0);

	/* init a multi stack */
	multi_handle = curl_multi_init();

	/* add the individual transfers */
	curl_multi_add_handle(multi_handle, http_handle);

	/* we start some action by calling perform right away */
	curl_multi_perform(multi_handle, &still_running);

	while (still_running)
	{
		CURLMcode mc; /* curl_multi_wait() return code */
		int numfds;

		/* wait for activity, timeout or "nothing" */
		mc = curl_multi_wait(multi_handle, NULL, 0, 1000, &numfds);

		if (mc != CURLM_OK)
		{
			fprintf(stderr, "curl_multi_wait() failed, code %d.\n", mc);
			break;
		}

		/* 'numfds' being zero means either a timeout or no file descriptors to
       wait for. Try timeout on first occurrence, then assume no file
       descriptors and no file descriptors to wait for means wait for 100
       milliseconds. */

		if (!numfds)
		{
			repeats++; /* count number of repeated zero numfds */
			if (repeats > 1)
			{
				WAITMS(100); /* sleep 100 milliseconds */
			}
		}
		else
			repeats = 0;

		curl_multi_perform(multi_handle, &still_running);
	}

	curl_multi_remove_handle(multi_handle, http_handle);

	curl_easy_cleanup(http_handle);

	curl_multi_cleanup(multi_handle);

	curl_global_cleanup();

	return 0;
}
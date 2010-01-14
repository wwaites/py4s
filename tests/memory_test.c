/*

Test program to reproduce memory leak in 4store client code.

Compile on OSX:

cc -g -Isrc -I/usr/local/include/rasqal -I/opt/local/include/glib-2.0 -I/opt/local/lib/glib-2.0/include -I/opt/local/include   -c tests/memory_test.c 
cc -o memory_test memory_test.o -Lbuild/lib.macosx-10.5-i386-2.6 -lpy4s -L/usr/local/lib -lraptor

Compile on Linux:

cc -g -Isrc -I/usr/include/rasqal `pkg-config glib-2.0 --cflags-only-I` -c tests/memory_test.c
cc -o memory_test memory_test.o build/lib.linux-x86_64-2.6/libpy4s.a -lrasqal -lraptor -lglib-2.0

 */
#include <stdio.h>
#include <stdlib.h>
#include <frontend/query.h>
#ifdef LINUX
#include <mcheck.h>
#endif

#define ASK_QUERY "ASK WHERE { ?s ?p ?o }"
#define SELECT_QUERY "SELECT * WHERE { ?s a ?o } LIMIT 1"
#define QUERY SELECT_QUERY

int main(int argc, char *argv[]) {
	fsp_link *link;
	fs_query_state *qs;
	fs_query *qr;
	raptor_uri *bu;
	int i;

#ifdef LINUX
	mtrace();
#endif

	link = fsp_open_link("ukgov_finances_cra", NULL, FS_OPEN_HINT_RW);
	raptor_init();
	fs_hash_init(fsp_hash_type(link));
	bu = raptor_new_uri((unsigned char *)"local:");
	fsp_no_op(link, 0);

	qs = fs_query_init(link);
	for (i=0;i<atoi(argv[1]);i++) {
		//printf("--------- %d ----------\n", i);
		qr = fs_query_execute(qs, link, bu, QUERY, 0, 3, 0);
		fs_query_free(qr);
		fs_query_cache_flush(qs, 0);
	}

	fs_query_fini(qs);
	raptor_free_uri(bu);
	raptor_finish();
	fsp_close_link(link);

#ifdef LINUX
	muntrace();
#endif
}

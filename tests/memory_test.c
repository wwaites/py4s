#include <stdio.h>
#include <frontend/query.h>

/*
 * cc -O2 -g -Isrc -I/usr/local/include/rasqal -I/opt/local/include/glib-2.0 -I/opt/local/lib/glib-2.0/include -I/opt/local/include   -c memory_test.c 
 * cc -o memory_test memory_test.o -L. libpy4s.dylib -L/usr/local/lib -lraptor
 */

int main(int argc, char *argv) {
	fsp_link *link;
	fs_query_state *qs;
	fs_query *qr;
	raptor_uri *bu;
	
	link = fsp_open_link("ukgov_finances_cra", NULL, FS_OPEN_HINT_RW);
	raptor_init();
	fs_hash_init(fsp_hash_type(link));
	bu = raptor_new_uri((unsigned char *)"local:");
	fsp_no_op(link, 0);

	qs = fs_query_init(link);
	for (;;) {
		qr = fs_query_execute(qs, link, bu, "ASK WHERE { ?s ?p ?o }", 0, 3, 0);
		fs_query_free(qr);
		fs_query_cache_flush(qs, 0);
	}
	/* never get here... */
	fs_query_fini(qs);
	raptor_free_uri(bu);
	raptor_finish();
}

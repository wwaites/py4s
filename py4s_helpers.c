#include <stdio.h>
#include <frontend/query.h>

void py4s_print_warnings(fs_query *);
int py4s_query_ask(fs_query *);

void py4s_print_warnings(fs_query *q) {
    if (q->warnings) {
        GSList *it;
        for (it = q->warnings; it; it = it->next) {
            if (it->data) {
                fprintf(stdout, "# %s\n", (char *)it->data);
            } else {
                fprintf(stdout, "# null warning\n");
            }
        }
        g_slist_free(q->warnings);
        q->warnings = NULL;
    }
}


int py4s_query_ask(fs_query *q) {
	return q->ask;
}
int py4s_query_bool(fs_query *q) {
	return q->boolean;
}

#include <stdio.h>
#include <frontend/query.h>
#include "py4s_helpers.h"

GSList *py4s_query_warnings(fs_query *q) {
    /* must free at the caller! */
    GSList *warnings = q->warnings;
    q->warnings = NULL;
    return warnings;
}
/*
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
*/

int py4s_query_ask(fs_query *q) {
	return q->ask;
}
int py4s_query_bool(fs_query *q) {
	return q->boolean;
}

#include <stdio.h>
#include <frontend/query.h>
#include "py4s_helpers.h"

GSList *py4s_query_warnings(fs_query *q) {
    /* must free at the caller! */
    GSList *warnings = q->warnings;
    q->warnings = NULL;
    return warnings;
}

int py4s_query_ask(fs_query *q) {
	return q->ask;
}
int py4s_query_bool(fs_query *q) {
	return q->boolean;
}
int py4s_query_construct(fs_query *q) {
	return q->construct;
}
rasqal_query *py4s_query_rasqal_query(fs_query *q) {
	return q->rq;
}

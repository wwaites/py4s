cdef extern from "common/datatypes.h":
	ctypedef unsigned long long int fs_rid
	ctypedef struct fs_rid_vector:
		pass
	fs_rid_vector *fs_rid_vector_new(int length)
	void fs_rid_vector_append(fs_rid_vector *v, fs_rid r)
	void fs_rid_vector_free(fs_rid_vector *v)

cdef extern from "common/4store.h":
	ctypedef struct fsp_link:
		pass
	ctypedef int fsp_hash_enum

	ctypedef int fs_segment

	fsp_link *fsp_open_link(char *name, char *pw, int ro)
	void fsp_close_link(fsp_link *link)
	fsp_hash_enum fsp_hash_type(fsp_link *link)
	int fsp_no_op(fsp_link *link, fs_segment segment)
	char *fsp_link_features(fsp_link *link)

	int fsp_delete_model_all(fsp_link *link, fs_rid_vector *mvec)

cdef extern from "common/hash.h":
	void fs_hash_init(fsp_hash_enum type)
	fs_rid (*fs_hash_uri)(char *str)

cdef extern from "raptor.h":
	ctypedef struct raptor_uri:
		pass
		
	raptor_uri *raptor_new_uri(unsigned char *uri)
	char *raptor_uri_as_string(raptor_uri *uri)

cdef extern from "frontend/query.h":
	ctypedef struct fs_query_state:
		pass
	ctypedef struct fs_query:
		pass
	fs_query_state *fs_query_init(fsp_link *link)
	void fs_query_fini(fs_query_state *qs)
	void fs_query_cache_flush(fs_query_state *qs, int verbose)
	fs_query *fs_query_execute(fs_query_state *qs, fsp_link *link,
				raptor_uri *bu, char *query, int flags,
				int opt_level, int soft_limit)
	void fs_query_free(fs_query *q)


cdef extern from "frontend/results.h":
	ctypedef int fs_result_type
	ctypedef struct fs_row:
		char		*name
		fs_rid		rid
		fs_result_type	type
		char		*lex
		char		*dt
		char		*lang
	int fs_query_get_columns(fs_query *q)
	bint fs_query_errors(fs_query *q)
	fs_row *fs_query_fetch_header_row(fs_query *q)
	fs_row *fs_query_fetch_row(fs_query *q)

cdef extern from "frontend/import.h":
	int fs_import_stream_start(fsp_link *link, char *model_uri, char *mimetype, int has_o_index, int *count)
	int fs_import_stream_data(fsp_link *link, unsigned char *data, size_t count)
	int fs_import_stream_finish(fsp_link *link, int *count, int *errors)

cdef extern from "glib/gslist.h":
	ctypedef struct GSList:
		char		*data
		GSList		*next
	void g_slist_free(GSList *list)

cdef extern from "py4s_helpers.h":
	GSList *py4s_query_warnings(fs_query *q)
	bint py4s_query_ask(fs_query *q)
	bint py4s_query_bool(fs_query *q)

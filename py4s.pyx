cimport py4s
try:
	from rdflib.term import URIRef, Literal, BNode, Identifier
	from rdflib.graph import Graph
except ImportError:
	from rdflib import URIRef, Literal, BNode, Identifier
	from rdflib.Graph import Graph

class FourStoreError(Exception):
	pass

cdef class FourStore:
	cdef py4s.fsp_link *_link
	cdef bytes _name
	cdef bytes _pw
	cdef int _ro
	def __cinit__(self, name, pw="", ro=0):
		self._name = name
		self._pw = pw
		self._ro = ro
	def __dealloc__(self):
		if self._link:
			py4s.fsp_close_link(self._link)
	def connect(self):
		self._link = py4s.fsp_open_link(self._name, self._pw, self._ro)
		if not self._link:
			raise FourStoreError("Connection Failed")
		hash_type = py4s.fsp_hash_type(self._link)
		py4s.fs_hash_init(hash_type)
	def noop(self, segment = 0):
		if not self._link:
			raise FourStoreError("Not Connected")
		return py4s.fsp_no_op(self._link, segment)
	def cursor(self):
		if not self._link:
			raise FourStoreError("Not Connected")
		return Cursor(self)

	def query(self, *av, **kw):
		return self.cursor.query(*av, **kw)

def _n3(s):
	return " ".join([x.n3() for x in s])

cdef class Cursor:
	cdef py4s.fsp_link *_link
	cdef py4s.fs_query_state *_qs
	cdef py4s.fs_query *_qr
	cdef bool _transaction
	cdef dict _features
	cpdef public list warnings

	def __cinit__(self, FourStore store):
		self._link = store._link
		self._qs = py4s.fs_query_init(store._link)
		features = py4s.fsp_link_features(store._link).strip().split(" ")
		self._features = dict([ (x, True) for x in features ])
	def __dealloc__(self):
		if self._qs:
			if self._qr:
				py4s.fs_query_free(self._qr)
			py4s.fs_query_fini(self._qs)

	def flush(self):
		py4s.fs_query_cache_flush(self._qs, 0)

	def execute(self, query, base_uri="local:", initNs={}, opt_level=3, soft_limit=0):
		if self._qr:
			py4s.fs_query_free(self._qr)
		if initNs:
			prefixes = []
			for x in initNs:
				prefixes.append("PREFIX %s: <%s>" % (x, initNs[x]))
			query = "\n".join(prefixes) + "\n" + query
		self.warnings = []
		cdef py4s.raptor_uri *bu = py4s.raptor_new_uri(base_uri)
		self._qr = py4s.fs_query_execute(self._qs, self._link, bu,
				query, 0, opt_level, soft_limit)
		return QueryResults(self)

	def delete_model(self, model=None, all=False):
		if not model and not all:
			raise FourStoreError("Nothing to Delete")
		if isinstance(model, Graph):
			model_uri = model.identifier
		else:
			model_uri = model
		cdef fs_rid_vector *mvec = py4s.fs_rid_vector_new(0)
		cdef fs_rid uri_hash
		if model_uri:
			uri_hash = py4s.fs_hash_uri(model_uri)
		else:
			uri_hash = 0x8000000000000000
		py4s.fs_rid_vector_append(mvec, uri_hash)
		py4s.fsp_delete_model_all(self._link, mvec)
		self.flush()

	def add(self, statement, model_uri="local:"):
		n = _n3(statement)
		q = "ASK WHERE { GRAPH <%s> " % model_uri + "{" + n + " } }"
		if not self.execute(q, model_uri):
			if not self._transaction:
				self.transaction(model_uri)
				transaction = True
			else:
				transaction = False
			data = n + " ."
			py4s.fs_import_stream_data(self._link, data, len(data))
			if transaction:
				self.commit()

	def transaction(self, model_uri="local:"):
		content_type = "application/x-turtle"
		if self._transaction:
			raise FourStoreError("Already in Transaction")
		has_o_index = "no_o_index" in self._features
		cdef int import_count[1]
		py4s.fs_import_stream_start(self._link, model_uri, content_type,
			has_o_index, import_count)
		self._transaction = True

	def commit(self):
		if not self._transaction:
			raise FourStoreError("No Transaction")
		cdef int import_count[1]
		cdef int error_count[1]
		py4s.fs_import_stream_finish(self._link, import_count, error_count)
		self._transaction = False
		self.flush()

	def add_model(self, model):
		if not self._transaction:
			self.transaction(model.identifier)
			transaction = True
		else:
			transaction = False
		for statement in model.triples((None, None, None)):
			self.add(statement)
		if transaction:
			self.commit()

cdef _node(fs_row r):
	if r.type == 0: # FS_TYPE_NONE
		return r.name
	if r.type == 1: # FS_TYPE_URI
		return URIRef(r.lex)
	if r.type == 2: # FS_TYPE_LITERAL
		kw = {}
		if r.dt:
			kw["datatype"] = URIRef(r.dt)
		if r.lang:
			kw["lang"] = r.lang
		return Literal(r.lex, **kw)
	if r.type == 3: # FS_TYPE_BNODE
		return BNode(r.lex)
	raise FourStoreError("Unknown row type %d" % r.type)

class _ResultRow(list):
	def __init__(self, bindings, row):
		self._bindings = bindings
		super(_ResultRow, self).__init__(row)
	def __getitem__(self, k):
		if isinstance(k, str):
			return self[self._bindings[k]]
		return super(_ResultRow, self).__getitem__(k)

cdef class QueryResults:
	cdef py4s.fs_query *_qr
	cdef Cursor _qs
	cdef int _cols
	cdef list _header
	cdef dict _bindings
	def __cinit__(self, Cursor qs):
		self._qs = qs
		self._qr = qs._qr
		self._cols = py4s.fs_query_get_columns(self._qr)
		cdef fs_row *h = py4s.fs_query_fetch_header_row(self._qr)
		self._header = [(_node(h[x]), x) for x in range(self._cols)]
		self._bindings = dict(self._header)
		self._get_warnings()
		if py4s.fs_query_errors(self._qr):
			raise FourStoreError("Bad Query")
	cdef _get_warnings(self):
		cdef py4s.GSList *warnings = py4s.py4s_query_warnings(self._qr)
		cdef py4s.GSList *w = warnings
		while w:
			self._qs.warnings.append(str(w.data))
			w = w.next
		if warnings:
			g_slist_free(warnings)
	@property
	def bindings(self):
		return [x[0] for x in self._header]
	def __iter__(self):
		return self
	def __next__(self):
		cdef fs_row *row = py4s.fs_query_fetch_row(self._qr)
		if not row: raise StopIteration
		self._get_warnings()
		result = [_node(row[x]) for x in range(self._cols)]
		return _ResultRow(self._bindings, result)
	def __nonzero__(self):
		return py4s.py4s_query_bool(self._qr)

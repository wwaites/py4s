from ctypes import *
try:
	from rdflib.term import URIRef, Literal, BNode, Identifier
	from rdflib.graph import Graph
except ImportError:
	from rdflib import URIRef, Literal, BNode, Identifier
	from rdflib.Graph import Graph

__all__ = ["FS_OPEN_HINT_RW", "FS_OPEN_HINT_RO", "FourStore", "FourStoreError"]

import os
from distutils import ccompiler
cc = ccompiler.new_compiler()
heredir = os.path.dirname(__file__)
libfs_dir, unused = os.path.split(heredir)
libfs_path = os.path.join(libfs_dir, cc.library_filename("py4s", "dylib"))

libfs = CDLL(libfs_path)
libraptor = CDLL(cc.library_filename("raptor", "dylib"))
#librasqal = CDLL(cc.library_filename("rasqal", "dylib"))
del cc

FS_OPEN_HINT_RW = 0
FS_OPEN_HINT_RO = 1

MEMORY_DEBUG = 0

def debug_alloc(f):
	if not MEMORY_DEBUG:
		return f
	def _f(*av, **kw):
		ret = f(*av, **kw)
		print repr(ret), "alloc"
		return ret
	return _f
def debug_free(f):
	if not MEMORY_DEBUG:
		return f
	def _f(obj, *av, **kw):
		print repr(obj), "free"
		f(obj, *av, **kw)
	return _f

class FS_NODE(Structure):
	FS_TYPE_NONE = 0
	FS_TYPE_URI = 1
	FS_TYPE_LITERAL = 2
	FS_TYPE_BNODE = 3

	_fields_ = [
        	("name", c_char_p),
		("rid", c_int64),
        	("type", c_int),
		("lex", c_char_p),
		("dt", c_char_p),
		("lang", c_char_p)
	]
	
	datatypes = {}
	def __str__(self):
		if self.type == self.FS_TYPE_URI:
			return "<%s>" % (self.lex,)
		elif self.type == self.FS_TYPE_LITERAL:
			s = '"%s"' % (self.lex,)
			if self.lang:
				s = s + "@%s" % (self.lang,)
			if self.dt:
				s = s + "^^<%s>" % (self.dt,)
			return s
		elif self.type == self.FS_TYPE_BNODE:
			return self.lex
		elif self.type == self.FS_TYPE_NONE:
			return self.name
		else:
			return "Xnode?X"
	def to_node(self):
		if self.type == self.FS_TYPE_URI:
			return URIRef(self.lex)
		if self.type == self.FS_TYPE_LITERAL:
			kw = {}
			if self.dt:
				datatype = self.datatypes.get(self.dt, None)
				if datatype is None:
					datatype = self.datatypes[self.dt] = URIRef(self.dt)
				kw["datatype"] = datatype
			if self.lang:
				kw["language"] = self.lang
			n = Literal(self.lex, **kw)
			return n
		if self.type == self.FS_TYPE_BNODE:
			return BNode(self.lex)
		if self.type == self.FS_TYPE_NONE:
			return self.name
		raise TypeError("%s" % (self,))

class QUERY(c_void_p):
	fs_query_free = libfs["fs_query_free"]
	fs_query_free = debug_free(fs_query_free)
	fs_query_errors = libfs["fs_query_errors"]
	fs_query_get_columns = libfs["fs_query_get_columns"]
	fs_query_fetch_header_row = libfs["fs_query_fetch_header_row"]
	fs_query_fetch_row = libfs["fs_query_fetch_row"]
	fs_query_fetch_header_row.restype = POINTER(FS_NODE)
	fs_query_fetch_row.restype = POINTER(FS_NODE)
	py4s_print_warnings = libfs["py4s_print_warnings"]
	py4s_query_ask = libfs["py4s_query_ask"]
	py4s_query_bool = libfs["py4s_query_bool"]
	def __del__(self):
		self.fs_query_free(self)
	def errors(self):
		return self.fs_query_errors(self)
	def warnings(self):
		self.py4s_print_warnings(self)
	def columns(self):
		return self.fs_query_get_columns(self)
	def header(self):
		return self.fs_query_fetch_header_row(self)
	def fetchrow(self):
		return self.fs_query_fetch_row(self)
	@property
	def ask(self):
		return self.py4s_query_ask(self)
	def __nonzero__(self):
		return bool(self.py4s_query_bool(self))
fs_query_execute = libfs["fs_query_execute"]
fs_query_execute.restype = QUERY
fs_query_execute = debug_alloc(fs_query_execute)

class QUERY_STATE(c_void_p):
	fs_query_fini = libfs["fs_query_fini"]
	fs_query_fini = debug_free(fs_query_fini)
	fs_query_cache_flush = libfs["fs_query_cache_flush"]
	def __del__(self):
		self.fs_query_fini(self)
	def flush(self):
		self.fs_query_cache_flush(self, 0)
fs_query_init = libfs["fs_query_init"]
fs_query_init.restype = QUERY_STATE
fs_query_init = debug_alloc(fs_query_init)

class FS_LINK(c_void_p):
	fsp_close_link = libfs["fsp_close_link"]
	fsp_close_link = debug_free(fsp_close_link)
	fsp_link_features = libfs["fsp_link_features"]
	fsp_link_features.restype = c_char_p
	def __del__(self):
		self.fsp_close_link(self)
	@property
	def features(self):
		if not hasattr(self, "__features__"):
			features = self.fsp_link_features(self).strip().split(" ")
			self.__features__ = dict([ (x, True) for x in features ])
		return self.__features__
fsp_open_link = libfs["fsp_open_link"]
fsp_open_link.restype = FS_LINK
fsp_open_link = debug_alloc(fsp_open_link)

class FS_RID(c_ulonglong):
	pass
FS_RID_NULL = FS_RID(0x8000000000000000)
class FS_RID_VECTOR(c_void_p):
	fs_rid_vector_append = libfs["fs_rid_vector_append"]
	fs_rid_vector_free = libfs["fs_rid_vector_free"]
	fs_rid_vector_free = debug_free(fs_rid_vector_free)
	def __del__(self):
		self.fs_rid_vector_free(self)
	def append(self, uri_hash):
		self.fs_rid_vector_append(self, uri_hash)
fs_rid_vector_new = libfs["fs_rid_vector_new"]
fs_rid_vector_new.restype = FS_RID_VECTOR
fs_rid_vector_new = debug_alloc(fs_rid_vector_new)
FS_RID_VECTOR.fs_rid_vector_append.argtypes = [FS_RID_VECTOR, FS_RID]

class RAPTOR_URI(c_void_p):
	raptor_free_uri = libraptor["raptor_free_uri"]
	raptor_free_uri = debug_free(raptor_free_uri)
	raptor_uri_as_string = libraptor["raptor_uri_as_string"]
	raptor_uri_as_string.restype = c_char_p
	def __del__(self):
		self.raptor_free_uri(self)
	def __str__(self):
		return self.raptor_uri_as_string(self)
	def __repr__(self):
		return '<URI "%s" at 0x%08x>' % (self, cast(self, c_void_p).value)
raptor_new_uri = libraptor["raptor_new_uri"]
raptor_new_uri.restype = RAPTOR_URI
raptor_new_uri = debug_alloc(raptor_new_uri)

class QueryResults(object):
	def __init__(self, qs, link, model_uri, query, flags, opt_level, soft_limit):
		self.qr = fs_query_execute(qs, link, model_uri, query, flags, opt_level, soft_limit)
		if self.qr.errors():
			print "#QUERY:\n", query
			self.qr.warnings()
			raise FourStoreError("Bad Query")
		## have to read the header, otherwise bus error
		self.__ncols__ = self.qr.columns()
		header = self.qr.header()
		self.bindings = dict([(header[i].to_node(), i) for i in range(self.__ncols__)])
	def __nonzero__(self):
		if not hasattr(self, "__boolean__"):
			self.__boolean__ = bool(self.qr)
		return self.__boolean__
	def __iter__(self):
		while True:
			row = self.fetchone()
			if not row:
				break
			yield row
	def fetchone(self):
		row = self.qr.fetchrow()
		if not row: return None
		class DictList(list):
			def __getitem__(dl, key):
				if isinstance(key, int):
					return super(DictList, dl).__getitem__(key)
				else:
					return super(DictList, dl).__getitem__(self.bindings[key])
		return DictList([row[i].to_node() for i in range(self.__ncols__)])

	def fetchall(self):
		return list(self)

def n3(statement):
	t = map(lambda x: x.n3(), statement)
	return " ".join(t).encode("iso-8859-1")

class Cursor(object):
	fs_import_stream_start = libfs["fs_import_stream_start"]
	fs_import_stream_start.argtypes = [FS_LINK, c_char_p, c_char_p, c_int, POINTER(c_int)]
	fs_import_stream_data = libfs["fs_import_stream_data"]
	fs_import_stream_finish = libfs["fs_import_stream_finish"]
	fsp_delete_model_all = libfs["fsp_delete_model_all"]
	def __init__(self, store):
		if not store.link:
			raise FourStoreError("not connected")
		self.store = store
		self.query_state = fs_query_init(store.link)
		self.flags = 0
		self.in_transaction = False

        def transaction(self, model_uri="local:"):
		content_type="application/x-turtle"
		if not self.store.link:
			raise FourStoreError("not connected")
		if self.in_transaction:
			raise FourStoreError("already in transaction")
		has_o_index = "no_o_index" not in self.store.link.features
		self.import_count = c_int(0)
		self.fs_import_stream_start(self.store.link, model_uri, content_type,
					has_o_index, byref(self.import_count))
		self.in_transaction = True

	def commit(self):
		if not self.store.link:
			raise FourStoreError("not connected")
		if not self.in_transaction:
			raise FourStoreError("no transaction")
		error_count = c_int(0)
		self.fs_import_stream_finish(self.store.link, byref(self.import_count), byref(error_count))
		self.in_transaction = False
		self.query_state.flush()
		#print "IMPORTED", self.import_count.value, "ERRORS", error_count.value
		return self.import_count.value, error_count.value

	def _import_data(self, data):
		#print "IMPORTING", data
		if not self.store.link:
			raise FourStoreError("not connected")
		self.fs_import_stream_data(self.store.link, data, len(data))

	def execute(self, query, model_uri="local:", initNs=None, opt_level=3, soft_limit=0):
		if not self.store.link:
			raise FourStoreError("not connected")
		if MEMORY_DEBUG: print "----------- QUERY -----------"
		model_uri = self.store.model_uri(model_uri)
		#self.query_state = fs_query_init(self.link)
		if initNs:
			prefixes = map(lambda x: "PREFIX %s: <%s>" % (x, initNs[x]), initNs)
			query = "\n".join(prefixes) + "\n" + query
		query = query.encode("iso-8859-1")
		return QueryResults(self.query_state, self.store.link, model_uri,
			query, self.flags, opt_level, soft_limit)

	def add(self, statement, model_uri="local:"):
		if MEMORY_DEBUG: print "------------ ADD ------------"
		n = n3(statement)
		q = "ASK WHERE { GRAPH <%s> " % model_uri + "{ " + n + " } }"
		if not self.execute(q, model_uri):
			if not self.in_transaction:
				self.transaction(model_uri)
				transaction = True
			else:
				transaction = False
			self._import_data(n + " .")
			if transaction:
				self.commit()

	def add_model(self, model):
		"""
		model should be an RDFLib Graph instance and should have
		the identifier set to something reasonable
		"""
		if not self.in_transaction:
			self.transaction(model.identifier)
			transaction = True
		else:
			transaction = False

		for statement in model.triples((None, None, None)):
			self.add(statement, model.identifier)

		if transaction:
			self.commit()

	def delete_model(self, model_uri=None, all=False):
		if not self.store.link:
			raise FourStoreError("not connected")
		if not model_uri and not all:
			raise FourStoreError("nothing to delete")
		mvec = fs_rid_vector_new(0)
		if model_uri:
			uri_hash = self.store.fs_hash_uri(model_uri)
		else:
			uri_hash = FS_RID_NULL
		mvec.append(uri_hash)
		self.fsp_delete_model_all(self.store.link, mvec)
		self.query_state.flush()

class FourStoreError(Exception):
	"4S Error"

class FourStore(object):
	fsp_no_op = libfs["fsp_no_op"]
	fsp_hash_type = libfs["fsp_hash_type"]
	fs_hash_init = libfs["fs_hash_init"]
	#fs_update = libfs["fs_update"]
	raptor_init = libraptor["raptor_init"]
	raptor_finish = libraptor["raptor_finish"]

	__models__ = {}
	raptor_init_done = False

	def __init__(self, store, password=None, mode=FS_OPEN_HINT_RW):
		if not self.raptor_init_done:
			self.raptor_init()
			self.raptor_init_done = True
		self.store = store
		self.password = password
		self.mode = mode
		self.link = None

        def connect(self):
		self.link = fsp_open_link(self.store, self.password, self.mode)

		self.fs_hash_init(self.fsp_hash_type(self.link))
		### very kludgy. got to be a better way
		fhi = POINTER(c_int).in_dll(libfs, "fs_hash_uri")[0]
		for fname in "fs_hash_uri_md5", "fs_hash_uri_umac", "fs_hash_uri_crc64":
			if fhi == c_int.in_dll(libfs, fname).value:
				self.fs_hash_uri = libfs[fname]
				self.fs_hash_uri.argtypes = [c_char_p]
				self.fs_hash_uri.restype = FS_RID
				break
		
		self.fsp_no_op(self.link, 0)

	def close(self):
		self.link = None

	def model_uri(self, model_uri):
		uri = self.__models__.get(model_uri, None)
		if uri is None:
			uri = raptor_new_uri(model_uri)
			self.__models__[model_uri] = uri
		return uri

	def cursor(self):
		return Cursor(self)

	### API conformant to RDFLib
	def query(self, *av, **kw):
		return self.cursor().execute(*av, **kw)

	def triples(self, (s, p, o), *av, **kw):
		bindings = []
		filter = []
		if s is None:
			bindings.append("?s")
			filter.append("?s")
		else:
			filter.append(s.n3())
		if p is None:
			bindings.append("?p")
			filter.append("?p")
		else:
			filter.append(p.n3())
		if o is None:
			bindings.append("?o")
			filter.append("?o")
		else:
			filter.append(o.n3())
		query = "SELECT " + " ".join(bindings) + \
			" WHERE { " + " ".join(filter) + " }"
		for result in self.query(query, *av, **kw):
			statement = []
			if s is None: statement.append(result["s"])
			else: statement.append(s)
			if p is None: statement.append(result["p"])
			else: statement.append(p)
			if o is None: statement.append(result["o"])
			else: statement.append(o)
			yield tuple(statement)

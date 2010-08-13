cimport _py4s as py4s

try:
    from rdflib.term import URIRef, Literal, BNode, Identifier, Variable
    from rdflib.graph import ConjunctiveGraph, Graph
except ImportError:
    from rdflib import URIRef, Literal, BNode, Identifier, Variable
    from rdflib.Graph import ConjunctiveGraph, Graph
from logging import getLogger
log = getLogger("py4s")

def version():
    from pkg_resources import working_set, Requirement, Environment
    r = Requirement.parse("py4s")
    d = Environment().best_match(r, working_set)
    return d.version
version = version()

include "rnode.pyx"

class FourStoreError(Exception):
    pass

cdef class FourStoreClient:
    cdef py4s.fsp_link *_link
    cdef dict _namespace
    cdef dict _prefix
    cpdef public int opt_level
    cpdef public int soft_limit

    def __dealloc__(self):
        if self._link:
            py4s.fsp_close_link(self._link)

    def open(self, configuration, create=False):
        log.info("py4s %s starting up" % version)
        conf = configuration.split(",", 1)
        self.opt_level = 3
        self.soft_limit = 0
        if len(conf) == 2:
            options = [x.split("=") for x in conf[1].split(",")]
            for k,v in options:
                if k == "soft_limit":
                    self.soft_limit = int(v)
                elif k == "opt_level":
                    self.opt_level = int(v)
                else:
                    raise FourStoreError("Unknown option: %s" % (k,))
        cdef char *name = conf[0]
        cdef char *pw = ""
        cdef int ro = 0
        if self._link:
            raise FourStoreError("Already Open")
        self._link = py4s.fsp_open_link(name, pw, ro)
        if not self._link:
            raise FourStoreError("Could not connect to back-end")
        hash_type = py4s.fsp_hash_type(self._link)
        py4s.fs_hash_init(hash_type)
        return py4s.fsp_no_op(self._link, 0)

    def cursor(self):
        return _Cursor(self)

def _n3(s):
    return " ".join([x.n3() for x in s])

def _quote_encode(self):
    ## Taken from rdflib 3 for working with rdflib 2.4.2...
    # This simpler encoding doesn't work; a newline gets encoded as "\\n",
    # which is ok in sourcecode, but we want "\n".
    #encoded = self.encode('unicode-escape').replace(
    #        '\\', '\\\\').replace('"','\\"')
    #encoded = self.replace.replace('\\', '\\\\').replace('"','\\"')

    # NOTE: Could in theory chose quotes based on quotes appearing in the
    # string, i.e. '"' and "'", but N3/turtle doesn't allow "'"(?).

    # which is nicer?
    # if self.find("\"")!=-1 or self.find("'")!=-1 or self.find("\n")!=-1:
    if "\n" in self:
        # Triple quote this string.
        encoded = self.replace('\\', '\\\\')
        if '"""' in self:
            # is this ok?
            encoded = encoded.replace('"""','\\"""')
        if encoded.endswith('"'):
            encoded = encoded[:-1] + "\\\""
        return '"""%s"""' % encoded
    else:
        return '"%s"' % self.replace('\n','\\n').replace('\\', '\\\\'
            ).replace('"', '\\"')

def _get_context(c):
    if c is None or isinstance(c, ConjunctiveGraph):
        return "local:"
    elif isinstance(c, Graph):
        return c.identifier
    else:
        return c

cdef class _Cursor:
    cdef py4s.fsp_link *_link
    cdef py4s.fs_query_state *_qs
    cdef py4s.fs_query *_qr
    cdef char *_query
    cdef bool _transaction
    cdef dict _pending
    cdef dict _features
    cpdef public list warnings
    cpdef public FourStoreClient store

    def __cinit__(self, FourStoreClient store):
        self.store = store
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

    def execute(self, query, context="local:", initNs={}):
        if self._qr:
            py4s.fs_query_free(self._qr)
        if not isinstance(query, unicode):
            query = query.encode("utf-8")
        if initNs:
            prefixes = []
            for x in initNs:
                prefixes.append(u"PREFIX %s: <%s>" % (x, initNs[x]))
            query = u"\n".join(prefixes) + u"\n" + query
        self.warnings = []

        context = _get_context(context)

        # silly hoop for unicode data
        py_uquery = query.encode("utf-8")
        self._query = py_uquery

        cdef py4s.raptor_uri *bu = py4s.raptor_new_uri(context)
        self._qr = py4s.fs_query_execute(self._qs, self._link, bu,
                self._query, 0,
                self.store.opt_level, self.store.soft_limit)
        results = _QueryResults(self)

        # construct and describe queries return a graph
        if py4s.py4s_query_construct(self._qr):
            from py4s import deskolemise
            g = Graph()
            for statement in results:
                g.add(deskolemise(statement))
            return g

        return results

    def delete_graph(self, graph=None, all=False):
        if graph is None and all is False:
            raise FourStoreError("Nothing to Delete")
        if isinstance(graph, Graph):
            context = graph.identifier
        else:
            context = graph
        cdef fs_rid_vector *mvec = py4s.fs_rid_vector_new(0)
        cdef fs_rid uri_hash
        if context:
            uri_hash = py4s.fs_hash_uri(context)
        else:
            uri_hash = 0x8000000000000000
        py4s.fs_rid_vector_append(mvec, uri_hash)
        py4s.fsp_delete_model_all(self._link, mvec)
        self.flush()

    def add_graph(self, graph, replace=True):
        cdef unsigned char *udata
        cdef int import_count[1]
        cdef int error_count[1]

        assert isinstance(graph, Graph)
        if replace:
            self.delete_graph(graph)

        content_type = "application/x-turtle"
        has_o_index = "no_o_index" in self._features
        py4s.fs_import_stream_start(self._link, graph.identifier,
                                    content_type, has_o_index, import_count)

        # ensure we have stable bnode identifiers
        from py4s import SkolemGraph
        _skg = SkolemGraph(graph)

        data = _skg.serialize(format="nt")
        udata = data
        py4s.fs_import_stream_data(self._link, udata, len(udata))

        py4s.fs_import_stream_finish(self._link, import_count, error_count)
        log.debug("import count: %s errors: %s" % (import_count[0], error_count[0]))

        self.flush()

    def update(self, query):
        cdef char *uquery
        cdef char *message
        py_uquery = query.encode("utf-8")
        uquery = py_uquery
        py4s.fs_update(self._link, uquery, &message, 1)
        if message != NULL:
            log.error("update: %s\n%s" % (message, uquery))
            raise FourStoreError("%s" % (message,))

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
            kw["lang"] = r.lang.lower()
        return Literal(r.lex, **kw)
    if r.type == 3: # FS_TYPE_BNODE
        if r.lex.startswith("_:"):
            nodename = r.lex.lstrip("_:")
        else:
            nodename = r.lex
        return BNode(nodename)
    raise FourStoreError("Unknown row type %d" % r.type)

class _ResultRow(list):
    def __init__(self, bindings, row):
        self._bindings = bindings
        super(_ResultRow, self).__init__(row)
    def __getitem__(self, k):
        if isinstance(k, str):
            return self[self._bindings[k]]
        elif isinstance(k, Variable):
            return self[self._bindings[str(k)]]
        else:
            return super(_ResultRow, self).__getitem__(k)

cdef class _QueryResults:
    cdef py4s.fs_query *_qr
    cdef _Cursor _qs
    cdef int _cols
    cdef list _header
    cdef dict _bindings
    def __cinit__(self, _Cursor qs):
        self._qs = qs
        self._qr = qs._qr
        self._cols = py4s.fs_query_get_columns(self._qr)
        cdef fs_row *h = py4s.fs_query_fetch_header_row(self._qr)
        self._header = [(_node(h[x]), x) for x in range(self._cols)]
        self._bindings = dict(self._header)
        self._get_warnings()
        if py4s.fs_query_errors(self._qr):
            log.error("bad query:\n%s" % qs._query)
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
        if py4s.py4s_query_construct(self._qr):
            return self._construct_triple()
        else:
            return self._fetch_row()

    cdef _fetch_row(self):
        cdef fs_row *row
        row = py4s.fs_query_fetch_row(self._qr)
        self._get_warnings()
        if not row: raise StopIteration
        result = [_node(row[x]) for x in range(self._cols)]
        return _ResultRow(self._bindings, result)

    cdef fs_row *_construct_row
    cdef py4s.rasqal_query *_construct_rasqal_query
    cdef int _construct_triple_index
    cdef _construct_triple(self):
        cdef py4s.rasqal_triple *t
        if not self._construct_row:
            self._construct_row = py4s.fs_query_fetch_row(self._qr)
            self._get_warnings()
            if not self._construct_row: raise StopIteration
            self._construct_rasqal_query = \
                py4s.py4s_query_rasqal_query(self._qr)
            self._construct_triple_index = 0
        t = py4s.rasqal_query_get_construct_triple(
            self._construct_rasqal_query,
            self._construct_triple_index
        )
        if not t:
            self._construct_row = NULL
            return self._construct_triple() ## next row
        else:
            self._construct_triple_index += 1
            return (
                py4s._rnode(self._bindings, self._construct_row, t.subject),
                py4s._rnode(self._bindings, self._construct_row, t.predicate),
                py4s._rnode(self._bindings, self._construct_row, t.object),
            )

    def __nonzero__(self):
        return py4s.py4s_query_bool(self._qr)

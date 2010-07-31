try:
    from rdflib.graph import Graph
    from rdflib.term import BNode, URIRef, Variable
except ImportError:
    from rdflib.Graph import Graph
    from rdflib import BNode, URIRef, Variable
from rdflib.store import Store, VALID_STORE, NO_STORE
from rdflib.plugin import register
from _py4s import FourStoreClient, FourStoreError, _n3, log, version

__all__ = ["FourStore", "FourStoreError", "LazyFourStore"]

def skolemise((s,p,o)):
    if isinstance(s, BNode):
        s = URIRef("bnode:%s" % s)
    if isinstance(p, BNode):
        p = URIRef("bnode:%s" % p)
    if isinstance(o, BNode):
        o = URIRef("bnode:%s" % o)
    return s,p,o

def deskolemise(statement):
    def _dst(x):
        if isinstance(x, URIRef) and x.startswith("bnode:"):
            _unused, bnid = x.split(":", 1)
            return BNode(bnid)
        return x
    return map(_dst, statement)

class FourStore(FourStoreClient, Store):
    context_aware = True
    def __init__(self, configuration=None):
        self.__namespace = {}
        self.__prefix = {}
        if configuration:
            self.open(configuration)
    def bind(self, prefix, namespace):
        self.__namespace[prefix] = namespace
        self.__prefix[namespace] = prefix
    def namespace(self, prefix):
        return self.__namespace.get(prefix, None)
    def prefix(self, namespace):
        return self.__prefix.get(namespace, None)
    def namespaces(self):
        return self.__namespace.items()

    def query(self, *av, **kw):
        """Execute a SPARQL Query"""
        return self.cursor().execute(*av, **kw)
    def add(self, statement, context="local:", **kw):
        """Add triples to the graph -- very inefficiently"""
        if isinstance(context, Graph): context = context.identifier
        cursor = self.cursor()

        g = Graph(identifier=context)
        g.add(statement)

        construct = u"CONSTRUCT { ?s ?p ?o } WHERE { "
        if context != "local:": construct += u"GRAPH <%s> { " % context
        construct += u"?s ?p ?o"
        if context != "local:": construct += u" }"
        construct += u" }"
        g += cursor.execute(construct)

        cursor.add_graph(g, replace=True)

    def addN(self, slist, **kw):
        """Add a list of quads to the graph -- very inefficiently"""
        graphs = {}
        cursor = self.cursor()
        for s,p,o,c in slist:
            if isinstance(c, Graph): c = c.identifier
            g = graphs.get(c)
            if g is None:
                q = u"CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <%s> { ?s ?p ?o } }" % c
                g = Graph(identifier=c)
                g += cursor.execute(q)
                graphs[c] = g
            g.add((s,p,o))
        for g in graphs.values():
            cursor.add_graph(g, replace=True)

    def remove(self, statement, context="local:"):
        """Remove a triple from the graph"""
        log.debug("remove(%s) from %s" % (statement, context))
        if isinstance(context, Graph): _context = context.identifier
        else: _context = context
        s,p,o = skolemise(statement)
        bindings = (
            s or Variable("s"),
            p or Variable("p"),
            o or Variable("o")
        )

        construct = u"CONSTRUCT { " + _n3(bindings) + u" } WHERE { " 
        if _context != "local:": construct += u"GRAPH <%s> { " % _context
        construct += _n3(bindings)
        if _context != "local:": construct += u" }"
        construct += u" }"

        result = self.cursor().execute(construct)
        if len(result) > 0:
            delete = u"DELETE { "
            if _context != "local:": delete += u"GRAPH <%s> { " % _context
            delete += u" .\n".join([_n3(skolemise(x)) for x in result.triples((None, None, None))])
            if _context != "local:": delete += u" }"
            delete += u" }"

            print delete

            self.cursor().update(delete)

    def __contains__(self, statement, context="local:"):
        if isinstance(context, Graph): _context = context.identifier
        else: _context = context
        s,p,o = skolemise(statement)
        query = u"ASK WHERE { "
        if _context != "local:": query += u"GRAPH <%s> { " % _context
        query += u" ".join([x.n3() for x in (s,p,o)])
        if  _context != "local:": query += u" }"
        query += u" }"
        return bool(self.cursor().execute(query))

    def contexts(self, statement=None):
        if statement is None: statement = (None,None,None)
        s,p,o = skolemise(statement)
        bindings = (
            s or Variable("s"),
            p or Variable("p"),
            o or Variable("o")
        )
        query = u"SELECT DISTINCT ?g WHERE { GRAPH ?g { "
        query += u" ".join([x.n3() for x in bindings])
        query += u" } }"
        for g, in self.query(query):
            yield Graph(self, identifier=g)

    def triples(self, statement, context="local:", **kw):
        """Return triples matching (s,p,o) pattern"""

        ## shortcut if we are just checking the existence
        if all(statement):
            if self.__contains__(statement, context):
                yield statement, context
            return

        if isinstance(context, Graph): _context = context.identifier
        else: _context = context
        s,p,o = skolemise(statement)
        bindings = (
            s or Variable("s"),
            p or Variable("p"),
            o or Variable("o")
        )
        query = u"SELECT DISTINCT " + u" ".join([x.n3() for x in bindings if isinstance(x, Variable)])
        query += u" WHERE { "
        if _context != "local:": query += u"GRAPH <%s> { " % _context
        query += u" ".join([x.n3() for x in bindings])
        if _context != "local:": query += u" }"
        query += u" }"
        results = self.cursor().execute(query, **kw)
        for row in results:
            triple = []
            for b in bindings:
                if isinstance(b, Variable):
                    triple.append(row[b])
                else:
                    triple.append(b)
            yield (tuple(deskolemise(triple)), context)

class LazyFourStore(Store):
    def __init__(self, *av, **kw):
        self.__av__ = av
        self.__kw__ = kw
        self.__store__ = None
    def __getattribute__(self, attr):
        if attr in ("__av__", "__kw__", "__store__"):
            return super(LazyFourStore, self).__getattribute__(attr)
        if attr == "context_aware":
            return True
        if self.__store__ is None:
            self.__store__ = FourStore(*self.__av__, **self.__kw__)
        return getattr(self.__store__, attr)

register("FourStore", Store, "py4s", "FourStore")
register("LazyFourStore", Store, "py4s", "LazyFourStore")

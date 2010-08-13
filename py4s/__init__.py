try:
    from rdflib.graph import Graph
    from rdflib.term import BNode, URIRef, Variable
except ImportError:
    from rdflib.Graph import Graph
    from rdflib import BNode, URIRef, Variable
from rdflib.store import Store, VALID_STORE, NO_STORE
from rdflib.plugin import register
from _py4s import FourStoreClient, FourStoreError, _n3, log, version, _get_context

__all__ = ["FourStore", "FourStoreError", "LazyFourStore"]

def skolemise(statement):
    def _sk(x):
        if isinstance(x, BNode):
            return URIRef("bnode:%s" % x)
        return x
    return tuple(map(_sk, statement))

def deskolemise(statement):
    def _dst(x):
        if isinstance(x, URIRef) and x.startswith("bnode:"):
            _unused, bnid = x.split(":", 1)
            return BNode(bnid)
        return x
    return tuple(map(_dst, statement))


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

    def exists(self, statement, context=None):
        context = _get_context(context)
        s,p,o = skolemise(statement)
        q = u"ASK WHERE { GRAPH <%s> { %s %s %s } }" % (context, s.n3(), p.n3(), o.n3())
        return bool(self.query(q))

    def add(self, statement, context=None, **kw):
        """Add triples to the graph"""
        context = _get_context(context)

        if not self.exists(statement, context):
            cursor = self.cursor()
            g = Graph(identifier=context)
            g.add(skolemise(statement))
            cursor.add_graph(g, replace=False)

    def addN(self, slist, **kw):
        """Add a list of quads to the graph"""
        graphs = {}
        for s,p,o,c in slist:
            c = _get_context(c)
            g = graphs.get(c, Graph(identifier=c))
            if not self.exists((s,p,o), c):
                g.add(skolemise((s,p,o)))
        cursor = self.cursor()
        for g in graphs.values():
            cursor.add_graph(g, replace=False)

    def remove(self, statement, context=None):
        """Remove a triple from the graph"""
        log.debug("remove(%s) from %s" % (statement, context))
        _context = _get_context(context)
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
        _skg = SkolemGraph(result)

        print result.serialize(format="n3")
        if len(result) > 0:
            delete = u"DELETE DATA { "
            if _context != "local:": delete += u"GRAPH <%s> {\n" % _context
            delete += _skg.serialize(format="nt")
            if _context != "local:": delete += u"}"
            delete += u" }"

            print delete

            self.cursor().update(delete)

    def __contains__(self, *av, **kw):
        return self.exists(*av, **kw)

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

    def triples(self, statement, context=None, **kw):
        """Return triples matching (s,p,o) pattern"""

        ## shortcut if we are just checking the existence
        if all(statement):
            if self.__contains__(statement, context):
                yield statement, context
            return

        _context = _get_context(context)

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
            yield (deskolemise(triple), context)

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

class SkolemGraph(Graph):
    def __init__(self, g):
        super(SkolemGraph, self).__init__(g.store, identifier=g.identifier)
    def triples(self, *av, **kw):
        for statement in super(SkolemGraph, self).triples(*av, **kw):
            yield skolemise(statement)

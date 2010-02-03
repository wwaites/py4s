from rdflib.graph import Graph
from rdflib.term import Variable
from rdflib.store import Store, VALID_STORE, NO_STORE
from rdflib.plugin import register
from _py4s import FourStoreClient

__all__ = ["FourStore"]

class FourStore(FourStoreClient, Store):
	def __init__(self, *av, **kw):
		self.__namespace = {}
		self.__prefix = {}
		super(FourStore, self).__init__(*av, **kw)
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
	def add(self, *av, **kw):
		"""Add triples to the graph"""
		return self.cursor().add(*av, **kw)
	def addN(self, slist, **kw):
		"""Add a list of quads to the graph"""
		for s,p,o,c in slist:
			self.add((s,p,o), context=c, **kw)
	def remove(self, *av, **kw):
		"""Remove a triple from the graph (unimplemented)"""
		raise FourStoreError("Triple Removal Not Implemented")

	def __contains__(self, statement):
		q = "ASK WHERE { " + " ".join([x.n3() for x in statement]) + " }"
		return bool(self.cursor().execute(q))

	def triples(self, statement, context=None, *av, **kw):
		"""Return triples matching (s,p,o) pattern"""
		if isinstance(context, Graph): _context = context.identifier
		else: _context = context
		s,p,o = statement
		bindings = (
			s or Variable("s"),
			p or Variable("p"),
			o or Variable("o")
		)
		query = "SELECT DISTINCT " + " ".join([x.n3() for x in bindings if isinstance(x, Variable)])
		query += " WHERE { "
		if _context: query += "GRAPH <%s> { " % _context
		query += " ".join([x.n3() for x in bindings])
		if _context: query += " }"
		query += " }"
		results = self.cursor().execute(query, *av, **kw)
		for row in results:
			triple = []
			for b in bindings:
				if isinstance(b, Variable):
					triple.append(row[b])
				else:
					triple.append(b)
			yield (tuple(triple), context)
			
	def subjects(self, predicate=Variable("p"), object=Variable("o"), context=None, **kw):
		if isinstance(context, Graph): context = context.identifier
		q = "SELECT DISTINCT ?s WHERE { "
		if context: q += "GRAPH <%s> { " % context
		q += " ".join([x.n3() for x in (Variable("s"), predicate, object)])
		if context: q += " }"
		q += " }"
		for s, in self.cursor().execute(q, **kw):
			yield s
	def predicates(self, subject=Variable("s"), object=Variable("o"), context=None, **kw):
		if isinstance(context, Graph): context = context.identifier
		q = "SELECT DISTINCT ?p WHERE { "
		if context: q += "GRAPH <%s> { " % context
		q += " ".join([x.n3() for x in (subject, Variable("p"), object)])
		if context: q += " }"
		q += " }"
		for p, in self.cursor().execute(q, **kw):
			yield p
	def objects(self, subject=Variable("s"), predicate=Variable("p"), context=None, **kw):
		if isinstance(context, Graph): context = context.identifier
		q = "SELECT DISTINCT ?o WHERE { "
		if context: q += "GRAPH <%s> { " % context
		q += " ".join([x.n3() for x in (subject, predicate, Variable("o"))])
		if context: q += " }"
		q += " }"
		for o, in self.cursor().execute(q, **kw):
			yield o

register("FourStore", Store, "py4s", "FourStore")

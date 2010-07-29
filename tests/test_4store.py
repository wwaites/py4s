from py4s import FourStore
try:
    from rdflib.term import URIRef, Literal
    from rdflib.namespace import RDF, RDFS
    from rdflib.graph import Graph, ConjunctiveGraph
except ImportError:
    from rdflib import URIRef, RDF, RDFS, Literal
    from rdflib.Graph import Graph, ConjunctiveGraph
from StringIO import StringIO

TEST_GRAPH = "http://example.org/"
store = FourStore("py4s_test")

class TestClass:
    def test_00_purge(self):
        cursor = store.cursor()
        cursor.delete_graph(all=True)
        assert not store.query("ASK WHERE { ?s ?p ?o }")

    def test_01_no_query(self):
        cursor = store.cursor()

    def test_10_add_graph(self):
        cursor = store.cursor()
        g = Graph(identifier=TEST_GRAPH)
        g.add((URIRef("http://irl.styx.org/"), RDFS.label, Literal("Idiosyntactix Research Laboratiries")))
        g.add((URIRef("http://www.okfn.org/"), RDFS.label, Literal("Open Knowledge Foundation")))
        cursor.add_graph(g)
        graphs = list(map(lambda x: x[0], store.query("SELECT DISTINCT ?g WHERE { graph ?g { ?s ?p ?o } } ORDER BY ?g")))
        assert graphs == [URIRef(TEST_GRAPH)]

    def test_11_add_triple(self):
        g = Graph(store, identifier=TEST_GRAPH)
        s = (
            URIRef("http://irl.styx.org/foo"),
            RDF.type,
            URIRef("http://irl.styx.org/Thing")
        )
        g.add(s)
        for count, in store.query("SELECT DISTINCT COUNT(?s) AS c WHERE { graph <%s> { ?s ?p ?o } }" % TEST_GRAPH): pass
        assert count == 3

    def test_20_remove(self):
        assert store.query("ASK WHERE { ?s ?p ?o }")
        g = ConjunctiveGraph(store)
        for ctx in g.contexts():
            store.remove((None,None,None), context=ctx)
        assert not store.query("ASK WHERE { ?s ?p ?o }")
    def test_13_construct(self):
        g = store.query("CONSTRUCT { <http://foo> ?p ?o } WHERE { ?s ?p ?o } LIMIT 2")
        assert len(g) == 2
    def test_14_triples(self):
        g = Graph(store, identifier=TEST_GRAPH)
        for s,p,o in g.triples((URIRef("http://irl.styx.org/foo"), None, None)):
            ## should only have one
            assert (s,p,o) == (URIRef("http://irl.styx.org/foo"), RDF.type, URIRef("http://irl.styx.org/Thing"))
    def test_15_serialize(self):
        g = Graph(store, identifier=TEST_GRAPH)
        data = g.serialize(format="n3")
        Graph().parse(StringIO(data), format="n3")
    def test_99_delete_graph(self):
        cursor = store.cursor()
        cursor.delete_graph(TEST_GRAPH)
        graphs = list(map(lambda x: x[0], store.query("SELECT DISTINCT ?g WHERE { graph ?g { ?s ?p ?o } } ORDER BY ?g")))
        assert graphs == []

if __name__ == '__main__':
    t = TestClass()
    t.test_add_triple()
    t.test_add_triple()
    print "-----------------------------"


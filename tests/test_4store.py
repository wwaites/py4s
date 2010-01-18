from py4s import FourStore
try:
	from rdflib.term import URIRef
	from rdflib.namespace import RDF
except ImportError:
	from rdflib import URIRef, RDF

store = FourStore("py4s_test")
class TestClass:
	def test_count_types(self):
		for count, in store.query("SELECT COUNT(?s) AS C WHERE { ?s a ?o }"):
			break
	def test_iter_types(self):
		for row in store.query("SELECT ?s ?o WHERE { ?s a ?o }"):
			print row
	def test_add_triple(self):
		s = (
			URIRef("http://irl.styx.org/"),
			RDF.type,
			URIRef("http://irl.styx.org/Thing")
		)
		store.add(s)
	def test_delete_graph(self):
		store.delete_model()

if __name__ == '__main__':
	t = TestClass()
	t.test_add_triple()
	t.test_add_triple()
	print "-----------------------------"


from py4s import FourStore
try:
	from rdflib.term import URIRef
	from rdflib.namespace import RDF
except ImportError:
	from rdflib import URIRef, RDF

store = FourStore("py4s_test")
store.connect()
class TestClass:
	def test_count_types(self):
		cursor = store.cursor()
		for count, in cursor.query("SELECT COUNT(?s) AS C WHERE { ?s a ?o }"):
			break
	def test_iter_types(self):
		cursor = store.cursor()
		for row in cursor.query("SELECT ?s ?o WHERE { ?s a ?o }"):
			print row
	def test_add_triple(self):
		cursor = store.cursor()
		s = (
			URIRef("http://irl.styx.org/"),
			RDF.type,
			URIRef("http://irl.styx.org/Thing")
		)
		cursor.add(s)
	def test_delete_graph(self):
		cursor = store.cursor()
		cursor.delete_model()

if __name__ == '__main__':
	t = TestClass()
	t.test_add_triple()
	t.test_add_triple()
	print "-----------------------------"


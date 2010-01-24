
LITERALS = (
	RASQAL_LITERAL_STRING,
	# RASQAL_LITERAL_XSD_STRING
	RASQAL_LITERAL_BOOLEAN,
	RASQAL_LITERAL_INTEGER,
	RASQAL_LITERAL_FLOAT,
	RASQAL_LITERAL_DOUBLE,
	# RASQAL_LITERAL_DECIMAL
	RASQAL_LITERAL_DATETIME,
	RASQAL_LITERAL_UDT,
)

cdef _rnode(dict bindings, fs_row *row, rasqal_literal *l):
	if l.type == RASQAL_LITERAL_URI:
		return URIRef(py4s.raptor_uri_as_string(l.value.uri))
	if l.type == RASQAL_LITERAL_VARIABLE:
		return _node(row[bindings[l.value.variable.name]])
	if l.type in LITERALS:
		kw = {}
		if l.language:
			kw["lang"] = l.language
		if l.datatype:
			kw["datatype"] = URIRef(py4s.raptor_uri_as_string(l.datatype))
		if l.type in (RASQAL_LITERAL_INTEGER, RASQAL_LITERAL_BOOLEAN):
			val = l.value.integer
		elif l.type == (RASQAL_LITERAL_FLOAT, RASQAL_LITERAL_DOUBLE):
			val = l.value.floating
		else:
			val = l.string
		return Literal(val, **kw)
	if l.type == RASQAL_LITERAL_BLANK:
		return BNode(l.string) # ???
	raise FourStoreError("Unknown Raptor Node: %d" % l.type)

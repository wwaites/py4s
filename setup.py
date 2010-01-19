from setuptools import setup, find_packages, Library
import sys, os, re

version = '0.3'

try:
	os.stat("src")
except OSError:
	try:
		os.stat("../4store/src")
	except OSError:
		print """
Could not find 4store sources in ../4store/src

Either ensure that the 4store source distribution is in ../4store
or else make a symbolic link from its src subdirectory to the 
py4s directory.

Recommend you use the git HEAD version of 4store, available from

	git://github.com/garlik/4store.git

Or the experimental branch with support for multiple simultaneous
clients from

	git://github.com/wwaites/4store.git

and remember use the latest version of rasqal and configure it
with

	'--enable-query-languages=sparql rdql laqrs'

so as to have COUNT support.
"""
		sys.exit(1)
	os.symlink("../4store/src", "src")

def get_includes(pkg):
	fp = os.popen("pkg-config --cflags-only-I %s" % (pkg,))
	includes = fp.read().strip().replace("-I", "").split(" ")
	fp.close()
	return includes
def get_libs(pkg):
	fp = os.popen("pkg-config --libs-only-L %s" % (pkg,))
	libdirs = fp.read().strip().replace("-L", "").split(" ")
	fp.close()
	fp = os.popen("pkg-config --libs-only-l %s" % (pkg,))
	libs = fp.read().strip().replace("-l", "").split(" ")
	fp.close()
	return libdirs, libs
def uniqify(l):
	seen = []
	for e in l:
		if not e or e in seen: continue
		seen.append(e)
	return seen
extra_includes = get_includes("glib-2.0") + get_includes("raptor") + get_includes("rasqal")
extra_includes = uniqify(extra_includes)
glib_dirs, glib_libs = get_libs("glib-2.0")
raptor_dirs, raptor_libs = get_libs("raptor")
rasqal_dirs, rasqal_libs = get_libs("rasqal")
library_dirs = uniqify(glib_dirs + rasqal_dirs + raptor_dirs)
libraries = uniqify(glib_libs + rasqal_libs + raptor_libs)

define_macros=[]

if not os.system("pkg-config rasqal --atleast-version=0.9.14"):
	define_macros.append(("HAVE_LAQRS", 1))
if not os.system("pkg-config rasqal --atleast-version=0.9.16"):
	define_macros.append(("HAVE_RASQAL_WORLD", 1))
try:
	os.stat("/usr/include/dns_sd.h")
	define_macros.append(("USE_DNS_SD", 1))
except OSError:
	pass

libpy4s = Library(
        name="py4s",
        sources=[
		"src/common/4s-common.c",
		"src/common/4s-client.c",
		"src/common/4s-mdns.c",
		"src/common/datatypes.c",
		"src/common/error.c",
		"src/common/umac.c",
		"src/common/rijndael-alg-fst.c",
		"src/common/md5.c",
		"src/common/hash.c",
		"src/common/msort.c",
		"src/common/qsort.c",
		"src/frontend/query.c",
		"src/frontend/query-cache.c",
		"src/frontend/query-datatypes.c",
		"src/frontend/filter.c",
		"src/frontend/filter-datatypes.c",
		"src/frontend/decimal.c",
		"src/frontend/results.c",
		"src/frontend/optimiser.c",
		"src/frontend/query-data.c",
		"src/frontend/order.c",
		"src/frontend/import.c",
		"py4s_helpers.c",
	],
	extra_compile_args=["-std=gnu99"],
        define_macros=define_macros,
	include_dirs=["src"] + extra_includes,
	library_dirs=library_dirs,
 	libraries=["pcre"] + libraries,
)

setup(name='py4s',
	version=version,
	description="Python C bindings for 4store",
	long_description="""\
Python C bindings for 4store""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='4store rdf triplestore',
	author='William Waites',
	author_email='wwaites_at_gmail.com',
	url='http://github.com/wwaites/py4s',
	license='GPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		"nose",
		"rdflib",
	],
	entry_points="""
	# -*- Entry points: -*-
	""",
	ext_modules=[libpy4s],
)

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))


# -- Project information -----------------------------------------------------

project = 'igsn_lib'
copyright = '2020, iSamples'
author = 'iSamples'

# The full version, including alpha/beta/rc tags
release = '0.1'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    #"myst_nb",
    "sphinx_copybutton",
    "sphinx_togglebutton",
    "sphinxcontrib.bibtex",
    "jupyter_sphinx",
    #"sphinx_thebe",
    #"sphinx_automodapi.automodapi",
    "sphinx.ext.autosummary",
    "sphinx.ext.autodoc",
    #"sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.graphviz",
    "nbsphinx",
    "sphinx.ext.mathjax",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

graphviz_output_format = "svg"

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_book_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_css_files = [
    'css/custom.css',
]

theme_extra_footer = """<small>This material is based upon work supported by the 
National Science Foundation under Grant Numbers <a href='https://nsf.gov/awardsearch/showAward?AWD_ID=2004839'>2004839</a>,
<a href='https://nsf.gov/awardsearch/showAward?AWD_ID=2004562'>2004562</a>, 
<a href='https://nsf.gov/awardsearch/showAward?AWD_ID=2004642'>2004642</a>,
and <a href='https://nsf.gov/awardsearch/showAward?AWD_ID=2004815'>2004815</a>. Any opinions, findings, and conclusions 
 or recommendations expressed in this material are those of the author(s) and do not necessarily 
 reflect the views of the National Science Foundation.</small>"""

html_theme_options = {
    "repository_url": "https://github.com/isamplesorg/igsn_lib",
    "use_repository_button": True,
    "use_edit_page_button": True,
    "use_issues_button": True,
    "extra_footer":theme_extra_footer,
}

# try to exclude deprecated
def skip_deprecated(app, what, name, obj, skip, options):
    if hasattr(obj, "func_dict") and "__deprecated__" in obj.func_dict:
        print("skipping " + name)
        return True
    return skip or False

def setup(app):
    app.connect('autodoc-skip-member', skip_deprecated)
    try:
        import importlib
        from docutils.statemachine import StringList
        from sphinx.pycode import ModuleAnalyzer, PycodeError
        from sphinx.ext.autosummary import Autosummary
        from sphinx.ext.autosummary import get_documenter
        from docutils.parsers.rst import directives
        from sphinx.util.inspect import safe_getattr
        import re

        class AutoFuncSummary(Autosummary):
            """
            Based on https://github.com/markovmodel/PyEMMA/blob/devel/doc/source/conf.py
            Use it like::
              .. autofuncsummary:: MODULE_NAME
                 :functions:
            to generate a table of functions in a module.
            """

            #option_spec = {
            #    'functions': directives.unchanged,
            #}
            option_spec = Autosummary.option_spec
            option_spec["functions"] = directives.unchanged
            option_spec["classes"] = directives.unchanged

            required_arguments = 1

            @staticmethod
            def get_members(obj, typ, include_public=None):
                if not include_public:
                    include_public = []
                items = []
                for name in dir(obj):
                    try:
                        documenter = get_documenter(app, safe_getattr(obj, name), obj)
                        #print(str(documenter))
                    except AttributeError:
                        continue
                    if documenter.objtype == typ:
                        items.append(name)
                public = [x for x in items if x in include_public or not x.startswith('_')]
                return public, items

            def run(self):
                module_name = self.arguments[0]
                try:
                    #m = __import__(module_name, globals(), locals(), [], 0)
                    m = importlib.import_module(module_name)
                    if 'functions' in self.options:
                        _, methods = self.get_members(m, 'function', ['__init__'])
                        self.content = ["~%s.%s" % (module_name, method) for method in methods if not method.startswith('_')]
                    if 'classes' in self.options:
                        _, methods = self.get_members(m, 'class', ['__init__'])
                        self.content = ["~%s.%s" % (module_name, method) for method in methods if
                                        not method.startswith('_')]
                finally:
                    return super(AutoFuncSummary, self).run()


        app.add_directive('autofuncsummary', AutoFuncSummary)
    except BaseException as e:
        raise e



# Napolean docstring settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = False
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = True
napoleon_use_param = True
napoleon_use_rtype = True
PLUGIN_VERSION=0.0.2
PLUGIN_ID=qlik-qvx

plugin:
	ant
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json java-lib java-formats

include ./Makefile.inc

#!
echo "Main-Class: infoDynamicSE.InfoProvider" > mc.mf
cd ../src
jar -cvfm ../classes/infoDynamicSE.jar ../jobs/mc.mf org/dcache/services/infoCollector/GlueSchemaV1_2*.class src/org/dcache/services/infoCollector/Schema.class infoDynamicSE/*.class
rm -f ../jobs/mc.mf  

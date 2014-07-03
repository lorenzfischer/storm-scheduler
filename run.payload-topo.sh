JAR_FOLDER="stools-topos/target"
LATEST_JAR=$(ls -1r $JAR_FOLDER/stools-topos*jar-with-dependencies.jar | head -1)
CMD="storm jar $LATEST_JAR ch.uzh.ddis.stools.topos.PayloadTopology -w 20 -p 20 -d 8 -mts 60 -msp 50000"
echo $CMD
eval $CMD

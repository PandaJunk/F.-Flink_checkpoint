javac -cp "../flink/lib/*:." FlinkCheckpointAnalysis.java
jar cfe FlinkCheckpointAnalysis.jar FlinkCheckpointAnalysis FlinkCheckpointAnalysis*.class
/opt/flink/bin/flink run taskmanager.memory.process.size=4096m FlinkCheckpointAnalysis.jar
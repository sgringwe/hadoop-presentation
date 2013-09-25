javac -classpath ${HADOOP_HOME}/hadoop-core-0.20.2-cdh3u3.jar -d wordcount_classes src/WordCountJob.java
jar -cvf wordcount.jar -C wordcount_classes/ .

bin/hadoop dfs -mkdir /user/sringwelski/wordcount/output

bin/hadoop dfs -copyFromLocal ~/Code/hadoop-presentation/data/file02 /user/sringwelski/wordcount/input

bin/hadoop dfs -cat /user/sringwelski/wordcount/input/file01

bin/hadoop jar ~/Code/hadoop-presentation/wordcount.jar com.sgringwe.wordcount.WordCountJob /user/sringwelski/wordcount/input /user/sringwelski/wordcount/output2

bin/hadoop dfs -cat /user/sringwelski/wordcount/output2/part-00000
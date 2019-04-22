
https://supergloo.com/spark-scala/spark-broadcast-accumulator-examples-scala/

https://spark.apache.org/docs/latest/rdd-programming-guide.html


https://stackoverflow.com/questions/1510126/free-dtd-to-xsd-conversion-utility

SBT assembly jar issue:

https://stackoverflow.com/questions/36437814/how-to-work-efficiently-with-sbt-spark-and-provided-dependencies

https://github.com/sbt/sbt-assembly

QuickStart VM install spark 2.x :
https://medium.com/@yesilliali/installing-apache-spark-2-x-to-cloudera-quickstart-wm-dd5314e6d9bd


Decription:

In this project we process the dblp xml dataset and calculate the Page ranks for all professors and publication venues in the dataset.

The program follows the following structure:

1. Load xml input using databricks spark xml:

2. Split the xml object using the "article" tag into a list of xml elements:

3. For each article xml, we extract the Professors and publication venues and generate tuples for each combination:

4. A list of tuples is fed into the Page Rank algorithm and page ranks are calculated for each item (professor/publication venue)


Input: The input file can loaded in the file system or hdfs. The path to the file needs to be provided in the config.

Output: The output of the program is a map of (K->V) pairs where K is the professor/publication venue and V is the page rank value.

Config:

spark {

  hadoopDir = "C:\\winutils"  								[Path to winutils.exe file. For windows only, for other OS leave blank ""]
  xmlLibrary = "com.databricks.spark.xml"  					[The xml library used to process the input]
  rowTag = "article" 										[The xml tag used to split the input]
  filePath = "sample.xml" 									[The input file path. For hdfs use following 			                    
																e.g. "//hdfs://quickstart.cloudera:8020/user/spark/dblp.xml"]
  accumulatorName = "Mapping"								[Name of the accumulator used to store the tuples]
  iterations = 10											[Number of iterations used by the page rank algorithm]
}

Setup:

Logging framework: Logback
Config framework: Typesafe config
Build framework: SBT
Programming Language: Scala
 
Environment:

1. Local IntelliJ: Execute the scala program com.uic.spark.PageRankMain

2. Cloudera QuickStart VM: 

	a. Create fat jar using 
	
		sbt assembly
		
		
	b. Start the spark shell using the following command

		
		/usr/bin/spark-shell --master local[*] --jars /home/cloudera/hw5/spark-xml_2.11-0.5.0.jar,/home/cloudera/hw5/cs441_hw5-assembly-0.1.jar
		
		Note: The path to the databricks spark-xml and page rank program jars need to be passed to the shell
		
	c. In the shell, enter the following
		
		com.uic.spark.PageRankMain.main(null)
		

		
	


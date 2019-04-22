
Decription:

In this project we process the dblp xml dataset and calculate the Page ranks for all professors and publication venues in the dataset.

The program follows the following structure:

1. Load xml input using databricks spark xml and split the xml object using the "article" tag into a list of xml elements:


		sparkSession.sqlContext.read.format("com.databricks.spark.xml")
		  .option("rowTag", "article")
		  .load("sample.xml")

	  The above code creates a sql.DataFrame. Dataframe is a distributed collection of data that provides the benefits of RDD. 
	  Using a Dataframe, we can query named columns within the dataframe which makes it easy to access data using xml tags. In 
	  the above case articles can be accessed like a list.

	

2. For each article xml, we extract the Professors and publication venues and generate tuples for each combination:


		<author>A1</author>
		<author>A2</author>
		<publication>P1</publication>

	This generates the following tuples - (A1,A2), (A2,A1), (A1,P1), (P1,A1), (A2,P1) (P1,A2)
	
	
4. The list of tuples is fed into the Page Rank algorithm and page ranks are calculated for each item (professor/publication venue)


Input: 

The input file can loaded in the file system or hdfs. The path to the file needs to be provided as an argument. (e.g. "//hdfs://quickstart.cloudera:8020/user/spark/dblp.xml")

Output:

The output of the program is a file with (K->V) pairs where K is the professor/publication venue and V is the page rank value.

Config:

	Config files : application.conf & test_application.conf

	Note: Some of the following configs are only used for test
	
		spark {

		  hadoopDir = "C:\\winutils"  								[Path to winutils.exe file. For windows only, for other OS leave blank ""]
		  xmlLibrary = "com.databricks.spark.xml"  					[The xml library used to process the input]
		  rowTag = "article" 										[The xml tag used to split the input]
		  accumulatorName = "Mapping"								[Name of the accumulator used to store the tuples]
		  iterations = 10											[Number of iterations used by the page rank algorithm]
		}

Setup:

	Logging framework: Logback
	
	Config framework: Typesafe config
	
	Build framework: SBT
	
	Programming Language: Scala
 
Environment and how to run:

1. Local IntelliJ: Execute the scala program com.uic.spark.PageRankMain or use sbt as follows:


	sbt clean compile "run <input_file> <output_file> <no_of_iterations>"    


2. Cloudera QuickStart VM: 

	Note: The spark-core and spark-sql libraries are already part of the spark shell. Thus change their scope to "provided" in the build.sbt
	
	a. Create fat jar using 
	
		sbt assembly
		
		
	b. Start the spark shell using the following command

		
		/usr/bin/spark-shell --master local[*] --jars /home/cloudera/hw5/spark-xml_2.11-0.5.0.jar,/home/cloudera/hw5/cs441_hw5-assembly-0.1.jar
		
		Note: The path to the databricks spark-xml and page rank program jars need to be passed to the shell
		
	c. In the shell, enter the following
		
		com.uic.spark.PageRankMain.main(Array("<input_file>", "<output_file>", "<no_of_iterations>"))
		

Unit Test Cases:

Execute test cases using : sbt test

Unit test cases for the following scenarios have been implemented:

	1. "XML to tuple mapping": A sample xml is converted to list of tuples. Finally we test if all expected tuples are generated
	
	2. "Tuples to Page Rank": Using a list of tuples generate the page ranks for each item
	
	3. "XML without row tags": Test for xml row tag not found in the input xml
	
	4. "Empty mapping list": Test page rank algorithm for empty mapping list
	
	5. "Two item mapping": Test page rank for A->B and B->A mapping. Page rank for both items should be 1.



		
	


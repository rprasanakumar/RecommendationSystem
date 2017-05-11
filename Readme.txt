
General Class generation:
Execution:


Generating the jar file -- > jar -cvf <<jarName>>.jar -C <<Class file path>> .

1.javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <filename>.java -d build -Xlint


2.Take the jar file for the generated class files and give the runtime arguments as inputfile path and outputfile path.
	e.g --> hadoop jar jarname.jar classname <<INputFilepath>> <<outputFilePath>>





Java Files:
Preprocessing Input:<Input file of user profile> <Input file of user rating> <Outputpath>

Input File : Book Dataset
Calculating Correlation scores(Cosine,Correlation,Jaccard)

//Coreleation score based on country
1. BookCountryCoeff.java
2. BookCountryCosine.java
3. BookCountryJac.java

Input file : Output file of preprocessing step
Parameters : Input File Location, Output file

//correlation score based on age
4. BookAgeCoeff.java
5. BookAgeCosine.java
6. BookAgeJac.java

Input file : Output file of preprocessing step
Parameters : Input File Location, Output file

//coorelation score based on age and country
7. BookCountryAgeCoeff.java
8. BookCountryAgeCosine.java
9. BookCountryAgeJac.java

Input file : Output file of preprocessing step
Parameters : Input File Location, Output file

User Recommendation
BookReco.java

Input file : Any one of the Output files from the above 9 Java Programs
Parameters : Input File Location, Output file

# Solution for HelloFresh Data Engineering Test
Solution for HelloFresh Data Engineering Test is builded as a python package for easy integration for other applications.

## Approach and Data Exploration
Applicaton is written as a package(HelloFresh) and which has been imported in run.py to need the given requirements in the problem statement.

While creating an instance of HelloFresh Class, which expects 1 argument i.e, URI for json data (as per problem statement an Amazon s3 (https://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json) URI is passed as argument while creating HelloFresh Instance in run.py line 4)

Instance init will set the variables to default values and perform the get request for given uri, extract json from response and constructs spark dataframe as df.

Method config() is used on created instance to set the upperbound of easy difficulty and lowerbound of hard difficulty, medium difficulty will be considered as between easy and hard.

Method get() is used on configured instance with 1 potential argument keyword, Which will be used to Filter Spark Dataframe with ingredients. This method will Extract Cook Time and Prep Time for filtered data in Spark Dataframe.

After Extracting Cook Time and Prep Time, String Data (xHxM) will be converted to integer minutes and compute average Total Time for Each difficulty (i.e, Easy, Medium, Hard), which will be segregated based on configued values from config() method.

Method save() will take filepath as a argument. If path doen't exist, It will create path tree and stores the output as given filename (Ex: report.csv inside output dir).

Method __ str__() has been defined for ease use in print statements or to extract output string from object.

__API Documentation and Call Graphs for HelloFresh Package is documented using Doxygen, Output of Doxygen can be found in doxygen dir (doxygen/html/index.html)__

__CI / CD and Performance Tuning is Explained in CI_CD&Performance_tuning.docx file__

## Assumptions / Considerations
- Dataset is available from an s3 (or alternate request) urls.
- Request url should return a json data.
- OpenJDK Version should be greater than 1.8
- Running with python 3.x
- Following python packages are installed:
-- pyspark == 3.0.0
-- requests == 2.23.0

## Executing the solution
To Execute the Spark Application with N CPUs and nGB driver memory
```
spark-submit --master local[*] --executor-cores=%NUMBER_OF_PROCESSORS% --driver-memory 4G run.py
```
or simply run
```
python run.py
```
## Unit Test
Unit test for application is done based on unittest framework in python. There are 6 test cases written to perform full-loop unit tests on package HelloFresh can be found in test.py
To perform unit test on the module, Run the test.py with following command:
```
python test.py
```
Output:
```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
C:\Users\Admin\AppData\Local\Programs\Python\Python36\lib\site-packages\pyspark\sql\session.py:378: UserWarning: inferring schema from dict is deprecated,please use pyspark.sql.Row instead
  warnings.warn("inferring schema from dict is deprecated,"
C:\Users\Admin\AppData\Local\Programs\Python\Python36\lib\socket.py:657: ResourceWarning: unclosed <socket.socket fd=1028, family=AddressFamily.AF_INET, type=SocketKind.SOCK_STREAM, proto=0, laddr=('127.0.0.1', 59256), raddr=('127.0.0.1', 59255)>
  self._sock = None
...
----------------------------------------------------------------------
Ran 6 tests in 9.256s

OK

SUCCESS: The process with PID 19388 (child process of PID 18880) has been terminated.
SUCCESS: The process with PID 18880 (child process of PID 5496) has been terminated.
SUCCESS: The process with PID 5496 (child process of PID 16276) has been terminated.
```

## Code Coverage
Code Coverage is tested with python's coverage package, to install coverage package run the following command
```
pip install coverage==5.3
```
To Run Code Coverage
```
coverage run test.py
```
Output:
```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
c:\users\admin\appdata\local\programs\python\python36\lib\site-packages\pyspark\sql\session.py:378: UserWarning: inferring schema from dict is deprecated,please use pyspark.sql.Row instead
  warnings.warn("inferring schema from dict is deprecated,"
c:\users\admin\appdata\local\programs\python\python36\lib\socket.py:657: ResourceWarning: unclosed <socket.socket fd=1064, family=AddressFamily.AF_INET, type=SocketKind.SOCK_STREAM, proto=0, laddr=('127.0.0.1', 59336), raddr=('127.0.0.1', 59335)>
  self._sock = None
...
----------------------------------------------------------------------
Ran 6 tests in 9.213s

OK
```

To get report of code coverage
```
coverage report
```
Output:
```
Name                       Stmts   Miss  Cover
----------------------------------------------
HelloFresh\__init__.py         2      0   100%
HelloFresh\hellofresh.py      96      3    97%
test.py                       42      0   100%
----------------------------------------------
TOTAL                        140      3    98%
```
To generate report as HTML run the following command. Output of html report is placed in htmlcov dir.
```
coverage html
```
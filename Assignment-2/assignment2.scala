// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program.
val inputFilePath = "sample_input.txt";
val outputDirPath = "sample_output.txt";

// Write your solution here
// read input text file
val file = sc.textFile(inputFilePath, 1);

// remove blank lines
val lines = file.filter(x => x.contains("http"));

// get URL and payload size (in B, KB or MB) for each HTTP request
val urls = lines.map(x => (x.split(",")(0), x.split(",")(3)));

// convert payload size into bytes for each HTTP request
val payload = urls.map(x => (x._1, x._2.split("[BKM]")(0).toLong * (x._2.contains("K").compare(false) * 1024 + x._2.contains("M").compare(false) * 1024 * 1024 + ((!x._2.contains("K")).compare(false) * (!x._2.contains("M")).compare(false)))));

// compute minimum payload size (in B)
val minp = payload.reduceByKey((x, y) => (x >= y).compare(false) * y + (x < y).compare(false) * x);

// compute maximum payload size (in B)
val maxp = payload.reduceByKey((x, y) => (x >= y).compare(false) * x + (x < y).compare(false) * y);

// compute mean of payload sizes (in B)
var temp1 = payload.map(x => (x._1, (x._2, 1)));
temp1 = temp1.reduceByKey{case((x1, x2), (y1, y2)) => ((x1 + y1), (x2 + y2))};
val meanp = temp1.mapValues{case(x, y) => x / y};

// compute variance of payload sizes (in B)
var temp2 = payload.join(meanp).map(x => (x._1, ((x._2._1 - x._2._2) * (x._2._1 - x._2._2), 1)));
temp2 = temp2.reduceByKey{case((x1, x2), (y1, y2)) => ((x1 + y1), (x2 + y2))};
val varp = temp2.mapValues{case(x, y) => x / y};

// merge all relevant computations into a single RDD
var temp3 = minp.join(maxp);
var temp4 = meanp.join(varp);
var temp5 = temp3.join(temp4).mapValues(x => (x._1._1, x._1._2, x._2._1, x._2._2));

// write result RDD to output file
val result = temp5.map(x => x._1 + "," + x._2._1.toString + "B," + x._2._2.toString + "B," + x._2._3.toString + "B," + x._2._4.toString + "B");
result.saveAsTextFile(outputDirPath);

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml.XML
import scala.collection.mutable._
import scalaj.http.Http
import org.json4s.JValue
import org.json4s.Xml.toJson
import org.json4s.jackson.JsonMethods._
	
	
object CaseIndex {
	// generate sets of strings and combine them for each entity type (location, organization, person) for each case report
	def generate_et(xmlstr: String): (String, String, String) ={
		// create empty set for each entity type (location, organization, person)
		var loc = Set[String]()
		var org = Set[String]()
		var per = Set[String]()
        
		// load XML object from string
		val xmlobj = XML.loadString(xmlstr)

		// access contents of word tags from XML object
		val word = xmlobj \\ "word"

		// iterate over NER tag contents of XML object and add the word corresponding to each matching entity type to its respective set
		for ((k, v) <- (xmlobj \\ "NER").view.zipWithIndex) {
			if (k.text == "LOCATION") loc += word(v).text
			if (k.text == "ORGANIZATION") org += word(v).text
			if (k.text == "PERSON") per += word(v).text
		}
        
		// return 3-tuple consisting of single strings (derived from the mutable set of strings) for each entity type
		return (loc.mkString(" "), org.mkString(" "), per.mkString(" "))
	}
    
	def main(args: Array[String]) {
		val inputFiles = args(0)
		val conf = new SparkConf().setAppName("CaseIndex").setMaster("local")

		// CoreNLP server address
		val nlp_url = "http://localhost:9000"
		
		// Elasticsearch server address (with index name)
		val es_url = "http://localhost:9200/legal_idx"

		// create a Scala Spark Context
		val sc = new SparkContext(conf)
		
		// load input data (list of legal case reports) and set key as file name
		var input = sc.wholeTextFiles(inputFiles + "/*.xml")
		input = input.map{case(key, value) => (key.split("/").last.dropRight(4), value)}
		
		// map each file string to its corresponding XML object
		val xml = input.map{case(key, value) => (key, value, XML.loadString(value))}
		
		// send each XML object (in String format) to CoreNLP server for data curation
		val nlp_resp = xml.map{case(key, value, xmlobj) => (key, value, Http(nlp_url).postData(xmlobj.text).param("outputFormat", "xml").param("annotators", "ner").timeout(readTimeoutMs=200000, connTimeoutMs=30000).asString.body)}

		// generate sets of each entity type (location, organization, person) for each case report
		var entities = nlp_resp.map{case(key, value, xmlstr) => (key, value.replace("\n", " "), generate_et(xmlstr))}
		entities = entities.map{case(key, value, et) => (key, value.replace("\"", "\\\""), et)}

		// create Elasticsearch index
		val es_idx = Http(es_url + "?pretty").method("PUT").timeout(readTimeoutMs=500000, connTimeoutMs=30000).asString
        
		// create Elasticsearch mapping with entity types (file_name, file_content, location, organization, person)
		val es_map = Http(es_url + "/cases/_mapping?pretty").postData("{\"cases\": {\"properties\": {\"file_name\" : {\"type\": \"text\"}, \"file_content\" : {\"type\": \"text\"}, \"location\" : {\"type\": \"text\"}, \"organization\" : {\"type\": \"text\"}, \"person\" : {\"type\": \"text\"}}}}").header("content-type", "application/json").method("PUT").timeout(readTimeoutMs=500000, connTimeoutMs=30000).asString

		// create Elasticsearch document for each case report
		val es_resp = entities.map{case(key, value, (loc, org, per)) => (Http(es_url + "/cases/" + key.toString + "?pretty").postData("{\"file_name\" : \"" + key + "\", \"file_content\" : \"" + value + "\", \"location\" : \"" + loc + "\", \"organization\" : \"" + org + "\", \"person\" : \"" + per + "\"}").method("PUT").header("content-type", "application/json").timeout(readTimeoutMs=500000, connTimeoutMs=30000).asString.code)}
		
		es_resp.foreach(println)	
	}
}


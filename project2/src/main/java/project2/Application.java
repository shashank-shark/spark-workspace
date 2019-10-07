package project2;

public class Application {
	
	public static void main (String[] args) {
//		InferCSVSchema parser = new InferCSVSchema();
//		parser.printSchema();
		
//		DefineCSVSchema schemaParser = new DefineCSVSchema();
//		schemaParser.printDefinedSchema();
		
		JSONLineParser jsonParser = new JSONLineParser();
		jsonParser.parseJSONLines();
	}
}
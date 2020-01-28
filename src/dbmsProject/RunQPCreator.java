package dbmsProject;

public class RunQPCreator{
	
	public static void main(String args[]) throws Exception{
		//Accept query inputs from user
		Payload payload=new Payload();
		payload.create();
		
		QPCreator qpCreator = new QPCreator(payload);
		//dscreator.sequenceOfGroups();
		qpCreator.createInputRow();
		qpCreator.createOutputRow();
		qpCreator.createQueryProcessor();
	}
}
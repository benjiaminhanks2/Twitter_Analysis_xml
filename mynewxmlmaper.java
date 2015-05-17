//package myxmlparser;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mynewxmlmaper  extends Mapper<LongWritable, Text,Text,NullWritable> {
	  
	@Override
	  protected void map(LongWritable key, Text value,
	                    Context context)
	      throws
	      IOException, InterruptedException {
	    String document = value.toString();
	    System.out.println("‘" + document + "‘");
	        try {
	      XMLStreamReader reader =
	          XMLInputFactory.newInstance().createXMLStreamReader(new
	              ByteArrayInputStream(document.getBytes()));
	      String propertyName = "";
	      String propertyValue = "";
	      String currentElement = "";
	      while (reader.hasNext()) {
	        int code = reader.next();
	        switch (code) {
	          case XMLStreamConstants.START_ELEMENT: //START_ELEMENT:
	            currentElement = reader.getLocalName();
	            break;
	          case XMLStreamConstants.CHARACTERS:  //CHARACTERS:
	           // if (currentElement.equalsIgnoreCase("item")) {
	              propertyName += reader.getText();
	              System.out.println(propertyName);
	    //        } else if (currentElement.equalsIgnoreCase("item")) {
	      //        propertyValue += reader.getText();
	       //       //System.out.println(propertyValue);
	           // }
	            break;
	        }
	      }
	      reader.close();
	      String writvalue=propertyName.replace("\n"," ");
	      //String newpropname=StringUtils.substringBetween(propertyName,"<![CDATA[","]]");
	      context.write(new Text(writvalue.trim()),NullWritable.get());
	     
	    }
	        catch(Exception e){
	                throw new IOException(e);

	                }

	  }
	}


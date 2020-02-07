package com.ibm.app.csv.app;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.ibm.app.csv.schema.CSVRecord;
import com.ibm.app.enums.CountryEnum;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameTranslateMappingStrategy;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Collections;
import com.ibm.app.csv.constants.IKafkaConstants;
import com.ibm.app.csv.consumer.ConsumerCreator;
import com.ibm.app.csv.producer.ProducerCreator;

public class CSVKafkaProcessor {
	
private static final String SAMPLE_CSV_FILE_PATH = "/home/edyoda/CSVToKafka/testInputData.csv";

public static void main(String[] args) {
	

Reader reader; 

try { 
    reader = Files.newBufferedReader(Paths.get(SAMPLE_CSV_FILE_PATH));


CsvToBean<CSVRecord> csvToBean = new CsvToBeanBuilder(reader)
.withType(CSVRecord.class)
.withIgnoreLeadingWhiteSpace(true)
.build();

List<CSVRecord> recordList = csvToBean.parse();
// print details of Bean object 
/*for (CSVRecord e : recordList) { 
	System.out.println("in for loop");
    System.out.println(e.getEmpId()); 
    System.out.println(e.getEmpName());
}*/ 
	//runProducer(recordList);
Stream.of(CountryEnum.values()).map(r->r.topicName).collect(Collectors.toList()).forEach(s->runConsumer(java.util.Collections.singletonList(s)));
//runConsumer();
} 
catch (IOException e) { 

    // TODO Auto-generated catch block 
    e.printStackTrace(); 
} 


}
static void runProducer(List<CSVRecord> recordList) {
	Producer<Long, CSVRecord> producer = ProducerCreator.createProducer();
	//String TOPIC_NAME = IKafkaConstants.TOPIC_NAME;
 
	for (int index = 1; index < recordList.size(); index++) {
		CSVRecord c = new CSVRecord();
		c = recordList.get(index);
		 
		final ProducerRecord<Long, CSVRecord> record = new ProducerRecord<Long, CSVRecord>(CountryEnum.getCountry(c.getCountry()).topicName,c);
		
		try {
			RecordMetadata metadata = producer.send(record).get();
			//producer.send(record, new DemoCallback());
			System.out.println(index);
			System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
					+ " with offset " + metadata.offset());
		} catch (ExecutionException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		} catch (InterruptedException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		}
	}
}

static void runConsumer(List<String> topics) {
	Consumer<Long, CSVRecord> consumer = ConsumerCreator.createConsumer();
	
	consumer.subscribe(topics);
	int noMessageToFetch = 0;

	while (true) {
		final ConsumerRecords<Long, CSVRecord> consumerRecords = consumer.poll(1000);
		if (consumerRecords.count() == 0) {
			noMessageToFetch++;
			if (noMessageToFetch > 10)
				break;
			else
				continue;
		}

		consumerRecords.forEach(record -> {
			System.out.println("Record Key " + record.key());
			System.out.println("Record value " + record.value().getCustomerID());
			System.out.println("Record value " + record.value().getCountry());
			System.out.println("Record partition " + record.partition());
			System.out.println("Record offset " + record.offset());
		});
		consumer.commitAsync();
	}
	consumer.close();
}

}

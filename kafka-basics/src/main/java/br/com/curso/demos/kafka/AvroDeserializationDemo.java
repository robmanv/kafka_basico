package br.com.curso.demos.kafka;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;

public class AvroDeserializationDemo {
    public static void main(String args[]) throws Exception{

        //Instantiating the Schema.Parser class.
        Schema schema = new Schema.Parser().parse(new File("/Users/robsvel/projetos/kafka-beginners-course/kafka-basics/src/main/resources/avro/emp.avsc"));
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("/Users/robsvel/projetos/kafka-beginners-course/kafka-basics/src/main/resources/mydata.txt"), datumReader);
        GenericRecord emp = null;

        while (dataFileReader.hasNext()) {
            emp = dataFileReader.next(emp);
            System.out.println(emp);
        }
        System.out.println("hello");
    }
}

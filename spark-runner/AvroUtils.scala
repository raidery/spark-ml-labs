import org.apache.avro.io._
import org.apache.avro.specific._
import org.apache.avro.file.DataFileWriter
import java.util.ArrayList
import java.io.File
import example.avro._
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.file.DataFileReader
import org.apache.avro.util.ByteBufferOutputStream
import java.io._
object Writeit {
  def f {
    val e = new EnrichedEvent
    val columns = new ArrayList[CharSequence]
    columns.add("first")
    columns.add(null)
    columns.add("third")
    e.setColumns(columns)
    e.setVersion("V")

    val datumWriter = new SpecificDatumWriter[EnrichedEvent];
    val dataFileWriter = new DataFileWriter(datumWriter)
    dataFileWriter.create(e.getSchema, new File("demo.avro"))
    dataFileWriter.append(e)
    dataFileWriter.close()
  }

  def createStream: ByteArrayOutputStream = {
    val e = new EnrichedEvent
    val outStream = new ByteArrayOutputStream
    // val encoder = EncoderFactory.get().jsonEncoder(e.getSchema, outStream)
    val encoder = EncoderFactory.get().directBinaryEncoder(outStream, null)
    println(encoder.getClass)
    val columns = new ArrayList[CharSequence]
    columns.add("first")
    columns.add(null)
    columns.add("third")
    e.setColumns(columns)
    e.setVersion("V")

    val datumWriter = new SpecificDatumWriter[EnrichedEvent](e.getSchema);

    datumWriter.write(e, encoder)
    datumWriter.getClass.getMethods.foreach(println)
    encoder.flush
    println(new String(outStream.toByteArray))
    println(outStream.getClass)
    println(outStream.toString)
    outStream
  }
}

object Readit {
  def f {
    // val schema = new Schema.Parser().parse(new File("EnrichedEvent.avsc"))
    val input = new File("demo.avro")
    val datumReader = new GenericDatumReader[GenericRecord]
    val dataFileReader = new DataFileReader[GenericRecord](input, datumReader)
    var user: GenericRecord = null
    while (dataFileReader.hasNext()) {
      user = dataFileReader.next(user)
      println(user)
    }
  }

  def fromStream(stream: ByteArrayOutputStream) = {
    val datumReader = new SpecificDatumReader[EnrichedEvent]
    val decoder = DecoderFactory.get().binaryDecoder(stream.toString.getBytes, null)
    val reader = new SpecificDatumReader[EnrichedEvent]
    reader.setSchema(new EnrichedEvent().getSchema)
    val record = reader.read(null, decoder)
    println(record)
    reader.read(null, decoder)
  }
}

object App {
  def main(args: Array[String]) {
    val stream = Writeit.createStream
    Readit.fromStream(stream)
    
    // Readit.f
  }
}

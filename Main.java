
package com.google.cloud.solutions.rtdp;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converter works as a streaming Dataflow job to receive temperature, coordinates and other
 * attributes from Cloud Pub/Sub and then, convert and load them into BigQuery.
 * @author teppeiy
 *
 */
public class Converter {
  private static final Logger LOG = LoggerFactory.getLogger(Converter.class);

  private static NetHttpTransport TRANSPORT = new NetHttpTransport( );
  private static JacksonFactory JSON_FACTORY = new JacksonFactory( );

  /**
   * RtdpOptions extends DataflowPipelineOptions to retrieve a Pub/Sub topic as a command
   * line argument.
   * @author teppeiy
   *
   */
  public interface RtdpOptions extends DataflowPipelineOptions {
    String getTopic();

    void setTopic(String topic);
  }

  public Converter() {}

  /** Starts the DataFlow convertor. */
  public void startConverter(RtdpOptions options) throws IOException {
    final String projectId = options.getProject();
    final String topic = options.getTopic();
    final String datasetId = "iotds";
    final String tableName = "temp_Clock_Angle";

    String id = Long.toString(System.currentTimeMillis());
    options.setJobName("converter-" + id);
	
	
    // Function to calculate the angle
    static int calcAngle(double h, double m)
    {
        // validate the input
        if (h <0 || m < 0 || h >12 || m > 60)
            System.out.println("Wrong input");
 
        if (h == 12)
            h = 0;
             if (m == 60)
       {
        m = 0;
        h += 1;
        if(h>12)
          h = h-12;
        } 
  
        // Calculate the angles moved by hour and minute hands
        // with reference to 12:00
        int hour_angle = (int)(0.5 * (h*60 + m));
        int minute_angle = (int)(6*m);
 
        // Find the difference between two angles
        int angle = Math.abs(hour_angle - minute_angle);
 
        // smaller angle of two possible angles
        angle = Math.min(360-angle, angle);
 
        return angle;
    }
   
    GoogleCredential credential = GoogleCredential.getApplicationDefault(TRANSPORT, JSON_FACTORY);
    Bigquery bigquery = new Bigquery(new NetHttpTransport(),new calcAngle(), credential);
    Dataset dataset = new Dataset();
    DatasetReference datasetRef = new DatasetReference();
    datasetRef.setProjectId(projectId);
    datasetRef.setDatasetId(datasetId);
    dataset.setDatasetReference(datasetRef);
    try {
      bigquery.datasets().insert(projectId, dataset).execute();
      LOG.debug("Creating dataset : " + datasetId);
    } catch (IOException e) {
      LOG.debug(datasetId + " dataset already exists.");
    }

    TableReference ref = new TableReference();
    ref.setProjectId(projectId);
    ref.setDatasetId(datasetId);
    ref.setTableId(tableName);

    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
    fields.add(new TableFieldSchema().setName("Time").setType("Datetime"));
    fields.add(new TableFieldSchema().setName("Angle").setType("Int"));
   
    TableSchema schema = new TableSchema().setFields(fields);

    Pipeline p = Pipeline.create(options);
    p.apply(
            PubsubIO.readStrings()
                .fromTopic("projects/" + options.getProject() + "/topics/" + topic))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(ParDo.of(new RowGenerator()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(ref)
                .withSchema(schema)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.alwaysRetry())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    p.run();
  }

  /**
   * RowGenerator parses a comma separated record and converts it into BigQuery TableRow.
   * @author teppeiy
   *
   */
  public static class RowGenerator extends DoFn<String, TableRow> implements Serializable {
    private static final long serialVersionUID = -1366613943065649148L;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /** Processes an element. */
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      String[] attrs = c.element().split(",");
      LOG.debug(attrs[0]);
      TableRow row =
          new TableRow()
              .set("time"),
              .set("angle");
    }
  }

  /** Main entry point. */
  public static void main(String[] args)
      throws IOException, InterruptedException, GeneralSecurityException {
    PipelineOptionsFactory.register(RtdpOptions.class);
    RtdpOptions converterOpts = PipelineOptionsFactory.fromArgs(args).as(RtdpOptions.class);
    Clockhandtoangle clock_Angle= new Clockhandtoangle();
    Converter bench = new Converter();
    System.out.println("Starting Converter");
    bench.startConverter(converterOpts);
  }
}

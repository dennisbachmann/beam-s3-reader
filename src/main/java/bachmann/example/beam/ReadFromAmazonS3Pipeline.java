package bachmann.example.beam;

import bachmann.example.beam.options.S3PipelineOptions;
import bachmann.example.beam.transforms.PrintLineTransform;
import bachmann.example.beam.transforms.ReadFromS3Transform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFromAmazonS3Pipeline {

    private static final Logger LOG = LoggerFactory.getLogger(ReadFromAmazonS3Pipeline.class);

    /**
     * Reads a file from S3 and outputs each line via logger.
     *
     * This is a Beam POC to test integration with Amazon S3 (wildcard supported).
     *
     * @param args contains program arguments:
     *             --awsCredentialsProvider="{\"@type\" : \"AWSStaticCredentialsProvider\", \"awsAccessKeyId\" : \"AWS_ACCESS_KEY\", \"awsSecretKey\" : \"AWS_SECRET_KEY\"}"
     *             --awsRegion="AWS_REGION i.e. us-east-1"
     *             --s3Bucket="BUCKET_NAME"
     *             --s3FilePattern="FILE_PATTERN"
     */
    public static void main(String[] args) {
        S3PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(S3PipelineOptions.class);

        PTransform<PInput, PCollection<String>> fileReaderTransform = ReadFromS3Transform.Builder
                .newBuilder()
                .setBucket(options.getS3Bucket())
                .setPath(options.getS3FilePattern())
                .build();

        PTransform<PCollection<String>, PCollection<String>> logPrinter = PrintLineTransform.usingLogger(LOG);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read files", fileReaderTransform)
                .apply("Print lines", logPrinter);

        pipeline.run();
    }
}

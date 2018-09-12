package bachmann.example.beam.options;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.Validation;

public interface S3PipelineOptions extends S3Options {

    @Validation.Required
    String getS3Bucket();
    void setS3Bucket(String bucketName);

    @Validation.Required
    String getS3FilePattern();
    void setS3FilePattern(String filePattern);
}

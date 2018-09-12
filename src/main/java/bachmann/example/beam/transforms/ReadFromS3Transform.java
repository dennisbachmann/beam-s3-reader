package bachmann.example.beam.transforms;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

public class ReadFromS3Transform extends PTransform<PInput, PCollection<String>> {

    public static class Builder {

        private String bucket;
        private String path;

        private Builder() {}

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder setPath(String path) {
            this.path = path;
            return this;
        }

        public PTransform<PInput, PCollection<String>> build() {
            return new ReadFromS3Transform(bucket, path);
        }
    }

    private final String bucket;
    private final String path;

    private ReadFromS3Transform(String bucket, String path) {
        this.bucket = bucket;
        this.path = path;
    }

    @Override
    public PCollection<String> expand(PInput input) {
        String path = buildFilePattern();

        return input
                .getPipeline()
                .apply("Read files from S3", TextIO.read().from(path));
    }

    private String buildFilePattern() {
        return String.format("s3://%s/%s", this.bucket, this.path);
    }
}

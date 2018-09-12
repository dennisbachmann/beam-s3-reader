package bachmann.example.beam.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintLineTransform extends PTransform<PCollection<String>, PCollection<String>> {

    private final Logger logger;

    public static PTransform<PCollection<String>, PCollection<String>> usingLogger(Logger logger) {
        return new PrintLineTransform(logger);
    }

    private PrintLineTransform(Logger logger) {
        if (logger == null) {
            this.logger = LoggerFactory.getLogger(PrintLineTransform.class);
        } else {
            this.logger = logger;
        }
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply("Print lines", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                String line = context.element();

                logger.info(line);

                context.output(line);
            }
        }));
    }
}

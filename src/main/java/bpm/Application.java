package bpm;

import bpm.log_editor.storage.LogRepository;
import bpm.log_editor.storage.LogService;
import bpm.log_editor.storage.StorageProperties;
import bpm.log_editor.storage.StorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import bpm.statistics.Arc;
import bpm.statistics.Interval;
import bpm.statistics.spark.ActivityStatistics;
import bpm.statistics.spark.AssistantFunctions;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;

import java.io.IOException;

@SpringBootApplication
@EnableConfigurationProperties(StorageProperties.class)
@EnableMongoRepositories
public class Application {

	@Autowired
	private LogRepository repo;

	public static void main(String[] args) throws IOException {
		//MongoJDBC mongo = new MongoJDBC();
		SpringApplication.run(Application.class, args);
		//testSpark();
	}

	@Bean
	CommandLineRunner init(StorageService storageService) {
		return (args) -> {
            //storageService.deleteAll();
            storageService.init();
			LogService.init(repo);
			repo.findAll();
		};
	}

	 public static void testSpark() {
		//Predefined arc
		//TO Medeiros
		//Arc a = new Arc("ArtificialStartTask2", "ArtificialStartTask");
		//TO Amtega
		Arc a = new Arc("700", "700");
		//TO Amtega
		Interval in = new Interval(1388531160000l, 1388531280000l);

		AssistantFunctions.setConfig("Ozona.csv0");
		//JavaRDD<Document> staticsReduced = AssistantFunctions.getInitialRDD();
		//Utils.printDocument(staticsReduced.take(100));

		//AssistantFunctions.orderRDD("staticsReduced", "trace", "timestampf");
		//Utils.printDocument(staticsReduced.take(100));

		//AssistantFunctions.setConfig("staticsReduced", false);

		ActivityStatistics.activityFrequency();
		ActivityStatistics.activityFrequencyCuartil();
		ActivityStatistics.activityAverages();
		ActivityStatistics.activityLongerShorterDuration();
/*

		TracesStatistics.traceFrequency();
		TracesStatistics.traceFrequencyRelative();

		AssistantFunctions.setConfig("staticsReduced", false, in);

		TracesStatistics.traceFrequency();
		TracesStatistics.traceFrequencyRelative(); */

		hold();
	}

	//A method to keep Java running, so that we can explore Spark instance.
	public static void hold() {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry
                .addResourceHandler("/files/**")
                .addResourceLocations("file:/opt/files/");
    }
}

package bpm.services;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import com.mongodb.DBCollection;
import es.usc.citius.prodigen.domainLogic.exceptions.*;
import es.usc.citius.prodigen.domainLogic.workflow.Task.Task;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.geneticMining.CMTask.CMTask;
import es.usc.citius.prodigen.domainLogic.workflow.algorithms.geneticMining.individual.CMIndividual;
import es.usc.citius.prodigen.spark.FHMSpark;
import es.usc.citius.prodigen.util.IndividualVO;
import es.usc.citius.womine.input.InputSpark;
import es.usc.citius.womine.model.Graph;
import es.usc.citius.womine.model.Pattern;
import es.usc.citius.womine.model.Trace;
import gnu.trove.map.TIntObjectMap;
import bpm.log_editor.data_types.*;
import bpm.log_editor.parser.CSVparser;
import bpm.log_editor.parser.ConstantsImpl;
import bpm.log_editor.parser.XESparser;
import bpm.log_editor.storage.MongoDAO;
import bpm.log_editor.storage.LogService;
import bpm.log_editor.storage.StorageProperties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import bpm.log_editor.storage.StorageService;
import bpm.statistics.Arc;
import bpm.statistics.spark.*;

import static bpm.log_editor.parser.CSVparser.savedParsed;
import static bpm.log_editor.storage.MongoDAO.db;
import static bpm.log_editor.storage.MongoDAO.individualToVO;

@RestController
public class StatsController implements Serializable{

    private final StorageService storageService;

    @Autowired
    public StatsController(StorageService storageService) {
        this.storageService = storageService;
        CSVparser.storageService = storageService;
        LogService.storageService = storageService;
    }

    //---PATTERNS---//
    //Queries a log with a determined hierarchy
    @CrossOrigin
    @RequestMapping(value = "/frequentPatterns", method = RequestMethod.GET)
    public String frequentPatters(@RequestParam("file") String file, String data, Double threshold) {
        System.out.println(file);
        System.out.println(data);
        return LogService.getLogByName(file).getFrequentPatterns(Integer.parseInt(data), threshold);
    }

    //Queries a log with a determined hierarchy
    @CrossOrigin
    @RequestMapping("/infrequentPatterns")
    public String infrequentPatters(@RequestParam("file") String file, String data, Double threshold) {
        System.out.println(file);
        System.out.println(data);
        return LogService.getLogByName(file).getInfrequentPatterns(Integer.parseInt(data), threshold);
    }

    //Saves a frequent log
    @CrossOrigin
    @RequestMapping(value = "/frequentPatterns", method = RequestMethod.POST)
    public LogFile saveFrequentPattern(@RequestParam("file") String file, Integer model, Pattern data) {
        LogService.getLogByName(file).addPattern(model, data, "frequent");
        return LogService.getLogByName(file);
    }

    //Saves an  infrequent log
    @CrossOrigin
    @RequestMapping(value = "/infrequentPatterns", method = RequestMethod.POST)
    public LogFile saveInfrequentPattern(@RequestParam("file") String file, Integer model, Pattern data) {
        System.out.println(file);
        System.out.println(model);
        System.out.println(data);
        LogService.getLogByName(file).addPattern(model, data, "infrequent");
        return LogService.getLogByName(file);
    }

    //---TRACES---//
    @CrossOrigin
    @RequestMapping(value = "/traces", method = RequestMethod.GET)
    public List<String> getTraceActivities(@RequestParam("file") String file, Integer data, String tracename) {
        System.out.println(file);
        System.out.println(data);
        System.out.println(tracename);

        //Statistics
        //TODO Revise this. Delete.
        AssistantFunctions.setConfig(file + data);

        //Return activities
        return null;
    }

    //---STATS FUNCTIONS---//
    //Activity stats
    @CrossOrigin
    @RequestMapping("logsStats/{id}/activity")
    public ActivityStatisticsVO activityStats(@PathVariable String id) {

        AssistantFunctions.setConfig(id);

        //Create new return object
        ActivityStatisticsVO result = new ActivityStatisticsVO();

        //Activity frequencies
        result.activityFrequency = ActivityStatistics.activityFrequency();
        result.activityRelativeFrequency = ActivityStatistics.activityRelativeFrequency();
        result.activityFrequencyCuartil = ActivityStatistics.activityFrequencyCuartil();
        result.activityAverages = ActivityStatistics.activityAverages();
        result.activityLongerShorterDuration = ActivityStatistics.activityLongerShorterDuration();

        return result;
    }

    //Activity stats
    @CrossOrigin
    @RequestMapping("logsStats/{id}/arc")
    public List<Arc> arcStats(@PathVariable String id) throws EmptyLogException, InvalidFileExtensionException, MalformedFileException, WrongLogEntryException, NonFinishedWorkflowException {
        JavaRDD<Document> orderRDD = AssistantFunctions.setConfig(id);

        //Save activities RDD
        JavaRDD<Document> activities = AssistantFunctions.getAllActivities(orderRDD, false, false);
        AssistantFunctions.setInitialRDD(activities);
        //Save traces RDD
        JavaRDD<Document> traces = AssistantFunctions.getAllTraces(false);
        AssistantFunctions.setTracesRDD(traces);

        //Necesitamos modelo y log
        CMIndividual model = FHMSpark.mine(SparkConnection.getContext(), AssistantFunctions.getTracesRDD());
        ConstantsImpl.setModelFormat(es.usc.citius.womine.utils.constants.Constants.CN_EXTENSION);

        HashMap<Integer, Task> intToTaskLo = new HashMap<>();
        TIntObjectMap<CMTask> tasks = model.getTasks();
        for (int i = 0; i < tasks.size(); i++) {
            CMTask cmTask = tasks.get(i);
            intToTaskLo.put(cmTask.getTask().getMatrixID(), cmTask.getTask());
        }

        IndividualVO individualVO = individualToVO(model, intToTaskLo);

        Graph graph = new Graph(individualVO);

        HashMap<Integer, es.usc.citius.womine.model.Node> nodesSpark = graph.getNodesSpark();
        Map<String, Integer> relationIDs = new HashMap<>();
        for (Map.Entry<Integer, es.usc.citius.womine.model.Node> node : nodesSpark.entrySet()) {
            relationIDs.put(node.getValue().getId().replace(":complete", ""), node.getKey());
        }

        JavaPairRDD<String, Trace> caseInstances = InputSpark.getCaseInstances(graph, individualVO.getFormat(), AssistantFunctions.getTracesRDD(), relationIDs, SparkConnection.getContext());

        List<Arc> arcs = ArcsStatistics.arcsFrequencyOrderedSpark(caseInstances, intToTaskLo);

        ConstantsImpl.setModelFormat(es.usc.citius.womine.utils.constants.Constants.CM_EXTENSION);
        return arcs;
    }

    //Activity stats
    @CrossOrigin
    @RequestMapping("logsStats/{id}/trace")
    public TraceStatisticsVO traceStats(@PathVariable String id) {
        JavaRDD<Document> orderRDD = AssistantFunctions.setConfig(id);

        //Save activities RDD
        JavaRDD<Document> activities = AssistantFunctions.getAllActivities(orderRDD, false, false);
        AssistantFunctions.setInitialRDD(activities);
        //Save traces RDD
        JavaRDD<Document> traces = AssistantFunctions.getAllTraces(false);
        AssistantFunctions.setTracesRDD(traces);

        //Create new return object
        TraceStatisticsVO result = new TraceStatisticsVO();

        //Trace frequencies
        result.traceFrequency = TracesStatistics.traceFrequency();
        result.traceRelativeFrequency = TracesStatistics.traceFrequencyRelative();
        result.tracesAverages = TracesStatistics.tracesAverages();
        result.traceLongerShorterDuration = TracesStatistics.traceLongerShorterDuration();
        result.traceFrequency2 = TracesStatistics.traceFrequency2();
        result.firstLastTimeTraceActivity = TracesStatistics.firstLastTimeTraceActivity();
        result.tracesCuartil = TracesStatistics.timeTracesCuartil();

        return result;
    }

    //Upload a log with a specific format for Stats
    @CrossOrigin
    @RequestMapping("/uploadLogStats")
    public String insertLog(@RequestParam("file") MultipartFile file) {
        int randomNum = ThreadLocalRandom.current().nextInt(1, 99999998 + 1);
        Path path = Paths.get(StorageProperties.getLocation());
        File f = new File(path.toString() + "/" +  randomNum);
        while (f.exists()) {
            randomNum = ThreadLocalRandom.current().nextInt(1, 99999998 + 1);
            f = new File(path.toString() + "/" + randomNum);
        }
        storageService.store(file, String.valueOf(randomNum));
        storageService.load(String.valueOf(randomNum)).toString();

        savedParsed(String.valueOf(randomNum), f.getPath());

        return String.valueOf(randomNum);
    }

    //Delete a file for Stats
    @CrossOrigin
    @RequestMapping(value = "logsStats/{id}", method = RequestMethod.DELETE)
    public ResponseEntity deLog(@PathVariable("id") String id) throws IOException {
        Path path = Paths.get(StorageProperties.getLocation());
        File f = new File(path.toString() + "/" +  id);
        if (f.exists()) {
            if (f.delete()) {
                DBCollection lo = db.getCollection(id);
                lo.drop();
                return ResponseEntity.status(HttpStatus.OK).build();
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            }
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
    }
}


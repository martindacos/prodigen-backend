package bpm.services;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

import bpm.statistics.charts.ChartGenerator;
import com.google.gson.Gson;
import com.mongodb.*;
import com.mongodb.util.JSON;
import es.usc.citius.prodigen.domainLogic.exceptions.*;
import bpm.log_editor.data_types.*;
import bpm.log_editor.parser.CSVparser;
import bpm.log_editor.parser.XESparser;
import bpm.log_editor.storage.MongoDAO;
import bpm.log_editor.storage.LogService;
import org.knowm.xchart.BitmapEncoder;
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
import org.zeroturnaround.zip.ZipUtil;

import static bpm.log_editor.parser.Constants.*;

@RestController
public class LogController implements Serializable {

    private final StorageService storageService;
    public static MongoClient mongoClient = new MongoClient(HOST, PORT);
    public static DB db = mongoClient.getDB(DB_NAME);

    @Autowired
    public LogController(StorageService storageService) {
        this.storageService = storageService;
        CSVparser.storageService = storageService;
        LogService.storageService = storageService;
    }

    //----BASIC LOG FUNCTIONS----//
    //Lists all files in the server
    @CrossOrigin
    @GetMapping("/archivos")
    public ArrayList<LogFile> listUploadedFiles(Model model) throws IOException {
        return LogService.getLogFiles();
    }

    //Lists all files in the server
    @CrossOrigin
    @GetMapping("/logs")
    public ArrayList<LogFile> listUploadedLogs(Model model) throws IOException {
        return LogService.getLogFiles();
    }

    //Get a log by its id
    @CrossOrigin
    @RequestMapping(value = "/logs/{id:.+}", method = RequestMethod.GET)
    public LogFile getLog(@PathVariable("id") String id) throws IOException {
        return LogService.getLogByName(id);
    }

    //Lists all mongo collections in the server
    @CrossOrigin
    @GetMapping("/dbs")
    public Set<String> listDataBases(Model model) throws IOException {
        return MongoDAO.getCollections();
    }

    //Get a log by its id
    @CrossOrigin
    @RequestMapping(value = "/db/{id:.+}", method = RequestMethod.GET)
    public LogFile getLogContent(@PathVariable("id") String id) throws IOException {
        return LogService.getLogByName(id);
    }

    //Saves file to the server and registers LogFile in collection
    @CrossOrigin
    @RequestMapping("/fileUpload")
    public ResponseEntity insertLog(@RequestParam("file") MultipartFile file, String name, String state) {
        //Save file
        name = storageService.store(file, name);

        String newname = name;
        //Convert to .csv
        if (name.contains(".xes")) {
            File file1 = new File(storageService.load(name).toString());
            String[] split = file1.getName().split("\\.");
            String fileName =  split[0] + ".csv";
            fileName = storageService.checkName(fileName);
            newname = XESparser.read(file1, fileName);
            File oldFile = new File(storageService.load(name).toString());
            oldFile.delete();
        }

        //Insert in db
        String[] parseName = newname.split(".csv");
        String r = parseName[0];
        LogService.insertLog(r, newname, state);
        return ResponseEntity.status(HttpStatus.OK).contentType(MediaType.TEXT_PLAIN).body(r);
    }

    //Config the file
    @CrossOrigin
    @RequestMapping("/loadConfig")
    public ResponseEntity loadConfig(String name,  @RequestParam("config") MultipartFile config) {
        //Insert config to db
        name = storageService.store(config, name + ".ini");

        DBCollection configColl;
        if (db.getCollection("config") != null) {
            configColl = db.getCollection("config");
        } else {
            configColl = db.createCollection("config", null);
        }

        //Read Config File
        Config c;
        try {
            c = new Config(name);
        } catch (Exception e) {
            e.printStackTrace();
            //Delete
            File f = new File("upload-dir/" + name);
            if (f.exists()) {
                f.delete();
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
        Gson gson = new Gson();
        BasicDBObject parse = (BasicDBObject) JSON.parse(gson.toJson(c));
        BasicDBObject obj = new BasicDBObject("name", name);
        obj.put("dir", "upload-dir/" + name);
        obj.put("config", parse);
        obj.put("editable", true);
        configColl.save(obj);

        return ResponseEntity.status(HttpStatus.OK).build();
    }

    //Config the file
    @CrossOrigin
    @RequestMapping("/readConfig")
    public ResponseEntity loadConfig(String logId, String name) {
        //Read Config File
        Config c;
        try {
            c = new Config(name);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

        LogFile log = LogService.getLogByName(logId);

        log.setConfigName(name);

        Headers headers = new Headers();
        List<String> columns = Arrays.asList(c.getColumns().split(","));
        headers.setData(columns);
        log.insertFile(headers);

        log.setHierarchyCols(headers);

        log.setTraceActTime(c.getTrace_id(), c.getActivity_id(),
                c.getTimestamp(), c.getFinal_timestamp());

        log.setDate(c.getDate_format());
        log.setState(LOADED);
        return ResponseEntity.status(HttpStatus.OK).build();
    }

    //Deletes a file
    @CrossOrigin
    @RequestMapping(value = "/logs/{id:.+}", method = RequestMethod.DELETE)
    public void deleteLog(@PathVariable("id") String id) throws IOException {
        LogService.deleteLog(LogService.getLogByName(id));
    }


    //----COMPLEX LOG FUNCTIONS----//
    //Returns log column headers
    @CrossOrigin
    @RequestMapping("/headers")
    public Headers getFileHeaders(@RequestParam("file") String file) {
        return LogService.getLogByName(file).getHeaders();
    }

    //Sets the columns used to create hierarchies
    @CrossOrigin
    @RequestMapping(value = "/hierarchyCols", method = RequestMethod.POST)
    public void setHierarchyCols(@RequestParam("file") String file, Headers headers) {
        LogService.getLogByName(file).setHierarchyCols(headers);
    }

    //Sets the trace, activity and timestamp columns
    @CrossOrigin
    @RequestMapping(value = "/activityCol", method = RequestMethod.POST)
    public void setActIdTime(@RequestParam("file") String file, String trace, String act, String timestamp, String timestampf) {
        LogService.getLogByName(file).setTraceActTime(trace, act, timestamp, timestampf);
        LogService.getLogByName(file).setSampleDate();
        //return CSVparser.removeColumns(file, headers, storageService);
    }

    //Sets the columns used to create hierarchies
    @CrossOrigin
    @RequestMapping("/date")
    public void setDateFormat(@RequestParam("file") String file, String data) {
        LogService.getLogByName(file).setDate(data);
    }

    //Removes the non selected columns from a log
    @CrossOrigin
    @RequestMapping(value = "/filterLog", method = RequestMethod.POST)
    public Headers insertLogToDB(@RequestParam("file") String file, Headers headers) {
        LogService.getLogByName(file).insertFile(headers);
        //CSVparser.removeColumns(file, headers, storageService);
        return LogService.getLogByName(file).getHeaders();
    }

    //Returns the uniques of the columns to filter a log
    @CrossOrigin
    @RequestMapping(value = "/nulls", method = RequestMethod.POST)
    public Headers deleteNulls(@RequestParam("file") String file, String column, String value) {
        LogService.getLogByName(file).replaceNulls(column, value);
        return LogService.getLogByName(file).getHeaders();
    }

    //Returns coll uniques
    @CrossOrigin
    @RequestMapping(value = "/db", method = RequestMethod.GET)
    public HashMap<String, List<String>> db(@RequestParam("db") String db) {
        return LogService.getLogByName(db).UniquesToFilter();
    }

    //Returns the uniques of the columns to filter a log
    @CrossOrigin
    @RequestMapping(value = "/replaceValues", method = RequestMethod.POST)
    public HashMap<String, List<String>> replaceValues(@RequestParam("file") String file, String column, Headers values, String replacement) {
        LogService.getLogByName(file).replaceValues(column, values.getData(), replacement);
        return LogService.getLogByName(file).UniquesToFilter();
    }

    //Queries a log with a determined hierarchy
    @CrossOrigin
    @RequestMapping(value = "/hierarchy", method = RequestMethod.POST)
    public ArrayList<MinedLog> mineLog(@RequestParam("file") String file, @RequestBody ArrayList<Node> nodes) throws EmptyLogException, InvalidFileExtensionException, MalformedFileException, WrongLogEntryException, NonFinishedWorkflowException, ParseException, FileNotFoundException, UnsupportedEncodingException {
        Hierarchy h = new Hierarchy();
        h.setNodes(nodes);
        LogService.getLogByName(file).setTree(h);
        LogService.save(LogService.getLogByName(file));
        MongoDAO.queryLog(LogService.getLogByName(file), h);
        return null;
    }

    //Queries a log with a determined hierarchy
    @CrossOrigin
    @RequestMapping(value = "/hierarchyConfig", method = RequestMethod.POST)
    public ArrayList<MinedLog> mineLogConfig(@RequestParam("file") String file) throws EmptyLogException, InvalidFileExtensionException, MalformedFileException, WrongLogEntryException, NonFinishedWorkflowException, ParseException, FileNotFoundException, UnsupportedEncodingException {
        Hierarchy h = new Hierarchy();
        LogService.getLogByName(file).setTree(h);
        LogService.save(LogService.getLogByName(file));
        MongoDAO.queryLog(LogService.getLogByName(file), h);
        return null;
    }

    //Delete a model from a log
    @CrossOrigin
    @RequestMapping("/model")
    public LogFile deleteModel(@RequestParam("file") String file, String data) {
        LogFile logByName = LogService.getLogByName(file);
        int index = Integer.parseInt(data);
        LogService.deleteOneLog(logByName, index);

        return logByName.deleteModel(index);
    }

    //Delete a model from a log
    @CrossOrigin
    @RequestMapping("/download")
    public ResponseEntity<Resource> downloadModel(@RequestParam("file") String file) throws IOException {
        File fi = new File(file);
        InputStreamResource resource = new InputStreamResource(new FileInputStream(fi));
        HttpHeaders h = new HttpHeaders();
        h.add("Content-Disposition", "attachment; filename=\"" + fi.getName() + "\"");
        return ResponseEntity.ok()
                .headers(h)
                .contentLength(fi.length())
                .contentType(MediaType.parseMediaType("application/octet-stream"))
                .body(resource);
    }

    //Generate report
    @CrossOrigin
    @RequestMapping(value = "/report", produces = "application/zip")
    public ResponseEntity<Resource> generateReport(@RequestParam("file") String file, String data) throws IOException {

        //Get log
        LogFile logByName = LogService.getLogByName(file);
        int index = Integer.parseInt(data);
        MinedLog model = logByName.getModels().get(index);

        //Create directory
        File theDir = new File("download-dir/" + file);
        theDir.mkdir();

        //Activity frequency
        BitmapEncoder.saveJPGWithQuality(
                ChartGenerator.BarChart(
                        (ArrayList<String>) model.getActStats().activityFrequency.stream().map(tupla -> tupla._1).collect(Collectors.toList()),
                        (ArrayList<Double>) model.getActStats().activityFrequency.stream().map(tupla -> tupla._2).collect(Collectors.toList())
                ), "download-dir/" + file + "/ActivityFrequencyChart.jpg", 1);


        //CREATE ZIP
        ZipUtil.pack(new File("download-dir/" + file + "/"), new File("src/main/resources/static/" + file + ".zip"));

        return null;


    }

    //Edit configuration for report
    @CrossOrigin
    @RequestMapping(value = "/editReportConfig", method = RequestMethod.POST)
    public ResponseEntity editReportConfig(@RequestParam("file") String file, @RequestBody Config config){
        //Initialize null values well & check fails
        if (config.checkNulls()) {
            return ResponseEntity.status(HttpStatus.NOT_ACCEPTABLE).build();
        }

        String configName = config.getConfigName();
        boolean b = !config.getConfigName().equals(config.getOldName());

        BasicDBObject query = new BasicDBObject("name", configName);
        DBCollection con = db.getCollection("config");
        DBCursor cursor = con.find(query);
        DBObject confBD = new BasicDBObject();
        Gson gson = new Gson();

        if (b) {
            //We would overwrite another configuration file
            if (cursor.hasNext()) {
                //Update name
                configName = storageService.checkName(configName);
                //Update names on config
                config.setConfigName(configName);
            }
            //Updates old name
            config.setOldName(configName);
            BasicDBObject parse = (BasicDBObject) JSON.parse(gson.toJson(config));
            confBD.put("config", parse);
            confBD.put("name", configName);
            confBD.put("dir", "upload-dir/" + configName);
            confBD.put("editable", true);
            con.save(confBD);
        } else {
            //Update old name
            config.setOldName(configName);
            BasicDBObject parse = (BasicDBObject) JSON.parse(gson.toJson(config));
            confBD.put("config", parse);
            if (cursor.hasNext()) {
                DBObject next = cursor.next();
                Boolean editable = (Boolean) next.get("editable");
                if (editable == false) {
                    return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
                }
                BasicDBObject set = new BasicDBObject("$set", confBD);
                //Update document
                con.update(query, set);
            } else {
                confBD.put("name", configName);
                confBD.put("dir", "upload-dir/" + configName);
                confBD.put("editable", true);
                con.save(confBD);
            }
        }

        config.saveConfigFile(configName);

        LogFile log = LogService.getLogByName(file);
        //Update config name if changes
        if (log != null) {
            log.setConfigName(configName);
        }
        return ResponseEntity.status(HttpStatus.OK).contentType(MediaType.TEXT_PLAIN).body(configName);
    }

    //Lists all configs in the server
    @CrossOrigin
    @GetMapping("/configs")
    public List<String> listConfig() throws IOException {
        List<String> configs = new ArrayList<>();
        DBCollection con = db.getCollection("config");
        BasicDBObject query = new BasicDBObject();
        DBCursor cursor = con.find(query);
        while (cursor.hasNext()) {
            DBObject next = cursor.next();
            String name = (String) next.get("name");
            configs.add(name);
        }
        return configs;
    }

    //Get a config by its id
    @CrossOrigin
    @RequestMapping(value = "/config/{id}", method = RequestMethod.GET)
    public Config getConfig(@PathVariable("id") String id) throws IOException {
        BasicDBObject query = new BasicDBObject("name", id + ".ini");
        DBCollection con = db.getCollection("config");
        DBCursor cursor = con.find(query);
        Config c = null;
        if (cursor.hasNext()) {
            DBObject next = cursor.next();
            c = new Gson().fromJson(next.get("config").toString(), Config.class);
        }
        return c;
    }

    //Deletes a config
    @CrossOrigin
    @RequestMapping(value = "/config/{id}", method = RequestMethod.DELETE)
    public ResponseEntity deleteConfig(@PathVariable("id") String id) throws IOException {
        //Remove form BD
        BasicDBObject query = new BasicDBObject("name", id + ".ini");
        query.append("editable", true);
        DBCollection con = db.getCollection("config");
        DBObject andRemove = con.findAndRemove(query);
        if (andRemove == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }

        //Remove File
        File f = new File("upload-dir/" + id + ".ini");
        if (f.exists()) {
            f.delete();
        }

        //Delete Config of Logs
        query = new BasicDBObject("configName", id + ".ini");
        con = db.getCollection("logFile");
        BasicDBObject update = new BasicDBObject();
        update.put("$unset", new BasicDBObject("configName", ""));
        con.updateMulti(query, update);

        return ResponseEntity.status(HttpStatus.OK).build();
    }
}


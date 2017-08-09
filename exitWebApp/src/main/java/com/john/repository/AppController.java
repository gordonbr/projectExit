package com.john.repository;

import com.google.common.collect.Maps;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jonathasalves on 19/07/2017.
 */
@RestController
@RequestMapping("/app/")
public class AppController {

    @PostMapping("/")
    public String appFunctionSelector(@RequestParam(name = "appname", required = true) String appName,
                                      @RequestParam(name = "action", required = true) String action){

        return "ok";
    }

    @RequestMapping(method = RequestMethod.GET)
    public String tudo(){
        return "tudo mapiado";
    }


    private void startScalaApp(String appName) throws IOException, InterruptedException {

        Map<String, String> env = Maps.newHashMap();
        env.put("SPARK_PRINT_LAUNCH_COMMAND", "1");

//        Process sparkLauncher =  new SparkLauncher(env)
//                .setSparkHome("/Users/jonathasalves/Programming/spark")
//                .setAppResource("/Users/jonathasalves/Programming/jars/exitCoreApp-1.0-SNAPSHOT-jar-with-dependencies.jar")
//                .setMainClass("com.john.exitCoreApp.App")
//                .setMaster("spark://Jonathass-MacBook-Pro.local:7077")
//                .addSparkArg("--verbose")
//                .launch();
//       sparkLauncher.waitFor();

        SparkAppHandle sparkLauncher =  new SparkLauncher(env)
                .setSparkHome("/Users/jonathasalves/Programming/spark")
                .setAppResource("/Users/jonathasalves/Programming/jars/exitCoreApp-1.0-SNAPSHOT-jar-with-dependencies.jar")
                .setMainClass("com.john.exitCoreApp.App")
                .setMaster("spark://Jonathass-MacBook-Pro.local:7077")
                .addSparkArg("--verbose")
                .startApplication();


        while(!sparkLauncher.getState().isFinal()) {
            System.out.println("Current state: "+ sparkLauncher.getState());
            System.out.println("App Id "+ sparkLauncher.getAppId());
            Thread.sleep(1000L);
            // other stuffs you want to do
            //
        }

    }

    public static void main(String args[]) throws IOException, InterruptedException {
        AppController appController = new AppController();
        appController.startScalaApp("blabla");
    }
}

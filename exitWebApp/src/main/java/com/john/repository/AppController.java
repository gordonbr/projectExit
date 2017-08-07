package com.john.repository;

import org.springframework.web.bind.annotation.*;

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


    private void startScalaApp(String appName){

    }
}

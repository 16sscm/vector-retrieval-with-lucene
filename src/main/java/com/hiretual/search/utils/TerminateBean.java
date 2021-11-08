package com.hiretual.search.utils;
import javax.annotation.PreDestroy;
import  com.hiretual.search.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class TerminateBean {
    Logger logger =
      LoggerFactory.getLogger(TerminateBean.class);
    @PreDestroy
    public void preDestroy() {
        try{
            IndexBuildService.shutDown();
        }catch(Exception e){
            e.printStackTrace();
        }
        
        logger.info("service is shutdown elegantly");
    }
}

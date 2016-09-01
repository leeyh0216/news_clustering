package com.dgcse.common;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by leeyh on 2016. 8. 14..
 * Log Module
 */
public class LogInstance {
    private static final String PRODUCTION_LOG = "PRODUCTION_LOGGER";
    private static final String DEBUG_LOG = "DEBUG_LOGGER";
    public static final boolean isDebug = true;

    /**
     * Debug, Production 상태에 따른 Logger를 반환한다
     * @return Logger
     */
    public static Logger getLogger(){
        if(isDebug)
            return LogManager.getLogger(DEBUG_LOG);
        else
            return LogManager.getLogger(PRODUCTION_LOG);
    }
}

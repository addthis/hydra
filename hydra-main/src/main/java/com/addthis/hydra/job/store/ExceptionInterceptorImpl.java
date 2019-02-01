package com.addthis.hydra.job.store;

import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.log.Log;
import java.sql.SQLException;

import java.util.Properties;

public class ExceptionInterceptorImpl implements ExceptionInterceptor{
    @Override public ExceptionInterceptor init(Properties props, Log log) {
        return this;
    }

    @Override public void destroy() {

    }

    @Override public Exception interceptException(Exception sqlEx) {
        return new SQLException("Mysql Blob initiation error!", sqlEx);
    }
}

package org.example;

import org.hyperledger.besu.plugin.*;
import org.hyperledger.besu.plugin.BesuPlugin;
import com.google.auto.service.AutoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@AutoService(BesuPlugin.class)
public class MyBesuPlugin implements BesuPlugin {

    private BesuContext context;

    private static Logger LOG = LogManager.getLogger();

    public static void main(String[] args) {
        System.out.println("Hello world!");
    }

    @Override
    public void stop() {
        LOG.info("Stopping Matt's new plugin");
    }

    @Override
    public void start() {
      LOG.info("Starting Matt's plugin");
    }

    @Override
    public void register(final BesuContext context) {
        LOG.info("Registering Matt's plugin");
        this.context = context;
    }
}
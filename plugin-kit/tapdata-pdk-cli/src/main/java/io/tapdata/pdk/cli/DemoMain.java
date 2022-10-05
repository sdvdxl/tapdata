package io.tapdata.pdk.cli;

import io.tapdata.pdk.core.utils.CommonUtils;

public class DemoMain {

  public static void main(String[] args) {
    CommonUtils.setProperty("pdk_external_jar_path", "./connectors/dist");
    args = new String[]{
        "test", "-c", "plugin-kit/tapdata-pdk-cli/src/main/resources/config/demo.json",
//                "-t", "io.tapdata.pdk.tdd.tests.source.StreamReadTest",
//                "-t", "io.tapdata.pdk.tdd.tests.source.BatchReadTest",
//                "-t", "io.tapdata.pdk.tdd.tests.target.CreateTableTest",
//                "-t", "io.tapdata.pdk.tdd.tests.target.DMLTest",
        "connectors/demo-connector",};
    Main.registerCommands().execute(args);
  }
}

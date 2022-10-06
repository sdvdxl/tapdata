package io.tapdata.connector.demo;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class DemoConnector extends ConnectorBase {

  private AtomicInteger counter = new AtomicInteger();
  private static final String TAG = "DEMO_CONNECTOR";
  private volatile boolean running = false;

  @Override
  public void onStart(TapConnectionContext connectionContext) throws Throwable {
    running = true;
    TapLogger.warn(TAG, "demo 连接配置: {} is null? {}", connectionContext.getConnectionConfig(),
        connectionContext.getConnectionConfig() == null);
    if (connectionContext.getConnectionConfig() == null) {
      connectionContext.setConnectionConfig(new DataMap());
    }
    connectionContext.getConnectionConfig().put("_key_", "sdvdxl");
    TapLogger.info(TAG, "Demo Connector start :{}", connectionContext.getConnectionConfig());
  }

  @Override
  public void onStop(TapConnectionContext connectionContext) throws Throwable {
    TapLogger.info(TAG, "Demo Connector stop :{}", connectionContext.getConnectionConfig());
    running = false;
  }

  @Override
  public void registerCapabilities(ConnectorFunctions connectorFunctions,
      TapCodecsRegistry codecRegistry) {
    TapLogger.info(TAG, "Demo Connector registerCapabilities");

    connectorFunctions.supportBatchRead(this::batchRead);
    connectorFunctions.supportStreamRead(this::streamRead);
    connectorFunctions.supportWriteRecord(this::writeRecord);
  }

  private void streamRead(TapConnectorContext nodeContext, List<String> tableList,
      Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
    TapLogger.info(TAG, "表名：{}", tableList);
    List<TapEvent> list = TapSimplify.list();

    while (running) {
      for (String tname : tableList) {
        Thread.sleep(1000);
        Map<String, Object> map = new HashMap<>();
        map.put("a", counter.get());
        list.add(TapSimplify.insertRecordEvent(map, tname)
            .referenceTime(System.currentTimeMillis()));
        TapLogger.info(TAG, "发送数据：{}", map);
        if (list.size() >= recordSize) {
          consumer.accept(list, TapSimplify.list());
        }
      }

    }
  }

  private void writeRecord(TapConnectorContext tapConnectorContext,
      List<TapRecordEvent> tapRecordEvents, TapTable tapTable,
      Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
    WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
    listResult.insertedCount(tapRecordEvents.size());
    for (TapRecordEvent event : tapRecordEvents) {
      Map<String, Object> data;

      if (event instanceof TapInsertRecordEvent) {
        data = ((TapInsertRecordEvent) event).getAfter();
      } else if (event instanceof TapUpdateRecordEvent) {
        data = ((TapUpdateRecordEvent) event).getAfter();
      } else if (event instanceof TapDeleteRecordEvent) {
        data = ((TapDeleteRecordEvent) event).getBefore();
      } else {
        data = new HashMap<>();
      }
      TapLogger.info(TAG, "收到消息，表 {}， 消息：{}", tapTable.getName(), data);

    }
    writeListResultConsumer.accept(listResult);
  }

  private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offsetState,
      int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
    if (counter.incrementAndGet() > 100) {
      eventsOffsetConsumer.accept(TapSimplify.list(), TapSimplify.list());
      return;
    }

    List<TapEvent> list = TapSimplify.list();
    Map<String, Object> map = new HashMap<>();
    map.put("a", counter.get());
    list.add(new TapInsertRecordEvent().init().table(table.getName()).after(map)
        .referenceTime(System.currentTimeMillis()));

    eventsOffsetConsumer.accept(list, TapSimplify.list());


  }

  @Override
  public void discoverSchema(TapConnectionContext connectionContext, List<String> tables,
      int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
    consumer.accept(list(
        //Define first table
        table("empty-table1")
            //Define a field named "id", origin field type, whether is primary key and primary key position
            .add(field("id", "VARCHAR").isPrimaryKey(true))
            .add(field("description", "TEXT"))
            .add(field("name", "VARCHAR"))
            .add(field("age", "DOUBLE")), table("t2")
    ));
  }

  @Override
  public ConnectionOptions connectionTest(TapConnectionContext connectionContext,
      Consumer<TestItem> consumer) throws Throwable {
    consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, "成功"));
    String ak = connectionContext.getConnectionConfig().getString("ak");
    String as = connectionContext.getConnectionConfig().getString("as");
    TapLogger.info(TAG, "测试demo ak: {}, as: {}", ak, as);
    ConnectionOptions connectionOptions = ConnectionOptions.create();
    connectionOptions.setConnectionString("OK");
    return connectionOptions;
  }

  @Override
  public int tableCount(TapConnectionContext connectionContext) throws Throwable {
    return 2;
  }


}
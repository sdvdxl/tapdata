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
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapStringValue;
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
import java.util.stream.Collectors;

@TapConnectorClass("spec.json")
public class DemoConnector extends ConnectorBase {

  private AtomicInteger counter = new AtomicInteger();
  private final String TAG = "##DEMO_CONNECTOR##" + this + "##";
  private volatile boolean running = false;

  @Override
  public void onStart(TapConnectionContext connectionContext) throws Throwable {
    running = true;
    TapLogger.warn(
        TAG,
        "demo 启动 连接配置: {} is null? {}",
        connectionContext.getConnectionConfig(),
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
  public void registerCapabilities(
      ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
    TapLogger.info(TAG, "Demo Connector registerCapabilities");

    // 批量读取
    connectorFunctions.supportBatchRead(this::batchRead);
    // 增量读取
    connectorFunctions.supportStreamRead(this::streamRead);

    //
    connectorFunctions.supportGetTableNamesFunction(this::getTableNames);

    // 一定要实现，否则引擎报错
    connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
    // 写入
    connectorFunctions.supportWriteRecord(this::writeRecord);
  }

  private void getTableNames(
      TapConnectionContext tapConnectionContext,
      int batchSize,
      Consumer<List<String>> listConsumer) {
    List<String> tableNames = list("t1");
    TapLogger.info(TAG, "获取表名：{}", tableNames);
    listConsumer.accept(tableNames);
  }

  private Object timestampToStreamOffset(
      TapConnectorContext connectorContext, Long offsetStartTime) {
    return TapSimplify.list();
  }

  private void streamRead(
      TapConnectorContext nodeContext,
      List<String> tableList,
      Object offsetState,
      int recordSize,
      StreamReadConsumer consumer)
      throws Throwable {
    TapLogger.info(TAG, "增量读取 表名：{}", tableList);
    List<TapEvent> list = TapSimplify.list();

    while (running) {
      for (String tname : tableList) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {

        }
        Map<String, Object> map = new HashMap<>();
        int num = counter.incrementAndGet();
        map.put("string", String.valueOf(num));
        map.put("int", num);
        list.add(
            TapSimplify.insertRecordEvent(map, tname).referenceTime(System.currentTimeMillis()));
        TapLogger.info(TAG, "发送数据：{}", map);
        if (list.size() >= recordSize) {
          consumer.accept(list, TapSimplify.list());
          list.clear();
        }
      }
    }
  }

  private void writeRecord(
      TapConnectorContext tapConnectorContext,
      List<TapRecordEvent> tapRecordEvents,
      TapTable tapTable,
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
      String msg =
          data.entrySet().stream()
              .map(
                  (e) -> {
                    return e.getKey()
                        + " : "
                        + e.getValue()
                        + " type: "
                        + e.getValue().getClass().getSimpleName();
                  })
              .collect(Collectors.joining("; "));
      TapLogger.info(TAG, "收到消息，表 {}， 消息：{}", tapTable.getName(), msg);
    }
    // TODO 写入库数据
    writeListResultConsumer.accept(listResult);
    tapRecordEvents.clear();
  }

  private void batchRead(
      TapConnectorContext connectorContext,
      TapTable table,
      Object offsetState,
      int eventBatchSize,
      BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
    //    if (counter.incrementAndGet() > 100) {
    //      eventsOffsetConsumer.accept(TapSimplify.list(), TapSimplify.list());
    //      return;
    //    }
    //
    //    List<TapEvent> list = TapSimplify.list();
    //    Map<String, Object> map = new HashMap<>();
    //    map.put("a", new TapStringValue(String.valueOf(counter.incrementAndGet())));
    //    list.add(
    //        new TapInsertRecordEvent()
    //            .init()
    //            .table(table.getName())
    //            .after(map)
    //            .referenceTime(System.currentTimeMillis()));
    //
    //    eventsOffsetConsumer.accept(list, TapSimplify.list());
    eventsOffsetConsumer.accept(TapSimplify.list(), TapSimplify.list());
  }

  /**
   * 一定要实现，否则引擎报错
   *
   * @param connectionContext
   * @param tables
   * @param tableSize
   * @param consumer
   * @throws Throwable
   */
  @Override
  public void discoverSchema(
      TapConnectionContext connectionContext,
      List<String> tables,
      int tableSize,
      Consumer<List<TapTable>> consumer)
      throws Throwable {
    TapLogger.info(TAG, "获取schema：{}", connectionContext.getConnectionConfig());

    // tableCount 不处理
    List<TapTable> tableList = tables.stream().map(t -> table(t).add(field("ss","DATE_TYPE"))).collect(Collectors.toList());
    consumer.accept(tableList);
  }

  @Override
  public ConnectionOptions connectionTest(
      TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
    TapLogger.info(TAG, "测试连接：{}", connectionContext.getConnectionConfig());

    consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, "成功"));
    ConnectionOptions connectionOptions = ConnectionOptions.create();
    connectionOptions.setConnectionString("OK");
    return connectionOptions;
  }

  @Override
  public int tableCount(TapConnectionContext connectionContext) throws Throwable {
    int count = 1;
    TapLogger.info(TAG, "表数量：{}", count);
    return count;
  }
}

package org.tron.eventplugin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.Filters;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.pf4j.util.StringUtils;
import org.tron.mongodb.MongoConfig;
import org.tron.mongodb.MongoManager;
import org.tron.mongodb.MongoTemplate;

@Slf4j(topic = "event")
public class MongodbSenderImpl implements AutoCloseable {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static MongodbSenderImpl instance = null;

  @Getter
  BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
  private final ExecutorService service = new ThreadPoolExecutor(8, 8,
      0L, TimeUnit.MILLISECONDS, queue);

  private boolean loaded = false;
  @Getter
  private BlockingQueue<Object> triggerQueue = new LinkedBlockingQueue<>();

  private String blockTopic = "";
  private String transactionTopic = "";
  private String contractEventTopic = "";
  private String contractLogTopic = "";
  private String solidityTopic = "";
  private String solidityEventTopic = "";
  private String solidityLogTopic = "";

  // === DeFi Feature ===
  private String blockContractLogTopic = "";
  private final String filterCollection = "filters";

  // === TronLink Feature ===
  private String trc20TrackerTopic = "";
  private String transferTrackerTopic = "";
  private String freezeTrackerTopic = "";
  private String stakeTrackerTopic = "";
  private String multiAuthTrackerTopic = "";
  private String shieldedTRC20TrackerTopic = "";
  private String shieldedSolidityTRC20TrackerTopic = "";

  // === JustLend Feature ===
  private String justlendTrackerTopic = "";

  private Thread triggerProcessThread;
  private boolean isRunTriggerProcessThread = true;

  private MongoManager mongoManager;
  private Map<String, MongoTemplate> mongoTemplateMap;

  private String dbName;
  private String dbUserName;
  private String dbPassword;
  private int version; // 1: no index, 2: has index

  private MongoConfig mongoConfig;

  public static MongodbSenderImpl getInstance() {
    if (Objects.isNull(instance)) {
      synchronized (MongodbSenderImpl.class) {
        if (Objects.isNull(instance)) {
          instance = new MongodbSenderImpl();
        }
      }
    }
    return instance;
  }

  public void setDBConfig(String dbConfig) {
    if (StringUtils.isNullOrEmpty(dbConfig)) {
      return;
    }

    String[] params = dbConfig.split("\\|");
    if (params.length != 3 && params.length != 4) {
      return;
    }

    dbName = params[0];
    dbUserName = params[1];
    dbPassword = params[2];
    version = 1;

    if (params.length == 4) {
      version = Integer.parseInt(params[3]);
    }

    loadMongoConfig();
  }

  public void setServerAddress(final String serverAddress) {
    if (StringUtils.isNullOrEmpty(serverAddress)) {
      return;
    }

    String[] params = serverAddress.split(":");
    if (params.length != 2) {
      return;
    }

    String mongoHostName;
    int mongoPort;

    try {
      mongoHostName = params[0];
      mongoPort = Integer.parseInt(params[1]);
    } catch (Exception e) {
      log.error("SetServerAddress failed", e);
      return;
    }

    if (Objects.isNull(mongoConfig)) {
      mongoConfig = new MongoConfig();
    }

    mongoConfig.setHost(mongoHostName);
    mongoConfig.setPort(mongoPort);
  }

  public void init() {
    if (loaded) {
      return;
    }

    if (Objects.isNull(mongoManager)) {
      mongoManager = new MongoManager();
      mongoManager.initConfig(mongoConfig);
    }

    mongoTemplateMap = new HashMap<>();
    createCollections();

    triggerProcessThread = new Thread(triggerProcessLoop);
    triggerProcessThread.start();
    loaded = true;
  }

  private void createCollections() {
    if (mongoConfig.enabledIndexes()) {
      Map<String, Boolean> indexOptions = new HashMap<>();
      indexOptions.put("blockNumber", true);
      mongoManager.createCollection(blockTopic, indexOptions);

      indexOptions = new HashMap<>();
      indexOptions.put("transactionId", true);
      mongoManager.createCollection(transactionTopic, indexOptions);

      indexOptions = new HashMap<>();
      indexOptions.put("latestSolidifiedBlockNumber", true);
      mongoManager.createCollection(solidityTopic, indexOptions);

      indexOptions = new HashMap<>();
      indexOptions.put("uniqueId", true);
      mongoManager.createCollection(solidityEventTopic, indexOptions);
      mongoManager.createCollection(contractEventTopic, indexOptions);

      indexOptions = new HashMap<>();
      indexOptions.put("uniqueId", true);
      indexOptions.put("contractAddress", false);
      mongoManager.createCollection(solidityLogTopic, indexOptions);
      mongoManager.createCollection(contractLogTopic, indexOptions);

      // === DeFi Feature ===
      indexOptions = new HashMap<>();
      indexOptions.put("name", true);
      mongoManager.createCollection(filterCollection, indexOptions);

      indexOptions = new HashMap<>();
      indexOptions.put("blockNumber", false);
      mongoManager.createCollection(blockContractLogTopic, indexOptions);
    } else {
      mongoManager.createCollection(blockTopic);
      mongoManager.createCollection(transactionTopic);
      mongoManager.createCollection(contractLogTopic);
      mongoManager.createCollection(contractEventTopic);
      mongoManager.createCollection(solidityTopic);
      mongoManager.createCollection(solidityEventTopic);
      mongoManager.createCollection(solidityLogTopic);

      // === DeFi Feature ===
      mongoManager.createCollection(filterCollection);
      mongoManager.createCollection(blockContractLogTopic);

      // === TronLink Feature ===
      mongoManager.createCollection(trc20TrackerTopic);
      mongoManager.createCollection(transferTrackerTopic);
      mongoManager.createCollection(freezeTrackerTopic);
      mongoManager.createCollection(stakeTrackerTopic);
      mongoManager.createCollection(multiAuthTrackerTopic);
      mongoManager.createCollection(shieldedSolidityTRC20TrackerTopic);
      mongoManager.createCollection(shieldedTRC20TrackerTopic);

      // === JustLend Feature ===
      mongoManager.createCollection(justlendTrackerTopic);
    }

    createMongoTemplate(blockTopic);
    createMongoTemplate(transactionTopic);
    createMongoTemplate(contractLogTopic);
    createMongoTemplate(contractEventTopic);
    createMongoTemplate(solidityTopic);
    createMongoTemplate(solidityEventTopic);
    createMongoTemplate(solidityLogTopic);

    // === DeFi Feature ===
    createMongoTemplate(filterCollection);
    createMongoTemplate(blockContractLogTopic);

    // === TronLink Feature ===
    createMongoTemplate(trc20TrackerTopic);
    createMongoTemplate(transferTrackerTopic);
    createMongoTemplate(freezeTrackerTopic);
    createMongoTemplate(stakeTrackerTopic);
    createMongoTemplate(multiAuthTrackerTopic);
    createMongoTemplate(shieldedSolidityTRC20TrackerTopic);
    createMongoTemplate(shieldedTRC20TrackerTopic);

    // === JustLend Feature ===
    createMongoTemplate(justlendTrackerTopic);
  }

  private void loadMongoConfig() {
    if (Objects.isNull(mongoConfig)) {
      mongoConfig = new MongoConfig();
    }

    if (StringUtils.isNullOrEmpty(dbName)) {
      return;
    }

    Properties properties = new Properties();

    try {
      InputStream input = getClass().getClassLoader().getResourceAsStream("mongodb.properties");
      if (Objects.isNull(input)) {
        return;
      }
      properties.load(input);

      int connectionsPerHost = Integer.parseInt(properties.getProperty("mongo.connectionsPerHost"));
      int threadsAllowedToBlockForConnectionMultiplier = Integer.parseInt(
          properties.getProperty("mongo.threadsAllowedToBlockForConnectionMultiplier"));

      mongoConfig.setDbName(dbName);
      mongoConfig.setUsername(dbUserName);
      mongoConfig.setPassword(dbPassword);
      mongoConfig.setVersion(version);
      mongoConfig.setConnectionsPerHost(connectionsPerHost);
      mongoConfig.setThreadsAllowedToBlockForConnectionMultiplier(
          threadsAllowedToBlockForConnectionMultiplier);
    } catch (Exception e) {
      log.error("LoadMongoConfig failed", e);
    }
  }

  private MongoTemplate createMongoTemplate(final String collectionName) {
    MongoTemplate template = mongoTemplateMap.get(collectionName);
    if (Objects.nonNull(template)) {
      return template;
    }

    template = new MongoTemplate(mongoManager) {
      @Override
      protected String collectionName() {
        return collectionName;
      }

      @Override
      protected <T> Class<T> getReferencedClass() {
        return null;
      }
    };

    mongoTemplateMap.put(collectionName, template);
    return template;
  }

  public void setTopic(int triggerType, String topic) {
    EventTopic eventTopic = EventTopic.getEventTopicByType(triggerType, topic);
    if (eventTopic == null) {
      log.error("Unknown trigger type {}", triggerType);
      return;
    }
    switch (eventTopic) {
      case BLOCK_TRIGGER:
        blockTopic = topic;
        break;
      case TRANSACTION_TRIGGER:
        transactionTopic = topic;
        break;
      case CONTRACT_EVENT_TRIGGER:
        contractEventTopic = topic;
        break;
      case CONTRACT_LOG_TRIGGER:
        contractLogTopic = topic;
        break;
      case SOLIDITY_TRIGGER:
        solidityTopic = topic;
        break;
      case SOLIDITY_EVENT:
        solidityEventTopic = topic;
        break;
      case SOLIDITY_LOG:
        solidityLogTopic = topic;
        break;
      case BLOCK_CONTRACT_LOG_TRIGGER:
        blockContractLogTopic = topic;
        break;
      case TRC20TRACKER_TRIGGER:
        trc20TrackerTopic = topic;
        break;
      case TRANSFER_TRACKER_TRIGGER:
      case TRANSFER_TRIGGER:
        transferTrackerTopic = topic;
        break;
      case FREEZE_TRACKER_TRIGGER:
        freezeTrackerTopic = topic;
        break;
      case STAKE_TRACKER_TRIGGER:
        stakeTrackerTopic = topic;
        break;
      case MULTIAUTH_TRACKER_TRIGGER:
        multiAuthTrackerTopic = topic;
        break;
      case SHIELDED_TRC20SOLIDITYTRACKER_TRIGGER:
        shieldedSolidityTRC20TrackerTopic = topic;
        break;
      case SHIELDED_TRC20TRACKER_TRIGGER:
        shieldedTRC20TrackerTopic = topic;
        break;
      case JUSTLEND_TRACKER_TRIGGER:
        justlendTrackerTopic = topic;
        break;
      case BLOCK_ERASE_TRIGGER:
        break;
    }
  }

  public void upsertEntityLong(MongoTemplate template, Object data, String indexKey) {
    String dataStr = (String) data;
    try {
      JsonNode jsStr = OBJECT_MAPPER.readTree(dataStr);
      Long indexValue = getLong(jsStr, indexKey);
      if (indexValue != null) {
        template.upsertEntity(indexKey, indexValue, dataStr);
      } else {
        template.addEntity(dataStr);
      }
    } catch (Exception ex) {
      log.error("upsertEntityLong exception happened in parse object ", ex);
    }
  }

  public void upsertEntityString(MongoTemplate template, Object data, String indexKey) {
    String dataStr = (String) data;
    try {
      JsonNode jsStr = OBJECT_MAPPER.readTree(dataStr);
      String indexValue = getString(jsStr, indexKey);
      if (indexValue != null) {
        template.upsertEntity(indexKey, indexValue, dataStr);
      } else {
        template.addEntity(dataStr);
      }
    } catch (Exception ex) {
      log.error("upsertEntityString exception happened in parse object ", ex);
    }
  }

  public void handleBlockEvent(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(blockTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(blockTopic);
    if (Objects.nonNull(template)) {
      service.execute(() -> {
        if (mongoConfig.enabledIndexes()) {
          upsertEntityLong(template, data, "blockNumber");
        } else {
          template.addEntity((String) data);
        }
      });
    }
  }

  public void handleTransactionTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(transactionTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(transactionTopic);
    if (Objects.nonNull(template)) {
      service.execute(() -> {
        if (mongoConfig.enabledIndexes()) {
          upsertEntityString(template, data, "transactionId");
        } else {
          template.addEntity((String) data);
        }
      });
    }
  }

  public void handleSolidityTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(solidityTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(solidityTopic);
    if (Objects.nonNull(template)) {
      service.execute(() -> {
        if (mongoConfig.enabledIndexes()) {
          upsertEntityLong(template, data, "latestSolidifiedBlockNumber");
        } else {
          template.addEntity((String) data);
        }
      });
    }
  }

  public void handleInsertContractTrigger(MongoTemplate template, Object data, String indexKey) {
    if (mongoConfig.enabledIndexes()) {
      upsertEntityString(template, data, indexKey);
    } else {
      template.addEntity((String) data);
    }
  }

  // will not delete when removed is set to true
  public void handleContractLogTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(contractLogTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(contractLogTopic);
    if (Objects.nonNull(template)) {
      service.execute(() -> handleInsertContractTrigger(template, data, "uniqueId"));
    }
  }

  public void handleContractEventTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(contractEventTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(contractEventTopic);
    if (Objects.nonNull(template)) {
      service.execute(() -> {
        String dataStr = (String) data;
        if (dataStr.contains("\"removed\":true")) {
          try {
            JsonNode jsStr = OBJECT_MAPPER.readTree(dataStr);
            String uniqueId = getString(jsStr, "uniqueId");
            if (uniqueId != null) {
              template.delete("uniqueId", uniqueId);
            }
          } catch (Exception ex) {
            log.error("unknown exception happened in parse object ", ex);
          }
        } else {
          handleInsertContractTrigger(template, data, "uniqueId");
        }
      });
    }
  }

  public void handleSolidityLogTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(solidityLogTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(solidityLogTopic);
    if (Objects.nonNull(template)) {
      service.execute(() -> handleInsertContractTrigger(template, data, "uniqueId"));
    }
  }

  public void handleSolidityEventTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(solidityEventTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(solidityEventTopic);
    if (Objects.nonNull(template)) {
      service.execute(() -> handleInsertContractTrigger(template, data, "uniqueId"));
    }
  }

  public void handleBlockContractLogTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(blockContractLogTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(blockContractLogTopic);
    if (Objects.nonNull(template)) {
      try {
        Document trigger = Document.parse((String) data);
        String blockHash = trigger.getString("blockHash");
        Long blockNumber = getLong(trigger.get("blockNumber"));
        if (StringUtils.isNullOrEmpty(blockHash) || blockNumber == null) {
          return;
        }

        List<Document> exists = template.queryByCondition(Filters.and(
            Filters.eq("blockNumber", blockNumber),
            Filters.eq("blockHash", blockHash)));
        if (exists == null || exists.isEmpty()) {
          template.addEntity((String) data);
        } else {
          Object transactionList = trigger.get("transactionList");
          template.update("transactionList", transactionList, "blockHash", blockHash);
        }
      } catch (Exception e) {
        log.error("handleBlockContractLogTrigger in mongo error ", e);
        throw e;
      }
    }
  }

  public void handleTrc20Trigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(trc20TrackerTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(trc20TrackerTopic);
    if (Objects.nonNull(template)) {
      addTrackerEntity("handleTrc20Trigger", template, data);
    }
  }

  public void handleTransferTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(transferTrackerTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(transferTrackerTopic);
    if (Objects.nonNull(template)) {
      addTrackerEntity("handleTransferTrigger", template, data);
    }
  }

  public void handleFreezeTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(freezeTrackerTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(freezeTrackerTopic);
    if (Objects.nonNull(template)) {
      addTrackerEntity("handleFreezeTrigger", template, data);
    }
  }

  public void handleStakeTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(stakeTrackerTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(stakeTrackerTopic);
    if (Objects.nonNull(template)) {
      addTrackerEntity("handleStakeTrigger", template, data);
    }
  }

  public void handleMultiAuthTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(multiAuthTrackerTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(multiAuthTrackerTopic);
    if (Objects.nonNull(template)) {
      addTrackerEntity("handleMultiAuthTrigger", template, data);
    }
  }

  public void handleShieldedTrc20Trigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(shieldedTRC20TrackerTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(shieldedTRC20TrackerTopic);
    if (Objects.nonNull(template)) {
      addTrackerEntity("handleShieldedTrc20Trigger", template, data);
    }
  }

  public void handleShieldedTrc20SolidityTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(shieldedTRC20TrackerTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(shieldedTRC20TrackerTopic);
    if (Objects.nonNull(template)) {
      updateSolidityByBlockHash("handleShieldedTrc20SolidityTrigger", template, data);
    }
  }

  public void handleJustLendTrackerTrigger(Object data) {
    if (Objects.isNull(data) || StringUtils.isNullOrEmpty(justlendTrackerTopic)) {
      return;
    }

    MongoTemplate template = mongoTemplateMap.get(justlendTrackerTopic);
    if (Objects.nonNull(template)) {
      addTrackerEntity("handleJustLendTrackerTrigger", template, data);
    }
  }

  private final Runnable triggerProcessLoop =
      () -> {
        while (isRunTriggerProcessThread) {
          try {
            String triggerData = (String) triggerQueue.poll(1, TimeUnit.SECONDS);

            if (Objects.isNull(triggerData)) {
              continue;
            }

            JsonNode jsonObject = OBJECT_MAPPER.readTree(triggerData);
            EventTopic eventTopic = getEventTopic(jsonObject, triggerData);
            if (eventTopic == null) {
              log.error("Not matched triggerName in data {}", triggerData);
              continue;
            }
            switch (eventTopic) {
              case BLOCK_CONTRACT_LOG_TRIGGER:
                handleBlockContractLogTrigger(triggerData);
                break;
              case BLOCK_TRIGGER:
                handleBlockEvent(triggerData);
                break;
              case TRANSACTION_TRIGGER:
                handleTransactionTrigger(triggerData);
                break;
              case CONTRACT_LOG_TRIGGER:
                handleContractLogTrigger(triggerData);
                break;
              case CONTRACT_EVENT_TRIGGER:
                handleContractEventTrigger(triggerData);
                break;
              case SOLIDITY_TRIGGER:
                handleSolidityTrigger(triggerData);
                break;
              case SOLIDITY_LOG:
                handleSolidityLogTrigger(triggerData);
                break;
              case SOLIDITY_EVENT:
                handleSolidityEventTrigger(triggerData);
                break;
              case TRC20TRACKER_TRIGGER:
                handleTrc20Trigger(triggerData);
                break;
              case SHIELDED_TRC20SOLIDITYTRACKER_TRIGGER:
                handleShieldedTrc20SolidityTrigger(triggerData);
                break;
              case SHIELDED_TRC20TRACKER_TRIGGER:
                handleShieldedTrc20Trigger(triggerData);
                break;
              case TRANSFER_TRACKER_TRIGGER:
              case TRANSFER_TRIGGER:
                handleTransferTrigger(triggerData);
                break;
              case MULTIAUTH_TRACKER_TRIGGER:
                handleMultiAuthTrigger(triggerData);
                break;
              case FREEZE_TRACKER_TRIGGER:
                handleFreezeTrigger(triggerData);
                break;
              case STAKE_TRACKER_TRIGGER:
                handleStakeTrigger(triggerData);
                break;
              case JUSTLEND_TRACKER_TRIGGER:
                handleJustLendTrackerTrigger(triggerData);
                break;
              case BLOCK_ERASE_TRIGGER:
                break;
            }
            log.debug("handle triggerData: {}", triggerData);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          } catch (Exception ex) {
            log.error("unknown exception happened in process capsule loop", ex);
          } catch (Throwable throwable) {
            log.error("unknown throwable happened in process capsule loop", throwable);
          }
        }
      };

  public String getEventFilterList() {
    if (Objects.isNull(mongoTemplateMap)) {
      return null;
    }

    MongoTemplate template = mongoTemplateMap.get(filterCollection);
    if (Objects.nonNull(template)) {
      List<Document> filters = template.queryByCondition(Filters.exists("disable", false));
      return com.mongodb.util.JSON.serialize(filters);
    }
    return null;
  }

  private void addTrackerEntity(String handlerName, MongoTemplate template, Object data) {
    try {
      template.addEntity((String) data);
    } catch (DuplicateKeyException e) {
      log.warn("{}, mongo error, duplicate key: blockhash, jsonData={}", handlerName, data);
    } catch (MongoWriteException ex) {
      String message = ex.getMessage();
      if (message != null && message.contains("duplicate key error")) {
        log.warn("{} in mongo error, duplicate key: blockhash, jsonData={}", handlerName, data);
      } else {
        log.error("{} in mongo error ", handlerName, ex);
        throw ex;
      }
    } catch (RuntimeException e) {
      log.error("{} in mongo error ", handlerName, e);
      throw e;
    }
  }

  private void updateSolidityByBlockHash(String handlerName, MongoTemplate template, Object data) {
    try {
      Document trigger = Document.parse((String) data);
      String blockHash = trigger.getString("blockHash");
      if (StringUtils.isNotNullOrEmpty(blockHash)) {
        template.update("solidity", Boolean.TRUE, "blockHash", blockHash);
      }
    } catch (RuntimeException ex) {
      log.error("{} in mongo error ", handlerName, ex);
      throw ex;
    }
  }

  private EventTopic getEventTopic(JsonNode jsonObject, String triggerData) {
    if (jsonObject.has("triggerName")) {
      String triggerName = getString(jsonObject, "triggerName");
      EventTopic eventTopic = EventTopic.getEventTopicByName(triggerName);
      if (eventTopic != null) {
        return eventTopic;
      }
      log.error("Not matched triggerName {} in data {}", triggerName, triggerData);
    }
    return getEventTopicByContent(triggerData);
  }

  private EventTopic getEventTopicByContent(String triggerData) {
    EventTopic[] eventTopics = {
        EventTopic.BLOCK_CONTRACT_LOG_TRIGGER,
        EventTopic.BLOCK_TRIGGER,
        EventTopic.TRANSACTION_TRIGGER,
        EventTopic.CONTRACT_LOG_TRIGGER,
        EventTopic.CONTRACT_EVENT_TRIGGER,
        EventTopic.SOLIDITY_TRIGGER,
        EventTopic.SOLIDITY_LOG,
        EventTopic.SOLIDITY_EVENT,
        EventTopic.TRC20TRACKER_TRIGGER,
        EventTopic.SHIELDED_TRC20SOLIDITYTRACKER_TRIGGER,
        EventTopic.SHIELDED_TRC20TRACKER_TRIGGER,
        EventTopic.TRANSFER_TRACKER_TRIGGER,
        EventTopic.TRANSFER_TRIGGER,
        EventTopic.MULTIAUTH_TRACKER_TRIGGER,
        EventTopic.FREEZE_TRACKER_TRIGGER,
        EventTopic.STAKE_TRACKER_TRIGGER,
        EventTopic.JUSTLEND_TRACKER_TRIGGER
    };
    for (EventTopic eventTopic : eventTopics) {
      if (triggerData.contains(eventTopic.getName())) {
        return eventTopic;
      }
    }
    return null;
  }

  private Long getLong(JsonNode node, String key) {
    JsonNode value = node.get(key);
    if (Objects.isNull(value) || value.isNull()) {
      return null;
    }
    if (value.isNumber()) {
      return value.longValue();
    }
    String valueText = value.asText(null);
    if (StringUtils.isNullOrEmpty(valueText)) {
      return null;
    }
    return Long.parseLong(valueText);
  }

  private Long getLong(Object value) {
    if (Objects.isNull(value)) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return Long.parseLong(String.valueOf(value));
  }

  private String getString(JsonNode node, String key) {
    JsonNode value = node.get(key);
    if (Objects.isNull(value) || value.isNull()) {
      return null;
    }
    return value.asText();
  }

  @Override
  public void close() {
    log.info("Closing MongodbSender...");
    isRunTriggerProcessThread = false;
    if (triggerProcessThread != null) {
      triggerProcessThread.interrupt();
      try {
        triggerProcessThread.join(1000);
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for triggerProcessThread to stop");
        Thread.currentThread().interrupt();
      }
    }
    service.shutdown();
    try {
      if (!service.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
        service.shutdownNow();
        if (!service.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
          log.warn("Mongo service thread pool did not terminate");
        }
      }
    } catch (InterruptedException ie) {
      service.shutdownNow();
      Thread.currentThread().interrupt();
    }
    if (mongoManager != null) {
      mongoManager.close();
    }
    log.info("MongodbSender closed.");
  }
}

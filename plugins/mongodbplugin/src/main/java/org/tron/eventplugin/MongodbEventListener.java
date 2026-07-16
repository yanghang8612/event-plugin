package org.tron.eventplugin;

import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.tron.common.logsfilter.IPluginEventListener;

@Slf4j(topic = "event")
@Extension
public class MongodbEventListener implements IPluginEventListener {

  @Override
  public void setServerAddress(String address) {
    if (Objects.isNull(address) || address.isEmpty()) {
      return;
    }
    MongodbSenderImpl.getInstance().setServerAddress(address);
  }

  @Override
  public void setTopic(int eventType, String topic) {
    MongodbSenderImpl.getInstance().setTopic(eventType, topic);
  }

  @Override
  public void setDBConfig(String dbConfig) {
    MongodbSenderImpl.getInstance().setDBConfig(dbConfig);
  }

  @Override
  public void start() {
    // MongodbSenderImpl should never init until server address is set.
    MongodbSenderImpl.getInstance().init();
  }

  @Override
  public void stop() {
    MongodbSenderImpl.getInstance().close();
  }

  @Override
  public void handleBlockEvent(Object data) {
    offer(data);
  }

  @Override
  public void handleTransactionTrigger(Object data) {
    offer(data);
  }

  @Override
  public void handleContractLogTrigger(Object data) {
    offer(data);
  }

  @Override
  public void handleContractEventTrigger(Object data) {
    offer(data);
  }

  @Override
  public void handleSolidityTrigger(Object data) {
    offer(data);
  }

  @Override
  public void handleSolidityLogTrigger(Object data) {
    offer(data);
  }

  @Override
  public void handleSolidityEventTrigger(Object data) {
    offer(data);
  }

  @Override
  public int getPendingSize() {
    return MongodbSenderImpl.getInstance().getTriggerQueue().size()
        + MongodbSenderImpl.getInstance().getQueue().size();
  }

  @Override
  public String getEventFilterList() {
    return MongodbSenderImpl.getInstance().getEventFilterList();
  }

  @Override
  public void handleBlockContractLogTrigger(Object data) {
    offer(data);
  }

  @Override
  public void handleTRC20Event(Object data) {
    log.info("  >>>> data:{}", data);
    if (containsTopic(data, EventTopic.TRC20TRACKER_TRIGGER)) {
      MongodbSenderImpl.getInstance().handleTrc20Trigger(data);
    }
  }

  @Override
  public void handleFreezeBalanceEvent(Object data) {
    log.info("  >>>> data:{}", data);
    if (containsTopic(data, EventTopic.FREEZE_TRACKER_TRIGGER)) {
      MongodbSenderImpl.getInstance().handleFreezeTrigger(data);
    }
  }

  @Override
  public void handleStakeBalanceEvent(Object data) {
    log.info("  >>>> data:{}", data);
    if (containsTopic(data, EventTopic.STAKE_TRACKER_TRIGGER)) {
      MongodbSenderImpl.getInstance().handleStakeTrigger(data);
    }
  }

  @Override
  public void handleShieldedTRC20Event(Object data) {
    if (containsTopic(data, EventTopic.SHIELDED_TRC20TRACKER_TRIGGER)) {
      MongodbSenderImpl.getInstance().handleShieldedTrc20Trigger(data);
    } else if (containsTopic(data, EventTopic.SHIELDED_TRC20SOLIDITYTRACKER_TRIGGER)) {
      MongodbSenderImpl.getInstance().handleShieldedTrc20SolidityTrigger(data);
    }
  }

  @Override
  public void handleTransferEvent(Object data) {
    log.info("  >>>> handleTransferEvent data:{}", data);
    if (containsTopic(data, EventTopic.TRANSFER_TRACKER_TRIGGER, EventTopic.TRANSFER_TRIGGER)) {
      MongodbSenderImpl.getInstance().handleTransferTrigger(data);
    }
  }

  @Override
  public void handleMultiAuthTrigger(Object data) {
    log.info("  >>>> handleMultiAuthTrigger data:{}", data);
    if (containsTopic(data, EventTopic.MULTIAUTH_TRACKER_TRIGGER)) {
      MongodbSenderImpl.getInstance().handleMultiAuthTrigger(data);
    }
  }

  @Override
  public void handleJustLendTrackerTrigger(Object data) {
    log.info("  >>>> handleJustLendTrackerTrigger data:{}", data);
    if (containsTopic(data, EventTopic.JUSTLEND_TRACKER_TRIGGER)) {
      MongodbSenderImpl.getInstance().handleJustLendTrackerTrigger(data);
    }
  }

  private void offer(Object data) {
    if (Objects.nonNull(data)) {
      MongodbSenderImpl.getInstance().getTriggerQueue().offer(data);
    }
  }

  private boolean containsTopic(Object data, EventTopic... eventTopics) {
    if (Objects.isNull(data)) {
      return false;
    }
    String triggerData = String.valueOf(data);
    for (EventTopic eventTopic : eventTopics) {
      if (triggerData.contains(eventTopic.getName())) {
        return true;
      }
    }
    return false;
  }
}

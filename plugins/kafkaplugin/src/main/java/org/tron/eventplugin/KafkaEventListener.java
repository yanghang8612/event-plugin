package org.tron.eventplugin;

import java.util.Objects;
import org.pf4j.Extension;
import org.tron.common.logsfilter.IPluginEventListener;

@Extension
public class KafkaEventListener implements IPluginEventListener {

  @Override
  public void setServerAddress(String address) {
    if (Objects.isNull(address) || address.isEmpty()) {
      return;
    }
    KafkaSenderImpl.getInstance().setServerAddress(address);
  }

  @Override
  public void setTopic(int eventType, String topic) {
    KafkaSenderImpl.getInstance().setTopic(eventType, topic);
  }

  @Override
  public void setDBConfig(String dbConfig) {
    // empty implementation
  }

  @Override
  public void start() {
    // KafkaSenderImpl should never init until server address is set.
    KafkaSenderImpl.getInstance().init();
  }

  @Override
  public void stop() {
    KafkaSenderImpl.getInstance().close();
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
    return KafkaSenderImpl.getInstance().getTriggerQueue().size();
  }

  @Override
  public String getEventFilterList() {
    return null;
  }

  @Override
  public void handleBlockContractLogTrigger(Object data) {
    offer(data);
  }

  @Override
  public void handleTRC20Event(Object data) {
    offer(data);
  }

  @Override
  public void handleFreezeBalanceEvent(Object data) {
    offer(data);
  }

  @Override
  public void handleStakeBalanceEvent(Object data) {
    offer(data);
  }

  @Override
  public void handleShieldedTRC20Event(Object data) {
    offer(data);
  }

  @Override
  public void handleTransferEvent(Object data) {
    offer(data);
  }

  @Override
  public void handleMultiAuthTrigger(Object data) {
    offer(data);
  }

  @Override
  public void handleJustLendTrackerTrigger(Object data) {
    offer(data);
  }

  private void offer(Object data) {
    if (Objects.nonNull(data)) {
      KafkaSenderImpl.getInstance().getTriggerQueue().offer(data);
    }
  }
}

package org.tron.common.logsfilter.trigger;

import java.util.Locale;
import lombok.Getter;

@Getter
public enum EventTopic {
  BLOCK_TRIGGER(0, "blockTrigger"),
  TRANSACTION_TRIGGER(1, "transactionTrigger"),
  CONTRACT_LOG_TRIGGER(2, "contractLogTrigger"),
  CONTRACT_EVENT_TRIGGER(3, "contractEventTrigger"),
  SOLIDITY_TRIGGER(4, "solidityTrigger"),
  SOLIDITY_EVENT(5, "solidityEventTrigger"),
  SOLIDITY_LOG(6, "solidityLogTrigger"),
  BLOCK_CONTRACT_LOG_TRIGGER(7, "blockContractLogTrigger"),
  TRC20TRACKER_TRIGGER(1000, "trc20TrackerTrigger"),
  JUSTLEND_TRACKER_TRIGGER(1001, "justLendTracker"),
  BLOCK_ERASE_TRIGGER(1002, "blockErasedTrigger"),
  SHIELDED_TRC20TRACKER_TRIGGER(1003, "shieldedTRC20Tracker"),
  SHIELDED_TRC20SOLIDITYTRACKER_TRIGGER(1004, "shieldedTRC20SolidityTracker"),
  TRANSFER_TRACKER_TRIGGER(1005, "transferTracker"),
  TRANSFER_TRIGGER(1005, "transferTrigger"),
  MULTIAUTH_TRACKER_TRIGGER(1006, "multiAuthTracker"),
  FREEZE_TRACKER_TRIGGER(1010, "freezeBalanceTrigger"),
  STAKE_TRACKER_TRIGGER(1020, "stakeBalanceTrigger");

  private final Integer type;
  private final String name;

  EventTopic(Integer type, String name) {
    this.type = type;
    this.name = name;
  }

  public static EventTopic getEventTopicByType(int topicType) {
    for (EventTopic member : values()) {
      if (member.getType() == topicType) {
        return member;
      }
    }
    return null;
  }

  public static EventTopic getEventTopicByType(int topicType, String topic) {
    if (topic != null) {
      String normalizedTopic = topic.toLowerCase(Locale.ROOT);
      for (EventTopic member : values()) {
        if (member.getType() == topicType
            && normalizedTopic.contains(member.getName().toLowerCase(Locale.ROOT))) {
          return member;
        }
      }
      if (topicType == JUSTLEND_TRACKER_TRIGGER.getType()
          && normalizedTopic.contains("justlend")) {
        return JUSTLEND_TRACKER_TRIGGER;
      }
      if (topicType == TRANSFER_TRACKER_TRIGGER.getType()
          && normalizedTopic.contains("transfer")) {
        return TRANSFER_TRACKER_TRIGGER;
      }
    }
    return getEventTopicByType(topicType);
  }

  public static EventTopic getEventTopicByName(String topicName) {
    for (EventTopic member : values()) {
      if (member.getName().equals(topicName)) {
        return member;
      }
    }
    return null;
  }
}

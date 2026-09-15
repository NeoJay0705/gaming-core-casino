package localmq

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

var durableNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

func validateTopicName(topic string) error {
	if !durableNamePattern.MatchString(topic) {
		return fmt.Errorf("topic %q must match [a-z0-9][a-z0-9._-]{0,127}", topic)
	}
	return nil
}

func validateGroupName(group string) error {
	if !durableNamePattern.MatchString(group) {
		return fmt.Errorf("group %q must match [a-z0-9][a-z0-9._-]{0,127}", group)
	}
	return nil
}

func validGroupType(value string) bool {
	return value == string(GroupTypeProtected) || value == string(GroupTypeBestEffort)
}

func validInitialPosition(value string) bool {
	return value == string(InitialEarliest) || value == string(InitialLatest)
}

func validGroupState(value string) bool {
	switch GroupState(value) {
	case GroupInitializing, GroupActive, GroupDeleting, GroupDeleted:
		return true
	default:
		return false
	}
}

func validateTopicAndGroup(topic, group string) error {
	if err := validateTopicName(topic); err != nil {
		return err
	}
	if err := validateGroupName(group); err != nil {
		return err
	}
	return nil
}

func validateLaneID(laneID string) error {
	if strings.TrimSpace(laneID) == "" || laneID != strings.TrimSpace(laneID) {
		return errors.New("lane_id is required and must not contain whitespace")
	}
	id, err := uuid.Parse(laneID)
	if err != nil || id.Version() != 7 {
		return fmt.Errorf("lane_id %q must be UUIDv7", laneID)
	}
	return nil
}

func validateRequestID(requestID string) error {
	if requestID == "" || requestID != strings.TrimSpace(requestID) || !durableNamePattern.MatchString(requestID) {
		return fmt.Errorf("request_id %q must be a non-empty safe identifier", requestID)
	}
	return nil
}

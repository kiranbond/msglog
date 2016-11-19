// Copyright (c) 2014 ZeroStack Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// MsgLog implements a persistent log for message streams. Streams are produced
// and consumed in FIFO order.
//
// Messages are streamed to persistent backend as they are written after
// assigning a Message Sequence Number (MSN). The MSN is returned to the user
// and response is sent on the supplied channel when the message is stored on
// disk.
// Consumers can read the message log by supplying the MSN and the topic.
// Any read with an invalid MSN returns the next available MSN in that topic so
// consumers can discover the MSNs available for reading. When the consumer(s)
// no longer needs a message corresponding to an MSN, it can Commit with an MSN
// that will let the MsgLog clean up the log in the background.

package msglog

import (
  "container/heap"
  "sync"
  "time"

  "github.com/golang/glog"

  "zerostack/common/util"
)

const (
  // Number of stale messages at which cleanup gets triggered even if busy
  cCleanupThreshold     int64         = 1000
  cCleanupRetryDuration time.Duration = 30 * time.Second
  cDefaultTTL           time.Duration = 3600 * time.Second
)

// Response is sent by the MsgLog in the response channel to the
// Publisher.
type Response struct {
  Topic  string
  MSN    uint64
  Result error
}

// TopicState contains the first MSN known and the next MSN to be written.
// FirstMSN  - the known FirstMSN in the topic
// LastMSN   - used to figure out the next MSN to be used for writing
// CommitMSN - records the committed MSN from the caller.
type TopicState struct {
  FirstMSN  uint64
  LastMSN   uint64
  CommitMSN uint64
  TTL       time.Duration
}

// MsgStore is an interface to storage backend that is used by MsgLog. The
// backend needs to implement the listed functions and can be either a local
// storage or a distributed/replicated store.
type MsgStore interface {
  // Initialize the data store and return the map of MSNs state by topic.
  Init() (map[string]*TopicState, error)

  // Write the data for the topic and MSN into the store. Send Response on
  // the provided channel when disk IO is complete.
  Write(*msgStoreOp) error

  // SetTTL sets the ttl for a topic. If a topic does not have any ttl
  // explicitly set then it will be assigned the default TTL.
  SetTTL(topic string, ttl time.Duration) error

  // Read data for a topic with the MSN. The data is returned if MSN is found.
  // If the MSN is not found then an error is returned and also the next highest
  // known MSN found is returned.
  Read(string, uint64) ([]byte, uint64, error)

  // Commit indicates that all data corresponding to MSNs smaller than the
  // provided MSN in that topic is no longer needed.
  Commit(string, uint64) error

  // Flush all pending disk operations.
  Flush() error

  // DeletePartial deletes all files related to a single topic's commit from
  // first to last MSN (both inclusive) as specified in the params.
  DeletePartial(string, uint64, uint64) error

  // Delete deletes all files related to a single topic.
  Delete(string, time.Duration) error

  // DeleteAll deletes all files related to all topics.
  DeleteAll() error
}

// MsgLog is the main object that implements the streaming log.
type MsgLog struct {
  store MsgStore
  // state
  mu       sync.RWMutex           // mutex to protect shared state
  msnState map[string]*TopicState // TopicState by topic
  minTTL   time.Duration          // minimum known TTL
  ttlTimer *util.RestartTimer

  // disk ops
  topicOps map[string]*opQueue // op queue per topic

  // channels to signal disk controller
  diskOpCh       chan struct{}      // channel to signal disk controller ops
  pauseDiskOpCh  chan struct{}      // channel to signal disk to pause
  resumeDiskOpCh chan struct{}      // channel to signal disk to resume
  flushDiskOpCh  chan chan struct{} // channel to signal disk to do a Sync
  cleanupDiskCh  chan struct{}      // channel to signal disk to attempt cleanup
  resetTTLCh     chan time.Duration // channel to reset the TTL timer
}

// NewMsgLog creates and initializes the MsgLog object.
func NewMsgLog(store MsgStore) (*MsgLog, error) {
  msglog := MsgLog{
    store:          store,
    minTTL:         cDefaultTTL,
    ttlTimer:       util.NewRestartTimer(cDefaultTTL / 2),
    topicOps:       make(map[string]*opQueue),
    diskOpCh:       make(chan struct{}),
    flushDiskOpCh:  make(chan chan struct{}),
    resumeDiskOpCh: make(chan struct{}),
    pauseDiskOpCh:  make(chan struct{}),
    cleanupDiskCh:  make(chan struct{}),
    resetTTLCh:     make(chan time.Duration),
  }
  msnState, err := store.Init()
  if err != nil {
    return nil, err
  }
  // Initialize the state based on the init from the MsgStore.
  msglog.msnState = make(map[string]*TopicState)
  for topic, state := range msnState {
    msglog.msnState[topic] = new(TopicState)
    msglog.msnState[topic].FirstMSN = state.FirstMSN
    msglog.msnState[topic].LastMSN = state.LastMSN
    msglog.msnState[topic].CommitMSN = state.CommitMSN
    msglog.msnState[topic].TTL = state.TTL
    if msglog.minTTL > state.TTL && state.TTL != 0 {
      msglog.minTTL = state.TTL
    }
    var opQ opQueue
    msglog.topicOps[topic] = &opQ
  }

  // start the disk controller thread which does all disk ops in a single
  // thread. TODO(kiran): more threads by topic?
  go msglog.diskController()
  return &msglog, nil
}

// NewMsgLogFS is a helper function that instantiates a MsgStoreFS and a MsgLog
// instead of user having to do the steps separately.
func NewMsgLogFS(dir string) (*MsgLog, error) {
  store, err := NewMsgStoreFS(dir)
  if err != nil {
    return nil, err
  }
  return NewMsgLog(store)
}

// GetState returns the current state of the msglog
func (m *MsgLog) GetState() MsgStoreState {
  return m.msnState
}

// SetTTL sets the TTL for a topic.
func (m *MsgLog) SetTTL(topic string, ttl time.Duration) error {
  glog.Infof("setting ttl for topic %s to %v", topic, ttl)
  m.mu.Lock()
  _, found := m.msnState[topic]
  if !found {
    m.msnState[topic] = new(TopicState)
    m.msnState[topic].LastMSN = 1
  }
  m.msnState[topic].TTL = ttl
  m.mu.Unlock()
  m.resetTTLCh <- ttl
  return m.store.SetTTL(topic, ttl)
}

// Publish writes the data with the given topic to the message log. It returns
// the Message Sequence Number(MSN) on successfully queueing the request. The
// completion of the request is indicated on the done channel with the MSN
// and any error in writing it inside MsgStore.
func (m *MsgLog) Publish(topic string, data []byte, done chan *Response) (
  uint64, error) {

  m.mu.Lock()
  defer m.mu.Unlock()
  _, found := m.msnState[topic]
  if !found {
    m.msnState[topic] = new(TopicState)
    m.msnState[topic].LastMSN = 1
  } else {
    m.msnState[topic].LastMSN++
  }
  msn := m.msnState[topic].LastMSN

  glog.V(2).Infof("publishing to topic : %s, %d bytes, msn %d",
    topic, len(data), msn)

  op := &msgStoreOp{topic: topic, msn: msn, data: data, done: done}

  opQ, qFound := m.topicOps[topic]
  if !qFound {
    var q opQueue
    m.topicOps[topic] = &q
    opQ = &q
  }

  heap.Push(opQ, op)
  // signal the diskController
  go func() { m.diskOpCh <- struct{}{} }()
  return msn, nil
}

// Get returns the message with the provided msn. If the msn does not exist then
// the next msn found is returned with nil for data and an error.
// TODO(kiran): is it useful to also return the data and let caller decide
// what he wants to do with data, rdMSN and error?
func (m *MsgLog) Get(topic string, msn uint64) ([]byte, uint64, error) {
  return m.store.Read(topic, msn)
}

// Commit tells the messagelog that messages up to the provided msn are no
// longer needed and can be cleaned up.
func (m *MsgLog) Commit(topic string, msn uint64) error {
  glog.V(1).Infof("committing topic: %s msn %d", topic, msn)
  err := m.store.Commit(topic, msn)

  m.mu.Lock()
  defer m.mu.Unlock()

  // TODO(kiran): should we signal any other errors on non-existent topic being
  // committed.
  _, found := m.msnState[topic]
  if found && m.msnState[topic].CommitMSN < msn {
    m.msnState[topic].CommitMSN = msn
  }
  // signal the diskController
  go func() { m.cleanupDiskCh <- struct{}{} }()
  return err
}

// Clear clears all messages in the given topic.
func (m *MsgLog) Clear(topic string, msn uint64) error {
  m.pauseDiskOpCh <- struct{}{}
  defer func() { m.resumeDiskOpCh <- struct{}{} }()
  m.store.Delete(topic, time.Duration(0))
  return nil
}

// ClearAll clears all messages in all topics.
func (m *MsgLog) ClearAll() error {
  m.pauseDiskOpCh <- struct{}{}
  defer func() { m.resumeDiskOpCh <- struct{}{} }()
  m.store.DeleteAll()
  return nil
}

////////////////////////////////////////////////////////////////////////////////
// INTERNAL
////////////////////////////////////////////////////////////////////////////////

// diskController is the goroutine that dispatches the IO operations unless
// it is paused due to recovery channel when it will not do ops from the topicOps.
func (m *MsgLog) diskController() error {

  diskOpCh := m.diskOpCh

  m.ttlTimer.Restart()

  for {
    select {
    case ttl := <-m.resetTTLCh:
      m.mu.Lock() // for minTTL
      if m.minTTL > ttl && ttl != 0 {
        m.minTTL = ttl
        m.ttlTimer.Reset(m.minTTL / 2)
      }
      m.mu.Unlock()

      m.ttlTimer.Reset(ttl)

    case <-diskOpCh:
      m.executeMinOp()

    case rspCh := <-m.flushDiskOpCh:
      m.executeAllOps()
      m.store.Flush()
      rspCh <- struct{}{}

    case _ = <-m.pauseDiskOpCh:
      glog.V(2).Infof("disk controller paused")
      diskOpCh = nil

    case _ = <-m.resumeDiskOpCh:
      glog.V(2).Infof("disk controller resumed")
      m.store.Flush()
      diskOpCh = m.diskOpCh

    case <-m.cleanupDiskCh:
      glog.V(2).Infof("disk cleanup triggered")
      // TODO(kiran) - do restartable timer and restart if retry
      retry := m.tryCleanup()
      if retry {
        go func() {
          select {
          case <-time.After(cCleanupRetryDuration):
            m.cleanupDiskCh <- struct{}{}
          }
        }()
      }

    case <-m.ttlTimer.C:
      m.mu.Lock()
      glog.V(2).Infof("disk ttl cleanup triggered with minTTL %v", m.minTTL)
      m.mu.Unlock()
      m.ttlCleanup()
      m.ttlTimer.RestartExpired()
    }
  }
}

// executeMinOp picks the Op with min MSN from one of the op queues and executes
// the op.
// TODO (sunil): optimize this function to execute the ops in batch mode as well
func (m *MsgLog) executeMinOp() error {
  var opInf interface{}

  m.mu.Lock()
  // range on a map will select topics to process randomly giving fair chance to
  // each of the topic ops.
  for topic, q := range m.topicOps {
    if q.Len() > 0 {
      glog.V(2).Infof("found op to process for topic: %s", topic)
      opInf = heap.Pop(q) // pop the min item
      break
    }
  }
  m.mu.Unlock()
  if opInf == nil {
    glog.V(3).Infof("empty op queues, nothing to process")
    return nil
  }
  op := opInf.(*msgStoreOp)

  glog.V(2).Infof("executing opid %d for topic: %s", op.msn, op.topic)

  err := m.store.Write(op)
  if err != nil {
    glog.Errorf("error writing op : %d for topic: %s :: %v",
      op.msn, op.topic, err)
  }

  // signal the original MsgStore caller
  go func() { op.done <- &Response{op.topic, op.msn, err} }()
  return err
}

// numOps returns total number of Ops in all the topic op queues.
func (m *MsgLog) numOps() int {
  m.mu.RLock()
  defer m.mu.RUnlock()

  n := 0
  for _, q := range m.topicOps {
    n += q.Len()
  }
  return n
}

// executeAllOps picks all ops in sorted order of their id and executes them
// right away.
// TODO(kiran): Maybe we can just wait for disk controller to just finish
// all ops one by one using some waitgroup instead of executing them here and
// controller trying again when it picks up diskOpCh?
func (m *MsgLog) executeAllOps() error {
  numOps := m.numOps()
  for ii := 0; ii < numOps; ii++ {
    m.executeMinOp()
  }
  return nil
}

// tryCleanup attempt to cleanup the committed messages from the log if we are
// not very busy.
func (m *MsgLog) tryCleanup() bool {
  for {
    var cleanup, retry bool
    if m.numOps() == 0 {
      cleanup = true
    }
    var maxDiffTopic string
    var maxDiff int64

    m.mu.RLock()

    // find the topic with the highest number of stale messages to cleanup.
    // TODO(kiran): this is where we would also use the ttl to do time based
    // cleanup.
    for topic, state := range m.msnState {
      diff := int64(state.CommitMSN) - int64(state.FirstMSN)
      if diff > cCleanupThreshold {
        cleanup = true
      }
      if diff > maxDiff {
        maxDiff = diff
        maxDiffTopic = topic
      }
      // If there is something to be cleaned up, then we should retry if we do
      // not do it this time.
      if diff > 0 {
        retry = true
      }
    }

    m.mu.RUnlock()

    if !cleanup || maxDiff == 0 {
      return retry
    }

    // Change the FirstMSN to CommitMSN right now so it is not found in search
    // in the for loop while we cleanup
    m.mu.Lock()
    // since we released lock, it could have been deleted so check again.
    _, found := m.msnState[maxDiffTopic]
    if !found {
      glog.V(2).Infof("topic with max diff not found : %s", maxDiffTopic)
      continue
    }
    firstMSN := m.msnState[maxDiffTopic].FirstMSN
    commitMSN := m.msnState[maxDiffTopic].CommitMSN
    m.msnState[maxDiffTopic].FirstMSN = commitMSN + 1
    m.mu.Unlock()

    glog.V(2).Infof("scheduling cleanup topic: %s from %d to %d", maxDiffTopic,
      firstMSN, commitMSN)

    go func(topic string, first, last uint64) {
      err := m.store.DeletePartial(topic, first, last)
      if err != nil {
        glog.Errorf("error removing topic files for %s from %d to %d :: %v",
          maxDiffTopic, first, last, err)
      }
    }(maxDiffTopic, firstMSN, commitMSN)
  }
}

// ttlCleanup cleans up any topic files which are older than TTL.
// TODO(kiran): should we reset FirstMSN based on deleted files? Call simpler
// Reinit()?
func (m *MsgLog) ttlCleanup() error {
  m.mu.Lock()
  defer m.mu.Unlock()

  for topic, state := range m.msnState {
    m.store.Delete(topic, state.TTL)
  }
  return nil
}

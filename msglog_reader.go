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
// msglog Reader implements a reader interface to msglog that can track and
// take periodic snapshots of where in the msglog the reader has processed.
// There are two ways to use the Reader:
// - Use the GetNext which reads messages sequentially and is used
//   by readers who do not have an in-memory interface to the writers.
//   (This mode is also used by recovery)
//
//   Writer   ---> MsgLog ----> Reader(s)
//
// - Use the Commit where the writer/reader are sharing in
//   memory the messages and msglog is used for crash recovery and handling
//   failures by reader.
//
//   Writer   ---> In Memory   ----> Reader
//             \---> MsgLog    --/
//
// When using GetNext(true), the current msn is stated to be complete since
// the reader is asking for the next message. This can be disabled and
// explicit Commit can be used by using GetNext(false). This allows batch
// processing and out of order completion.
// Commit allows the reader to indicate out of order processing since the
// commit ids are stored internally and at a programmed interval, all the ids
// from last committed id to the sequentially complete ids are committed to
// msglog. NOTE: if user keeps sending non-sequential ids then the heap
// in reader will grow in memory. TODO(kiran) - how do we limit this?
//
// TODO(kiran): Is this too much flexibility to have two interfaces? Split into
// two reader interfaces?

package msglog

import (
  "container/heap"
  "encoding/json"
  "sync"

  "github.com/golang/glog"

  "zerostack/common/util"
)

// ReaderInf is an interface implemented by MsgLog readers.
type ReaderInf interface {
  // Seek to a specific entry in the MsgLog.
  Seek(msn uint64) error
  // GetNext reads the next entry and auto-increments the tracking MSN.
  // On an error, it returns the msn that was found and tracking MSN is set to
  // that MSN.
  GetNext(commit bool) ([]byte, uint64, error)
  // Commit indicates that the message with msn is committed.
  Commit(msn uint64) error
  // SaveSnapshot saves the state of the reader to the snapshot file.
  SaveSnapshot() error
  // LoadSnapshot loads the state of the reader from the snapshot file.
  LoadSnapshot() error
}

// Reader implements the ReaderInf and tracks a reader state into a MsgLog
// with optional snapshotting of state so user does not have to remember
// till what point it has read the MsgLog.
type Reader struct {
  // config
  msglog       *MsgLog
  topic        string
  snapshotDir  string
  snapshotFile string
  snapshotFreq int32
  // state
  mu          sync.Mutex
  currMSN     uint64
  commitMSN   uint64
  readCnt     int32
  commitCnt   int32
  snapshot    *util.Snapshot
  pendingMSNs *util.SyncUint64Heap
}

type commitState struct {
  CurrMSN   uint64 `json:"current_msn"`
  CommitMSN uint64 `json:"commit_msn"`
}

var _ ReaderInf = &Reader{}

// NewReader creates a new Reader for a MsgLog topic.
// msglog - the message log this reader is reading from
// topic - topic this reader will read from
// snapshotFile - file in which to save the reader state. Empty string
//                disables any snapshotting.
// snapshotFreq - number of reads after which the state should be
//                auto-snapshotted
func NewReader(msglog *MsgLog, topic string, snapshotDir, snapshotFile string,
  snapshotFreq int32) (*Reader, error) {

  var commitMSN uint64

  state := msglog.GetState()
  topicState, found := state[topic]
  if found {
    data, _ := state.MarshalTextString()
    glog.Infof("finished parsing MsgStore root. msnState=%s", data)
    commitMSN = topicState.CommitMSN
    glog.Infof("initializing commitMSN for topic: %s to %d", topic, commitMSN)
  } else {
    glog.Infof("no state found for topic: %s", topic)
  }

  reader := &Reader{
    msglog:       msglog,
    topic:        topic,
    snapshotDir:  snapshotDir,
    snapshotFile: snapshotFile,
    snapshotFreq: snapshotFreq,
    currMSN:      0,
    commitMSN:    commitMSN,
    readCnt:      0,
    commitCnt:    0,
    pendingMSNs:  util.NewSyncUint64Heap(),
  }

  if snapshotFile == "" {
    // snapshots are disabled
    glog.Warningf("snapshots are disabled for msglog reader for topic: %s",
      topic)
    return reader, nil
  }

  var err error
  reader.snapshot, err = util.NewSnapshot(snapshotDir, snapshotFile)
  if err != nil {
    glog.Errorf("error initializing reader snapshot :: %v", err)
    return nil, err
  }
  return reader, nil
}

// Seek just resets the tracking MSN to the input.
func (r *Reader) Seek(msn uint64) error {
  r.currMSN = msn
  return nil
}

// GetNext gets the message log with the current MSN and auto-increments
// the currMSN.
func (r *Reader) GetNext(commit bool) ([]byte, uint64, error) {
  buf, foundMSN, err := r.msglog.Get(r.topic, r.currMSN)
  if err != nil {
    glog.V(3).Infof("finished reading msglog :: %v", err)
    buf = nil
  }
  takeSnapshot := false

  r.mu.Lock()
  if commit {
    r.commitMSN = r.currMSN
  }
  r.currMSN++
  r.readCnt++
  if r.readCnt == r.snapshotFreq {
    takeSnapshot = true
    r.readCnt = 0
  }
  r.mu.Unlock()

  if takeSnapshot {
    r.SaveSnapshot()
  }
  return buf, foundMSN, err
}

// Commit is called when the user of the Reader has finished processing message
// with the input MSN.
func (r *Reader) Commit(msn uint64) error {

  var err error
  updateCommit := false

  glog.V(4).Infof("committing %s entry: %d, current commit=%d, commitcnt: %d",
    r.topic, msn, r.commitMSN, r.commitCnt)

  r.mu.Lock()

  heap.Push(r.pendingMSNs, msn)

  newCommitMSN := r.commitMSN

  r.commitCnt++
  if r.commitCnt >= r.snapshotFreq {
    r.commitCnt = 0
    // check for sequential MSNs after r.commitMSN in r.pendingMSNs
    count := r.pendingMSNs.Len()
    for count > 0 {
      next := heap.Pop(r.pendingMSNs)
      if next.(uint64) != newCommitMSN+1 {
        // put this back into heap
        heap.Push(r.pendingMSNs, next)
        // break out of loop
        count = 0
      } else {
        count--
        newCommitMSN = next.(uint64)
      }
    }
  }

  if newCommitMSN > r.commitMSN {
    r.commitMSN = newCommitMSN
    updateCommit = true
  }
  r.mu.Unlock()

  if updateCommit {
    glog.V(3).Infof("committing entry %d, upto %d", msn, newCommitMSN)
    err = r.msglog.Commit(r.topic, newCommitMSN)
    if err == nil {
      r.SaveSnapshot()
    }
  } else {
    glog.V(4).Infof("nothing to commit for msn: %d", msn)
  }

  return err
}

// SaveSnapshot saves the current state of the reader as a snapshot.
func (r *Reader) SaveSnapshot() error {
  if r.snapshot == nil {
    return nil
  }
  return r.snapshot.SaveText(r)
}

// LoadSnapshot loads the state of the reader from a snapshot. The user of the
// Reader should call this at the start of the process so that the Reader
// recovers state from its last good saved snapshot.
func (r *Reader) LoadSnapshot() error {
  return r.snapshot.LoadText(r)
}

// MarshalTextString is called back by SaveText.
func (r *Reader) MarshalTextString() ([]byte, error) {
  state := commitState{CurrMSN: r.currMSN, CommitMSN: r.commitMSN}
  return json.Marshal(state)
}

// UnmarshalTextString is called back by LoadSnapshot.
func (r *Reader) UnmarshalTextString(data []byte) error {
  state := commitState{}
  err := json.Unmarshal(data, &state)
  if err != nil {
    return err
  }
  r.currMSN = state.CurrMSN
  r.commitMSN = state.CommitMSN
  return nil
}

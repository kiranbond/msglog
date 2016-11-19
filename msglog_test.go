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

package msglog

import (
  crand "crypto/rand"
  "flag"
  "math/rand"
  "sync"
  "testing"
  "time"

  "github.com/golang/glog"
  "github.com/stretchr/testify/assert"
)

// TestMsgLogRandom will do randomized testing. The test adds some records and
// checkpoints and do a recovery.
func TestMsgLogRandom(t *testing.T) {

  flag.Set("stderrthreshold", "0")

  rand.Seed(time.Now().Unix())

  msgStore, errFS := NewMsgStoreFS("/tmp/zerostack_test_msglog")
  assert.Nil(t, errFS)
  assert.NotNil(t, msgStore)

  msglog, errMsgLog := NewMsgLog(msgStore)
  assert.Nil(t, errMsgLog)
  assert.NotNil(t, msglog)

  // randomly decide to do delete
  if rand.Intn(2) == 1 {
    glog.Infof("removing existing MsgLog")
    msglog.ClearAll()
  }

  glog.Info("inserting entries into MsgLog")

  doneCh := make(chan *Response)

  loopCnt := rand.Intn(100) + 1
  msnList := make(map[uint64]struct{})
  var firstMSN uint64

  for ii := 0; ii < loopCnt; ii++ {
    // insert random number of MsgLog Data entries
    size := rand.Intn(1000) + 1
    data := make([]byte, size)
    _, errRd := crand.Read(data)
    assert.Nil(t, errRd)

    id, errA := msglog.Publish("testtopic", data, doneCh)
    assert.Nil(t, errA)

    msnList[id] = struct{}{}
    if firstMSN == 0 {
      firstMSN = id
    }
  }

  glog.Infof("added %d data entries", loopCnt)

  var wg sync.WaitGroup
  // Now wait for all responses on the channel.
  for ii := 0; ii < loopCnt; ii++ {
    response := <-doneCh
    assert.Nil(t, response.Result)

    glog.V(2).Infof("received response for msn %d", response.MSN)
    _, found := msnList[response.MSN]
    assert.True(t, found, "received msn not in list : %d", response.MSN)

    // start a goroutine in parallel to read the records
    wg.Add(1)
    go func(msn uint64, wg *sync.WaitGroup) {
      data, rdMSN, errRd := msglog.Get("testtopic", response.MSN)
      assert.Nil(t, errRd)
      assert.NotNil(t, data)
      assert.Equal(t, msn, rdMSN)
      assert.NotEqual(t, len(data), 0)
      wg.Done()
    }(response.MSN, &wg)
  }

  wg.Wait()
  // Fake crash and recovery by just creating another MsgLog object
  msglogStore2, errFS2 := NewMsgStoreFS("/tmp/zerostack_test_msglog")
  assert.Nil(t, errFS2)
  assert.NotNil(t, msglogStore2)

  msglog2, errMsgLog := NewMsgLog(msglogStore2)
  assert.Nil(t, errMsgLog)
  assert.NotNil(t, msglog2)

  // Test that the MSNs are as expected when we do the reads.
  // TODO(kiran): test that the ids are from the previous list.
  for msn := range msnList {
    data, rdMSN, errRd := msglog2.Get("testtopic", msn)
    assert.Nil(t, errRd)
    assert.NotNil(t, data)
    assert.Equal(t, msn, rdMSN)
    assert.NotEqual(t, len(data), 0)
  }

  // Use Reader interface to read the message log.
  rdr, err := NewReader(msglog, "testtopic",
    "/tmp/zerostack_test_msglog_readers", "reader1.snapshot", 10)
  assert.Nil(t, err)
  assert.NotNil(t, rdr)

  err = rdr.Seek(firstMSN)
  assert.Nil(t, err)

  // read some entries more than the snapshotFreq and then try to
  // SaveSnapshot and LoadSnapshot to check snapshotting is working.
  // The checks that are done:
  // We reset the reader after 20 reads with 10 as the freq. The snapshotting
  // should now ensure that we are still reading valid MSN entries and also
  // the next MSN read should be more than the prevMSN which was read before the
  // reset of the rdr object.
  count := 0
  var prevMSN uint64
  for _ = range msnList {
    data, foundMSN, errGet := rdr.GetNext(true)
    assert.Nil(t, errGet)
    assert.NotNil(t, data)
    assert.True(t, foundMSN > prevMSN)
    prevMSN = foundMSN
    count++
    if count == 20 {
      // Reset the reader now using another object and LoadSnapshot.
      glog.Infof("resetting msglog to new reader")
      rdr, err = NewReader(msglog, "testtopic",
        "/tmp/zerostack_test_msglog_readers", "reader1.snapshot", 10)
      assert.Nil(t, err)
      assert.NotNil(t, rdr)
      err = rdr.LoadSnapshot()
      assert.Nil(t, err)
    }
  }

  glog.Infof("committing some messages")

  // range returns in random order so just do some commits.
  count = 0
  for msn := range msnList {
    msglog.Commit("testtopic", msn)
    count++
    if count > 40 {
      break
    }
  }

  // TODO(kiran): add test for checking that appropriate files have been deleted
  // after the Commit.
}

func TestMsgLogTTL(t *testing.T) {

  flag.Set("stderrthreshold", "0")

  rand.Seed(time.Now().Unix())

  msgStore, errFS := NewMsgStoreFS("/tmp/zerostack_test_msglog_ttl")
  assert.Nil(t, errFS)
  assert.NotNil(t, msgStore)

  msglog, errMsgLog := NewMsgLog(msgStore)
  assert.Nil(t, errMsgLog)
  assert.NotNil(t, msglog)

  // randomly decide to do delete
  if rand.Intn(2) == 1 {
    glog.Infof("removing existing MsgLog")
    msglog.ClearAll()
  }

  msglog.SetTTL("ttltopic", 1*time.Second)

  glog.Info("inserting entries into MsgLog")

  doneCh := make(chan *Response)

  loopCnt := rand.Intn(100) + 1
  msnList := make(map[uint64]struct{})
  var firstMSN uint64

  for ii := 0; ii < loopCnt; ii++ {
    // insert random number of MsgLog Data entries
    size := rand.Intn(10) + 1
    data := make([]byte, size)
    _, errRd := crand.Read(data)
    assert.Nil(t, errRd)

    id, errA := msglog.Publish("ttltopic", data, doneCh)
    assert.Nil(t, errA)

    msnList[id] = struct{}{}
    if firstMSN == 0 {
      firstMSN = id
    }
  }

  glog.Infof("added %d data entries", loopCnt)

  // Now wait for all responses on the channel.
  for ii := 0; ii < loopCnt; ii++ {
    response := <-doneCh
    assert.Nil(t, response.Result)

    glog.V(2).Infof("received response for msn %d", response.MSN)
    _, found := msnList[response.MSN]
    assert.True(t, found, "received msn not in list : %d", response.MSN)
  }

  _, rdMSN, errRd := msglog.Get("ttltopic", 0)
  // rdMSN should be valid
  assert.NotEqual(t, rdMSN, 0)

  // sleep longer than TTL
  time.Sleep(2 * time.Second)

  _, rdMSN, errRd = msglog.Get("ttltopic", 0)
  // read should fail and msn returned is 0 (no records)
  assert.NotNil(t, errRd)
  assert.Equal(t, rdMSN, uint64(0))
}

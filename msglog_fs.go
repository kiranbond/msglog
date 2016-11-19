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
// MessageStoreFS implements the streaming message filestore. It is meant for
// storing mostly fifo ordered writes and reads.
//
// The input message has a MSN(message sequence number) and data bytes. The
// data is written to file as records in an async manner and completions are
// sent back to caller in a channel. An index for the records is maintained
// in a MsgStoreIndex(btree). The data is stored in a file in sequential order.
// When the file reaches a maximum size, data is written to a new file. When we
// switch to a new file we also serialize the btree for that file so that it
// can be read when reads are happening to that file.
// The first index (masterIndex) tracks the list of starting MSNs for all the
// files in the message store by topic.
// The second level is an index for each file which tracks the offsets of each
// entry within that file. Note that it is possible at any time to reconstruct
// the index by fully parsing all the directory and all the files so losing
// this index is a performance issue and not a correctness issue.
//
// TODO(kiran): IMPROVEMENTS
// - loadOrCreateFileIndexLocked should be called without Lock so file
//   operations happen out of Lock.
// - improved cleanup which does not try to delete same thing multiple times.
// - Persisting and loading trees
// - As of now reads look for the data in the tree which gets
//   updated after hitting disk. It is possible to violate some guarantees and
//   improve perf by letting reads find writes which are still in the op queue
//   (memory) and hence get some better performance for streaming data.

package msglog

import (
  "bytes"
  "encoding/binary"
  "encoding/json"
  "errors"
  "flag"
  "fmt"
  "io"
  "io/ioutil"
  "os"
  "path"
  "path/filepath"
  "sort"
  "strconv"
  "strings"
  "sync"
  "sync/atomic"
  "time"

  "github.com/golang/glog"
  "github.com/google/btree"

  "zerostack/common/util"
)

const (
  cValidMagic        uint32 = 0xC001C0DE
  cInvalidMagic      uint32 = 0xDEADBEEF
  cStateSnapshotFile string = "msglogstate.snapshot"
)

// ErrEOF is returned when there is not more data to read.
var ErrEOF = errors.New("EOF")

var (
  // Sizes (256 MB max size each)
  // TODO(kiran): evaluate the right size for data file.
  // It is small to let topics be cleaned up faster with TTL.
  fMaxFileSize = flag.Int64("max_msgstore_file_size", int64(64*util.MB),
    "maximum size of a data entries file")
  fBTreeDegree = flag.Int("msgstore_btree_degree", 32,
    "MessageStore B-Tree degree")
)

// msnType is just a proxy for uint64 to implement the Less() needed for BTree.
type msnType uint64

// Less returns the uint64 comparison.
func (m msnType) Less(than btree.Item) bool {
  return uint64(m) < uint64(than.(msnType))
}

// indexNode is the structure that is stored in the BTree. It needs to implement
// the Less method to be stored in the tree.
type indexNode struct {
  msn    uint64
  offset int64
}

// Less implements the function needed for BTree to maintain the tree. The
// nodes are ordered by the MSN.
func (n indexNode) Less(than btree.Item) bool {
  return n.msn < than.(*indexNode).msn
}

// MsgStoreIndex adds the Marshal() and Unmarshal() methods to the btree.BTree
// data structure and facilitates serializing and deserializing tree to/from
// disk.
type MsgStoreIndex struct {
  *btree.BTree
}

// NewMsgStoreIndex creates a new MsgStoreIndex and initializes the BTree.
func NewMsgStoreIndex(degree int) *MsgStoreIndex {
  return &MsgStoreIndex{btree.New(degree)}
}

// Marshal saves the MsgStoreIndex to a buffer to be saved to snapshot.
func (m *MsgStoreIndex) Marshal() ([]byte, error) {
  // TODO(kiran): Implement this.
  return nil, nil
}

// Unmarshal reads the MsgStoreIndex from the loaded snapshot.
func (m *MsgStoreIndex) Unmarshal(data []byte) error {
  // TODO(kiran): Implement this.
  return nil
}

// msnMap is used to collect list of msns to be removed.
type msnMap map[uint64]struct{}

// msnMapAddFunc collects the msns to be removed.
func (t *msnMap) msnMapAddFunc(item btree.Item) bool {
  msn := uint64(item.(msnType))
  (*t)[msn] = struct{}{}
  return true
}

// IMPORTANT: Update this if you update the MsgStoreRecord below!
// This constant reflects the size of all fields other than the Data field in
// MsgStoreRecord.
const cRecordHeaderSize int64 = 16

// MsgStoreRecord is the structure of the record as stored on disk.
// - Magic: 4 bytes of cMsgLogValidMagic stored at the beginning of every
//          record
// - MSN:  message sequence number of the entry in data
// - DataSize: size of the data
// - Data: the MsgStore data sent by caller
type MsgStoreRecord struct {
  Magic    uint32
  MSN      uint64
  DataSize uint32
  Data     []byte
}

// msgStoreOp is the disk operation pending in the queue for the disk
// controller.
type msgStoreOp struct {
  topic string
  msn   uint64
  data  []byte
  done  chan *Response
}

// opQueue is min-heap of msgStoreOp items.
type opQueue []*msgStoreOp

// opQueue implements Sort.Interface methods
func (q opQueue) Len() int           { return len(q) }
func (q opQueue) Less(i, j int) bool { return q[i].msn < q[j].msn }
func (q opQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }

// opQueue Heap.Interface methods. Push/Pop takes pointer receiver because
// they modify slice length

// Push inserts given op in opQueue.
func (q *opQueue) Push(op interface{}) {
  *q = append(*q, op.(*msgStoreOp))
}

// Pop pops op with minimum MSN from the opQueue.
func (q *opQueue) Pop() interface{} {
  old := *q
  n := len(old)
  x := old[n-1]
  *q = old[0 : n-1]
  return x
}

// msgFileInfo holds file handles and other info to track open files.
type msgFileInfo struct {
  name     string   // name of the file
  file     *os.File // file handle
  offset   int64    // offset within the file
  firstMSN uint64   // firstMSN in the file (also in the name)
}

// MsgStoreState is the state of the message store to be snapshotted to disk
// on commit operations.
type MsgStoreState map[string]*TopicState

// MarshalTextString serializes the MsgStoreState to a buffer to be snapshotted.
func (m *MsgStoreState) MarshalTextString() ([]byte, error) {
  return json.Marshal(m)
}

// UnmarshalTextString loads the MsgStoreState from the snapshot buffer.
func (m *MsgStoreState) UnmarshalTextString(data []byte) error {
  return json.Unmarshal(data, m)
}

// MsgStoreFS implements the MsgStore interface using the filesystem. It needs
// as input the directory where it should store the files.
//
// root  - root directory where all MsgLogFS files are stored
// ttl   - map from topic to TTL for topic entries
// msnState - MsgStoreState that holds MSNs to snapshot and retrieve
// stateSnapshot - Snapshot object used for snapshotting msnState
// masterIndex - the btree that stores the firstMSN of each file
// filesIndex  - a map from filename (with firstMSN in the name) to a tree that
//               has offsets of all records in that file by MSN
// writeFiles - open files handles per topic for writing
// readFiles  - open file handles per topic for reading
// opID       - monotonic sequence id for counting disk ops
// opList     - list of disk operations in queue
type MsgStoreFS struct {
  root string
  ttl  map[string]*time.Duration

  // state
  mu            sync.Mutex // mutex for state variables other than opList
  msnState      MsgStoreState
  stateSnapshot *util.Snapshot
  masterIndex   map[string]*MsgStoreIndex
  filesIndex    map[string]*MsgStoreIndex
  writeFiles    map[string]*msgFileInfo
  readFiles     map[string]*msgFileInfo
}

// NewMsgStoreFS creates a new MsgStoreFS object and calls the initRoot() and
// ParseMsgStoreContents methods to do basic checks and initialize msnState from
// the current msnState on disk.
func NewMsgStoreFS(root string) (*MsgStoreFS, error) {
  msnState := make(map[string]*TopicState)
  stateSnapshot, err := util.NewSnapshot(root, cStateSnapshotFile)
  if err != nil || stateSnapshot == nil {
    glog.Errorf("could not initialize snapshot for msnState : %v", err)
    return nil, fmt.Errorf("error initializing snapshot :: %v", err)
  }
  store := &MsgStoreFS{
    root:          root,
    ttl:           make(map[string]*time.Duration),
    msnState:      msnState,
    stateSnapshot: stateSnapshot,
    masterIndex:   make(map[string]*MsgStoreIndex),
    filesIndex:    make(map[string]*MsgStoreIndex),
    writeFiles:    make(map[string]*msgFileInfo),
    readFiles:     make(map[string]*msgFileInfo),
  }
  return store, nil
}

// Init initializes the message store state by parsing the state of all the
// files in the store and returns the maps for first and last MSN by topic in
// the store.
func (m *MsgStoreFS) Init() (map[string]*TopicState, error) {
  err := m.initRoot()
  if err != nil {
    glog.Errorf("error initializing MsgStoreFS root :: %v", err)
    return nil, err
  }
  err = m.ParseMsgStoreContents()
  if err != nil {
    glog.Errorf("error parsing MsgStoreFS :: %v", err)
    return nil, err
  }
  return m.msnState, nil
}

// initRoot checks for the presence of the root directory and creates it if it
// is missing.
func (m *MsgStoreFS) initRoot() error {
  rootInfo, err := os.Stat(m.root)
  if err == nil && rootInfo.IsDir() {
    glog.V(2).Infof("found existing root : %s", m.root)
    return nil
  }
  if err == nil && !rootInfo.IsDir() {
    glog.Errorf("found a MsgStore root which is a file instead of dir")
    return fmt.Errorf("found a MsgStore root which is a file instead of dir")
  }
  glog.Infof("did not find the MsgStore root: %s :: %v", m.root, err)
  if os.IsNotExist(err) {
    errM := os.MkdirAll(m.root, os.FileMode(0777))
    if errM != nil {
      glog.Errorf("cannot create MsgStore root: %s :: %v", m.root, errM)
      return fmt.Errorf("cannot create MsgStore root")
    }
    glog.Infof("created MsgStore root : %s", m.root)
    return nil
  }
  glog.Errorf("cannot find or create MsgStore root: %s :: %v", m.root, err)
  return fmt.Errorf("cannot find or create MsgStore root")
}

// ParseMsgStoreContents initializes the MsgStore using the root directory.
func (m *MsgStoreFS) ParseMsgStoreContents() error {
  // Read and parse the MsgStore directory
  files, err := ioutil.ReadDir(m.root)
  if err != nil {
    glog.Errorf("could not read the MsgStore root : %s :: %v", m.root, err)
    return fmt.Errorf("could not read MsgStore root :: %v", err)
  }
  // Read the snapshot file and load the msgstore msnState
  // TODO(kiran): we can have non-existent snapshot. What checking can we do?
  err = m.stateSnapshot.LoadText(&m.msnState)
  if err != nil {
    glog.Warningf("unable to load state :: %v", err)
  }

  fileMap := make(map[string]map[uint64]string)
  keepMSN := make(map[string]uint64)
  firstMSN := make(map[string]uint64)
  lastMSN := make(map[string]uint64)
  // Parse through all the files and find the msns we should be using.
  for _, file := range files {
    msn, isData, topic, errP := m.parseFilename(file.Name())
    if errP != nil {
      // We also store meta-info files in same dir.
      glog.Infof("found non MsgStore file : %s :: %v", file.Name(), errP)
      continue
    }
    if isData {
      first, last, errC := m.createFileIndex(file.Name())
      if errC != nil {
        // Should we check first == msn?
        if first < firstMSN[topic] {
          firstMSN[topic] = first
        }
        if last > lastMSN[topic] {
          lastMSN[topic] = last
        }
      }
      _, found := fileMap[topic]
      if !found {
        fileMap[topic] = make(map[uint64]string)
      }
      fileMap[topic][msn] = file.Name()
    }
    // Store the highest MSN less than the CommitMSN. All files older than
    // this can be deleted.
    if isData && (msn > keepMSN[topic]) &&
      m.msnState[topic] != nil && (msn < m.msnState[topic].CommitMSN) {

      keepMSN[topic] = msn
    }
  }
  // Delete all stale MsgStore files
  for topic, topicMap := range fileMap {
    for id, name := range topicMap {
      // The file is older than last committed msn or has outlived the TTL
      var ttl time.Duration
      stat, err := os.Stat(m.root + "/" + name)
      if err == nil {
        if m.msnState[topic] != nil {
          ttl = m.msnState[topic].TTL
        } else {
          ttl = cDefaultTTL
        }
      }
      if id < keepMSN[topic] ||
        ((ttl > 0) && time.Now().Sub(stat.ModTime()) > ttl) {

        glog.V(1).Infof("deleting file %s", name)
        errRem := os.Remove(m.root + "/" + name)
        if errRem != nil {
          glog.Errorf("could not remove file %s :: %v", name, errRem)
        }
        // also delete the index file for this if it exists
        indexFile := makeIndexFilename(topic, id)
        errRem = os.Remove(m.root + "/" + indexFile)
        if errRem != nil {
          glog.Errorf("could not remove file %s :: %v", indexFile, errRem)
        }
        delete(topicMap, id)
      }
    }

    // Delete the topic if there are no files. Deleting while iterating is safe
    // in golang.
    // TODO(kiran): can we safely delete the last commit msn here too?
    if len(topicMap) == 0 {
      delete(fileMap, topic)
    } else {
      // add it to the index
      index := NewMsgStoreIndex(*fBTreeDegree)
      for id := range topicMap {
        index.ReplaceOrInsert(msnType(id))
      }
      m.mu.Lock()
      m.masterIndex[topic] = index
      m.mu.Unlock()
    }
  }
  // Now that we have cleaned up the directory, let us check what are the min
  // and max sequence IDs in the directory.
  if len(fileMap) > 0 {
    for topic, topicMap := range fileMap {
      msns := make(util.Uint64Slice, len(topicMap))
      ii := 0
      for key := range topicMap {
        msns[ii] = key
        ii++
      }
      sort.Sort(msns)

      m.mu.Lock()
      if m.msnState[topic] == nil {
        m.msnState[topic] = new(TopicState)
      }
      m.msnState[topic].FirstMSN = firstMSN[topic]
      m.msnState[topic].LastMSN = lastMSN[topic]
      if m.msnState[topic].FirstMSN > 0 &&
        m.msnState[topic].CommitMSN < m.msnState[topic].FirstMSN-1 {

        m.msnState[topic].CommitMSN = m.msnState[topic].FirstMSN - 1
      }
      m.mu.Unlock()
    }
  }
  data, _ := m.msnState.MarshalTextString()
  glog.Infof("finished parsing MsgStore root. msnState=%s", data)
  return nil
}

// SetTTL sets the TTL for a topic.
func (m *MsgStoreFS) SetTTL(topic string, ttl time.Duration) error {
  m.mu.Lock()
  defer m.mu.Unlock()
  // It is possible that a SetTTL is before any writes.
  if m.msnState[topic] == nil {
    m.msnState[topic] = new(TopicState)
  }
  m.msnState[topic].TTL = ttl
  return nil
}

// Write adds a MsgStoreEntry to the opList to be executed.
func (m *MsgStoreFS) Write(op *msgStoreOp) error {
  file, offset, err := m.doWrite(op)
  if err != nil {
    glog.Errorf("error doing write operation :: %v", err)
    return err
  }
  node := &indexNode{msn: op.msn, offset: offset}
  m.mu.Lock()
  if m.filesIndex[file] == nil {
    m.filesIndex[file] = NewMsgStoreIndex(*fBTreeDegree)
  }
  m.filesIndex[file].ReplaceOrInsert(node)
  m.mu.Unlock()
  return nil
}

// Read is is a synchronous call waiting for record with the input MSN to be
// read from MsgStore and returned. If the input MSN does not exist, we will
// return the next valid MSN and an error.
func (m *MsgStoreFS) Read(topic string, msn uint64) ([]byte, uint64, error) {
  rdFile, masterMSN := m.getReadFile(topic, msn)
  if rdFile == nil {
    foundMSN := m.findNextMSN(topic, msn)
    glog.V(1).Infof("no file data to read for topic %s msn %d (next MSN: %d)",
      topic, msn, foundMSN)
    return nil, foundMSN, ErrEOF
  }

  glog.V(1).Infof("reading file %s for msn %d", rdFile.name, msn)

  offset, err := m.getFileOffset(topic, msn, masterMSN)
  if err != nil {
    foundMSN := m.findNextMSN(topic, msn)
    // TODO(kiran): This can be back to Errorf after checking cleaning up
    // of btree on commit
    glog.V(2).Infof("could not find offset in file %s for msn %d (next MSN=%d)",
      rdFile.name, msn, foundMSN)
    return nil, foundMSN, ErrEOF
  }

  data, msn, numBytes, err := m.readOneRecord(rdFile.file, offset)
  if err != nil {
    glog.Errorf("error reading record : %v", err)
    return nil, 0, err
  }
  atomic.AddInt64(&rdFile.offset, int64(numBytes))

  return data, msn, nil
}

// Flush should sync the disk IO buffers in the MsgStore.
func (m *MsgStoreFS) Flush() error {
  return m.flushOpenFiles()
}

// Commit is a synchronous call to record the msnState of the commit for that
// topic. The cleanup of the log till the committed id is done lazily in the
// background.
// TODO(kiran): Implement the lazy or inline cleanup :)
func (m *MsgStoreFS) Commit(topic string, msn uint64) error {
  m.mu.Lock()
  defer m.mu.Unlock()
  // It is possible that a msn commit came faster than the Write.
  if m.msnState[topic] == nil {
    m.msnState[topic] = new(TopicState)
  }
  m.msnState[topic].CommitMSN = msn

  glog.V(3).Infof("committed for topic: %s, msn: %v", topic, msn)

  m.stateSnapshot.SaveText(&m.msnState)
  return nil
}

// DeletePartial marks some messages within the provided MSN range as stale
// and deletes files which corresponding to deleted entries. We only delete
// full files rather than deleting entries in files so we look for files which
// have all MSNs less than the lastMSN.
func (m *MsgStoreFS) DeletePartial(topic string, firstMSN uint64,
  lastMSN uint64) error {

  glog.V(2).Infof("removing messages for topic: %s from %d to %d", topic,
    firstMSN, lastMSN)

  var remList msnMap
  remList = make(map[uint64]struct{})

  m.mu.Lock()
  if m.masterIndex[topic] == nil {
    m.mu.Unlock()
    return nil
  }
  m.masterIndex[topic].AscendLessThan(msnType(lastMSN), remList.msnMapAddFunc)
  m.mu.Unlock()

  if len(remList) == 0 {
    glog.V(3).Infof("no files found to delete")
    return nil
  }

  // Get all MSNs other than the highest one that is less than lastMSN.
  // The highest one might be the open one so we should not delete it.
  // TODO(kiran): figure out a way to check for highest closed one?
  remSorted := make(util.Uint64Slice, len(remList))
  ii := 0
  for key := range remList {
    remSorted[ii] = key
    ii++
  }
  sort.Sort(remSorted)
  // drop the highest
  remSorted = remSorted[:len(remSorted)-1]

  if len(remSorted) == 0 {
    glog.V(3).Infof("no files to delete")
    return nil
  }

  for key := range remSorted {
    m.masterIndex[topic].Delete(msnType(key))
  }

  return m.removeFiles(topic, remSorted)
  // TODO(kiran): delete index files
}

// Delete deletes all data related to a topic older than duration. If duration
// is zero then all files of that topic are deleted.
// Note that disk operations are paused while deleting but the queue is not
// emptied so it is the users responsibility to do Delete when no ops have been
// sent to the store.
func (m *MsgStoreFS) Delete(topic string, dur time.Duration) error {

  glog.V(2).Infof("deleting files for topic %s with ttl %v", topic, dur)

  // TODO(kiran): can we make this one regexp?
  files, errDir := filepath.Glob(m.root + fmt.Sprintf("/*_%s", topic))
  if errDir != nil {
    glog.Errorf("could not read the MsgStore root : %s :: %v", m.root, errDir)
    return fmt.Errorf("could not read MsgStore root :: %v", errDir)
  }
  for _, file := range files {
    glog.V(2).Infof("checking file %v", file)
    stat, err := os.Stat(file)
    // Even if stat has an error and we cannot figure out time then lets delete
    // because this is some stray file.
    if dur == 0 || err != nil || time.Now().Sub(stat.ModTime()) > dur {
      glog.V(2).Infof("removing file %s", file)
      m.cleanFileRef(topic, path.Base(file))
      errR := os.Remove(file)
      if errR != nil {
        glog.Errorf("could not remove file : %s :: %v", file, errR)
        return errR
      }
    }
  }
  return nil
}

// DeleteAll deletes the root directory and recreates it effectively wiping out
// all MsgStore entries.
func (m *MsgStoreFS) DeleteAll() error {
  errR := os.RemoveAll(m.root)
  if errR != nil {
    glog.Errorf("could not remove MsgStore root :: %v", errR)
    return errR
  }
  return m.initRoot()
}

////////////////////////////////////////////////////////////////////////////////
// Internal functions

// Filename utilities

// makeDataFilename returns a data file name with the given msn.
func makeDataFilename(topic string, msn uint64) string {
  return fmt.Sprintf("%d_data_%s", msn, topic)
}

// makeIndexFilename returns an index filename with the msn number.
func makeIndexFilename(topic string, msn uint64) string {
  return fmt.Sprintf("%d_index_%s", msn, topic)
}

// parseFilename breaks the given filename into parts and checks if it follows
// one of the MsgStore naming schemes and is a valid MsgStore name.
func (m *MsgStoreFS) parseFilename(filename string) (
  uint64, bool, string, error) {

  var msn uint64
  var isData bool

  parts := strings.Split(filename, "_")

  if len(parts) != 3 {
    glog.Infof("found non MsgStore file %s", filename)
    return 0, false, "", fmt.Errorf("invalid MsgStore file %s", filename)
  }

  msn, err := strconv.ParseUint(parts[0], 10, 64)
  if err != nil {
    glog.Errorf("found invalid msn id in MsgStore file %s", filename)
    return 0, false, "", fmt.Errorf("invalid MsgStore file %s", filename)
  }

  if parts[1] == "data" {
    isData = true
  } else {
    glog.Errorf("found invalid type in MsgStore file %s", filename)
    return 0, false, "", fmt.Errorf("invalid MsgStore file %s", filename)
  }

  return msn, isData, parts[2], nil
}

// getReadFile returns a msgFileInfo and the masterMSN based on the MSN.
func (m *MsgStoreFS) getReadFile(topic string, msn uint64) (
  *msgFileInfo, uint64) {

  m.mu.Lock()
  defer m.mu.Unlock()

  if m.masterIndex[topic] == nil {
    return nil, 0
  }

  masterMSNInf := m.masterIndex[topic].GetPrevOrEqual(msnType(msn))
  if masterMSNInf == nil {
    glog.V(1).Infof("could not find file MSN for topic %s msn %d", topic, msn)
    return nil, 0
  }

  masterMSN := uint64(masterMSNInf.(msnType))
  dfile := makeDataFilename(topic, masterMSN)

  if m.readFiles[topic] != nil && m.readFiles[topic].file != nil {
    if m.readFiles[topic].name == dfile {
      return m.readFiles[topic], masterMSN
    }
    m.readFiles[topic].file.Close()
    m.readFiles[topic] = nil
  }

  filename := m.root + "/" + dfile
  file, err := os.OpenFile(filename, os.O_RDONLY, os.FileMode(0777))
  if err != nil {
    glog.Errorf("could not open file : %s :: %v", filename, err)
    return nil, 0
  }
  m.readFiles[topic] = &msgFileInfo{name: dfile, file: file, offset: 0}
  return m.readFiles[topic], masterMSN
}

// getFileOffset returns the offset for the msn in the given file. When the
// index is actually persisted on disk it will read the index into memory
// with the anticipation that more records from this will be read soon.
// If the msn does not exist in the index then it returns 0 and an error.
func (m *MsgStoreFS) getFileOffset(topic string, msn uint64, masterMSN uint64) (
  int64, error) {

  m.mu.Lock()
  defer m.mu.Unlock()

  filename := makeDataFilename(topic, masterMSN)
  // If the index does not exist then load or create the index.
  if m.filesIndex[filename] == nil {
    err := m.loadOrCreateFileIndexLocked(topic, masterMSN)
    if err != nil || m.filesIndex[filename] == nil {
      glog.Errorf("could not load index for : %s :: %v", filename, err)
      return 0, fmt.Errorf("not implemented")
    }
  }

  searchNode := &indexNode{msn: msn, offset: 0}
  node := m.filesIndex[filename].Get(searchNode)
  if node == nil {
    return 0, fmt.Errorf("not found")
  }

  return node.(*indexNode).offset, nil
}

// findNextMSN finds the msn in that topic which is greater than or equal to
// the supplied msn. We first look in the file with masterMSN less than this MSN
// and then in the next masterMSN file.
func (m *MsgStoreFS) findNextMSN(topic string, msn uint64) uint64 {
  m.mu.Lock()
  defer m.mu.Unlock()

  if m.masterIndex[topic] == nil {
    glog.V(1).Infof("empty index for topic : %s", topic)
    return 0
  }

  // First find a file which might contain this MSN. Note that when we get
  // called we already might have looked in this but lets do anyway.
  foundInf := m.masterIndex[topic].GetPrevOrEqual(msnType(msn))
  if foundInf != nil {
    masterMSN := uint64(foundInf.(msnType))
    filename := makeDataFilename(topic, masterMSN)

    if m.filesIndex[filename] == nil {
      err := m.loadOrCreateFileIndexLocked(topic, masterMSN)
      if err != nil || m.filesIndex[filename] == nil {
        glog.Errorf("could not load the file for masterMSN : %d :: %v",
          masterMSN, err)
        return 0
      }
    }

    searchNode := &indexNode{msn: msn, offset: 0}
    node := m.filesIndex[filename].GetNextOrEqual(searchNode)
    if node != nil {
      return node.(*indexNode).msn
    }
  }

  // If we did not find any firstMSN which is less or all MSNs in that file
  // are less than the input msn then lets look for the next file.
  foundInf = m.masterIndex[topic].GetNextOrEqual(msnType(msn))
  if foundInf == nil {
    // We did not find any msn less or greater than this msn!
    return 0
  }

  masterMSN := uint64(foundInf.(msnType))

  filename := makeDataFilename(topic, masterMSN)
  if m.filesIndex[filename] == nil {
    err := m.loadOrCreateFileIndexLocked(topic, masterMSN)
    if err != nil || m.filesIndex[filename] == nil {
      glog.Errorf("could not load the file for masterMSN : %d :: %v",
        masterMSN, err)
      return 0
    }
  }

  searchNode := &indexNode{msn: msn, offset: 0}
  node := m.filesIndex[filename].GetNextOrEqual(searchNode)
  if node != nil {
    return node.(*indexNode).msn
  }

  return 0
}

// getNextWriteFile returns an msgFileInfo based on the topic.
func (m *MsgStoreFS) getNextWriteFile(topic string, msn uint64) *msgFileInfo {

  m.mu.Lock()
  defer m.mu.Unlock()

  if m.writeFiles[topic] != nil {
    if m.writeFiles[topic].offset < *fMaxFileSize {
      return m.writeFiles[topic]
    }
    m.writeFiles[topic].file.Sync()
    m.writeFiles[topic].file.Close()
    m.writeFiles[topic] = nil
  }
  dfile := makeDataFilename(topic, msn)
  filename := m.root + "/" + dfile

  file, err := os.OpenFile(filename,
    os.O_RDWR|os.O_TRUNC|os.O_CREATE, os.FileMode(0777))

  if err != nil {
    glog.Errorf("could not open MsgStore file: %s :: %v", filename, err)
    return nil
  }

  glog.V(1).Infof("opened a new MsgStore file %s", dfile)

  m.writeFiles[topic] = &msgFileInfo{name: dfile, firstMSN: msn, file: file,
    offset: 0}
  if m.masterIndex[topic] == nil {
    m.masterIndex[topic] = NewMsgStoreIndex(*fBTreeDegree)
  }
  m.masterIndex[topic].ReplaceOrInsert(msnType(msn))
  return m.writeFiles[topic]
}

// parseHeader reads individual fields out of the header bytes read from the
// file.
func (m *MsgStoreFS) parseHeader(hdr []byte) (*MsgStoreRecord, error) {
  record := &MsgStoreRecord{}
  buf := bytes.NewBuffer(hdr)
  err := binary.Read(buf, binary.LittleEndian, &record.Magic)
  if err != nil {
    glog.Errorf("could not parse MsgStore data for Magic :: %v", err)
    return nil, fmt.Errorf("error parsing MsgStore data Magic :: %v", err)
  }
  if record.Magic != cValidMagic {
    glog.Errorf("invalid Magic %X", record.Magic)
    return nil, fmt.Errorf("invalid Magic :: %v", err)
  }
  err = binary.Read(buf, binary.LittleEndian, &record.MSN)
  if err != nil {
    glog.Errorf("could not parse MsgStore data for MSN :: %v", err)
    return nil, fmt.Errorf("error parsing MsgStore data MSN :: %v", err)
  }
  err = binary.Read(buf, binary.LittleEndian, &record.DataSize)
  if err != nil {
    glog.Errorf("could not parse MsgStore data for DataSize :: %v", err)
    return nil, fmt.Errorf("error parsing MsgStore data DataSize :: %v", err)
  }
  return record, nil
}

// writeRecord writes the MsgStoreRecord to a buffer and returns the []byte
// that can be written to file.
// TODO(kiran): We could do byte copy with endianness to make it faster?
func (m *MsgStoreFS) writeRecord(record *MsgStoreRecord) ([]byte, error) {
  buf := new(bytes.Buffer)
  err := binary.Write(buf, binary.LittleEndian, record.Magic)
  if err != nil {
    glog.Errorf("error encoding Magic :: %v", err)
    return nil, fmt.Errorf("error encoding Magic :: %v", err)
  }
  err = binary.Write(buf, binary.LittleEndian, record.MSN)
  if err != nil {
    glog.Errorf("error encoding MSN :: %v", err)
    return nil, fmt.Errorf("error encoding MSN :: %v", err)
  }
  err = binary.Write(buf, binary.LittleEndian, record.DataSize)
  if err != nil {
    glog.Errorf("error encoding DataSize :: %v", err)
    return nil, fmt.Errorf("error encoding DataSize :: %v", err)
  }
  err = binary.Write(buf, binary.LittleEndian, record.Data)
  if err != nil {
    glog.Errorf("error encoding Data :: %v", err)
    return nil, fmt.Errorf("error encoding Data :: %v", err)
  }
  invalidMagic := uint32(cInvalidMagic)
  err = binary.Write(buf, binary.LittleEndian, invalidMagic)
  if err != nil {
    glog.Errorf("error encoding invalidMagic :: %v", err)
    return nil, fmt.Errorf("error encoding invalidMagic :: %v", err)
  }

  return buf.Bytes(), nil
}

// doWrite does the actual disk IO for an op. It serializes the MsgStoreRecord
// into a buffer for writing into the file. The function returns the name of
// the file, the offset and an error status.
func (m *MsgStoreFS) doWrite(op *msgStoreOp) (string, int64, error) {
  record := MsgStoreRecord{Magic: cValidMagic, MSN: op.msn}
  record.DataSize = uint32(len(op.data))
  record.Data = op.data

  // TODO(kiran): Should we just create another []byte and copy the fields into
  // it rather than using encoding/binary?
  buf, errRW := m.writeRecord(&record)
  if errRW != nil || buf == nil {
    glog.Errorf("could not write record to buffer :: %v", errRW)
    return "", 0, fmt.Errorf("could not write record to buffer :: %v", errRW)
  }
  // The bytes written = header + record.Data + cInvalidMagic
  totalSize := cRecordHeaderSize + int64(record.DataSize) + 4

  wrFileInfo := m.getNextWriteFile(op.topic, op.msn)
  if wrFileInfo == nil {
    glog.Error("could not get file to write")
    return "", 0, fmt.Errorf("could not get file to write")
  }
  offset := wrFileInfo.offset
  count, errWr := wrFileInfo.file.WriteAt(buf, offset)
  if errWr != nil || int64(count) != totalSize {
    glog.Errorf("could not write to file : count %d :: %v", count, errWr)
    return "", 0, fmt.Errorf("could not write to file :: %v", errWr)
  }
  glog.V(2).Infof("wrote %d bytes to %s file for msn %d", totalSize,
    wrFileInfo.name, op.msn)

  // we will increment offset without the deadbeef part
  atomic.AddInt64(&wrFileInfo.offset, cRecordHeaderSize+int64(record.DataSize))
  return wrFileInfo.name, offset, nil
}

// readOneRecord reads one record from the supplied file handle at the offset.
func (m *MsgStoreFS) readOneRecord(file *os.File, offset int64) (
  []byte, uint64, int, error) {

  hdr := make([]byte, cRecordHeaderSize)

  // read header first
  count1, err := file.ReadAt(hdr, offset)
  numBytes := count1
  if err != nil || int64(count1) < cRecordHeaderSize {
    if err == io.EOF {
      glog.V(3).Infof("reached end of file ")
    } else {
      glog.Errorf("error reading MsgStore hdr :: %v", err)
    }
    return nil, 0, numBytes, err
  }
  // parse the header into the struct
  record, err := m.parseHeader(hdr)
  if err != nil {
    glog.Errorf("error parsing file data for MsgStoreRecord :: %v", err)
    return nil, 0, numBytes, fmt.Errorf("error parsing data :: %v", err)
  }
  // now read the data
  data := make([]byte, record.DataSize)
  count2, err := file.ReadAt(data, offset+int64(numBytes))
  numBytes += count2
  if err != nil || count2 < int(record.DataSize) ||
    len(data) < int(record.DataSize) {

    glog.Errorf("error in reading MsgStore data :: %v", err)
    return nil, 0, numBytes, fmt.Errorf("error parsing record :: %v", err)
  }

  glog.V(2).Infof("read record of size %d with MSN %d", record.DataSize,
    record.MSN)

  return data, record.MSN, numBytes, nil
}

// loadFileIndex loads a MsgStoreIndex into the fileIndex if the snapshot of
// the index exists on disk.
func (m *MsgStoreFS) loadFileIndex(topic string, masterMSN uint64) error {
  filename := makeIndexFilename(topic, masterMSN)
  stat, err := os.Stat(filename)
  if err != nil || stat.IsDir() {
    glog.V(2).Infof("did not find index file: %s :: %v", filename, err)
    return err
  }

  index := &MsgStoreIndex{}
  sn, err := util.NewSnapshot(m.root, filename)
  if err != nil {
    glog.V(1).Infof("could not initialize snapshot: %s :: %v", filename, err)
    return err
  }
  err = sn.Load(index)
  if err != nil {
    glog.V(1).Infof("could not load the index file: %s :: %v", filename, err)
    return err
  }

  dataFilename := makeDataFilename(topic, masterMSN)

  m.filesIndex[dataFilename] = index

  return nil
}

// createFileIndex is an expensive operation which will parse the full data
// file and create the index.
func (m *MsgStoreFS) createFileIndex(filename string) (uint64, uint64, error) {

  file, err := os.OpenFile(m.root+"/"+filename, os.O_RDONLY, os.FileMode(0777))
  if err != nil {
    glog.Errorf("could not open file : %s :: %v", filename, err)
    return 0, 0, fmt.Errorf("not found")
  }
  index := NewMsgStoreIndex(*fBTreeDegree)

  var firstMSN, lastMSN uint64
  offset := int64(0)
  for {
    _, msn, numBytes, err := m.readOneRecord(file, offset)
    if err != nil {
      if err == io.EOF {
        glog.V(3).Infof("reached end of file %s", filename)
      } else {
        glog.Errorf("error reading record :: %v", err)
      }
      break
    }
    if firstMSN == 0 {
      firstMSN = msn
    }
    if lastMSN < msn {
      lastMSN = msn
    }
    node := &indexNode{msn: msn, offset: offset}
    index.ReplaceOrInsert(node)
    offset += int64(numBytes)
  }

  glog.V(1).Infof("created index for file %s", filename)

  m.filesIndex[filename] = index
  return firstMSN, lastMSN, nil
}

// loadOrCreateFileIndexLocked calls loadFileIndex and then createFileIndex if
// needed.
func (m *MsgStoreFS) loadOrCreateFileIndexLocked(topic string,
  masterMSN uint64) error {

  err := m.loadFileIndex(topic, masterMSN)
  if err == nil {
    return nil
  }

  filename := makeDataFilename(topic, masterMSN)

  _, _, err = m.createFileIndex(filename)
  if err == nil {
    // TODO(kiran): snapshot this index to disk
  }
  return err
}

// removeFiles removes the list of files specified by the msnList slice.
func (m *MsgStoreFS) removeFiles(topic string, msnList []uint64) error {

  for _, msn := range msnList {
    filename := makeDataFilename(topic, msn)
    glog.V(2).Infof("removing file %s", filename)
    m.cleanFileRef(topic, path.Base(filename))
    err := os.Remove(m.root + "/" + filename)
    // TODO(kiran): avoid the ErrNotExist condition by not trying to delete an
    // already deleted file.
    if err != nil {
      glog.Errorf("could not remove file :: %v", err)
    } else {
      glog.V(2).Infof("removed file %s", filename)
    }
  }
  return nil
}

// flushOpenFiles flushes all open data and checkpoint files.
// TODO(kiran): Any os calls to flush os buffers?
func (m *MsgStoreFS) flushOpenFiles() error {
  var lastError error
  // TODO(kiran): locking for m.writeFiles without holding locks during Sync!!
  for _, writeFile := range m.writeFiles {
    if writeFile != nil {
      err := writeFile.file.Sync()
      if err != nil {
        lastError = err
        glog.Errorf("error flushing file %s :: %v", writeFile.name, err)
      }
    } else {
      glog.Warningf("unexpected nil in writeFiles")
    }
  }
  return lastError
}

// cleanFileRef removes the file from the different indexes and caches.
func (m *MsgStoreFS) cleanFileRef(topic, name string) error {
  m.mu.Lock()
  defer m.mu.Unlock()

  // delete from master Index
  if m.masterIndex[topic] != nil {
    msn, _, _, _ := m.parseFilename(name)
    m.masterIndex[topic].Delete(msnType(msn))
  }
  // dereference the file index since file is being deleted
  m.filesIndex[name] = nil
  // empty out any file handle caches. No need to close since we are deleting
  // this file anyway.
  if m.readFiles[topic] != nil && m.readFiles[topic].name == name {
    m.readFiles[topic] = nil
  }
  if m.writeFiles[topic] != nil && m.writeFiles[topic].name == name {
    m.writeFiles[topic] = nil
  }
  return nil
}

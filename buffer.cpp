/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
  : numBufs(bufs) {
  bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
  for (std::uint32_t i = 0; i < numBufs; i++){
    // flush dirty pages into files
    if (bufDescTable[i].dirty == true){
      if (File::isOpen(bufDescTable[i].file->filename())){
        (*(bufDescTable[i].file)).writePage(bufPool[i]);
        bufDescTable[i].dirty = false;
      } 
    }
  }
  // deallocate buf poll, buf desctable and hash table
  bufPool = NULL;
  bufDescTable = NULL;
  hashTable->~BufHashTbl();
}

void BufMgr::advanceClock()
{
  // advance the clock to next frame
  clockHand = (clockHand + 1) % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
    // go through all the frames to check
    // whether there is a bufferexceed exception
    bool pinned = true;
    for (std::uint32_t i = 0; i < numBufs; i++)
      if (bufDescTable[i].pinCnt == 0)
        pinned = false;
    if (pinned) throw BufferExceededException();
     // std::cout<<"pass pinned \n";
    // if there is no exception
    while (true) {
      advanceClock();
      // if there is no valid page in frame
      if (!bufDescTable[frame].valid) {
        break;
      } else {
        // if there is a valid page in frame
        if (bufDescTable[frame].refbit){
          // if frame is used recently,turn it to false
          // and advanse
          bufDescTable[frame].refbit = false;
        } else {
          // if pincount is zero, the frame can be set
          // else it should advance
          if (bufDescTable[frame].pinCnt == 0){
            // check ditry, if dirty, flush
            if (bufDescTable[frame].dirty){
              (*(bufDescTable[frame].file)).writePage(bufPool[frame]);
            }
              // remove the relation in the hash table and clear the frame
              hashTable->remove(bufDescTable[frame].file, 
                bufDescTable[frame].pageNo);
              bufDescTable[frame].Clear();
              break;
          }
        }
      } 
    }
}

  
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
    FrameId temp = 0;
    try {
        // look up file and pageid in the hashtable
        hashTable->lookup(file, pageNo, temp);
        // if it is in the buffer pool
        bufDescTable[temp].refbit = true;
        bufDescTable[temp].pinCnt = bufDescTable[temp].pinCnt + 1;
        page = &(bufPool[temp]);
    } catch(HashNotFoundException& e1) {
        // if it is not in the buffer pool
        allocBuf(clockHand);
        // read page from file and insert it into buffer pool
        bufPool[clockHand] = file->readPage(pageNo);
        // insert it into hashtable
        hashTable->insert(file, pageNo, clockHand);
        // invoke set()
        bufDescTable[clockHand].Set(file, pageNo);
        page = &(bufPool[clockHand]);
    }
}

void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
    FrameId temp;
    // check whether this page is in the hashtable
    try {
        hashTable->lookup(file, pageNo, temp);
    } catch(HashNotFoundException e1) {
        // if this page is not in the hash table
        return;
    }
    // if this page is in the hash table
    if (bufDescTable[temp].pinCnt == 0){
        // if the page is not pinned, throw page not pinned exception
        throw PageNotPinnedException((*file).filename(), pageNo, temp);
    }else {
        // if this page's pin is bigger than zero
        bufDescTable[temp].pinCnt = bufDescTable[temp].pinCnt - 1;
        if (dirty == true)
            bufDescTable[temp].dirty = true;
    }
}

void BufMgr::flushFile(const File* file) 
{
    // go through the frame array
    for (std::uint32_t i = 0; i < numBufs; i++) {
        BufDesc temp = bufDescTable[i];
        // check whether this frame's page belong to the given file
        if ((*(temp.file)).filename().compare((*file).filename()) == 0) {
            if (temp.pinCnt > 0) {
            // if the the page is pinned, throw page pinned exception
                throw PagePinnedException((*file).filename(), temp.pageNo, 
                  temp.frameNo);
            }
            if (temp.valid == false) {
            // if the frame is not valid, throw badbuffer exception
                throw BadBufferException(temp.frameNo, temp.dirty, 
                  temp.valid, temp.refbit);
            }
            if (temp.dirty == true) {
                // flush the page into the disk if it is dirty
                // and set the frame to not clean
                (*(temp.file)).writePage(bufPool[i]);
                temp.dirty = false;
            }
            // remove page from the hashtable
            hashTable->remove(file, temp.pageNo);
            // clear the bufdesc
            temp.Clear();
        }
    }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  // allocate a new page in the file
  badgerdb::Page new_page = file->allocatePage();
  // return the new page's page number
  pageNo = new_page.page_number();
  // obtain next frame
  allocBuf(clockHand);
  // update the new page into the frame
  bufPool[clockHand] = new_page;
  // add the relation to hash table
  hashTable->insert(file, pageNo, clockHand);
  // allocate the page to the frame
  bufDescTable[clockHand].Set(file, pageNo);
  // return the pointer to the allocated page
  page = &(bufPool[clockHand]);
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frameNo;
    // lookup the frame where the page is in
    hashTable->lookup(file, PageNo, frameNo);
    // if the page is pinned, throw exception
    if (bufDescTable[frameNo].pinCnt > 0)
      throw PagePinnedException(file->filename(), PageNo, frameNo);
    // remove the relation in the hash table
    hashTable->remove(file, PageNo);
    // clear the frame
    bufDescTable[frameNo].Clear();
    // delete the page from file
    (*file).deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
  int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
  {
    tmpbuf = &(bufDescTable[i]);
    std::cout << "FrameNo:" << i << " ";
    tmpbuf->Print();

    if (tmpbuf->valid == true)
      validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}

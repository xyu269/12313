/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
 * Group 5
 * Team members:
 * 1. Yingdong Chen  --9078286649
 * 2. Xinhui YU  --9075879172       
 * 3. Shiyi He  --9075856451
 *
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

/**
   * Destructor of BufMgr class
   */
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

/**
   * Advance clock to next frame in the buffer pool
   */
void BufMgr::advanceClock()
{
  // advance the clock to next frame
  clockHand = (clockHand + 1) % numBufs;
}

/**
   * Allocate a free frame.  
   *
   * @param frame     Frame reference, frame ID of allocated frame returned via this variable
   * @throws BufferExceededException If no such buffer is found which can be allocated
   */
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
//      advanceClock(); 
    }
}

/**
   * Reads the given page from the file into a frame and returns the pointer to page.
   * If the requested page is already present in the buffer pool pointer to that frame is returned
   * otherwise a new frame is allocated from the buffer pool for reading the page.
   *
   * @param file    File object
   * @param PageNo  Page number in the file to be read
   * @param page    Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
   */
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

/**
   * Unpin a page from memory since it is no longer required for it to remain in memory.
   *
   * @param file    File object
   * @param PageNo  Page number
   * @param dirty   True if the page to be unpinned needs to be marked dirty  
   * @throws  PageNotPinnedException If the page is not already pinned
   */
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

/**
   * Writes out all dirty pages of the file to disk.
   * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
   * Otherwise Error returned.
   *
   * @param file    File object
   * @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
   * @throws BadBufferException If any frame allocated to the file is found to be invalid
   */
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
            bufDescTable[i].Clear();
        }
    }
}

/**
   * Allocates a new, empty page in the file and returns the Page object.
   * The newly allocated page is also assigned a frame in the buffer pool.
   *
   * @param file    File object
   * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
   * @param page    Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
   */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  // allocate a new page in the file
  badgerdb::Page new_page = file->allocatePage();
  // return the new page's page number
  pageNo = new_page.page_number();
  //obtain next frame
  allocBuf(clockHand);
  // update the new page into the frame
  bufPool[clockHand] = new_page;
  // add the relation to hash table
  std::cout<<"Here \n";
  hashTable->insert(file, pageNo, clockHand);
  // allocate the page to the frame
  bufDescTable[clockHand].Set(file, pageNo);
  //return the pointer to the allocated page
  page = &(bufPool[clockHand]); 
}

/**
   * Delete page from file and also from buffer pool if present.
   * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
   *
   * @param file    File object
   * @param PageNo  Page number
   */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    try {
      FrameId frameNo;
      // lookup the frame where the page is in
      hashTable->lookup(file, PageNo, frameNo);
      //if the page is pinned, throw exception
      if (bufDescTable[frameNo].pinCnt > 0)
         throw PagePinnedException(file->filename(), PageNo, frameNo);
      // remove the relation in the hash table
      hashTable->remove(file, PageNo);
      // clear the frame
      bufDescTable[frameNo].Clear();
    } catch(HashNotFoundException e1){
    }
    // delete the page from file
    (*file).deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
  FrameId id;
  int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
  {
    tmpbuf = &(bufDescTable[i]);
    std::cout << "FrameNo:" << i << " ";
    tmpbuf->Print();

    if (tmpbuf->valid == true)
      validFrames++;
    // look up everything in the hashtable
    if(tmpbuf->file)
        hashTable->lookup(tmpbuf->file, tmpbuf->pageNo, id);
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
